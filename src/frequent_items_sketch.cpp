#include "datasketches_extension.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/list_vector.hpp"
#include "duckdb/common/vector/string_vector.hpp"
#include "duckdb/common/vector/struct_vector.hpp"
#include "duckdb/common/vector/unified_vector_format.hpp"
#include "duckdb/common/types/cast_helpers.hpp" // duckdb_fmt

#include <DataSketches/frequent_items_sketch.hpp>
#include <string>
#include <type_traits>

namespace duckdb
{

    // Runnable description examples over inline data: items are the strings '0' to '9', weight is the row number.
    static constexpr const char *DS_FREQ_ITEMS_EXAMPLE_ROWS = " FROM (SELECT (i % 10)::VARCHAR AS item, i AS weight FROM range(1000) t(i)) t";

    static const DSExampleSources &DSFreqItemsExamples()
    {
        static const DSExampleSources examples("datasketch_frequent_items(10, (i % 10)::VARCHAR)");
        return examples;
    }

    namespace
    {
        using FrequentItemsSketch = datasketches::frequent_items_sketch<std::string>;

        const vector<string> DS_FREQ_ITEMS_CATEGORIES = {"sketch", "frequent_items"};

        // Calls fun(T(), logical_type) for every item type the sketch accepts. BLOB is not one of them: a BLOB
        // argument is a serialized sketch to merge.
        template <class FUNC>
        void DSFreqItemsForEachItemType(FUNC &&fun)
        {
            fun(int8_t(), LogicalType::TINYINT);
            fun(int16_t(), LogicalType::SMALLINT);
            fun(int32_t(), LogicalType::INTEGER);
            fun(int64_t(), LogicalType::BIGINT);
            fun(uint8_t(), LogicalType::UTINYINT);
            fun(uint16_t(), LogicalType::USMALLINT);
            fun(uint32_t(), LogicalType::UINTEGER);
            fun(uint64_t(), LogicalType::UBIGINT);
            fun(float(), LogicalType::FLOAT);
            fun(double(), LogicalType::DOUBLE);
            fun(string_t(), LogicalType::VARCHAR);
        }

        // Converts an item to the string key stored in the sketch. Every item type is stored as a string so one
        // sketch type serves all inputs; the conversion must therefore be injective, or distinct items would be
        // counted together. Integers use their exact decimal form. FLOAT and DOUBLE are formatted the way DuckDB's
        // VARCHAR cast formats them (duckdb_fmt "{}"): the shortest string that round-trips to the same value
        // (e.g. 0.1, 1e-07), so items look exactly like CAST(x AS VARCHAR) and distinct values get distinct strings.
        template <typename T>
        std::string DSFreqItemsToString(const T &input)
        {
            if constexpr (std::is_same_v<T, string_t>)
            {
                return input.GetString();
            }
            else if constexpr (std::is_same_v<T, float> || std::is_same_v<T, double>)
            {
                return duckdb_fmt::format("{}", input);
            }
            else if constexpr (std::is_same_v<T, int8_t> || std::is_same_v<T, uint8_t>)
            {
                // Widen so std::to_string does not see a character type.
                return std::to_string(static_cast<int>(input));
            }
            else
            {
                return std::to_string(input);
            }
        }

        template <typename T>
        uint64_t DSFreqItemsToWeight(const T &weight)
        {
            if constexpr (std::is_signed_v<T>)
            {
                if (weight < 0)
                {
                    throw InvalidInputException("Frequent Items weight must be non-negative");
                }
            }
            return static_cast<uint64_t>(weight);
        }

        FrequentItemsSketch DSFreqItemsDeserialize(const string_t &blob)
        {
            return DSDeserialize<FrequentItemsSketch>(blob, "Frequent Items");
        }

        string_t DSFreqItemsSerialize(Vector &result, const FrequentItemsSketch &sketch)
        {
            auto bytes = sketch.serialize();
            return StringVector::AddStringOrBlob(result, reinterpret_cast<const char *>(bytes.data()), bytes.size());
        }

        datasketches::frequent_items_error_type DSFreqItemsErrorType(const string &name)
        {
            auto upper = StringUtil::Upper(name);
            if (upper == "NO_FALSE_POSITIVES")
            {
                return datasketches::NO_FALSE_POSITIVES;
            }
            if (upper == "NO_FALSE_NEGATIVES")
            {
                return datasketches::NO_FALSE_NEGATIVES;
            }
            throw InvalidInputException("datasketch_frequent_items_get_frequent: error_type must be 'NO_FALSE_POSITIVES' or "
                                        "'NO_FALSE_NEGATIVES', got '%s'",
                                        name);
        }

        // ============================================================
        // 1. Bind Data
        // ============================================================
        struct DSFreqItemsBindData : public FunctionData
        {
            DSFreqItemsBindData() : lg_max_k(10) {}
            explicit DSFreqItemsBindData(uint8_t lg_max_k) : lg_max_k(lg_max_k) {}
            unique_ptr<FunctionData> Copy() const override { return make_uniq<DSFreqItemsBindData>(lg_max_k); }
            bool Equals(const FunctionData &other_p) const override
            {
                return lg_max_k == other_p.Cast<DSFreqItemsBindData>().lg_max_k;
            }
            uint8_t lg_max_k;
        };

        // lg_max_k, when given, is the first of `full_arity` arguments. It is the log2 of the sketch's maximum hash map
        // size: the library computes 1 << lg_max_k and treats values below 3 as an error, so reject out-of-range values
        // instead of silently truncating them to uint8_t.
        template <idx_t FULL_ARITY>
        unique_ptr<FunctionData> DSFreqItemsBind(BindAggregateFunctionInput &input)
        {
            auto &arguments = input.GetArguments();
            if (arguments.size() < FULL_ARITY)
            {
                return make_uniq<DSFreqItemsBindData>();
            }
            auto lg_max_k = DSBindConstant(input.GetClientContext(), *arguments[0], "Frequent Items lg_max_k must be constant",
                                           "Frequent Items lg_max_k cannot be NULL")
                                .GetValue<int64_t>();
            if (lg_max_k < 3 || lg_max_k > 30)
            {
                throw BinderException("Frequent Items lg_max_k must be between 3 and 30, got: %lld", (long long)lg_max_k);
            }
            return make_uniq<DSFreqItemsBindData>(static_cast<uint8_t>(lg_max_k));
        }

        // ============================================================
        // 2. State & Operations
        // ============================================================
        struct DSFreqItemsState
        {
            FrequentItemsSketch *sketch = nullptr;
            ~DSFreqItemsState()
            {
                delete sketch;
            }
            void Create(const AggregateInputData &input)
            {
                if (!sketch)
                {
                    sketch = new FrequentItemsSketch(input.bind_data->Cast<DSFreqItemsBindData>().lg_max_k);
                }
            }
        };

        // Shared by the create, weighted create and merge aggregates. With no input rows, the result is NULL.
        struct DSFreqItemsOperationBase
        {
            template <class STATE>
            static void Initialize(STATE &state) { state.sketch = nullptr; }

            template <class STATE>
            static void Destroy(STATE &state, AggregateInputData &)
            {
                delete state.sketch;
                state.sketch = nullptr;
            }

            static bool IgnoreNull() { return true; }

            template <class INPUT_TYPE, class STATE, class OP>
            static void ConstantOperation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input, idx_t count)
            {
                for (idx_t i = 0; i < count; i++)
                {
                    OP::template Operation<INPUT_TYPE, STATE, OP>(state, input, unary_input);
                }
            }

            template <class STATE, class OP>
            static void Combine(const STATE &source, STATE &target, AggregateInputData &aggr)
            {
                if (!source.sketch)
                {
                    return;
                }
                target.Create(aggr);
                target.sketch->merge(*source.sketch);
            }

            template <class T, class STATE>
            static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data)
            {
                if (state.sketch)
                {
                    target = DSFreqItemsSerialize(finalize_data.result, *state.sketch);
                }
                else
                {
                    finalize_data.ReturnNull();
                }
            }
        };

        // Raw items, each counted once.
        struct DSFreqItemsOperation : DSFreqItemsOperationBase
        {
            template <class A_TYPE, class STATE, class OP>
            static void Operation(STATE &state, const A_TYPE &input, AggregateUnaryInput &idata)
            {
                state.Create(idata.input);
                state.sketch->update(DSFreqItemsToString(input), 1);
            }
        };

        // Serialized sketches, merged.
        struct DSFreqItemsMergeOperation : DSFreqItemsOperationBase
        {
            template <class A_TYPE, class STATE, class OP>
            static void Operation(STATE &state, const A_TYPE &input, AggregateUnaryInput &idata)
            {
                state.Create(idata.input);
                state.sketch->merge(DSFreqItemsDeserialize(input));
            }
        };

        // Raw items with weights: the SQL equivalent of frequent_items_sketch::update(item, weight).
        struct DSFreqItemsWeightedOperation : DSFreqItemsOperationBase
        {
            template <class A_TYPE, class W_TYPE, class STATE, class OP>
            static void Operation(STATE &state, const A_TYPE &input, const W_TYPE &weight, AggregateBinaryInput &idata)
            {
                state.Create(idata.input);
                state.sketch->update(DSFreqItemsToString(input), DSFreqItemsToWeight(weight));
            }
        };

        // ============================================================
        // 3. Scalar Functions
        // ============================================================

        // An item's estimate or bound: fun(sketch, key).
        template <typename T, class FUNC>
        void DSFreqItemsItemFunction(DataChunk &args, Vector &result, FUNC &&fun)
        {
            DSSketchCache<FrequentItemsSketch> cache;
            BinaryExecutor::Execute<string_t, T, int64_t>(args.data[0], args.data[1], result, [&](string_t sketch_blob, T item)
                                                          { return static_cast<int64_t>(fun(cache.Get(sketch_blob, "Frequent Items"), DSFreqItemsToString(item))); });
        }

        template <typename T>
        void DSFreqItemsEstimate(DataChunk &args, ExpressionState &, Vector &result)
        {
            DSFreqItemsItemFunction<T>(args, result, [](const FrequentItemsSketch &sketch, const std::string &key)
                                       { return sketch.get_estimate(key); });
        }

        template <typename T>
        void DSFreqItemsLowerBound(DataChunk &args, ExpressionState &, Vector &result)
        {
            DSFreqItemsItemFunction<T>(args, result, [](const FrequentItemsSketch &sketch, const std::string &key)
                                       { return sketch.get_lower_bound(key); });
        }

        template <typename T>
        void DSFreqItemsUpperBound(DataChunk &args, ExpressionState &, Vector &result)
        {
            DSFreqItemsItemFunction<T>(args, result, [](const FrequentItemsSketch &sketch, const std::string &key)
                                       { return sketch.get_upper_bound(key); });
        }

        template <class RESULT_TYPE, class FUNC>
        void DSFreqItemsSketchFunction(DataChunk &args, Vector &result, FUNC &&fun)
        {
            UnaryExecutor::Execute<string_t, RESULT_TYPE>(args.data[0], result, [&](string_t sketch_blob)
                                                          { return static_cast<RESULT_TYPE>(fun(DSFreqItemsDeserialize(sketch_blob))); });
        }

        void DSFreqItemsEpsilon(DataChunk &args, ExpressionState &, Vector &result)
        {
            DSFreqItemsSketchFunction<double>(args, result, [](const FrequentItemsSketch &sketch)
                                              { return sketch.get_epsilon(); });
        }

        void DSFreqItemsTotalWeight(DataChunk &args, ExpressionState &, Vector &result)
        {
            DSFreqItemsSketchFunction<int64_t>(args, result, [](const FrequentItemsSketch &sketch)
                                               { return sketch.get_total_weight(); });
        }

        void DSFreqItemsIsEmpty(DataChunk &args, ExpressionState &, Vector &result)
        {
            DSFreqItemsSketchFunction<bool>(args, result, [](const FrequentItemsSketch &sketch)
                                            { return sketch.is_empty(); });
        }

        void DSFreqItemsNumActive(DataChunk &args, ExpressionState &, Vector &result)
        {
            DSFreqItemsSketchFunction<int64_t>(args, result, [](const FrequentItemsSketch &sketch)
                                               { return sketch.get_num_active_items(); });
        }

        void DSFreqItemsGetFrequent(DataChunk &args, ExpressionState &, Vector &result)
        {
            BinaryExecutor::Execute<string_t, string_t, list_entry_t>(
                args.data[0], args.data[1], result, [&](string_t sketch_blob, string_t error_type)
                {
                    // The rows point into the sketch, so it must outlive them.
                    auto sketch = DSFreqItemsDeserialize(sketch_blob);
                    auto rows = sketch.get_frequent_items(DSFreqItemsErrorType(error_type.GetString()));
                    auto offset = ListVector::GetListSize(result);
                    ListVector::Reserve(result, offset + rows.size());
                    auto &entries = StructVector::GetEntries(ListVector::GetChildMutable(result));
                    auto items = FlatVector::GetDataMutable<string_t>(entries[0]);
                    auto estimates = FlatVector::GetDataMutable<int64_t>(entries[1]);
                    auto lower_bounds = FlatVector::GetDataMutable<int64_t>(entries[2]);
                    auto upper_bounds = FlatVector::GetDataMutable<int64_t>(entries[3]);
                    for (idx_t i = 0; i < rows.size(); i++)
                    {
                        items[offset + i] = StringVector::AddString(entries[0], rows[i].get_item());
                        estimates[offset + i] = static_cast<int64_t>(rows[i].get_estimate());
                        lower_bounds[offset + i] = static_cast<int64_t>(rows[i].get_lower_bound());
                        upper_bounds[offset + i] = static_cast<int64_t>(rows[i].get_upper_bound());
                    }
                    ListVector::SetListSize(result, offset + rows.size());
                    return list_entry_t(offset, rows.size());
                });
        }
    }

    // ============================================================
    // 4. Main Loader
    // ============================================================
    void LoadFrequentItemsSketch(ExtensionLoader &loader)
    {
        auto sketch_type = DSCreateSketchType(loader, "sketch_frequent_items", "Sketch type for Frequent Items Sketch");
        const auto &ex = DSFreqItemsExamples();
        const string rows = DS_FREQ_ITEMS_EXAMPLE_ROWS;

        // --- datasketch_frequent_items([lg_max_k,] data): build from items, or merge sketches. lg_max_k stays in the
        // argument list (see DSReadDataArgumentAt); the data follows it.
        {
            AggregateFunctionSet set("datasketch_frequent_items");
            vector<FunctionDescription> descs;
            DSFreqItemsForEachItemType([&](auto item, const LogicalType &item_type)
                                       {
                using T = decltype(item);
                auto fun = AggregateFunction::UnaryAggregate<DSFreqItemsState, T, string_t, DSFreqItemsOperation, AggregateDestructorType::LEGACY>(item_type, sketch_type);
                DSAddAggregate(set, descs, fun, DSFreqItemsBind<2>, {item_type}, {"data"},
                               "Creates a Frequent Items sketch to find heavy hitters in a data stream",
                               {"SELECT datasketch_frequent_items(item)" + rows});
                DSReadDataArgumentAt<1, DSFreqItemsState, T, DSFreqItemsOperation>(fun);
                DSAddAggregate(set, descs, fun, DSFreqItemsBind<2>, {LogicalType::INTEGER, item_type}, {"lg_max_k", "data"},
                               "Creates a Frequent Items sketch with the given log2 of max map size (lg_max_k)",
                               {"SELECT datasketch_frequent_items(10, item)" + rows}); });

            auto merge = AggregateFunction::UnaryAggregate<DSFreqItemsState, string_t, string_t, DSFreqItemsMergeOperation, AggregateDestructorType::LEGACY>(sketch_type, sketch_type);
            DSAddAggregate(set, descs, merge, DSFreqItemsBind<2>, {sketch_type}, {"sketch"},
                           "Merges existing Frequent Items sketches into a single sketch",
                           {ex.QueryPartials("datasketch_frequent_items(sketch)")});
            DSReadDataArgumentAt<1, DSFreqItemsState, string_t, DSFreqItemsMergeOperation>(merge);
            DSAddAggregate(set, descs, merge, DSFreqItemsBind<2>, {LogicalType::INTEGER, sketch_type}, {"lg_max_k", "sketch"},
                           "Merges existing Frequent Items sketches into a single sketch with the given log2 of max map size (lg_max_k)",
                           {ex.QueryPartials("datasketch_frequent_items(10, sketch)")});
            DSRegisterAggregates(loader, std::move(set), std::move(descs), DS_FREQ_ITEMS_CATEGORIES);
        }

        // --- datasketch_frequent_items_weighted([lg_max_k,] data, weight)
        {
            AggregateFunctionSet set("datasketch_frequent_items_weighted");
            vector<FunctionDescription> descs;
            DSFreqItemsForEachItemType([&](auto item, const LogicalType &item_type)
                                       {
                using T = decltype(item);
                auto add = [&](auto weight, const LogicalType &weight_type)
                {
                    using W = decltype(weight);
                    using OP = DSFreqItemsWeightedOperation;
                    auto fun = AggregateFunction::BinaryAggregate<DSFreqItemsState, T, W, string_t, OP, AggregateDestructorType::LEGACY>(item_type, weight_type, sketch_type);
                    fun.SetStateDestructorCallback(AggregateFunction::StateDestroy<DSFreqItemsState, OP>);
                    DSAddAggregate(set, descs, fun, DSFreqItemsBind<3>, {item_type, weight_type}, {"data", "weight"},
                                   "Creates a weighted Frequent Items sketch to find heavy hitters by total item weight; each row adds its weight to the item's count",
                                   {"SELECT datasketch_frequent_items_weighted(item, weight)" + rows});
                    DSReadDataArgumentsAt<1, 2, DSFreqItemsState, T, W, OP>(fun);
                    DSAddAggregate(set, descs, fun, DSFreqItemsBind<3>, {LogicalType::INTEGER, item_type, weight_type}, {"lg_max_k", "data", "weight"},
                                   "Creates a weighted Frequent Items sketch with the given log2 of max map size (lg_max_k); each row adds its weight to the item's count",
                                   {"SELECT datasketch_frequent_items_weighted(10, item, weight)" + rows});
                };
                add(int64_t(), LogicalType::BIGINT);
                add(uint64_t(), LogicalType::UBIGINT); });
            DSRegisterAggregates(loader, std::move(set), std::move(descs), DS_FREQ_ITEMS_CATEGORIES);
        }

        // --- An item's estimate and bounds, for every item type
        auto register_item_function = [&](const string &name, const string &description, auto make_function)
        {
            ScalarFunctionSet set {Identifier(name)};
            vector<FunctionDescription> descs;
            DSFreqItemsForEachItemType([&](auto item, const LogicalType &item_type)
                                       { DSAddScalar(set, descs, ScalarFunction({sketch_type, item_type}, LogicalType::BIGINT, make_function(item)),
                                                     {"sketch", "item"}, description, {ex.Query(name + "(sketch, '3')")}); });
            DSRegisterScalars(loader, std::move(set), std::move(descs), DS_FREQ_ITEMS_CATEGORIES);
        };
        register_item_function("datasketch_frequent_items_estimate", "Returns the estimated frequency count for a specific item",
                               [](auto item) { return DSFreqItemsEstimate<decltype(item)>; });
        register_item_function("datasketch_frequent_items_lower_bound", "Returns the lower bound frequency estimate for a specific item",
                               [](auto item) { return DSFreqItemsLowerBound<decltype(item)>; });
        register_item_function("datasketch_frequent_items_upper_bound", "Returns the upper bound frequency estimate for a specific item",
                               [](auto item) { return DSFreqItemsUpperBound<decltype(item)>; });

        // --- Sketch metadata
        auto register_sketch_function = [&](const string &name, const LogicalType &return_type, scalar_function_t function,
                                            const string &description)
        {
            DSRegisterScalar(loader, ScalarFunction(Identifier(name), {sketch_type}, return_type, std::move(function)),
                             DSDescription(description, {ex.Query(name + "(sketch)")}, {"sketch"}, {sketch_type}),
                             DS_FREQ_ITEMS_CATEGORIES);
        };
        register_sketch_function("datasketch_frequent_items_epsilon", LogicalType::DOUBLE, DSFreqItemsEpsilon,
                                 "Returns the epsilon value (relative error) of the sketch");
        register_sketch_function("datasketch_frequent_items_total_weight", LogicalType::BIGINT, DSFreqItemsTotalWeight,
                                 "Returns the total weight (sum of all item counts) processed by the sketch");
        register_sketch_function("datasketch_frequent_items_is_empty", LogicalType::BOOLEAN, DSFreqItemsIsEmpty,
                                 "Returns true if the sketch is empty");
        register_sketch_function("datasketch_frequent_items_num_active", LogicalType::BIGINT, DSFreqItemsNumActive,
                                 "Returns the number of active items currently tracked by the sketch");

        // --- The frequent items, with their estimates and bounds
        {
            child_list_t<LogicalType> fields;
            fields.push_back({"item", LogicalType::VARCHAR});
            fields.push_back({"estimate", LogicalType::BIGINT});
            fields.push_back({"lower_bound", LogicalType::BIGINT});
            fields.push_back({"upper_bound", LogicalType::BIGINT});
            DSRegisterScalar(loader,
                             ScalarFunction("datasketch_frequent_items_get_frequent", {sketch_type, LogicalType::VARCHAR},
                                            LogicalType::LIST(LogicalType::STRUCT(std::move(fields))), DSFreqItemsGetFrequent),
                             DSDescription("Returns a list of frequent items with their estimates and bounds. Use 'NO_FALSE_POSITIVES' or 'NO_FALSE_NEGATIVES' for error type",
                                           {ex.Query("datasketch_frequent_items_get_frequent(sketch, 'NO_FALSE_POSITIVES')")},
                                           {"sketch", "error_type"}, {sketch_type, LogicalType::VARCHAR}),
                             DS_FREQ_ITEMS_CATEGORIES);
        }
    }

}
