#include "datasketches_extension.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/parsed_data/create_aggregate_function_info.hpp"
#include "duckdb/parser/parsed_data/create_type_info.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

#include <DataSketches/frequent_items_sketch.hpp>
#include <string>
#include <type_traits>

using namespace duckdb;

namespace duckdb_datasketches {

    using FrequentItemsSketch = datasketches::frequent_items_sketch<std::string>;

    // ============================================================
    // 1. Helpers & Bind Data
    // ============================================================
    struct DSFreqItemsBindData : public FunctionData {
        DSFreqItemsBindData() : lg_max_k(10) {}
        explicit DSFreqItemsBindData(uint8_t lg_max_k) : lg_max_k(lg_max_k) {}
        unique_ptr<FunctionData> Copy() const override { return make_uniq<DSFreqItemsBindData>(lg_max_k); }
        bool Equals(const FunctionData &other_p) const override {
            return lg_max_k == other_p.Cast<DSFreqItemsBindData>().lg_max_k;
        }
        uint8_t lg_max_k;
    };

    unique_ptr<FunctionData> DSFreqItemsBind(ClientContext &context, AggregateFunction &function,
                                              vector<unique_ptr<Expression>> &arguments) {
        uint8_t lg_max_k = 10;
        if (arguments.size() == 2) {
            if (!arguments[0]->IsFoldable()) throw BinderException("Frequent Items lg_max_k must be constant");
            Value k_val = ExpressionExecutor::EvaluateScalar(context, *arguments[0]);
            if (!k_val.IsNull()) lg_max_k = (uint8_t)k_val.GetValue<int32_t>();
            Function::EraseArgument(function, arguments, 0);
        }
        return make_uniq<DSFreqItemsBindData>(lg_max_k);
    }

    // ============================================================
    // 2. State & Operations
    // ============================================================
    struct DSFreqItemsState {
        FrequentItemsSketch* sketch = nullptr;
        ~DSFreqItemsState() { if (sketch) delete sketch; }
        void Create(uint8_t lg_max_k) { if (!sketch) sketch = new FrequentItemsSketch(lg_max_k); }
    };

    // Operation for RAW ITEMS (VARCHAR, INTEGER, BIGINT)
    struct DSFreqItemsOperation {
        template <class STATE> static void Initialize(STATE &state) { state.sketch = nullptr; }
        template <class STATE> static void Destroy(STATE &state, AggregateInputData &) { if (state.sketch) delete state.sketch; }
        static bool IgnoreNull() { return true; }

        template <class A_TYPE, class STATE, class OP>
        static void Operation(STATE &state, const A_TYPE &input, AggregateUnaryInput &idata) {
            auto &bind_data = idata.input.bind_data->template Cast<DSFreqItemsBindData>();
            state.Create(bind_data.lg_max_k);

            if constexpr (std::is_same_v<A_TYPE, string_t>) {
                state.sketch->update(input.GetString(), 1);
            } else {
                state.sketch->update(std::to_string(input), 1);
            }
        }

        template <class INPUT_TYPE, class STATE, class OP>
        static void ConstantOperation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input, idx_t count) {
            for (idx_t i = 0; i < count; i++) {
                Operation<INPUT_TYPE, STATE, OP>(state, input, unary_input);
            }
        }

        template <class STATE, class OP>
        static void Combine(const STATE &source, STATE &target, AggregateInputData &aggr) {
            if (!source.sketch) return;
            if (!target.sketch) {
                 auto &bind_data = aggr.bind_data->template Cast<DSFreqItemsBindData>();
                 target.Create(bind_data.lg_max_k);
            }
            target.sketch->merge(*source.sketch);
        }

        template <class T, class STATE>
        static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
            if (state.sketch) {
                 auto serialized = state.sketch->serialize();
                 target = StringVector::AddStringOrBlob(finalize_data.result, std::string(serialized.begin(), serialized.end()));
            } else {
                 auto &bind_data = finalize_data.input.bind_data->template Cast<DSFreqItemsBindData>();
                 FrequentItemsSketch empty_sketch(bind_data.lg_max_k);
                 auto serialized = empty_sketch.serialize();
                 target = StringVector::AddStringOrBlob(finalize_data.result, std::string(serialized.begin(), serialized.end()));
            }
        }
    };

    // Operation for MERGING SKETCHES (BLOB)
    struct DSFreqItemsMergeOperation {
        template <class STATE> static void Initialize(STATE &state) { DSFreqItemsOperation::Initialize<STATE>(state); }
        template <class STATE> static void Destroy(STATE &state, AggregateInputData &aggr) { DSFreqItemsOperation::Destroy<STATE>(state, aggr); }
        static bool IgnoreNull() { return true; }

        template <class A_TYPE, class STATE, class OP>
        static void Operation(STATE &state, const A_TYPE &input, AggregateUnaryInput &idata) {
            auto &bind_data = idata.input.bind_data->template Cast<DSFreqItemsBindData>();
            state.Create(bind_data.lg_max_k);
            auto input_sketch = FrequentItemsSketch::deserialize(input.GetDataUnsafe(), input.GetSize());
            state.sketch->merge(input_sketch);
        }

        template <class INPUT_TYPE, class STATE, class OP>
        static void ConstantOperation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input, idx_t count) {
            for (idx_t i = 0; i < count; i++) {
                Operation<INPUT_TYPE, STATE, OP>(state, input, unary_input);
            }
        }

        template <class STATE, class OP>
        static void Combine(const STATE &source, STATE &target, AggregateInputData &aggr) {
            DSFreqItemsOperation::Combine<STATE, OP>(source, target, aggr);
        }

        template <class T, class STATE>
        static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize) {
            DSFreqItemsOperation::Finalize<T, STATE>(state, target, finalize);
        }
    };

    // ============================================================
    // 3. Scalar Helpers
    // ============================================================
    static FrequentItemsSketch DeserializeSketch(const string_t& blob) {
        return FrequentItemsSketch::deserialize(blob.GetDataUnsafe(), blob.GetSize());
    }

    // --- TEMPLATED SCALAR FUNCTIONS (Support string_t, int32_t, int64_t) ---

    template<typename T>
    static void DSFreqItemsEstimate(DataChunk &args, ExpressionState &state, Vector &result) {
        BinaryExecutor::Execute<string_t, T, int64_t>(args.data[0], args.data[1], result, args.size(),
            [&](string_t sketch_blob, T item) {
                auto sketch = DeserializeSketch(sketch_blob);
                if constexpr (std::is_same_v<T, string_t>) {
                    return (int64_t)sketch.get_estimate(item.GetString());
                } else {
                    return (int64_t)sketch.get_estimate(std::to_string(item));
                }
            });
    }

    template<typename T>
    static void DSFreqItemsLowerBound(DataChunk &args, ExpressionState &state, Vector &result) {
        BinaryExecutor::Execute<string_t, T, int64_t>(args.data[0], args.data[1], result, args.size(),
            [&](string_t sketch_blob, T item) {
                auto sketch = DeserializeSketch(sketch_blob);
                if constexpr (std::is_same_v<T, string_t>) {
                    return (int64_t)sketch.get_lower_bound(item.GetString());
                } else {
                    return (int64_t)sketch.get_lower_bound(std::to_string(item));
                }
            });
    }

    template<typename T>
    static void DSFreqItemsUpperBound(DataChunk &args, ExpressionState &state, Vector &result) {
        BinaryExecutor::Execute<string_t, T, int64_t>(args.data[0], args.data[1], result, args.size(),
            [&](string_t sketch_blob, T item) {
                auto sketch = DeserializeSketch(sketch_blob);
                if constexpr (std::is_same_v<T, string_t>) {
                    return (int64_t)sketch.get_upper_bound(item.GetString());
                } else {
                    return (int64_t)sketch.get_upper_bound(std::to_string(item));
                }
            });
    }

    static void DSFreqItemsEpsilon(DataChunk &args, ExpressionState &state, Vector &result) {
        UnaryExecutor::Execute<string_t, double>(args.data[0], result, args.size(),
            [&](string_t sketch_blob) {
                return DeserializeSketch(sketch_blob).get_epsilon();
            });
    }

    static void DSFreqItemsTotalWeight(DataChunk &args, ExpressionState &state, Vector &result) {
        UnaryExecutor::Execute<string_t, int64_t>(args.data[0], result, args.size(),
            [&](string_t sketch_blob) {
                return (int64_t)DeserializeSketch(sketch_blob).get_total_weight();
            });
    }

    static void DSFreqItemsIsEmpty(DataChunk &args, ExpressionState &state, Vector &result) {
        UnaryExecutor::Execute<string_t, bool>(args.data[0], result, args.size(),
            [&](string_t sketch_blob) {
                return DeserializeSketch(sketch_blob).is_empty();
            });
    }

    static void DSFreqItemsNumActive(DataChunk &args, ExpressionState &state, Vector &result) {
        UnaryExecutor::Execute<string_t, int64_t>(args.data[0], result, args.size(),
            [&](string_t sketch_blob) {
                return (int64_t)DeserializeSketch(sketch_blob).get_num_active_items();
            });
    }

    static void DSFreqItemsGetFrequent(DataChunk &args, ExpressionState &state, Vector &result) {
        idx_t count = args.size();
        UnifiedVectorFormat sketch_data, type_data;
        args.data[0].ToUnifiedFormat(count, sketch_data);
        args.data[1].ToUnifiedFormat(count, type_data);

        ListVector::Reserve(result, count * 5);
        ListVector::SetListSize(result, 0);

        auto &child_entry = ListVector::GetEntry(result);
        auto &struct_children = StructVector::GetEntries(child_entry);
        auto item_vec = struct_children[0].get();
        auto est_vec = struct_children[1].get();
        auto lb_vec = struct_children[2].get();
        auto ub_vec = struct_children[3].get();

        idx_t current_offset = 0;

        for(idx_t i = 0; i < count; i++) {
            idx_t sketch_idx = sketch_data.sel->get_index(i);
            idx_t type_idx = type_data.sel->get_index(i);

            if (!sketch_data.validity.RowIsValid(sketch_idx)) {
                FlatVector::SetNull(result, i, true);
                continue;
            }

            string_t type_str = ((string_t*)type_data.data)[type_idx];
            auto err_type = (type_str.GetString() == "NO_FALSE_NEGATIVES") ?
                            datasketches::NO_FALSE_NEGATIVES : datasketches::NO_FALSE_POSITIVES;

            string_t sketch_blob = ((string_t*)sketch_data.data)[sketch_idx];
            auto sketch = DeserializeSketch(sketch_blob);
            auto rows = sketch.get_frequent_items(err_type);

            auto list_data = ListVector::GetData(result);
            list_data[i].offset = current_offset;
            list_data[i].length = rows.size();

            ListVector::Reserve(result, current_offset + rows.size());

            for (auto &row : rows) {
                FlatVector::GetData<string_t>(*item_vec)[current_offset] = StringVector::AddString(*item_vec, row.get_item());
                FlatVector::GetData<int64_t>(*est_vec)[current_offset] = row.get_estimate();
                FlatVector::GetData<int64_t>(*lb_vec)[current_offset] = row.get_lower_bound();
                FlatVector::GetData<int64_t>(*ub_vec)[current_offset] = row.get_upper_bound();
                current_offset++;
            }
        }
        ListVector::SetListSize(result, current_offset);
    }

    // ============================================================
    // 4. Registration Helpers
    // ============================================================
    static LogicalType CreateFrequentItemsSketchType(ExtensionLoader &loader) {
        auto new_type = LogicalType(LogicalTypeId::BLOB);
        auto new_type_name = "sketch_frequent_items";

        auto type_info = CreateTypeInfo(new_type_name, LogicalType::BLOB);
        type_info.temporary = false;
        type_info.internal = true;
        type_info.comment = "Sketch type for Frequent Items Sketch";
        new_type.SetAlias(new_type_name);

        auto &system_catalog = Catalog::GetSystemCatalog(loader.GetDatabaseInstance());
        auto data = CatalogTransaction::GetSystemTransaction(loader.GetDatabaseInstance());
        system_catalog.CreateType(data, type_info);

        loader.RegisterCastFunction(LogicalType::BLOB, new_type, DefaultCasts::ReinterpretCast, 1);
        loader.RegisterCastFunction(new_type, LogicalType::BLOB, DefaultCasts::ReinterpretCast, 1);

        return new_type;
    }

    template <typename T>
    void RegisterFreqItems(AggregateFunctionSet &set, LogicalType input_type, LogicalType result_type) {
        auto fun = AggregateFunction::UnaryAggregateDestructor<DSFreqItemsState, T, string_t, DSFreqItemsOperation, AggregateDestructorType::LEGACY>(
            input_type, result_type);
        fun.bind = DSFreqItemsBind;
        fun.arguments = {input_type};
        set.AddFunction(fun);
        fun.arguments = {LogicalType::INTEGER, input_type};
        set.AddFunction(fun);
    }

    // ============================================================
    // 5. Main Loader
    // ============================================================
    void LoadFrequentItemsSketch(ExtensionLoader &loader) {
        auto sketch_type = CreateFrequentItemsSketchType(loader);

        AggregateFunctionSet sketch_agg("datasketch_frequent_items");

        // --- 1. REGISTER AGGREGATE TYPES (String, Int, BigInt) ---
        RegisterFreqItems<string_t>(sketch_agg, LogicalType::VARCHAR, sketch_type);
        RegisterFreqItems<int32_t>(sketch_agg, LogicalType::INTEGER, sketch_type);
        RegisterFreqItems<int64_t>(sketch_agg, LogicalType::BIGINT, sketch_type);

        // --- 2. MERGE SKETCHES ---
        auto fun_merge = AggregateFunction::UnaryAggregateDestructor<DSFreqItemsState, string_t, string_t, DSFreqItemsMergeOperation, AggregateDestructorType::LEGACY>(
            sketch_type, sketch_type);
        fun_merge.bind = DSFreqItemsBind;
        fun_merge.arguments = {sketch_type};
        sketch_agg.AddFunction(fun_merge);
        fun_merge.arguments = {LogicalType::INTEGER, sketch_type};
        sketch_agg.AddFunction(fun_merge);

        loader.RegisterFunction(CreateAggregateFunctionInfo(sketch_agg));

        // --- SCALAR FUNCTIONS (ESTIMATES & BOUNDS) ---
        // We use ScalarFunctionSet to register overloads for VARCHAR, INTEGER, and BIGINT

        // ESTIMATE
        {
            ScalarFunctionSet set("datasketch_frequent_items_estimate");
            set.AddFunction(ScalarFunction({sketch_type, LogicalType::VARCHAR}, LogicalType::BIGINT, DSFreqItemsEstimate<string_t>));
            set.AddFunction(ScalarFunction({sketch_type, LogicalType::INTEGER}, LogicalType::BIGINT, DSFreqItemsEstimate<int32_t>));
            set.AddFunction(ScalarFunction({sketch_type, LogicalType::BIGINT}, LogicalType::BIGINT, DSFreqItemsEstimate<int64_t>));
            loader.RegisterFunction(CreateScalarFunctionInfo(std::move(set)));
        }

        // LOWER BOUND
        {
            ScalarFunctionSet set("datasketch_frequent_items_lower_bound");
            set.AddFunction(ScalarFunction({sketch_type, LogicalType::VARCHAR}, LogicalType::BIGINT, DSFreqItemsLowerBound<string_t>));
            set.AddFunction(ScalarFunction({sketch_type, LogicalType::INTEGER}, LogicalType::BIGINT, DSFreqItemsLowerBound<int32_t>));
            set.AddFunction(ScalarFunction({sketch_type, LogicalType::BIGINT}, LogicalType::BIGINT, DSFreqItemsLowerBound<int64_t>));
            loader.RegisterFunction(CreateScalarFunctionInfo(std::move(set)));
        }

        // UPPER BOUND
        {
            ScalarFunctionSet set("datasketch_frequent_items_upper_bound");
            set.AddFunction(ScalarFunction({sketch_type, LogicalType::VARCHAR}, LogicalType::BIGINT, DSFreqItemsUpperBound<string_t>));
            set.AddFunction(ScalarFunction({sketch_type, LogicalType::INTEGER}, LogicalType::BIGINT, DSFreqItemsUpperBound<int32_t>));
            set.AddFunction(ScalarFunction({sketch_type, LogicalType::BIGINT}, LogicalType::BIGINT, DSFreqItemsUpperBound<int64_t>));
            loader.RegisterFunction(CreateScalarFunctionInfo(std::move(set)));
        }

        // --- METADATA FUNCTIONS ---
        loader.RegisterFunction(CreateScalarFunctionInfo(ScalarFunction("datasketch_frequent_items_epsilon",
            {sketch_type}, LogicalType::DOUBLE, DSFreqItemsEpsilon)));

        loader.RegisterFunction(CreateScalarFunctionInfo(ScalarFunction("datasketch_frequent_items_total_weight",
            {sketch_type}, LogicalType::BIGINT, DSFreqItemsTotalWeight)));

        loader.RegisterFunction(CreateScalarFunctionInfo(ScalarFunction("datasketch_frequent_items_is_empty",
            {sketch_type}, LogicalType::BOOLEAN, DSFreqItemsIsEmpty)));

        loader.RegisterFunction(CreateScalarFunctionInfo(ScalarFunction("datasketch_frequent_items_num_active",
            {sketch_type}, LogicalType::BIGINT, DSFreqItemsNumActive)));

        // --- GET FREQUENT LIST ---
        child_list_t<LogicalType> struct_fields;
        struct_fields.push_back({"item", LogicalType::VARCHAR});
        struct_fields.push_back({"estimate", LogicalType::BIGINT});
        struct_fields.push_back({"lower_bound", LogicalType::BIGINT});
        struct_fields.push_back({"upper_bound", LogicalType::BIGINT});

        loader.RegisterFunction(CreateScalarFunctionInfo(ScalarFunction("datasketch_frequent_items_get_frequent",
            {sketch_type, LogicalType::VARCHAR}, LogicalType::LIST(LogicalType::STRUCT(struct_fields)), DSFreqItemsGetFrequent)));
    }

} // namespace duckdb_datasketches
