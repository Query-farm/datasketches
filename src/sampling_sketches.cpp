// The weighted sampling sketches: VarOpt (a variance-optimal sample of K items, with an estimate of the total
// weight of any subset) and EBPPS (a sample whose inclusion probabilities are exactly proportional to the weights).

#include "datasketches_generated_common.hpp"

#include <DataSketches/ebpps_sketch.hpp>
#include <DataSketches/var_opt_sketch.hpp>
#include <DataSketches/var_opt_union.hpp>

#include <map>
#include <unordered_set>

namespace duckdb
{
    namespace
    {
        template <class T>
        using DSVarOptSketch = datasketches::var_opt_sketch<DSItem<T>, DSAllocator<DSItem<T>>>;
        template <class T>
        using DSVarOptUnion = datasketches::var_opt_union<DSItem<T>, DSAllocator<DSItem<T>>>;
        template <class T>
        using DSEbppsSketch = datasketches::ebpps_sketch<DSItem<T>, DSAllocator<DSItem<T>>>;

        // Both libraries accept K up to 2^31 - 2, but EBPPS reserves room for K items up front (per group), so K is
        // kept to a size a sample can reasonably have.
        constexpr int64_t DS_SAMPLE_MAX_K = int64_t(1) << 20;

        // Calls fun(T(), logical_type) for every item type the sampling sketches accept.
        template <class FUNC>
        void DSSampleForEachItemType(FUNC &&fun)
        {
            fun(int8_t(), LogicalType::TINYINT);
            fun(int16_t(), LogicalType::SMALLINT);
            fun(int32_t(), LogicalType::INTEGER);
            fun(int64_t(), LogicalType::BIGINT);
            fun(float(), LogicalType::FLOAT);
            fun(double(), LogicalType::DOUBLE);
            fun(uint8_t(), LogicalType::UTINYINT);
            fun(uint16_t(), LogicalType::USMALLINT);
            fun(uint32_t(), LogicalType::UINTEGER);
            fun(uint64_t(), LogicalType::UBIGINT);
            fun(string_t(), LogicalType::VARCHAR);
        }

        // What differs between the two sketches.
        template <class T>
        struct DSVarOptTraits
        {
            using Sketch = DSVarOptSketch<T>;
            static constexpr const char *NAME = "VarOpt";
            static constexpr const char *SQL = "varopt";

            // Merging goes through a union of at most `k` samples.
            static void Merge(Sketch &target, Sketch &&source, uint32_t k)
            {
                DSVarOptUnion<T> u(k);
                u.update(std::move(target));
                u.update(std::move(source));
                target = u.get_result();
            }
            static void Merge(Sketch &target, const Sketch &source, uint32_t k)
            {
                DSVarOptUnion<T> u(k);
                u.update(std::move(target));
                u.update(source);
                target = u.get_result();
            }
        };

        template <class T>
        struct DSEbppsTraits
        {
            using Sketch = DSEbppsSketch<T>;
            static constexpr const char *NAME = "EBPPS";
            static constexpr const char *SQL = "ebpps";

            // The merged sample has the smaller K of the two.
            static void Merge(Sketch &target, Sketch &&source, uint32_t)
            {
                target.merge(std::move(source));
            }
            static void Merge(Sketch &target, const Sketch &source, uint32_t)
            {
                target.merge(source);
            }
        };

        // Runs fun, reporting library errors (e.g. a negative weight) as invalid input of the sketch's aggregate.
        template <class TRAITS, class FUNC>
        void DSSampleGuard(FUNC &&fun)
        {
            try
            {
                fun();
            }
            catch (Exception &)
            {
                throw;
            }
            catch (std::exception &e)
            {
                throw InvalidInputException("datasketch_%s: %s", TRAITS::SQL, e.what());
            }
        }

        // ============================================================
        // 1. Bind Data
        // ============================================================
        struct DSSampleBindData : public FunctionData
        {
            explicit DSSampleBindData(uint32_t k) : k(k) {}
            unique_ptr<FunctionData> Copy() const override { return make_uniq<DSSampleBindData>(k); }
            bool Equals(const FunctionData &other_p) const override { return k == other_p.Cast<DSSampleBindData>().k; }
            uint32_t k;
        };

        template <class TRAITS>
        unique_ptr<FunctionData> DSSampleBind(BindAggregateFunctionInput &input)
        {
            auto k = DSBindConstant(input.GetClientContext(), *input.GetArguments()[0],
                                    string(TRAITS::NAME) + " can only take a constant K value",
                                    string(TRAITS::NAME) + " K value cannot be NULL")
                         .template GetValue<int64_t>();
            if (k < 1 || k > DS_SAMPLE_MAX_K)
            {
                throw BinderException("%s K value must be between 1 and %lld, got: %lld", TRAITS::NAME, (long long)DS_SAMPLE_MAX_K,
                                      (long long)k);
            }
            return make_uniq<DSSampleBindData>(static_cast<uint32_t>(k));
        }

        // ============================================================
        // 2. State & Operations
        // ============================================================
        template <class TRAITS>
        struct DSSampleState
        {
            typename TRAITS::Sketch *sketch = nullptr;
            ~DSSampleState()
            {
                delete sketch;
            }
            void Create(AggregateInputData &input)
            {
                if (!sketch)
                {
                    sketch = new typename TRAITS::Sketch(input.bind_data->Cast<DSSampleBindData>().k);
                }
            }
        };

        // Shared by the build, weighted build and merge aggregates. With no input rows, the result is NULL.
        template <class TRAITS>
        struct DSSampleOperationBase
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

            // A repeated value is sampled as that many items.
            template <class INPUT_TYPE, class STATE, class OP>
            static void ConstantOperation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input, idx_t count)
            {
                for (idx_t i = 0; i < count; i++)
                {
                    OP::template Operation<INPUT_TYPE, STATE, OP>(state, input, unary_input);
                }
            }

            template <class STATE, class OP>
            static void Combine(const STATE &source, STATE &target, AggregateInputData &aggr_input_data)
            {
                if (!source.sketch)
                {
                    return;
                }
                const bool destructive = aggr_input_data.combine_type == AggregateCombineType::ALLOW_DESTRUCTIVE;
                if (!target.sketch)
                {
                    if (destructive)
                    {
                        // The source is discarded after this call, so take its sketch rather than copy it.
                        target.sketch = source.sketch;
                        const_cast<STATE &>(source).sketch = nullptr;
                    }
                    else
                    {
                        target.sketch = new typename TRAITS::Sketch(*source.sketch);
                    }
                    return;
                }
                auto k = aggr_input_data.bind_data->template Cast<DSSampleBindData>().k;
                if (destructive)
                {
                    TRAITS::Merge(*target.sketch, std::move(*const_cast<STATE &>(source).sketch), k);
                }
                else
                {
                    TRAITS::Merge(*target.sketch, *source.sketch, k);
                }
            }

            template <class RESULT_TYPE, class STATE>
            static void Finalize(STATE &state, RESULT_TYPE &target, AggregateFinalizeData &finalize_data)
            {
                if (state.sketch)
                {
                    target = DSAddBytes(finalize_data.result, state.sketch->serialize());
                }
                else
                {
                    finalize_data.ReturnNull();
                }
            }
        };

        // Each row is an item of weight 1.
        template <class TRAITS>
        struct DSSampleOperation : DSSampleOperationBase<TRAITS>
        {
            template <class A_TYPE, class STATE, class OP>
            static void Operation(STATE &state, const A_TYPE &input, AggregateUnaryInput &idata)
            {
                state.Create(idata.input);
                state.sketch->update(DSItemIn(input), 1.0);
            }
        };

        template <class TRAITS>
        struct DSSampleWeightedOperation : DSSampleOperationBase<TRAITS>
        {
            template <class A_TYPE, class W_TYPE, class STATE, class OP>
            static void Operation(STATE &state, const A_TYPE &input, const W_TYPE &weight, AggregateBinaryInput &idata)
            {
                state.Create(idata.input);
                DSSampleGuard<TRAITS>([&]() { state.sketch->update(DSItemIn(input), weight); });
            }
        };

        template <class TRAITS>
        struct DSSampleMergeOperation : DSSampleOperationBase<TRAITS>
        {
            template <class A_TYPE, class STATE, class OP>
            static void Operation(STATE &state, const A_TYPE &input, AggregateUnaryInput &idata)
            {
                auto incoming = DSDeserialize<typename TRAITS::Sketch>(input, TRAITS::NAME);
                state.Create(idata.input);
                TRAITS::Merge(*state.sketch, std::move(incoming), idata.input.bind_data->template Cast<DSSampleBindData>().k);
            }
        };

        // ============================================================
        // 3. Scalar Functions
        // ============================================================
        template <class TRAITS, class RESULT_TYPE, class FUNC>
        void DSSampleSketchFunction(DataChunk &args, Vector &result, FUNC &&fun)
        {
            DSSketchCache<typename TRAITS::Sketch> cache;
            UnaryExecutor::Execute<string_t, RESULT_TYPE>(args.data[0], result, [&](string_t blob)
                                                          { return fun(cache.Get(blob, TRAITS::NAME)); });
        }

        template <class TRAITS>
        void DSSampleK(DataChunk &args, ExpressionState &, Vector &result)
        {
            DSSampleSketchFunction<TRAITS, uint32_t>(args, result, [](const typename TRAITS::Sketch &sketch) { return sketch.get_k(); });
        }

        template <class TRAITS>
        void DSSampleN(DataChunk &args, ExpressionState &, Vector &result)
        {
            DSSampleSketchFunction<TRAITS, uint64_t>(args, result, [](const typename TRAITS::Sketch &sketch) { return sketch.get_n(); });
        }

        template <class TRAITS>
        void DSSampleIsEmpty(DataChunk &args, ExpressionState &, Vector &result)
        {
            DSSampleSketchFunction<TRAITS, bool>(args, result, [](const typename TRAITS::Sketch &sketch) { return sketch.is_empty(); });
        }

        template <class TRAITS>
        void DSSampleDescribe(DataChunk &args, ExpressionState &, Vector &result)
        {
            DSSampleSketchFunction<TRAITS, string_t>(args, result, [&](const typename TRAITS::Sketch &sketch)
                                                     { return DSAddText(result, sketch.to_string()); });
        }

        template <class T>
        void DSVarOptNumSamples(DataChunk &args, ExpressionState &, Vector &result)
        {
            DSSampleSketchFunction<DSVarOptTraits<T>, uint32_t>(args, result, [](const DSVarOptSketch<T> &sketch)
                                                                { return sketch.get_num_samples(); });
        }

        template <class T>
        void DSEbppsCumulativeWeight(DataChunk &args, ExpressionState &, Vector &result)
        {
            DSSampleSketchFunction<DSEbppsTraits<T>, double>(args, result, [](const DSEbppsSketch<T> &sketch)
                                                             { return sketch.get_cumulative_weight(); });
        }

        template <class T>
        void DSEbppsC(DataChunk &args, ExpressionState &, Vector &result)
        {
            DSSampleSketchFunction<DSEbppsTraits<T>, double>(args, result, [](const DSEbppsSketch<T> &sketch) { return sketch.get_c(); });
        }

        // The sampled items and their adjusted weights, as a LIST(STRUCT(item, weight)).
        template <class T>
        void DSVarOptSamples(DataChunk &args, ExpressionState &, Vector &result)
        {
            DSSampleSketchFunction<DSVarOptTraits<T>, list_entry_t>(args, result, [&](const DSVarOptSketch<T> &sketch)
                                                                    { return DSAppendList(result, sketch.get_num_samples(), [&](Vector &child, idx_t offset)
                                                                                          {
                auto &entries = StructVector::GetEntries(child);
                auto &item_vector = entries[0];
                auto items = FlatVector::GetDataMutable<T>(item_vector);
                auto weights = FlatVector::GetDataMutable<double>(entries[1]);
                idx_t i = offset;
                for (auto sample : sketch)
                {
                    items[i] = DSItemOut<T>(item_vector, sample.first);
                    weights[i] = sample.second;
                    i++;
                } }); });
        }

        // The sampled items, as a LIST: EBPPS samples have no per-item weights.
        template <class T>
        void DSEbppsSamples(DataChunk &args, ExpressionState &, Vector &result)
        {
            DSSampleSketchFunction<DSEbppsTraits<T>, list_entry_t>(args, result, [&](const DSEbppsSketch<T> &sketch)
                                                                   {
                auto samples = sketch.get_result();
                return DSAppendList(result, samples.size(), [&](Vector &child, idx_t offset)
                                    {
                    auto items = FlatVector::GetDataMutable<T>(child);
                    for (idx_t i = 0; i < samples.size(); i++)
                    {
                        items[offset + i] = DSItemOut<T>(child, samples[i]);
                    } }); });
        }

        // The estimated total weight of the items in a list, with bounds, from a VarOpt sample.
        template <class T>
        void DSVarOptSubsetSum(DataChunk &args, ExpressionState &, Vector &result)
        {
            auto &sketch_vector = args.data[0];
            auto &items_vector = args.data[1];
            UnifiedVectorFormat items_children;
            ListVector::GetChild(items_vector).ToUnifiedFormat(items_children);
            auto items_data = UnifiedVectorFormat::GetData<T>(items_children);

            UnifiedVectorFormat sketches, lists;
            sketch_vector.ToUnifiedFormat(sketches);
            items_vector.ToUnifiedFormat(lists);
            auto sketch_data = UnifiedVectorFormat::GetData<string_t>(sketches);
            auto list_data = UnifiedVectorFormat::GetData<list_entry_t>(lists);

            auto &fields = StructVector::GetEntries(result);
            auto lower = FlatVector::GetDataMutable<double>(fields[0]);
            auto estimate = FlatVector::GetDataMutable<double>(fields[1]);
            auto upper = FlatVector::GetDataMutable<double>(fields[2]);
            auto total = FlatVector::GetDataMutable<double>(fields[3]);

            DSSketchCache<DSVarOptSketch<T>> cache;
            for (idx_t row = 0; row < args.size(); row++)
            {
                auto s = sketches.sel->get_index(row);
                auto l = lists.sel->get_index(row);
                if (!sketches.validity.RowIsValid(s) || !lists.validity.RowIsValid(l))
                {
                    FlatVector::SetNull(result, row, true);
                    continue;
                }
                auto &list = list_data[l];
                std::unordered_set<DSItem<T>> subset;
                for (idx_t i = 0; i < list.length; i++)
                {
                    auto index = items_children.sel->get_index(list.offset + i);
                    if (items_children.validity.RowIsValid(index))
                    {
                        subset.insert(DSItemIn(items_data[index]));
                    }
                }
                auto summary = cache.Get(sketch_data[s], "VarOpt").estimate_subset_sum([&](const DSItem<T> &item)
                                                                                       { return subset.count(item) > 0; });
                lower[row] = summary.lower_bound;
                estimate[row] = summary.estimate;
                upper[row] = summary.upper_bound;
                total[row] = summary.total_sketch_weight;
            }
            if (args.AllConstant())
            {
                result.SetVectorType(VectorType::CONSTANT_VECTOR);
            }
        }

        // ============================================================
        // 4. Registration
        // ============================================================
        // Registers the aggregates and the scalar functions both sketches have; `register_extra(T(), item_type,
        // sketch_type, add)` adds the sketch's own scalar functions through add(name, function, return_type,
        // extra_arguments, parameter_names, description, example_call).
        template <template <class> class TRAITS, class EXTRA>
        void DSRegisterSampleSketch(ExtensionLoader &loader, const string &type_description, EXTRA &&register_extra)
        {
            using NAMES = TRAITS<int64_t>;
            const string sql = NAMES::SQL;
            const string name = "datasketch_" + sql;
            const vector<string> categories = {"sketch", sql};
            const string build = name + "(16, i::DOUBLE)";
            const string sketch_source = "(SELECT " + build + " AS sketch FROM range(1000) t(i)) s";
            const string partials = "(SELECT " + build + " AS sketch FROM range(1000) t(i) GROUP BY i % 4) s";

            AggregateFunctionSet aggregates {Identifier(name)};
            AggregateFunctionSet weighted_aggregates {Identifier(name + "_weighted")};
            vector<FunctionDescription> aggregate_descs, weighted_descs;
            std::map<string, std::pair<ScalarFunctionSet, vector<FunctionDescription>>> scalars;

            auto add_scalar = [&](const string &function, scalar_function_t fun, const LogicalType &sketch_type, const LogicalType &return_type,
                                  vector<LogicalType> extra_arguments, vector<string> parameter_names, const string &description,
                                  const string &example_call)
            {
                auto full_name = name + "_" + function;
                auto entry = scalars.find(full_name);
                if (entry == scalars.end())
                {
                    entry = scalars.emplace(full_name, std::make_pair(ScalarFunctionSet {Identifier(full_name)}, vector<FunctionDescription>())).first;
                }
                vector<LogicalType> arguments = {sketch_type};
                arguments.insert(arguments.end(), extra_arguments.begin(), extra_arguments.end());
                parameter_names.insert(parameter_names.begin(), "sketch");
                DSAddScalar(entry->second.first, entry->second.second, ScalarFunction(arguments, return_type, std::move(fun)),
                            std::move(parameter_names), description,
                            {"SELECT " + full_name + "(" + example_call + ") FROM " + sketch_source});
            };

            DSSampleForEachItemType([&](auto item, const LogicalType &item_type)
                                    {
                using T = decltype(item);
                using TR = TRAITS<T>;
                using STATE = DSSampleState<TR>;
                auto sketch_type = DSCreateSketchType(loader, "sketch_" + sql + "_" + StringUtil::Lower(item_type.ToString()),
                                                      type_description + " with embedded type " + item_type.ToString());

                using OP = DSSampleOperation<TR>;
                auto fun = AggregateFunction::UnaryAggregate<STATE, T, string_t, OP, AggregateDestructorType::LEGACY>(item_type, sketch_type);
                DSReadDataArgumentAt<1, STATE, T, OP>(fun);
                DSAddAggregate(aggregates, aggregate_descs, fun, DSSampleBind<TR>, {LogicalType::INTEGER, item_type}, {"k", "data"},
                               "Creates a sketch_" + sql + " sample of at most K of the values, each of weight 1",
                               {"SELECT " + build + " FROM range(1000) t(i)"});

                using MERGE = DSSampleMergeOperation<TR>;
                auto merge = AggregateFunction::UnaryAggregate<STATE, string_t, string_t, MERGE, AggregateDestructorType::LEGACY>(sketch_type, sketch_type);
                DSReadDataArgumentAt<1, STATE, string_t, MERGE>(merge);
                DSAddAggregate(aggregates, aggregate_descs, merge, DSSampleBind<TR>, {LogicalType::INTEGER, sketch_type}, {"k", "sketch"},
                               "Merges sketch_" + sql + " samples into one sample of at most K items",
                               {"SELECT " + name + "(16, sketch) FROM " + partials});

                using WEIGHTED = DSSampleWeightedOperation<TR>;
                auto weighted = AggregateFunction::BinaryAggregate<STATE, T, double, string_t, WEIGHTED, AggregateDestructorType::LEGACY>(item_type, LogicalType::DOUBLE, sketch_type);
                weighted.SetStateDestructorCallback(AggregateFunction::StateDestroy<STATE, WEIGHTED>);
                DSReadDataArgumentsAt<1, 2, STATE, T, double, WEIGHTED>(weighted);
                DSAddAggregate(weighted_aggregates, weighted_descs, weighted, DSSampleBind<TR>, {LogicalType::INTEGER, item_type, LogicalType::DOUBLE},
                               {"k", "data", "weight"},
                               "Creates a sketch_" + sql + " sample of at most K of the values, sampled in proportion to their weights",
                               {"SELECT " + name + "_weighted(16, i::DOUBLE, (i % 10 + 1)::DOUBLE) FROM range(1000) t(i)"});

                add_scalar("k", DSSampleK<TR>, sketch_type, LogicalType::UINTEGER, {}, {}, "Returns the sample's maximum size K", "sketch");
                add_scalar("n", DSSampleN<TR>, sketch_type, LogicalType::UBIGINT, {}, {}, "Returns the number of items the sketch has seen", "sketch");
                add_scalar("is_empty", DSSampleIsEmpty<TR>, sketch_type, LogicalType::BOOLEAN, {}, {}, "Returns true if the sketch has seen no items", "sketch");
                add_scalar("describe", DSSampleDescribe<TR>, sketch_type, LogicalType::VARCHAR, {}, {}, "Returns a description of the sketch", "sketch");
                register_extra(item, item_type, sketch_type, add_scalar); });

            DSRegisterAggregates(loader, std::move(aggregates), std::move(aggregate_descs), categories);
            DSRegisterAggregates(loader, std::move(weighted_aggregates), std::move(weighted_descs), categories);
            for (auto &entry : scalars)
            {
                DSRegisterScalars(loader, std::move(entry.second.first), std::move(entry.second.second), categories);
            }
        }
    } // namespace

    void LoadSamplingSketches(ExtensionLoader &loader)
    {
        DSRegisterSampleSketch<DSVarOptTraits>(
            loader, "Sketch type for VarOpt samples",
            [](auto item, const LogicalType &item_type, const LogicalType &sketch_type, auto &add)
            {
                using T = decltype(item);
                add("num_samples", DSVarOptNumSamples<T>, sketch_type, LogicalType::UINTEGER, {}, {},
                    "Returns the number of items in the sample (at most K)", "sketch");
                child_list_t<LogicalType> sample_fields = {{"item", item_type}, {"weight", LogicalType::DOUBLE}};
                add("samples", DSVarOptSamples<T>, sketch_type, LogicalType::LIST(LogicalType::STRUCT(sample_fields)), {}, {},
                    "Returns the sampled items with their adjusted weights. The total weight of the samples matching a "
                    "condition estimates the total weight of all items matching it",
                    "sketch");
                child_list_t<LogicalType> sum_fields = {{"lower_bound", LogicalType::DOUBLE},
                                                        {"estimate", LogicalType::DOUBLE},
                                                        {"upper_bound", LogicalType::DOUBLE},
                                                        {"total_weight", LogicalType::DOUBLE}};
                add("subset_sum", DSVarOptSubsetSum<T>, sketch_type, LogicalType::STRUCT(sum_fields), {LogicalType::LIST(item_type)},
                    {"items"},
                    "Returns the estimated total weight of the items in a list, with lower and upper bounds (at 2 standard "
                    "deviations), and the total weight of every item",
                    "sketch, [1.0, 2.0, 3.0]");
            });

        DSRegisterSampleSketch<DSEbppsTraits>(
            loader, "Sketch type for EBPPS samples",
            [](auto item, const LogicalType &item_type, const LogicalType &sketch_type, auto &add)
            {
                using T = decltype(item);
                add("cumulative_weight", DSEbppsCumulativeWeight<T>, sketch_type, LogicalType::DOUBLE, {}, {},
                    "Returns the total weight of the items the sketch has seen", "sketch");
                add("c", DSEbppsC<T>, sketch_type, LogicalType::DOUBLE, {}, {},
                    "Returns the expected sample size, C (at most K): the sample holds floor(C) or ceil(C) items", "sketch");
                add("samples", DSEbppsSamples<T>, sketch_type, LogicalType::LIST(item_type), {}, {},
                    "Returns the sampled items. Each item was included with probability proportional to its weight", "sketch");
            });
    }
} // namespace duckdb
