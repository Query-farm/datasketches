// The Density sketch: a sample of multi-dimensional points that estimates the density (Gaussian kernel) at any
// point.

#include "datasketches_generated_common.hpp"

#include <DataSketches/density_sketch.hpp>

namespace duckdb
{
    namespace
    {
        // The library's Gaussian kernel only takes std::vector<double>, so this sketch keeps std::allocator.
        using DensitySketch = datasketches::density_sketch<double>;
        using DensityPoint = std::vector<double>;

        const vector<string> DS_DENSITY_CATEGORIES = {"sketch", "density"};

        const DSExampleSources &DSDensityExamples()
        {
            static const DSExampleSources examples("datasketch_density(16, [i % 10, i % 7]::DOUBLE[])");
            return examples;
        }

        // Reads one LIST(DOUBLE) value as a point. NULL coordinates have no place in a point.
        DensityPoint DSDensityPoint(const list_entry_t &list, const UnifiedVectorFormat &children, const double *values,
                                    const char *function_name)
        {
            DensityPoint point(list.length);
            for (idx_t i = 0; i < list.length; i++)
            {
                auto index = children.sel->get_index(list.offset + i);
                if (!children.validity.RowIsValid(index))
                {
                    throw InvalidInputException("%s: point coordinates cannot be NULL", function_name);
                }
                point[i] = values[index];
            }
            return point;
        }

        // Runs fun, reporting library errors (e.g. a point of the wrong dimension) as invalid input.
        template <class FUNC>
        auto DSDensityGuard(const char *function_name, FUNC &&fun) -> decltype(fun())
        {
            try
            {
                return fun();
            }
            catch (Exception &)
            {
                throw;
            }
            catch (std::exception &e)
            {
                throw InvalidInputException("%s: %s", function_name, e.what());
            }
        }

        // ============================================================
        // 1. Bind Data
        // ============================================================
        struct DSDensityBindData : public FunctionData
        {
            explicit DSDensityBindData(uint16_t k) : k(k) {}
            unique_ptr<FunctionData> Copy() const override { return make_uniq<DSDensityBindData>(k); }
            bool Equals(const FunctionData &other_p) const override { return k == other_p.Cast<DSDensityBindData>().k; }
            uint16_t k;
        };

        unique_ptr<FunctionData> DSDensityBind(BindAggregateFunctionInput &input)
        {
            auto k = DSBindConstant(input.GetClientContext(), *input.GetArguments()[0], "Density can only take a constant K value",
                                    "Density K value cannot be NULL")
                         .GetValue<int64_t>();
            if (k < 2 || k > 65535)
            {
                throw BinderException("Density K value must be between 2 and 65535, got: %lld", (long long)k);
            }
            return make_uniq<DSDensityBindData>(static_cast<uint16_t>(k));
        }

        // ============================================================
        // 2. State & Operations
        // ============================================================
        struct DSDensityState
        {
            DensitySketch *sketch = nullptr;
            ~DSDensityState()
            {
                delete sketch;
            }
        };

        // Shared by the build and merge aggregates. A sketch takes its dimension from its first point (or sketch).
        // With no input rows, the result is NULL.
        struct DSDensityOperationBase
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
                        target.sketch = new DensitySketch(*source.sketch);
                    }
                    return;
                }
                DSDensityGuard("datasketch_density", [&]()
                               {
                    if (destructive) {
                        target.sketch->merge(std::move(*const_cast<STATE &>(source).sketch));
                    } else {
                        target.sketch->merge(*source.sketch);
                    } });
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

        // Points are LIST(DOUBLE) values, which the unary executor cannot hand over with their elements, so the build
        // aggregate has its own update callback (DSDensityUpdate); this operation provides the others.
        struct DSDensityBuildOperation : DSDensityOperationBase
        {
            template <class A_TYPE, class STATE, class OP>
            static void Operation(STATE &, const A_TYPE &, AggregateUnaryInput &)
            {
                throw InternalException("datasketch_density: points are read by DSDensityUpdate");
            }
        };

        // The build aggregate's update: inputs are (k, point).
        void DSDensityUpdate(Vector inputs[], AggregateInputData &aggr_input_data, idx_t input_count, Vector &states, idx_t count)
        {
            D_ASSERT(input_count == 2);
            auto k = aggr_input_data.bind_data->Cast<DSDensityBindData>().k;
            auto &points = inputs[1];
            UnifiedVectorFormat point_format, child_format, state_format;
            points.ToUnifiedFormat(point_format);
            ListVector::GetChild(points).ToUnifiedFormat(child_format);
            states.ToUnifiedFormat(state_format);
            auto lists = UnifiedVectorFormat::GetData<list_entry_t>(point_format);
            auto values = UnifiedVectorFormat::GetData<double>(child_format);
            auto state_pointers = UnifiedVectorFormat::GetData<DSDensityState *>(state_format);
            for (idx_t row = 0; row < count; row++)
            {
                auto index = point_format.sel->get_index(row);
                if (!point_format.validity.RowIsValid(index))
                {
                    continue;
                }
                auto &state = *state_pointers[state_format.sel->get_index(row)];
                auto point = DSDensityPoint(lists[index], child_format, values, "datasketch_density");
                DSDensityGuard("datasketch_density", [&]()
                               {
                    if (!state.sketch) {
                        state.sketch = new DensitySketch(k, static_cast<uint32_t>(point.size()));
                    }
                    state.sketch->update(std::move(point)); });
            }
        }

        struct DSDensityMergeOperation : DSDensityOperationBase
        {
            template <class A_TYPE, class STATE, class OP>
            static void Operation(STATE &state, const A_TYPE &input, AggregateUnaryInput &idata)
            {
                auto incoming = DSDeserialize<DensitySketch>(input, "Density");
                if (incoming.is_empty())
                {
                    return;
                }
                if (!state.sketch)
                {
                    auto k = idata.input.bind_data->template Cast<DSDensityBindData>().k;
                    state.sketch = new DensitySketch(k, incoming.get_dim());
                }
                DSDensityGuard("datasketch_density", [&]() { state.sketch->merge(std::move(incoming)); });
            }
        };

        // ============================================================
        // 3. Scalar Functions
        // ============================================================
        template <class RESULT_TYPE, class FUNC>
        void DSDensitySketchFunction(DataChunk &args, Vector &result, FUNC &&fun)
        {
            DSSketchCache<DensitySketch> cache;
            UnaryExecutor::Execute<string_t, RESULT_TYPE>(args.data[0], result, [&](string_t blob)
                                                          { return static_cast<RESULT_TYPE>(fun(cache.Get(blob, "Density"))); });
        }

        void DSDensityK(DataChunk &args, ExpressionState &, Vector &result)
        {
            DSDensitySketchFunction<uint16_t>(args, result, [](const DensitySketch &sketch) { return sketch.get_k(); });
        }

        void DSDensityDim(DataChunk &args, ExpressionState &, Vector &result)
        {
            DSDensitySketchFunction<uint32_t>(args, result, [](const DensitySketch &sketch) { return sketch.get_dim(); });
        }

        void DSDensityN(DataChunk &args, ExpressionState &, Vector &result)
        {
            DSDensitySketchFunction<uint64_t>(args, result, [](const DensitySketch &sketch) { return sketch.get_n(); });
        }

        void DSDensityNumRetained(DataChunk &args, ExpressionState &, Vector &result)
        {
            DSDensitySketchFunction<uint32_t>(args, result, [](const DensitySketch &sketch) { return sketch.get_num_retained(); });
        }

        void DSDensityIsEmpty(DataChunk &args, ExpressionState &, Vector &result)
        {
            DSDensitySketchFunction<bool>(args, result, [](const DensitySketch &sketch) { return sketch.is_empty(); });
        }

        void DSDensityIsEstimationMode(DataChunk &args, ExpressionState &, Vector &result)
        {
            DSDensitySketchFunction<bool>(args, result, [](const DensitySketch &sketch) { return sketch.is_estimation_mode(); });
        }

        void DSDensityDescribe(DataChunk &args, ExpressionState &, Vector &result)
        {
            DSDensitySketchFunction<string_t>(args, result, [&](const DensitySketch &sketch)
                                              { return DSAddText(result, sketch.to_string()); });
        }

        // The estimated density at a point.
        void DSDensityEstimate(DataChunk &args, ExpressionState &, Vector &result)
        {
            UnifiedVectorFormat sketch_format, point_format, child_format;
            args.data[0].ToUnifiedFormat(sketch_format);
            args.data[1].ToUnifiedFormat(point_format);
            ListVector::GetChild(args.data[1]).ToUnifiedFormat(child_format);
            auto blobs = UnifiedVectorFormat::GetData<string_t>(sketch_format);
            auto lists = UnifiedVectorFormat::GetData<list_entry_t>(point_format);
            auto values = UnifiedVectorFormat::GetData<double>(child_format);
            auto result_data = FlatVector::GetDataMutable<double>(result);

            DSSketchCache<DensitySketch> cache;
            for (idx_t row = 0; row < args.size(); row++)
            {
                auto s = sketch_format.sel->get_index(row);
                auto p = point_format.sel->get_index(row);
                if (!sketch_format.validity.RowIsValid(s) || !point_format.validity.RowIsValid(p))
                {
                    FlatVector::SetNull(result, row, true);
                    continue;
                }
                auto &sketch = cache.Get(blobs[s], "Density");
                auto point = DSDensityPoint(lists[p], child_format, values, "datasketch_density_estimate");
                if (point.size() != sketch.get_dim())
                {
                    throw InvalidInputException("datasketch_density_estimate: the point has %llu dimensions, the sketch %u",
                                                (unsigned long long)point.size(), sketch.get_dim());
                }
                result_data[row] = DSDensityGuard("datasketch_density_estimate", [&]() { return sketch.get_estimate(point); });
            }
            if (args.AllConstant())
            {
                result.SetVectorType(VectorType::CONSTANT_VECTOR);
            }
        }
    } // namespace

    // ============================================================
    // 4. Main Loader
    // ============================================================
    void LoadDensitySketch(ExtensionLoader &loader)
    {
        auto sketch_type = DSCreateSketchType(loader, "sketch_density", "Sketch type for Density sketches");
        const auto &ex = DSDensityExamples();
        auto point_type = LogicalType::LIST(LogicalType::DOUBLE);

        // --- datasketch_density(k, point), and datasketch_density(k, sketch) to merge. K stays in the argument list
        // (see DSReadDataArgumentAt); the data follows it.
        {
            AggregateFunctionSet set("datasketch_density");
            vector<FunctionDescription> descs;
            auto fun = AggregateFunction::UnaryAggregate<DSDensityState, list_entry_t, string_t, DSDensityBuildOperation, AggregateDestructorType::LEGACY>(point_type, sketch_type);
            fun.SetStateUpdateCallback(DSDensityUpdate);
            DSAddAggregate(set, descs, fun, DSDensityBind, {LogicalType::INTEGER, point_type}, {"k", "point"},
                           "Creates a Density sketch of multi-dimensional points (DOUBLE lists, all of the same length), which "
                           "estimates the density at any point with a Gaussian kernel. Larger K is more accurate",
                           {"SELECT datasketch_density(16, [i % 10, i % 7]::DOUBLE[]) FROM range(1000) t(i)"});
            auto merge = AggregateFunction::UnaryAggregate<DSDensityState, string_t, string_t, DSDensityMergeOperation, AggregateDestructorType::LEGACY>(sketch_type, sketch_type);
            DSReadDataArgumentAt<1, DSDensityState, string_t, DSDensityMergeOperation>(merge);
            DSAddAggregate(set, descs, merge, DSDensityBind, {LogicalType::INTEGER, sketch_type}, {"k", "sketch"},
                           "Merges Density sketches of points of the same dimension",
                           {ex.QueryPartials("datasketch_density(16, sketch)")});
            DSRegisterAggregates(loader, std::move(set), std::move(descs), DS_DENSITY_CATEGORIES);
        }

        DSRegisterScalar(loader, ScalarFunction("datasketch_density_estimate", {sketch_type, point_type}, LogicalType::DOUBLE, DSDensityEstimate),
                         DSDescription("Returns the estimated density at a point: the average of a Gaussian kernel, exp(-squared "
                                       "distance), between the point and every point the sketch has seen",
                                       {ex.Query("datasketch_density_estimate(sketch, [5.0, 3.0])")}, {"sketch", "point"},
                                       {sketch_type, point_type}),
                         DS_DENSITY_CATEGORIES);

        auto register_sketch_function = [&](const string &name, const LogicalType &return_type, scalar_function_t function,
                                            const string &description)
        {
            DSRegisterScalar(loader, ScalarFunction(Identifier(name), {sketch_type}, return_type, std::move(function)),
                             DSDescription(description, {ex.Query(name + "(sketch)")}, {"sketch"}, {sketch_type}), DS_DENSITY_CATEGORIES);
        };
        register_sketch_function("datasketch_density_k", LogicalType::USMALLINT, DSDensityK, "Returns the value of K for this sketch");
        register_sketch_function("datasketch_density_dim", LogicalType::UINTEGER, DSDensityDim,
                                 "Returns the number of dimensions of the sketch's points");
        register_sketch_function("datasketch_density_n", LogicalType::UBIGINT, DSDensityN,
                                 "Returns the number of points the sketch has seen");
        register_sketch_function("datasketch_density_num_retained", LogicalType::UINTEGER, DSDensityNumRetained,
                                 "Returns the number of points the sketch retains");
        register_sketch_function("datasketch_density_is_empty", LogicalType::BOOLEAN, DSDensityIsEmpty,
                                 "Returns true if the sketch has seen no points");
        register_sketch_function("datasketch_density_is_estimation_mode", LogicalType::BOOLEAN, DSDensityIsEstimationMode,
                                 "Returns true if the sketch has dropped points (its estimates are then approximate)");
        register_sketch_function("datasketch_density_describe", LogicalType::VARCHAR, DSDensityDescribe,
                                 "Returns a description of the sketch");
    }
} // namespace duckdb
