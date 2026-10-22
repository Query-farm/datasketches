// Tuple sketches with a DOUBLE summary per key (sketch_tuple_double).
//
// A Tuple sketch is a Theta sketch whose retained keys carry a summary. Here the summary is a DOUBLE combined by
// a mode: sum (default), min, max, or one (always 1). The mode is not stored in the sketch (as in the other
// DataSketches libraries), so functions that combine summaries (merge, union, intersection) take it as an option.

#include "datasketches_tuple_common.hpp"

#include <DataSketches/tuple_sketch.hpp>
#include <DataSketches/tuple_union.hpp>
#include <DataSketches/tuple_intersection.hpp>
#include <DataSketches/tuple_a_not_b.hpp>

#include <limits>

namespace duckdb
{
    namespace
    {
        enum class DSTupleMode : uint8_t
        {
            SUM,
            MIN,
            MAX,
            ONE
        };

        DSTupleMode DSTupleParseMode(const string &mode)
        {
            auto lower = StringUtil::Lower(mode);
            if (lower == "sum")
                return DSTupleMode::SUM;
            if (lower == "min")
                return DSTupleMode::MIN;
            if (lower == "max")
                return DSTupleMode::MAX;
            if (lower == "one")
                return DSTupleMode::ONE;
            throw InvalidInputException("Tuple sketch mode must be 'sum', 'min', 'max' or 'one', got '%s'", mode);
        }

        // Creates, updates and combines the DOUBLE summaries according to the mode.
        struct DSTupleDoublePolicy
        {
            DSTupleDoublePolicy() = default;
            explicit DSTupleDoublePolicy(DSTupleMode mode) : mode(mode) {}

            double create() const
            {
                switch (mode)
                {
                case DSTupleMode::MIN:
                    return std::numeric_limits<double>::infinity();
                case DSTupleMode::MAX:
                    return -std::numeric_limits<double>::infinity();
                default:
                    return 0.0;
                }
            }

            void update(double &summary, const double &value) const
            {
                (*this)(summary, value);
            }

            void operator()(double &summary, const double &other) const
            {
                switch (mode)
                {
                case DSTupleMode::SUM:
                    summary += other;
                    break;
                case DSTupleMode::MIN:
                    summary = MinValue(summary, other);
                    break;
                case DSTupleMode::MAX:
                    summary = MaxValue(summary, other);
                    break;
                case DSTupleMode::ONE:
                    summary = 1.0;
                    break;
                }
            }

            DSTupleMode mode = DSTupleMode::SUM;
        };

        using DSTupleUpdate = datasketches::update_tuple_sketch<double, double, DSTupleDoublePolicy>;
        using DSTupleCompact = datasketches::compact_tuple_sketch<double>;
        using DSTupleUnion = datasketches::tuple_union<double, DSTupleDoublePolicy>;
        using DSTupleIntersection = datasketches::tuple_intersection<double, DSTupleDoublePolicy>;
        using DSTupleANotB = datasketches::tuple_a_not_b<double>;

        struct DSTupleDoubleTraits
        {
            using Compact = DSTupleCompact;
            static constexpr const char *KIND = "Tuple";
            static constexpr const char *PREFIX = "datasketch_tuple";

            static Compact Deserialize(const string_t &blob)
            {
                auto seed = DSTupleSeedFor(blob);
                return ThetaGuard([&]()
                                  { return Compact::deserialize(blob.GetData(), blob.GetSize(), seed); },
                                  KIND);
            }

            static string_t Serialize(Vector &result, const Compact &sketch)
            {
                auto bytes = sketch.serialize();
                return StringVector::AddStringOrBlob(result, reinterpret_cast<const char *>(bytes.data()), bytes.size());
            }

            static Compact ANotB(uint64_t seed, const Compact &a, const Compact &b)
            {
                return DSTupleANotB(seed).compute(a, b);
            }

            static void CheckCompatible(const Compact &, const Compact &, const char *) {}
        };

        using DSTupleScalars = DSTupleFunctions<DSTupleDoubleTraits>;

        DSTupleCompact DSTupleEmptySketch(uint8_t lg_k, uint64_t seed)
        {
            DSTupleUpdate::builder b;
            b.set_lg_k(lg_k);
            b.set_seed(seed);
            return b.build().compact();
        }

        // ============================================================
        // Bind Data
        // ============================================================

        struct DSTupleBindData : public FunctionData
        {
            DSTupleBindData() = default;

            unique_ptr<FunctionData> Copy() const override
            {
                return make_uniq<DSTupleBindData>(*this);
            }

            bool Equals(const FunctionData &other_p) const override
            {
                auto &other = other_p.Cast<DSTupleBindData>();
                return lg_k == other.lg_k && infer_lg_k == other.infer_lg_k && seed == other.seed && p == other.p && mode == other.mode;
            }

            uint8_t lg_k = DS_THETA_DEFAULT_LG_K;
            bool infer_lg_k = false;
            uint64_t seed = DS_THETA_DEFAULT_SEED;
            float p = 1.0f;
            DSTupleMode mode = DSTupleMode::SUM;
        };

        DSTupleMode DSTupleBindMode(BindAggregateFunctionInput &input)
        {
            return DSTupleParseMode(input.GetNonNullConstant(Identifier("mode")).GetValue<string>());
        }

        template <bool HAS_LG_K>
        unique_ptr<FunctionData> DSTupleBindBuild(BindAggregateFunctionInput &input)
        {
            auto result = make_uniq<DSTupleBindData>();
            if (HAS_LG_K)
            {
                result->lg_k = DSTupleBindLgK(input, 0);
            }
            DSTupleBindSeedAndP(input, result->seed, result->p);
            result->mode = DSTupleBindMode(input);
            return std::move(result);
        }

        template <bool HAS_LG_K>
        unique_ptr<FunctionData> DSTupleBindMerge(BindAggregateFunctionInput &input)
        {
            auto result = make_uniq<DSTupleBindData>();
            if (HAS_LG_K)
            {
                result->lg_k = DSTupleBindLgK(input, 0);
            }
            else
            {
                result->infer_lg_k = true;
            }
            result->mode = DSTupleBindMode(input);
            return std::move(result);
        }

        // ============================================================
        // Build / Union Aggregate
        // ============================================================

        struct DSTupleState
        {
            DSTupleUpdate *update_sketch = nullptr;
            DSTupleUnion *union_sketch = nullptr;
            uint8_t union_lg_k = 0;
            uint64_t union_seed = DS_THETA_DEFAULT_SEED;

            ~DSTupleState()
            {
                delete update_sketch;
                delete union_sketch;
            }

            void CreateUpdateSketch(const DSTupleBindData &bind_data)
            {
                if (!update_sketch)
                {
                    DSTupleUpdate::builder b{DSTupleDoublePolicy(bind_data.mode)};
                    b.set_lg_k(bind_data.lg_k);
                    b.set_seed(bind_data.seed);
                    b.set_p(bind_data.p);
                    update_sketch = new DSTupleUpdate(b.build());
                }
            }

            // Creates the union, or grows it to a larger lg_k (re-adding what it holds so far).
            void EnsureUnion(uint8_t lg_k, uint64_t seed, DSTupleMode mode)
            {
                if (union_sketch && lg_k <= union_lg_k)
                {
                    return;
                }
                if (union_sketch)
                {
                    seed = union_seed;
                }
                DSTupleUnion::builder b{DSTupleDoublePolicy(mode)};
                b.set_lg_k(lg_k);
                b.set_seed(seed);
                auto grown = new DSTupleUnion(b.build());
                if (union_sketch)
                {
                    grown->update(union_sketch->get_result());
                    delete union_sketch;
                }
                union_sketch = grown;
                union_lg_k = lg_k;
                union_seed = seed;
            }

            bool HasData() const
            {
                return update_sketch || union_sketch;
            }

            uint64_t Seed(const DSTupleBindData &bind_data) const
            {
                return union_sketch ? union_seed : bind_data.seed;
            }

            uint8_t LgK(const DSTupleBindData &bind_data) const
            {
                return union_sketch ? union_lg_k : bind_data.lg_k;
            }
        };

        void DSTupleThrowSeedMismatch(const char *function_name, uint16_t a, uint16_t b)
        {
            throw InvalidInputException("%s: cannot combine Tuple sketches built with different seeds (seed hashes %d and %d)", function_name, a, b);
        }

        struct DSTupleOperationBase
        {
            template <class STATE>
            static void Initialize(STATE &state)
            {
                state.update_sketch = nullptr;
                state.union_sketch = nullptr;
                state.union_lg_k = 0;
                state.union_seed = DS_THETA_DEFAULT_SEED;
            }

            template <class STATE>
            static void Destroy(STATE &state, AggregateInputData &)
            {
                delete state.update_sketch;
                delete state.union_sketch;
                state.update_sketch = nullptr;
                state.union_sketch = nullptr;
            }

            static bool IgnoreNull() { return true; }

            template <class STATE, class OP>
            static void Combine(const STATE &source, STATE &target, AggregateInputData &aggr_input_data)
            {
                if (!source.HasData())
                    return;
                auto &bind_data = aggr_input_data.bind_data->template Cast<DSTupleBindData>();
                auto source_seed = source.Seed(bind_data);
                if (target.HasData() && ThetaSeedHash(source_seed) != ThetaSeedHash(target.Seed(bind_data)))
                {
                    DSTupleThrowSeedMismatch("datasketch_tuple", ThetaSeedHash(target.Seed(bind_data)), ThetaSeedHash(source_seed));
                }
                auto lg_k = MaxValue(source.LgK(bind_data), target.LgK(bind_data));
                target.EnsureUnion(lg_k, target.HasData() ? target.Seed(bind_data) : source_seed, bind_data.mode);
                if (target.update_sketch)
                {
                    target.union_sketch->update(target.update_sketch->compact());
                    delete target.update_sketch;
                    target.update_sketch = nullptr;
                }
                if (source.update_sketch)
                    target.union_sketch->update(source.update_sketch->compact());
                if (source.union_sketch)
                    target.union_sketch->update(source.union_sketch->get_result());
            }

            template <class T, class STATE>
            static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data)
            {
                if (state.union_sketch)
                {
                    target = DSTupleDoubleTraits::Serialize(finalize_data.result, state.union_sketch->get_result());
                }
                else if (state.update_sketch)
                {
                    target = DSTupleDoubleTraits::Serialize(finalize_data.result, state.update_sketch->compact());
                }
                else
                {
                    auto &bind_data = finalize_data.input.bind_data->template Cast<DSTupleBindData>();
                    target = DSTupleDoubleTraits::Serialize(finalize_data.result, DSTupleEmptySketch(bind_data.lg_k, bind_data.seed));
                }
            }
        };

        struct DSTupleCreateOperation : DSTupleOperationBase
        {
            template <class KEY_TYPE, class VALUE_TYPE, class STATE, class OP>
            static void Operation(STATE &state, const KEY_TYPE &key, const VALUE_TYPE &value, AggregateBinaryInput &idata)
            {
                auto &bind_data = idata.input.bind_data->template Cast<DSTupleBindData>();
                state.CreateUpdateSketch(bind_data);
                if constexpr (std::is_same_v<KEY_TYPE, duckdb::string_t>)
                {
                    state.update_sketch->update(key.GetData(), key.GetSize(), value);
                }
                else
                {
                    state.update_sketch->update(key, value);
                }
            }
        };

        struct DSTupleMergeOperation : DSTupleOperationBase
        {
            template <class A_TYPE, class STATE, class OP>
            static void Operation(STATE &state, const A_TYPE &blob, AggregateUnaryInput &idata)
            {
                auto &bind_data = idata.input.bind_data->template Cast<DSTupleBindData>();
                auto sketch = DSTupleDoubleTraits::Deserialize(blob);
                if (sketch.is_empty())
                {
                    return;
                }
                if (state.union_sketch && sketch.get_seed_hash() != ThetaSeedHash(state.union_seed))
                {
                    DSTupleThrowSeedMismatch("datasketch_tuple", ThetaSeedHash(state.union_seed), sketch.get_seed_hash());
                }
                auto lg_k = bind_data.infer_lg_k ? MaxValue(bind_data.lg_k, ThetaInferLgK(sketch)) : bind_data.lg_k;
                state.EnsureUnion(lg_k, ThetaSeedForHash(sketch.get_seed_hash()), bind_data.mode);
                ThetaGuard([&]()
                           { state.union_sketch->update(sketch); },
                           "Tuple");
            }

            template <class INPUT_TYPE, class STATE, class OP>
            static void ConstantOperation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input, idx_t count)
            {
                // Merging the same sketch repeatedly is not idempotent for mode sum: merge it count times.
                for (idx_t i = 0; i < count; i++)
                {
                    Operation<INPUT_TYPE, STATE, OP>(state, input, unary_input);
                }
            }
        };

        // ============================================================
        // Intersection Aggregate
        // ============================================================

        struct DSTupleIntersectBindData : public FunctionData
        {
            explicit DSTupleIntersectBindData(DSTupleMode mode) : mode(mode) {}
            unique_ptr<FunctionData> Copy() const override { return make_uniq<DSTupleIntersectBindData>(mode); }
            bool Equals(const FunctionData &other) const override { return mode == other.Cast<DSTupleIntersectBindData>().mode; }
            DSTupleMode mode;
        };

        unique_ptr<FunctionData> DSTupleBindIntersect(BindAggregateFunctionInput &input)
        {
            return make_uniq<DSTupleIntersectBindData>(DSTupleBindMode(input));
        }

        struct DSTupleIntersectState
        {
            DSTupleIntersection *intersection = nullptr;
            uint64_t seed = DS_THETA_DEFAULT_SEED;
            bool saw_empty = false;

            ~DSTupleIntersectState()
            {
                delete intersection;
            }

            void Add(const DSTupleCompact &sketch, DSTupleMode mode)
            {
                if (sketch.is_empty())
                {
                    saw_empty = true;
                    return;
                }
                if (!intersection)
                {
                    seed = ThetaSeedForHash(sketch.get_seed_hash());
                    intersection = new DSTupleIntersection(seed, DSTupleDoublePolicy(mode));
                }
                else if (sketch.get_seed_hash() != ThetaSeedHash(seed))
                {
                    DSTupleThrowSeedMismatch("datasketch_tuple_intersect_agg", ThetaSeedHash(seed), sketch.get_seed_hash());
                }
                ThetaGuard([&]()
                           { intersection->update(sketch); },
                           "Tuple");
            }
        };

        struct DSTupleIntersectOperation
        {
            template <class STATE>
            static void Initialize(STATE &state)
            {
                state.intersection = nullptr;
                state.seed = DS_THETA_DEFAULT_SEED;
                state.saw_empty = false;
            }

            template <class STATE>
            static void Destroy(STATE &state, AggregateInputData &)
            {
                delete state.intersection;
                state.intersection = nullptr;
            }

            static bool IgnoreNull() { return true; }

            template <class A_TYPE, class STATE, class OP>
            static void Operation(STATE &state, const A_TYPE &blob, AggregateUnaryInput &idata)
            {
                auto &bind_data = idata.input.bind_data->template Cast<DSTupleIntersectBindData>();
                state.Add(DSTupleDoubleTraits::Deserialize(blob), bind_data.mode);
            }

            template <class INPUT_TYPE, class STATE, class OP>
            static void ConstantOperation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input, idx_t count)
            {
                for (idx_t i = 0; i < count; i++)
                {
                    Operation<INPUT_TYPE, STATE, OP>(state, input, unary_input);
                }
            }

            template <class STATE, class OP>
            static void Combine(const STATE &source, STATE &target, AggregateInputData &aggr_input_data)
            {
                target.saw_empty = target.saw_empty || source.saw_empty;
                if (source.intersection)
                {
                    auto &bind_data = aggr_input_data.bind_data->template Cast<DSTupleIntersectBindData>();
                    target.Add(source.intersection->get_result(), bind_data.mode);
                }
            }

            template <class T, class STATE>
            static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data)
            {
                if (state.saw_empty)
                {
                    target = DSTupleDoubleTraits::Serialize(finalize_data.result, DSTupleEmptySketch(DS_THETA_DEFAULT_LG_K, state.seed));
                }
                else if (state.intersection)
                {
                    target = DSTupleDoubleTraits::Serialize(finalize_data.result, state.intersection->get_result());
                }
                else
                {
                    finalize_data.ReturnNull();
                }
            }
        };

        // ============================================================
        // Scalar Functions
        // ============================================================

        DSTupleCompact DSTupleUnionOf(const vector<string_t> &blobs, optional_idx explicit_lg_k, DSTupleMode mode)
        {
            vector<DSTupleCompact> sketches;
            sketches.reserve(blobs.size());
            ThetaSeedTracker seeds("datasketch_tuple_union", "Tuple");
            uint8_t lg_k = DS_THETA_DEFAULT_LG_K;
            for (auto &blob : blobs)
            {
                sketches.push_back(DSTupleDoubleTraits::Deserialize(blob));
                seeds.Add(sketches.back());
                lg_k = MaxValue(lg_k, ThetaInferLgK(sketches.back()));
            }
            if (explicit_lg_k.IsValid())
            {
                lg_k = static_cast<uint8_t>(explicit_lg_k.GetIndex());
            }
            return ThetaGuard([&]()
                              {
                                  DSTupleUnion::builder b{DSTupleDoublePolicy(mode)};
                                  b.set_lg_k(lg_k);
                                  b.set_seed(seeds.Seed());
                                  auto union_obj = b.build();
                                  for (auto &sketch : sketches)
                                  {
                                      union_obj.update(sketch);
                                  }
                                  return union_obj.get_result(); },
                              "Tuple");
        }

        // The sketches are the columns [first, last); the mode is the last column.
        DSTupleMode DSTupleRowMode(DataChunk &args, const ThetaRowArgs &row_args)
        {
            return DSTupleParseMode(row_args.Get<string_t>(args.ColumnCount() - 1).GetString());
        }

        void DSTupleUnionFunction(DataChunk &args, ExpressionState &, Vector &result)
        {
            auto result_data = FlatVector::GetDataMutable<string_t>(result);
            ThetaForEachRow(args, result, 0, args.ColumnCount() - 1, [&](idx_t row, const ThetaRowArgs &row_args)
                            { result_data[row] = DSTupleDoubleTraits::Serialize(result, DSTupleUnionOf(row_args.sketches, optional_idx(), DSTupleRowMode(args, row_args))); });
        }

        void DSTupleUnionWithKFunction(DataChunk &args, ExpressionState &, Vector &result)
        {
            auto result_data = FlatVector::GetDataMutable<string_t>(result);
            ThetaForEachRow(args, result, 1, args.ColumnCount() - 1, [&](idx_t row, const ThetaRowArgs &row_args)
                            {
                                auto lg_k = row_args.Get<int32_t>(0);
                                ThetaCheckLgK(lg_k, "datasketch_tuple_union");
                                result_data[row] = DSTupleDoubleTraits::Serialize(result, DSTupleUnionOf(row_args.sketches, optional_idx(static_cast<idx_t>(lg_k)), DSTupleRowMode(args, row_args))); });
        }

        void DSTupleIntersectFunction(DataChunk &args, ExpressionState &, Vector &result)
        {
            auto result_data = FlatVector::GetDataMutable<string_t>(result);
            ThetaForEachRow(args, result, 0, args.ColumnCount() - 1, [&](idx_t row, const ThetaRowArgs &row_args)
                            {
                                auto mode = DSTupleRowMode(args, row_args);
                                vector<DSTupleCompact> sketches;
                                sketches.reserve(row_args.sketches.size());
                                ThetaSeedTracker seeds("datasketch_tuple_intersect", "Tuple");
                                for (auto &blob : row_args.sketches)
                                {
                                    sketches.push_back(DSTupleDoubleTraits::Deserialize(blob));
                                    seeds.Add(sketches.back());
                                }
                                auto intersected = ThetaGuard([&]()
                                                              {
                                                                  DSTupleIntersection intersection(seeds.Seed(), DSTupleDoublePolicy(mode));
                                                                  for (auto &sketch : sketches)
                                                                  {
                                                                      intersection.update(sketch);
                                                                  }
                                                                  return intersection.get_result(); },
                                                              "Tuple");
                                result_data[row] = DSTupleDoubleTraits::Serialize(result, intersected); });
        }

        void DSTupleSummaries(DataChunk &args, ExpressionState &, Vector &result)
        {
            idx_t offset = 0;
            ListVector::SetListSize(result, 0);
            ThetaForEachRow(args, result, 0, 1, [&](idx_t row, const ThetaRowArgs &row_args)
                            {
                                auto sketch = DSTupleDoubleTraits::Deserialize(row_args.sketches[0]);
                                auto count = sketch.get_num_retained();
                                ListVector::Reserve(result, offset + count);
                                auto &child = ListVector::GetChildMutable(result);
                                auto &entries = StructVector::GetEntries(child);
                                auto hashes = FlatVector::GetDataMutable<uint64_t>(entries[0]);
                                auto summaries = FlatVector::GetDataMutable<double>(entries[1]);
                                auto list_data = FlatVector::GetDataMutable<list_entry_t>(result);
                                list_data[row] = list_entry_t(offset, count);
                                for (const auto &entry : sketch)
                                {
                                    hashes[offset] = entry.first;
                                    summaries[offset] = entry.second;
                                    offset++;
                                }
                                ListVector::SetListSize(result, offset); });
        }

        void DSTupleSum(DataChunk &args, ExpressionState &, Vector &result)
        {
            DSTupleScalars::UnaryRead<double>(args, result, [](const DSTupleCompact &sketch)
                                              {
                                                  double sum = 0;
                                                  for (const auto &entry : sketch)
                                                  {
                                                      sum += entry.second;
                                                  }
                                                  return sum / sketch.get_theta(); });
        }

        void DSTupleMean(DataChunk &args, ExpressionState &, Vector &result)
        {
            auto result_data = FlatVector::GetDataMutable<double>(result);
            ThetaForEachRow(args, result, 0, 1, [&](idx_t row, const ThetaRowArgs &row_args)
                            {
                                auto sketch = DSTupleDoubleTraits::Deserialize(row_args.sketches[0]);
                                if (sketch.get_num_retained() == 0)
                                {
                                    FlatVector::SetNull(result, row, true);
                                    return;
                                }
                                double sum = 0;
                                for (const auto &entry : sketch)
                                {
                                    sum += entry.second;
                                }
                                result_data[row] = sum / sketch.get_num_retained(); });
        }

        // ============================================================
        // Registration
        // ============================================================

        const vector<string> &DSTupleCategories()
        {
            static const vector<string> categories = {"sketch", "tuple"};
            return categories;
        }

        FunctionDescription DSTupleDescription(const string &description, const vector<string> &examples, const vector<string> &names,
                                               const vector<LogicalType> &types)
        {
            return DSSketchDescription(description, examples, names, types, DSTupleCategories());
        }

        void DSTupleAddMode(FunctionSignature &signature)
        {
            signature.AddKeywordOnly("mode", LogicalType::VARCHAR, Value("sum"));
        }

        template <typename T>
        void RegisterTupleBuild(AggregateFunctionSet &set, vector<FunctionDescription> &descs, const LogicalType &key_type, const LogicalType &sketch_type)
        {
            for (bool with_lg_k : {false, true})
            {
                auto fun = AggregateFunction::BinaryAggregate<DSTupleState, T, double, string_t, DSTupleCreateOperation, AggregateDestructorType::LEGACY>(
                    key_type, LogicalType::DOUBLE, sketch_type);
                fun.SetStateDestructorCallback(AggregateFunction::StateDestroy<DSTupleState, DSTupleCreateOperation>);
                auto signature = DSTupleSignature(with_lg_k, {key_type, LogicalType::DOUBLE}, sketch_type);
                DSTupleAddMode(signature);
                DSTupleAddSeedAndP(signature);
                fun.GetSignature() = std::move(signature);
                if (with_lg_k)
                {
                    fun.SetBindCallback(DSTupleBindBuild<true>);
                    DSReadDataArgumentsAt<1, 2, DSTupleState, T, double, DSTupleCreateOperation>(fun);
                }
                else
                {
                    fun.SetBindCallback(DSTupleBindBuild<false>);
                    DSReadDataArgumentsAt<0, 1, DSTupleState, T, double, DSTupleCreateOperation>(fun);
                }
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                set.AddFunction(fun);

                const string options = "Options: mode := 'sum' (default), 'min', 'max' or 'one' combines the values of a key; seed := (hash seed; "
                                       "sketches can only be combined with sketches built with the same seed); p := sampling probability in (0, 1]";
                // Runnable examples over inline data
                const string rows = " FROM (SELECT i % 100 AS user_id, i::DOUBLE AS amount FROM range(1000) t(i)) t";
                if (with_lg_k)
                {
                    descs.push_back(DSTupleDescription("Creates a Tuple sketch with the given log2 of nominal entries (lg_k, 4-26): a Theta sketch of the "
                                                       "distinct keys where each retained key carries a DOUBLE summary of its values. " +
                                                           options,
                                                       {"SELECT datasketch_tuple(14, user_id, amount)" + rows, "SELECT datasketch_tuple(14, user_id, amount, mode := 'max', seed := 42)" + rows},
                                                       {"lg_k", "key", "value", "mode", "seed", "p"},
                                                       {LogicalType::INTEGER, key_type, LogicalType::DOUBLE, LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::DOUBLE}));
                }
                else
                {
                    descs.push_back(DSTupleDescription("Creates a Tuple sketch (lg_k 12): a Theta sketch of the distinct keys where each retained key "
                                                       "carries a DOUBLE summary of its values. " +
                                                           options,
                                                       {"SELECT datasketch_tuple(user_id, amount)" + rows, "SELECT datasketch_tuple(user_id, amount, mode := 'min')" + rows},
                                                       {"key", "value", "mode", "seed", "p"},
                                                       {key_type, LogicalType::DOUBLE, LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::DOUBLE}));
                }
            }
        }

        ScalarFunction DSTupleVariadic(ScalarFunction fun, const vector<string> &names, const LogicalType &sketch_type)
        {
            fun = ThetaVariadic(std::move(fun), names, sketch_type);
            DSTupleAddMode(fun.GetSignature());
            return fun;
        }
    }

    void LoadTupleSketch(ExtensionLoader &loader)
    {
        auto sketch_type = DSCreateSketchType(loader, "sketch_tuple_double", "Sketch type for Tuple Sketch with DOUBLE summaries");
        auto theta_type = LogicalType(LogicalTypeId::BLOB).WithAlias("sketch_theta");
        const DSExampleSources ex("datasketch_tuple(i % 100, i::DOUBLE)");

        // --- datasketch_tuple: build from (key, value), or merge (union) sketches
        {
            AggregateFunctionSet set("datasketch_tuple");
            vector<FunctionDescription> descs;
            RegisterTupleBuild<int8_t>(set, descs, LogicalType::TINYINT, sketch_type);
            RegisterTupleBuild<int16_t>(set, descs, LogicalType::SMALLINT, sketch_type);
            RegisterTupleBuild<int32_t>(set, descs, LogicalType::INTEGER, sketch_type);
            RegisterTupleBuild<int64_t>(set, descs, LogicalType::BIGINT, sketch_type);
            RegisterTupleBuild<uint8_t>(set, descs, LogicalType::UTINYINT, sketch_type);
            RegisterTupleBuild<uint16_t>(set, descs, LogicalType::USMALLINT, sketch_type);
            RegisterTupleBuild<uint32_t>(set, descs, LogicalType::UINTEGER, sketch_type);
            RegisterTupleBuild<uint64_t>(set, descs, LogicalType::UBIGINT, sketch_type);
            RegisterTupleBuild<float>(set, descs, LogicalType::FLOAT, sketch_type);
            RegisterTupleBuild<double>(set, descs, LogicalType::DOUBLE, sketch_type);
            RegisterTupleBuild<string_t>(set, descs, LogicalType::VARCHAR, sketch_type);

            for (bool with_lg_k : {false, true})
            {
                auto fun = AggregateFunction::UnaryAggregate<DSTupleState, string_t, string_t, DSTupleMergeOperation, AggregateDestructorType::LEGACY>(
                    sketch_type, sketch_type);
                auto signature = DSTupleSignature(with_lg_k, {sketch_type}, sketch_type);
                DSTupleAddMode(signature);
                fun.GetSignature() = std::move(signature);
                if (with_lg_k)
                {
                    fun.SetBindCallback(DSTupleBindMerge<true>);
                    DSReadDataArgumentAt<1, DSTupleState, string_t, DSTupleMergeOperation>(fun);
                    descs.push_back(DSTupleDescription("Merges (unions) Tuple sketches into one with the given log2 of nominal entries (lg_k); the "
                                                       "summaries of a key are combined by mode := 'sum' (default), 'min', 'max' or 'one'",
                                                       {ex.QueryPartials("datasketch_tuple(16, sketch)"), ex.QueryPartials("datasketch_tuple(16, sketch, mode := 'max')")},
                                                       {"lg_k", "sketch", "mode"}, {LogicalType::INTEGER, sketch_type, LogicalType::VARCHAR}));
                }
                else
                {
                    fun.SetBindCallback(DSTupleBindMerge<false>);
                    DSReadDataArgumentAt<0, DSTupleState, string_t, DSTupleMergeOperation>(fun);
                    descs.push_back(DSTupleDescription("Merges (unions) Tuple sketches into one, keeping as many entries as the largest input "
                                                       "(at least lg_k 12); the summaries of a key are combined by mode := 'sum' (default), 'min', 'max' or 'one'",
                                                       {ex.QueryPartials("datasketch_tuple(sketch)"), ex.QueryPartials("datasketch_tuple(sketch, mode := 'max')")},
                                                       {"sketch", "mode"}, {sketch_type, LogicalType::VARCHAR}));
                }
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                set.AddFunction(fun);
            }
            ThetaRegisterAggregate(loader, set, descs);
        }

        // --- datasketch_tuple_intersect_agg
        {
            AggregateFunctionSet set("datasketch_tuple_intersect_agg");
            auto fun = AggregateFunction::UnaryAggregate<DSTupleIntersectState, string_t, string_t, DSTupleIntersectOperation, AggregateDestructorType::LEGACY>(
                sketch_type, sketch_type);
            auto signature = DSTupleSignature(false, {sketch_type}, sketch_type);
            DSTupleAddMode(signature);
            fun.GetSignature() = std::move(signature);
            fun.SetBindCallback(DSTupleBindIntersect);
            DSReadDataArgumentAt<0, DSTupleIntersectState, string_t, DSTupleIntersectOperation>(fun);
            fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
            set.AddFunction(fun);
            vector<FunctionDescription> descs;
            descs.push_back(DSTupleDescription("Intersects Tuple sketches across rows: keeps the keys present in every sketch, combining their summaries "
                                               "by mode := 'sum' (default), 'min', 'max' or 'one'. NULL sketches are skipped; returns NULL if there are no sketches",
                                               {ex.QueryOverlapping("datasketch_tuple_intersect_agg(sketch)"), ex.QueryOverlapping("datasketch_tuple_intersect_agg(sketch, mode := 'min')")},
                                               {"sketch", "mode"}, {sketch_type, LogicalType::VARCHAR}));
            ThetaRegisterAggregate(loader, set, descs);
        }

        // --- set operations
        {
            ScalarFunctionSet set("datasketch_tuple_union");
            set.AddFunction(DSTupleVariadic(ScalarFunction("datasketch_tuple_union", {sketch_type, sketch_type}, sketch_type, DSTupleUnionFunction),
                                            {"sketch_a", "sketch_b"}, sketch_type));
            set.AddFunction(DSTupleVariadic(ScalarFunction("datasketch_tuple_union", {LogicalType::INTEGER, sketch_type, sketch_type}, sketch_type, DSTupleUnionWithKFunction),
                                            {"lg_k", "sketch_a", "sketch_b"}, sketch_type));
            ThetaRegisterScalar(loader, std::move(set),
                                {DSTupleDescription("Returns the union of two or more Tuple sketches, keeping as many entries as the largest input (at least lg_k 12); "
                                                    "the summaries of a key are combined by mode := 'sum' (default), 'min', 'max' or 'one'",
                                                    {ex.Query("datasketch_tuple_union(sketch_a, sketch_b)"), ex.Query("datasketch_tuple_union(sketch_a, sketch_b, sketch_c, mode := 'max')")},
                                                    {"sketch_a", "sketch_b", "mode"}, {sketch_type, sketch_type, LogicalType::VARCHAR}),
                                 DSTupleDescription("Returns the union of two or more Tuple sketches with the given log2 of nominal entries (lg_k, 4-26); "
                                                    "the summaries of a key are combined by mode := 'sum' (default), 'min', 'max' or 'one'",
                                                    {ex.Query("datasketch_tuple_union(16, sketch_a, sketch_b)")},
                                                    {"lg_k", "sketch_a", "sketch_b", "mode"}, {LogicalType::INTEGER, sketch_type, sketch_type, LogicalType::VARCHAR})});
        }
        ThetaRegisterScalar(loader, DSTupleVariadic(ScalarFunction("datasketch_tuple_intersect", {sketch_type, sketch_type}, sketch_type, DSTupleIntersectFunction), {"sketch_a", "sketch_b"}, sketch_type),
                            DSTupleDescription("Returns the intersection of two or more Tuple sketches: the keys present in every sketch, with their summaries "
                                               "combined by mode := 'sum' (default), 'min', 'max' or 'one'",
                                               {ex.Query("datasketch_tuple_intersect(sketch_a, sketch_b)"), ex.Query("datasketch_tuple_intersect(sketch_a, sketch_b, mode := 'min')")},
                                               {"sketch_a", "sketch_b", "mode"}, {sketch_type, sketch_type, LogicalType::VARCHAR}));

        // --- summaries
        {
            child_list_t<LogicalType> entry;
            entry.emplace_back("hash", LogicalType::UBIGINT);
            entry.emplace_back("summary", LogicalType::DOUBLE);
            ThetaRegisterScalar(loader, ScalarFunction("datasketch_tuple_summaries", {sketch_type}, LogicalType::LIST(LogicalType::STRUCT(std::move(entry))), DSTupleSummaries),
                                DSTupleDescription("Returns the retained entries of the Tuple sketch as a list of {hash, summary} structs (the key hash and its summary)",
                                                   {ex.Query("datasketch_tuple_summaries(sketch)")}, {"sketch"}, {sketch_type}));
        }
        ThetaRegisterScalar(loader, ScalarFunction("datasketch_tuple_sum", {sketch_type}, LogicalType::DOUBLE, DSTupleSum),
                            DSTupleDescription("Returns the estimated total of the summaries over all distinct keys: the sum of the retained summaries divided by "
                                               "theta (with mode 'sum', the estimated sum of the values; exact when the sketch is not in estimation mode)",
                                               {ex.Query("datasketch_tuple_sum(sketch)")}, {"sketch"}, {sketch_type}));
        ThetaRegisterScalar(loader, ScalarFunction("datasketch_tuple_mean", {sketch_type}, LogicalType::DOUBLE, DSTupleMean),
                            DSTupleDescription("Returns the mean summary per distinct key (the mean of the retained summaries), or NULL if the sketch retains no entries",
                                               {ex.Query("datasketch_tuple_mean(sketch)")}, {"sketch"}, {sketch_type}));

        DSTupleScalars::Register(loader, sketch_type, theta_type, "Tuple sketch", DSTupleCategories(), ex);
    }
}
