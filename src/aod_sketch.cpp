// Array-of-doubles Tuple sketches (sketch_aod).
//
// Each retained key carries a fixed-length array of DOUBLE values (1-255, stored in the sketch); values of the
// same key are summed element-wise when building, merging, unioning and intersecting.

#include "datasketches_tuple_common.hpp"

#include <DataSketches/array_of_doubles_sketch.hpp>

#include <ostream>

namespace datasketches
{
    // tuple_sketch::to_string needs to be able to print the summaries (it only does when asked for the items).
    inline std::ostream &operator<<(std::ostream &os, const array<double> &values)
    {
        os << "[";
        for (uint8_t i = 0; i < values.size(); i++)
        {
            os << (i ? ", " : "") << values[i];
        }
        return os << "]";
    }
} // namespace datasketches

namespace duckdb
{
    namespace
    {
        using DSAodArray = datasketches::array<double>;
        using DSAodUpdate = datasketches::update_array_of_doubles_sketch;
        using DSAodCompact = datasketches::compact_array_of_doubles_sketch;
        using DSAodUnion = datasketches::array_of_doubles_union;

        constexpr idx_t DS_AOD_MAX_VALUES = 255;

        // Sums the arrays of a key element-wise (intersection).
        struct DSAodSumPolicy
        {
            explicit DSAodSumPolicy(uint8_t num_values = 1) : num_values(num_values) {}
            void operator()(DSAodArray &array, const DSAodArray &other) const
            {
                for (uint8_t i = 0; i < num_values; i++)
                {
                    array[i] += other[i];
                }
            }
            uint8_t get_num_values() const { return num_values; }
            uint8_t num_values;
        };

        using DSAodIntersection = datasketches::array_of_doubles_intersection<DSAodSumPolicy>;

        void DSAodThrowNumValues(const char *function_name, idx_t a, idx_t b)
        {
            throw InvalidInputException("%s: cannot combine array-of-doubles sketches with different numbers of values (%llu and %llu)",
                                        function_name, a, b);
        }

        void DSAodThrowSeedMismatch(const char *function_name, uint16_t a, uint16_t b)
        {
            throw InvalidInputException("%s: cannot combine Array of doubles sketches built with different seeds (seed hashes %d and %d)", function_name, a, b);
        }

        struct DSAodTraits
        {
            using Compact = DSAodCompact;
            static constexpr const char *KIND = "Array of doubles";
            static constexpr const char *PREFIX = "datasketch_aod";

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
                return datasketches::array_of_doubles_a_not_b(seed).compute(a, b);
            }

            static void CheckCompatible(const Compact &a, const Compact &b, const char *function_name)
            {
                if (!a.is_empty() && !b.is_empty() && a.get_num_values() != b.get_num_values())
                {
                    DSAodThrowNumValues(function_name, a.get_num_values(), b.get_num_values());
                }
            }
        };

        using DSAodScalars = DSTupleFunctions<DSAodTraits>;

        DSAodCompact DSAodEmptySketch(uint8_t lg_k, uint64_t seed, uint8_t num_values)
        {
            DSAodUpdate::builder b{datasketches::default_array_of_doubles_update_policy(num_values)};
            b.set_lg_k(lg_k);
            b.set_seed(seed);
            return b.build().compact();
        }

        // Tracks the number of values of the sketches being combined (empty sketches are ignored).
        struct DSAodNumValuesTracker
        {
            explicit DSAodNumValuesTracker(const char *function_name) : function_name(function_name) {}

            void Add(const DSAodCompact &sketch)
            {
                if (num_values == 0)
                {
                    fallback = sketch.get_num_values();
                }
                if (sketch.is_empty())
                {
                    return;
                }
                if (num_values == 0)
                {
                    num_values = sketch.get_num_values();
                }
                else if (num_values != sketch.get_num_values())
                {
                    DSAodThrowNumValues(function_name, num_values, sketch.get_num_values());
                }
            }

            uint8_t NumValues() const
            {
                return num_values ? num_values : MaxValue<uint8_t>(fallback, 1);
            }

            const char *function_name;
            uint8_t num_values = 0;
            uint8_t fallback = 1;
        };

        // ============================================================
        // Bind Data
        // ============================================================

        struct DSAodBindData : public FunctionData
        {
            unique_ptr<FunctionData> Copy() const override
            {
                return make_uniq<DSAodBindData>(*this);
            }

            bool Equals(const FunctionData &other_p) const override
            {
                auto &other = other_p.Cast<DSAodBindData>();
                return lg_k == other.lg_k && infer_lg_k == other.infer_lg_k && seed == other.seed && p == other.p;
            }

            uint8_t lg_k = DS_THETA_DEFAULT_LG_K;
            bool infer_lg_k = false;
            uint64_t seed = DS_THETA_DEFAULT_SEED;
            float p = 1.0f;
        };

        template <bool HAS_LG_K>
        unique_ptr<FunctionData> DSAodBindBuild(BindAggregateFunctionInput &input)
        {
            auto result = make_uniq<DSAodBindData>();
            if (HAS_LG_K)
            {
                result->lg_k = DSTupleBindLgK(input, 0);
            }
            DSTupleBindSeedAndP(input, result->seed, result->p);
            return std::move(result);
        }

        template <bool HAS_LG_K>
        unique_ptr<FunctionData> DSAodBindMerge(BindAggregateFunctionInput &input)
        {
            auto result = make_uniq<DSAodBindData>();
            if (HAS_LG_K)
            {
                result->lg_k = DSTupleBindLgK(input, 0);
            }
            else
            {
                result->infer_lg_k = true;
            }
            return std::move(result);
        }

        // ============================================================
        // Build / Union Aggregate
        // ============================================================

        struct DSAodState
        {
            DSAodUpdate *update_sketch = nullptr;
            DSAodUnion *union_sketch = nullptr;
            uint8_t union_lg_k = 0;
            uint64_t union_seed = DS_THETA_DEFAULT_SEED;
            //! The number of values per key (0 while unknown)
            uint8_t num_values = 0;
            //! A merged input sketch was empty: with no other input, the result is an empty sketch rather than NULL
            bool saw_empty_sketch = false;

            ~DSAodState()
            {
                delete update_sketch;
                delete union_sketch;
            }

            void CheckNumValues(idx_t count, const char *function_name)
            {
                if (num_values == 0)
                {
                    num_values = static_cast<uint8_t>(count);
                }
                else if (num_values != count)
                {
                    DSAodThrowNumValues(function_name, num_values, count);
                }
            }

            void CreateUpdateSketch(const DSAodBindData &bind_data, idx_t count)
            {
                if (num_values != 0 && num_values != count)
                {
                    throw InvalidInputException("datasketch_aod: every values list must have the same length (got %llu and %llu)", idx_t(num_values), count);
                }
                CheckNumValues(count, "datasketch_aod");
                if (!update_sketch)
                {
                    DSAodUpdate::builder b{datasketches::default_array_of_doubles_update_policy(num_values)};
                    b.set_lg_k(bind_data.lg_k);
                    b.set_seed(bind_data.seed);
                    b.set_p(bind_data.p);
                    update_sketch = new DSAodUpdate(b.build());
                }
            }

            // Creates the union, or grows it to a larger lg_k (re-adding what it holds so far).
            void EnsureUnion(uint8_t lg_k, uint64_t seed)
            {
                if (union_sketch && lg_k <= union_lg_k)
                {
                    return;
                }
                if (union_sketch)
                {
                    seed = union_seed;
                }
                DSAodUnion::builder b{datasketches::default_array_of_doubles_union_policy(num_values)};
                b.set_lg_k(lg_k);
                b.set_seed(seed);
                auto grown = new DSAodUnion(b.build());
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

            uint64_t Seed(const DSAodBindData &bind_data) const
            {
                return union_sketch ? union_seed : bind_data.seed;
            }

            uint8_t LgK(const DSAodBindData &bind_data) const
            {
                return union_sketch ? union_lg_k : bind_data.lg_k;
            }
        };

        struct DSAodOperation
        {
            template <class STATE>
            static void Initialize(STATE &state)
            {
                state.update_sketch = nullptr;
                state.union_sketch = nullptr;
                state.union_lg_k = 0;
                state.union_seed = DS_THETA_DEFAULT_SEED;
                state.num_values = 0;
                state.saw_empty_sketch = false;
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
                target.saw_empty_sketch = target.saw_empty_sketch || source.saw_empty_sketch;
                if (!source.HasData())
                    return;
                auto &bind_data = aggr_input_data.bind_data->template Cast<DSAodBindData>();
                auto source_seed = source.Seed(bind_data);
                if (target.HasData() && ThetaSeedHash(source_seed) != ThetaSeedHash(target.Seed(bind_data)))
                {
                    DSAodThrowSeedMismatch("datasketch_aod", ThetaSeedHash(target.Seed(bind_data)), ThetaSeedHash(source_seed));
                }
                target.CheckNumValues(source.num_values, "datasketch_aod");
                auto lg_k = MaxValue(source.LgK(bind_data), target.LgK(bind_data));
                target.EnsureUnion(lg_k, target.HasData() ? target.Seed(bind_data) : source_seed);
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
                    target = DSAodTraits::Serialize(finalize_data.result, state.union_sketch->get_result());
                }
                else if (state.update_sketch)
                {
                    target = DSAodTraits::Serialize(finalize_data.result, state.update_sketch->compact());
                }
                else if (state.saw_empty_sketch)
                {
                    auto &bind_data = finalize_data.input.bind_data->template Cast<DSAodBindData>();
                    target = DSAodTraits::Serialize(finalize_data.result, DSAodEmptySketch(bind_data.lg_k, bind_data.seed, MaxValue<uint8_t>(state.num_values, 1)));
                }
                else
                {
                    // no input rows: NULL, as for the other sketches' aggregates
                    finalize_data.ReturnNull();
                }
            }

            // Merging serialized sketches.
            template <class A_TYPE, class STATE, class OP>
            static void Operation(STATE &state, const A_TYPE &blob, AggregateUnaryInput &idata)
            {
                auto &bind_data = idata.input.bind_data->template Cast<DSAodBindData>();
                auto sketch = DSAodTraits::Deserialize(blob);
                if (sketch.is_empty())
                {
                    state.saw_empty_sketch = true;
                    return;
                }
                if (state.union_sketch && sketch.get_seed_hash() != ThetaSeedHash(state.union_seed))
                {
                    DSAodThrowSeedMismatch("datasketch_aod", ThetaSeedHash(state.union_seed), sketch.get_seed_hash());
                }
                state.CheckNumValues(sketch.get_num_values(), "datasketch_aod");
                auto lg_k = bind_data.infer_lg_k ? MaxValue(bind_data.lg_k, ThetaInferLgK(sketch)) : bind_data.lg_k;
                state.EnsureUnion(lg_k, ThetaSeedForHash(sketch.get_seed_hash()));
                ThetaGuard([&]()
                           { state.union_sketch->update(sketch); },
                           DSAodTraits::KIND);
            }

            template <class INPUT_TYPE, class STATE, class OP>
            static void ConstantOperation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input, idx_t count)
            {
                // the values of the same key add up, so merging a sketch twice is not the same as once
                for (idx_t i = 0; i < count; i++)
                {
                    Operation<INPUT_TYPE, STATE, OP>(state, input, unary_input);
                }
            }
        };

        // The update of the build aggregate: (key, values) at KEY_INDEX and KEY_INDEX + 1.
        template <class KEY_TYPE, idx_t KEY_INDEX>
        void DSAodBuildUpdate(Vector inputs[], AggregateInputData &aggr_input_data, idx_t input_count, Vector &states, idx_t count)
        {
            D_ASSERT(input_count > KEY_INDEX + 1);
            auto &bind_data = aggr_input_data.bind_data->Cast<DSAodBindData>();
            auto &list = inputs[KEY_INDEX + 1];
            UnifiedVectorFormat key_format, list_format, child_format, state_format;
            inputs[KEY_INDEX].ToUnifiedFormat(key_format);
            list.ToUnifiedFormat(list_format);
            ListVector::GetChild(list).ToUnifiedFormat(child_format);
            states.ToUnifiedFormat(state_format);
            auto keys = UnifiedVectorFormat::GetData<KEY_TYPE>(key_format);
            auto lists = UnifiedVectorFormat::GetData<list_entry_t>(list_format);
            auto values = UnifiedVectorFormat::GetData<double>(child_format);
            auto state_pointers = UnifiedVectorFormat::GetData<DSAodState *>(state_format);
            std::vector<double> buffer;
            for (idx_t i = 0; i < count; i++)
            {
                auto key_index = key_format.sel->get_index(i);
                auto list_index = list_format.sel->get_index(i);
                if (!key_format.validity.RowIsValid(key_index) || !list_format.validity.RowIsValid(list_index))
                {
                    continue;
                }
                auto entry = lists[list_index];
                if (entry.length == 0 || entry.length > DS_AOD_MAX_VALUES)
                {
                    throw InvalidInputException("datasketch_aod: values must have between 1 and %llu elements, got %llu", DS_AOD_MAX_VALUES, entry.length);
                }
                buffer.resize(entry.length);
                for (idx_t j = 0; j < entry.length; j++)
                {
                    auto child_index = child_format.sel->get_index(entry.offset + j);
                    if (!child_format.validity.RowIsValid(child_index))
                    {
                        throw InvalidInputException("datasketch_aod: values must not contain NULL");
                    }
                    buffer[j] = values[child_index];
                }
                auto &state = *state_pointers[state_format.sel->get_index(i)];
                state.CreateUpdateSketch(bind_data, entry.length);
                if constexpr (std::is_same_v<KEY_TYPE, duckdb::string_t>)
                {
                    state.update_sketch->update(keys[key_index].GetData(), keys[key_index].GetSize(), buffer.data());
                }
                else
                {
                    state.update_sketch->update(keys[key_index], buffer.data());
                }
            }
        }

        // ============================================================
        // Intersection Aggregate
        // ============================================================

        struct DSAodIntersectState
        {
            DSAodIntersection *intersection = nullptr;
            uint64_t seed = DS_THETA_DEFAULT_SEED;
            uint8_t num_values = 0;
            bool saw_empty = false;

            ~DSAodIntersectState()
            {
                delete intersection;
            }

            void Add(const DSAodCompact &sketch)
            {
                if (sketch.is_empty())
                {
                    saw_empty = true;
                    if (num_values == 0)
                    {
                        num_values = sketch.get_num_values();
                    }
                    return;
                }
                if (!intersection)
                {
                    seed = ThetaSeedForHash(sketch.get_seed_hash());
                    num_values = sketch.get_num_values();
                    intersection = new DSAodIntersection(seed, DSAodSumPolicy(num_values));
                    return Update(sketch);
                }
                if (sketch.get_seed_hash() != ThetaSeedHash(seed))
                {
                    DSAodThrowSeedMismatch("datasketch_aod_intersect_agg", ThetaSeedHash(seed), sketch.get_seed_hash());
                }
                if (sketch.get_num_values() != num_values)
                {
                    DSAodThrowNumValues("datasketch_aod_intersect_agg", num_values, sketch.get_num_values());
                }
                Update(sketch);
            }

            void Update(const DSAodCompact &sketch)
            {
                ThetaGuard([&]()
                           { intersection->update(sketch); },
                           DSAodTraits::KIND);
            }
        };

        struct DSAodIntersectOperation
        {
            template <class STATE>
            static void Initialize(STATE &state)
            {
                state.intersection = nullptr;
                state.seed = DS_THETA_DEFAULT_SEED;
                state.num_values = 0;
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
            static void Operation(STATE &state, const A_TYPE &blob, AggregateUnaryInput &)
            {
                state.Add(DSAodTraits::Deserialize(blob));
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
            static void Combine(const STATE &source, STATE &target, AggregateInputData &)
            {
                target.saw_empty = target.saw_empty || source.saw_empty;
                if (target.num_values == 0)
                {
                    target.num_values = source.num_values;
                }
                if (source.intersection)
                {
                    target.Add(source.intersection->get_result());
                }
            }

            template <class T, class STATE>
            static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data)
            {
                if (state.saw_empty)
                {
                    target = DSAodTraits::Serialize(finalize_data.result, DSAodEmptySketch(DS_THETA_DEFAULT_LG_K, state.seed, MaxValue<uint8_t>(state.num_values, 1)));
                }
                else if (state.intersection)
                {
                    target = DSAodTraits::Serialize(finalize_data.result, state.intersection->get_result());
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

        DSAodCompact DSAodUnionOf(const vector<string_t> &blobs, optional_idx explicit_lg_k)
        {
            vector<DSAodCompact> sketches;
            sketches.reserve(blobs.size());
            ThetaSeedTracker seeds("datasketch_aod_union", DSAodTraits::KIND);
            DSAodNumValuesTracker num_values("datasketch_aod_union");
            uint8_t lg_k = DS_THETA_DEFAULT_LG_K;
            for (auto &blob : blobs)
            {
                sketches.push_back(DSAodTraits::Deserialize(blob));
                seeds.Add(sketches.back());
                num_values.Add(sketches.back());
                lg_k = MaxValue(lg_k, ThetaInferLgK(sketches.back()));
            }
            if (explicit_lg_k.IsValid())
            {
                lg_k = static_cast<uint8_t>(explicit_lg_k.GetIndex());
            }
            return ThetaGuard([&]()
                              {
                                  DSAodUnion::builder b{datasketches::default_array_of_doubles_union_policy(num_values.NumValues())};
                                  b.set_lg_k(lg_k);
                                  b.set_seed(seeds.Seed());
                                  auto union_obj = b.build();
                                  for (auto &sketch : sketches)
                                  {
                                      union_obj.update(sketch);
                                  }
                                  return union_obj.get_result(); },
                              DSAodTraits::KIND);
        }

        void DSAodUnionFunction(DataChunk &args, ExpressionState &, Vector &result)
        {
            auto result_data = FlatVector::GetDataMutable<string_t>(result);
            ThetaForEachRow(args, result, 0, 0, [&](idx_t row, const ThetaRowArgs &row_args)
                            { result_data[row] = DSAodTraits::Serialize(result, DSAodUnionOf(row_args.sketches, optional_idx())); });
        }

        void DSAodUnionWithKFunction(DataChunk &args, ExpressionState &, Vector &result)
        {
            auto result_data = FlatVector::GetDataMutable<string_t>(result);
            ThetaForEachRow(args, result, 1, 0, [&](idx_t row, const ThetaRowArgs &row_args)
                            {
                                auto lg_k = row_args.Get<int32_t>(0);
                                ThetaCheckLgK(lg_k, "datasketch_aod_union");
                                result_data[row] = DSAodTraits::Serialize(result, DSAodUnionOf(row_args.sketches, optional_idx(static_cast<idx_t>(lg_k)))); });
        }

        void DSAodIntersectFunction(DataChunk &args, ExpressionState &, Vector &result)
        {
            auto result_data = FlatVector::GetDataMutable<string_t>(result);
            ThetaForEachRow(args, result, 0, 0, [&](idx_t row, const ThetaRowArgs &row_args)
                            {
                                vector<DSAodCompact> sketches;
                                sketches.reserve(row_args.sketches.size());
                                ThetaSeedTracker seeds("datasketch_aod_intersect", DSAodTraits::KIND);
                                DSAodNumValuesTracker num_values("datasketch_aod_intersect");
                                for (auto &blob : row_args.sketches)
                                {
                                    sketches.push_back(DSAodTraits::Deserialize(blob));
                                    seeds.Add(sketches.back());
                                    num_values.Add(sketches.back());
                                }
                                auto intersected = ThetaGuard([&]()
                                                              {
                                                                  DSAodIntersection intersection(seeds.Seed(), DSAodSumPolicy(num_values.NumValues()));
                                                                  for (auto &sketch : sketches)
                                                                  {
                                                                      intersection.update(sketch);
                                                                  }
                                                                  return intersection.get_result(); },
                                                              DSAodTraits::KIND);
                                result_data[row] = DSAodTraits::Serialize(result, intersected); });
        }

        void DSAodNumValues(DataChunk &args, ExpressionState &, Vector &result)
        {
            DSAodScalars::UnaryRead<int64_t>(args, result, [](const DSAodCompact &sketch)
                                             { return static_cast<int64_t>(sketch.get_num_values()); });
        }

        void DSAodSummaries(DataChunk &args, ExpressionState &, Vector &result)
        {
            idx_t offset = 0;
            idx_t value_offset = 0;
            ListVector::SetListSize(result, 0);
            ThetaForEachRow(args, result, 0, 1, [&](idx_t row, const ThetaRowArgs &row_args)
                            {
                                auto sketch = DSAodTraits::Deserialize(row_args.sketches[0]);
                                auto count = sketch.get_num_retained();
                                auto num_values = sketch.get_num_values();
                                ListVector::Reserve(result, offset + count);
                                auto &child = ListVector::GetChildMutable(result);
                                auto &entries = StructVector::GetEntries(child);
                                auto &values_list = entries[1];
                                ListVector::Reserve(values_list, value_offset + count * num_values);
                                auto hashes = FlatVector::GetDataMutable<uint64_t>(entries[0]);
                                auto value_entries = FlatVector::GetDataMutable<list_entry_t>(values_list);
                                auto values = FlatVector::GetDataMutable<double>(ListVector::GetChildMutable(values_list));
                                auto list_data = FlatVector::GetDataMutable<list_entry_t>(result);
                                list_data[row] = list_entry_t(offset, count);
                                for (const auto &entry : sketch)
                                {
                                    hashes[offset] = entry.first;
                                    value_entries[offset] = list_entry_t(value_offset, num_values);
                                    for (uint8_t i = 0; i < num_values; i++)
                                    {
                                        values[value_offset++] = entry.second[i];
                                    }
                                    offset++;
                                }
                                ListVector::SetListSize(values_list, value_offset);
                                ListVector::SetListSize(result, offset); });
        }

        // Per-column mean of the retained arrays (MEAN) or estimated per-column total (sum / theta).
        template <bool MEAN>
        void DSAodColumns(DataChunk &args, ExpressionState &, Vector &result)
        {
            idx_t offset = 0;
            ListVector::SetListSize(result, 0);
            ThetaForEachRow(args, result, 0, 1, [&](idx_t row, const ThetaRowArgs &row_args)
                            {
                                auto sketch = DSAodTraits::Deserialize(row_args.sketches[0]);
                                if (MEAN && sketch.get_num_retained() == 0)
                                {
                                    FlatVector::SetNull(result, row, true);
                                    return;
                                }
                                auto num_values = sketch.get_num_values();
                                std::vector<double> totals(num_values, 0.0);
                                for (const auto &entry : sketch)
                                {
                                    for (uint8_t i = 0; i < num_values; i++)
                                    {
                                        totals[i] += entry.second[i];
                                    }
                                }
                                double divisor = MEAN ? static_cast<double>(sketch.get_num_retained()) : sketch.get_theta();
                                ListVector::Reserve(result, offset + num_values);
                                auto values = FlatVector::GetDataMutable<double>(ListVector::GetChildMutable(result));
                                auto list_data = FlatVector::GetDataMutable<list_entry_t>(result);
                                list_data[row] = list_entry_t(offset, num_values);
                                for (uint8_t i = 0; i < num_values; i++)
                                {
                                    values[offset++] = totals[i] / divisor;
                                }
                                ListVector::SetListSize(result, offset); });
        }

        // ============================================================
        // Registration
        // ============================================================

        const vector<string> &DSAodCategories()
        {
            static const vector<string> categories = {"sketch", "aod"};
            return categories;
        }

        FunctionDescription DSAodDescription(const string &description, const vector<string> &examples, const vector<string> &names,
                                             const vector<LogicalType> &types)
        {
            return DSDescription(description, examples, names, types, DSAodCategories());
        }

        template <typename T>
        void RegisterAodBuild(AggregateFunctionSet &set, vector<FunctionDescription> &descs, const LogicalType &key_type, const LogicalType &sketch_type)
        {
            const auto values_type = LogicalType::LIST(LogicalType::DOUBLE);
            for (bool with_lg_k : {false, true})
            {
                auto fun = AggregateFunction::UnaryAggregate<DSAodState, string_t, string_t, DSAodOperation, AggregateDestructorType::LEGACY>(
                    key_type, sketch_type);
                auto signature = DSTupleSignature(with_lg_k, {key_type, values_type}, sketch_type);
                DSTupleAddSeedAndP(signature);
                fun.GetSignature() = std::move(signature);
                if (with_lg_k)
                {
                    fun.SetBindCallback(DSAodBindBuild<true>);
                    fun.SetStateUpdateCallback(DSAodBuildUpdate<T, 1>);
                }
                else
                {
                    fun.SetBindCallback(DSAodBindBuild<false>);
                    fun.SetStateUpdateCallback(DSAodBuildUpdate<T, 0>);
                }
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                set.AddFunction(fun);

                const string what = "an array-of-doubles sketch: a Theta sketch of the distinct keys where each retained key carries an array of "
                                    "DOUBLE values (1-255, the same length for every row), summed element-wise per key. Options: seed := (hash "
                                    "seed; sketches can only be combined with sketches built with the same seed); p := sampling probability in (0, 1]";
                // Runnable examples over inline data
                const string rows = " FROM (SELECT i % 100 AS user_id, 1.0::DOUBLE AS clicks, i * 0.5::DOUBLE AS revenue FROM range(1000) t(i)) t";
                if (with_lg_k)
                {
                    descs.push_back(DSAodDescription("Creates, with the given log2 of nominal entries (lg_k, 4-26), " + what,
                                                     {"SELECT datasketch_aod(14, user_id, [clicks, revenue])" + rows, "SELECT datasketch_aod(14, user_id, [clicks, revenue], seed := 42)" + rows},
                                                     {"lg_k", "key", "values", "seed", "p"},
                                                     {LogicalType::INTEGER, key_type, values_type, LogicalType::BIGINT, LogicalType::DOUBLE}));
                }
                else
                {
                    descs.push_back(DSAodDescription("Creates (lg_k 12) " + what,
                                                     {"SELECT datasketch_aod(user_id, [clicks, revenue])" + rows},
                                                     {"key", "values", "seed", "p"},
                                                     {key_type, values_type, LogicalType::BIGINT, LogicalType::DOUBLE}));
                }
            }
        }
    }

    void LoadAodSketch(ExtensionLoader &loader)
    {
        auto sketch_type = DSCreateSketchType(loader, "sketch_aod", "Sketch type for array-of-doubles Tuple Sketch");
        auto theta_type = LogicalType(LogicalTypeId::BLOB).WithAlias("sketch_theta");
        const DSExampleSources ex("datasketch_aod(i % 100, [1.0, i * 0.5]::DOUBLE[])");

        // --- datasketch_aod: build from (key, values), or merge (union) sketches
        {
            AggregateFunctionSet set("datasketch_aod");
            vector<FunctionDescription> descs;
            RegisterAodBuild<int8_t>(set, descs, LogicalType::TINYINT, sketch_type);
            RegisterAodBuild<int16_t>(set, descs, LogicalType::SMALLINT, sketch_type);
            RegisterAodBuild<int32_t>(set, descs, LogicalType::INTEGER, sketch_type);
            RegisterAodBuild<int64_t>(set, descs, LogicalType::BIGINT, sketch_type);
            RegisterAodBuild<uint8_t>(set, descs, LogicalType::UTINYINT, sketch_type);
            RegisterAodBuild<uint16_t>(set, descs, LogicalType::USMALLINT, sketch_type);
            RegisterAodBuild<uint32_t>(set, descs, LogicalType::UINTEGER, sketch_type);
            RegisterAodBuild<uint64_t>(set, descs, LogicalType::UBIGINT, sketch_type);
            RegisterAodBuild<float>(set, descs, LogicalType::FLOAT, sketch_type);
            RegisterAodBuild<double>(set, descs, LogicalType::DOUBLE, sketch_type);
            RegisterAodBuild<string_t>(set, descs, LogicalType::VARCHAR, sketch_type);

            for (bool with_lg_k : {false, true})
            {
                auto fun = AggregateFunction::UnaryAggregate<DSAodState, string_t, string_t, DSAodOperation, AggregateDestructorType::LEGACY>(
                    sketch_type, sketch_type);
                fun.GetSignature() = DSTupleSignature(with_lg_k, {sketch_type}, sketch_type);
                if (with_lg_k)
                {
                    fun.SetBindCallback(DSAodBindMerge<true>);
                    DSReadDataArgumentAt<1, DSAodState, string_t, DSAodOperation>(fun);
                    descs.push_back(DSAodDescription("Merges (unions) array-of-doubles sketches into one with the given log2 of nominal entries (lg_k); "
                                                     "the values of a key are summed element-wise",
                                                     {ex.QueryPartials("datasketch_aod(16, sketch)")}, {"lg_k", "sketch"}, {LogicalType::INTEGER, sketch_type}));
                }
                else
                {
                    fun.SetBindCallback(DSAodBindMerge<false>);
                    DSReadDataArgumentAt<0, DSAodState, string_t, DSAodOperation>(fun);
                    descs.push_back(DSAodDescription("Merges (unions) array-of-doubles sketches into one, keeping as many entries as the largest input "
                                                     "(at least lg_k 12); the values of a key are summed element-wise",
                                                     {ex.QueryPartials("datasketch_aod(sketch)")}, {"sketch"}, {sketch_type}));
                }
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                set.AddFunction(fun);
            }
            DSRegisterAggregates(loader, std::move(set), std::move(descs));
        }

        // --- datasketch_aod_intersect_agg
        {
            AggregateFunctionSet set("datasketch_aod_intersect_agg");
            auto fun = AggregateFunction::UnaryAggregate<DSAodIntersectState, string_t, string_t, DSAodIntersectOperation, AggregateDestructorType::LEGACY>(
                sketch_type, sketch_type);
            DSSetArguments(fun, {sketch_type});
            fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
            set.AddFunction(fun);
            vector<FunctionDescription> descs;
            descs.push_back(DSAodDescription("Intersects array-of-doubles sketches across rows: keeps the keys present in every sketch, summing their "
                                             "values element-wise. NULL sketches are skipped; returns NULL if there are no sketches",
                                             {ex.QueryOverlapping("datasketch_aod_intersect_agg(sketch)")}, {"sketch"}, {sketch_type}));
            DSRegisterAggregates(loader, std::move(set), std::move(descs));
        }

        // --- set operations
        {
            ScalarFunctionSet set("datasketch_aod_union");
            set.AddFunction(ThetaVariadic(ScalarFunction("datasketch_aod_union", {sketch_type, sketch_type}, sketch_type, DSAodUnionFunction),
                                          {"sketch_a", "sketch_b"}, sketch_type));
            set.AddFunction(ThetaVariadic(ScalarFunction("datasketch_aod_union", {LogicalType::INTEGER, sketch_type, sketch_type}, sketch_type, DSAodUnionWithKFunction),
                                          {"lg_k", "sketch_a", "sketch_b"}, sketch_type));
            DSRegisterScalars(loader, std::move(set),
                                {DSAodDescription("Returns the union of two or more array-of-doubles sketches (values of a key summed element-wise), keeping as "
                                                  "many entries as the largest input (at least lg_k 12)",
                                                  {ex.Query("datasketch_aod_union(sketch_a, sketch_b)"), ex.Query("datasketch_aod_union(sketch_a, sketch_b, sketch_c)")},
                                                  {"sketch_a", "sketch_b"}, {sketch_type, sketch_type}),
                                 DSAodDescription("Returns the union of two or more array-of-doubles sketches (values of a key summed element-wise) with the "
                                                  "given log2 of nominal entries (lg_k, 4-26)",
                                                  {ex.Query("datasketch_aod_union(16, sketch_a, sketch_b)")},
                                                  {"lg_k", "sketch_a", "sketch_b"}, {LogicalType::INTEGER, sketch_type, sketch_type})});
        }
        DSRegisterScalar(loader, ThetaVariadic(ScalarFunction("datasketch_aod_intersect", {sketch_type, sketch_type}, sketch_type, DSAodIntersectFunction), {"sketch_a", "sketch_b"}, sketch_type),
                            DSAodDescription("Returns the intersection of two or more array-of-doubles sketches: the keys present in every sketch, with "
                                             "their values summed element-wise",
                                             {ex.Query("datasketch_aod_intersect(sketch_a, sketch_b)"), ex.Query("datasketch_aod_intersect(sketch_a, sketch_b, sketch_c)")},
                                             {"sketch_a", "sketch_b"}, {sketch_type, sketch_type}));

        // --- values
        DSRegisterScalar(loader, ScalarFunction("datasketch_aod_num_values", {sketch_type}, LogicalType::BIGINT, DSAodNumValues),
                            DSAodDescription("Returns the number of values per key of the array-of-doubles sketch",
                                             {ex.Query("datasketch_aod_num_values(sketch)")}, {"sketch"}, {sketch_type}));
        {
            child_list_t<LogicalType> entry;
            entry.emplace_back("hash", LogicalType::UBIGINT);
            entry.emplace_back("values", LogicalType::LIST(LogicalType::DOUBLE));
            DSRegisterScalar(loader, ScalarFunction("datasketch_aod_summaries", {sketch_type}, LogicalType::LIST(LogicalType::STRUCT(std::move(entry))), DSAodSummaries),
                                DSAodDescription("Returns the retained entries of the array-of-doubles sketch as a list of {hash, values} structs (the key "
                                                 "hash and its values)",
                                                 {ex.Query("datasketch_aod_summaries(sketch)")}, {"sketch"}, {sketch_type}));
        }
        DSRegisterScalar(loader, ScalarFunction("datasketch_aod_column_means", {sketch_type}, LogicalType::LIST(LogicalType::DOUBLE), DSAodColumns<true>),
                            DSAodDescription("Returns the mean of each value column over the retained keys (the mean per distinct key), or NULL if the "
                                             "sketch retains no entries",
                                             {ex.Query("datasketch_aod_column_means(sketch)")}, {"sketch"}, {sketch_type}));
        DSRegisterScalar(loader, ScalarFunction("datasketch_aod_column_sums", {sketch_type}, LogicalType::LIST(LogicalType::DOUBLE), DSAodColumns<false>),
                            DSAodDescription("Returns the estimated total of each value column over all distinct keys: the column sums of the retained "
                                             "entries divided by theta (exact when the sketch is not in estimation mode)",
                                             {ex.Query("datasketch_aod_column_sums(sketch)")}, {"sketch"}, {sketch_type}));

        DSAodScalars::Register(loader, sketch_type, theta_type, "array-of-doubles sketch", DSAodCategories(), ex);
    }
}
