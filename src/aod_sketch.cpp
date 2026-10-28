// Array-of-doubles Tuple sketches (sketch_aod).
//
// Each retained key carries a fixed-length array of DOUBLE values (1-255, stored in the sketch); values of the
// same key are summed element-wise when building, merging, unioning and intersecting.

#include "datasketches_tuple_common.hpp"
#include "datasketches_theta_arena.hpp"

#include <DataSketches/array_of_doubles_sketch.hpp>

#include <ostream>

namespace datasketches
{
    // tuple_sketch::to_string needs to be able to print the summaries (it only does when asked for the items).
    template <class A>
    inline std::ostream &operator<<(std::ostream &os, const array<double, A> &values)
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
        // The library's classes, allocating through DuckDB's allocator (see DSAllocator): the sketches take their
        // allocator from the array type.
        using DSAodArray = datasketches::array<double, DSAllocator<double>>;
        using DSAodUpdate = datasketches::update_array_tuple_sketch<DSAodArray>;
        using DSAodCompact = datasketches::compact_array_tuple_sketch<DSAodArray>;
        using DSAodUnion = datasketches::array_tuple_union<DSAodArray>;

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

        using DSAodIntersection = datasketches::array_tuple_intersection<DSAodArray, DSAodSumPolicy>;

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
                return datasketches::array_tuple_a_not_b<DSAodArray>(seed).compute(a, b);
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

        using DSAodBindData = DSThetaFamilyBindData;

        template <bool HAS_LG_K>
        unique_ptr<FunctionData> DSAodBindBuild(BindAggregateFunctionInput &input)
        {
            auto result = make_uniq<DSAodBindData>();
            if (HAS_LG_K)
            {
                result->lg_k = DSTupleBindLgK(input, 0);
            }
            DSTupleBindSeedAndP(input, result->seed, result->p);
            DSThetaRecordConstants(input, *result, HAS_LG_K ? 1 : 0, HAS_LG_K ? 3 : 2);
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
            DSThetaRecordConstants(input, *result, HAS_LG_K ? 1 : 0, HAS_LG_K ? 2 : 1);
            return std::move(result);
        }

        // ============================================================
        // Aggregate states (see datasketches_theta_arena.hpp)
        // ============================================================

        // Every array (and so every entry) allocates in arena mode, so copies the library makes of entries land in
        // the current arena.
        struct DSAodStateTraits
        {
            using Alloc = DSAllocator<double>;
            using Entry = std::pair<uint64_t, DSAodArray>;
            using Update = DSAodUpdate;
            using Compact = DSAodCompact;
            using Union = DSAodUnion;
            using Intersection = DSAodIntersection;
            static constexpr bool SET_ONLY = false;
            static constexpr bool HAS_NUM_VALUES = true;
            static constexpr const char *KIND = "Array of doubles";
            static constexpr const char *BUILD_NAME = "datasketch_aod";
            static constexpr const char *INTERSECT_NAME = "datasketch_aod_intersect_agg";

            static uint64_t Key(const Entry &entry) { return entry.first; }
            static idx_t SummarySize(const DSThetaParams &params) { return sizeof(double) * params.num_values; }
            static void WriteSummary(const Entry &entry, data_ptr_t data, const DSThetaParams &params)
            {
                memcpy(data, entry.second.data(), SummarySize(params));
            }
            static Entry MakeEntry(uint64_t key, const_data_ptr_t data, const DSThetaParams &params)
            {
                DSAodArray values(params.num_values, 0.0, Alloc(DSArenaMode()));
                memcpy(values.data(), data, SummarySize(params));
                return Entry(key, std::move(values));
            }
            // default_array_tuple_union_policy: element-wise sum
            static void Fold(data_ptr_t data, const Entry &incoming, const DSThetaParams &params)
            {
                for (uint8_t i = 0; i < params.num_values; i++)
                {
                    double value;
                    memcpy(&value, data + i * sizeof(double), sizeof(double));
                    value += incoming.second[i];
                    memcpy(data + i * sizeof(double), &value, sizeof(double));
                }
            }
            static idx_t PayloadSize(const DSThetaParams &params) { return SummarySize(params); }
            static uint8_t NumValues(const Compact &sketch) { return sketch.get_num_values(); }

            template <class KEY>
            static void Apply(Update &sketch, const_data_ptr_t key, uint32_t length, const_data_ptr_t payload, const DSThetaParams &params)
            {
                double values[DS_AOD_MAX_VALUES];
                memcpy(values, payload, SummarySize(params));
                const double *update = values;
                if constexpr (std::is_same<KEY, string_t>::value)
                {
                    sketch.update(key, length, update);
                }
                else
                {
                    KEY typed;
                    memcpy(&typed, key, sizeof(KEY));
                    sketch.update(typed, update);
                }
            }

            static Update NewUpdate(const DSAodBindData &bind, const DSThetaParams &params)
            {
                DSAodUpdate::builder b{datasketches::default_array_tuple_update_policy<DSAodArray>(params.num_values, Alloc(DSArenaMode())),
                                       Alloc(DSArenaMode())};
                b.set_lg_k(bind.lg_k);
                b.set_seed(bind.seed);
                b.set_p(bind.p);
                return b.build();
            }

            static Union NewUnion(uint8_t lg_k, uint64_t seed, const DSThetaParams &params)
            {
                DSAodUnion::builder b{datasketches::default_array_tuple_union_policy<DSAodArray>(params.num_values), Alloc(DSArenaMode())};
                b.set_lg_k(lg_k);
                b.set_seed(seed);
                return b.build();
            }

            static Intersection NewIntersection(uint64_t seed, const DSThetaParams &params)
            {
                return Intersection(seed, DSAodSumPolicy(params.num_values), Alloc(DSArenaMode()));
            }

            static Compact Deserialize(const string_t &blob)
            {
                auto seed = DSTupleSeedFor(blob);
                return ThetaGuard([&]()
                                  { return Compact::deserialize(blob.GetData(), blob.GetSize(), seed, Alloc(DSArenaMode())); },
                                  KIND);
            }

            static Compact Empty(uint8_t lg_k, uint64_t seed, const DSThetaParams &params)
            {
                DSAodUpdate::builder b{datasketches::default_array_tuple_update_policy<DSAodArray>(params.num_values, Alloc(DSArenaMode())),
                                       Alloc(DSArenaMode())};
                b.set_lg_k(lg_k);
                b.set_seed(seed);
                return b.build().compact();
            }

            static auto Serialize(const Compact &sketch) -> decltype(sketch.serialize())
            {
                return sketch.serialize();
            }

            static void ThrowSeedMismatch(const char *function_name, uint16_t a, uint16_t b)
            {
                DSAodThrowSeedMismatch(function_name, a, b);
            }

            static void ThrowNumValues(const char *function_name, idx_t a, idx_t b)
            {
                DSAodThrowNumValues(function_name, a, b);
            }
        };

        template <class KEY>
        using DSAodState = DSThetaFamilyState<DSAodStateTraits, KEY>;
        using DSAodMergeState = DSAodState<string_t>;
        using DSAodIntersectState = DSThetaFamilyIntersectState<DSAodStateTraits>;
        // the values of the same key add up, so merging a sketch twice is not the same as once
        using DSAodOperation = DSThetaFamilyOperation<false>;
        using DSAodIntersectOperation = DSThetaFamilyIntersectOperation<false>;

        // The update of the build aggregate: (key, values) at KEY_INDEX and KEY_INDEX + 1.
        template <class KEY_TYPE, idx_t KEY_INDEX>
        void DSAodBuildUpdate(Vector inputs[], AggregateInputData &aggr_input_data, idx_t input_count, Vector &states, idx_t count)
        {
            D_ASSERT(input_count > KEY_INDEX + 1);
            using STATE = DSAodState<KEY_TYPE>;
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
            auto state_pointers = UnifiedVectorFormat::GetData<STATE *>(state_format);
            DSArenaScope scope(aggr_input_data.allocator);
            double buffer[DS_AOD_MAX_VALUES];
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
                if (state.num_values != 0 && state.num_values != entry.length)
                {
                    throw InvalidInputException("datasketch_aod: every values list must have the same length (got %llu and %llu)", idx_t(state.num_values), entry.length);
                }
                state.num_values = static_cast<uint8_t>(entry.length);
                auto bytes = DSThetaKeyBytes(keys[key_index]);
                state.AddRow(bytes.first, bytes.second, const_data_ptr_cast(buffer), bind_data, aggr_input_data.allocator);
            }
        }

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
                                  DSAodUnion::builder b{datasketches::default_array_tuple_union_policy<DSAodArray>(num_values.NumValues())};
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
                auto fun = AggregateFunction::UnaryAggregate<DSAodState<T>, string_t, string_t, DSAodOperation>(key_type, sketch_type);
                DSThetaSetStateExport<DSAodState<T>>(fun);
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
                auto fun = AggregateFunction::UnaryAggregate<DSAodMergeState, string_t, string_t, DSAodOperation>(sketch_type, sketch_type);
                DSThetaSetStateExport<DSAodMergeState>(fun);
                fun.GetSignature() = DSTupleSignature(with_lg_k, {sketch_type}, sketch_type);
                if (with_lg_k)
                {
                    fun.SetBindCallback(DSAodBindMerge<true>);
                    DSReadDataArgumentAt<1, DSAodMergeState, string_t, DSAodOperation>(fun);
                    descs.push_back(DSAodDescription("Merges (unions) array-of-doubles sketches into one with the given log2 of nominal entries (lg_k); "
                                                     "the values of a key are summed element-wise",
                                                     {ex.QueryPartials("datasketch_aod(16, sketch)")}, {"lg_k", "sketch"}, {LogicalType::INTEGER, sketch_type}));
                }
                else
                {
                    fun.SetBindCallback(DSAodBindMerge<false>);
                    DSReadDataArgumentAt<0, DSAodMergeState, string_t, DSAodOperation>(fun);
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
            auto fun = AggregateFunction::UnaryAggregate<DSAodIntersectState, string_t, string_t, DSAodIntersectOperation>(sketch_type, sketch_type);
            DSThetaSetStateExport<DSAodIntersectState>(fun);
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
