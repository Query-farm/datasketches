// Tuple sketches with a DOUBLE summary per key (sketch_tuple_double).
//
// A Tuple sketch is a Theta sketch whose retained keys carry a summary. Here the summary is a DOUBLE combined by
// a mode: sum (default), min, max, or one (always 1). The mode is not stored in the sketch (as in the other
// DataSketches libraries), so functions that combine summaries (merge, union, intersection) take it as an option.

#include "datasketches_tuple_common.hpp"
#include "datasketches_theta_arena.hpp"

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

        // The library's classes, allocating through DuckDB's allocator (see DSAllocator).
        using DSTupleAllocator = DSAllocator<double>;
        using DSTupleUpdate = datasketches::update_tuple_sketch<double, double, DSTupleDoublePolicy, DSTupleAllocator>;
        using DSTupleCompact = datasketches::compact_tuple_sketch<double, DSTupleAllocator>;
        using DSTupleUnion = datasketches::tuple_union<double, DSTupleDoublePolicy, DSTupleAllocator>;
        using DSTupleIntersection = datasketches::tuple_intersection<double, DSTupleDoublePolicy, DSTupleAllocator>;
        using DSTupleANotB = datasketches::tuple_a_not_b<double, DSTupleAllocator>;

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

        // ============================================================
        // Bind Data
        // ============================================================

        using DSTupleBindData = DSThetaFamilyBindData;

        uint8_t DSTupleBindMode(BindAggregateFunctionInput &input)
        {
            return static_cast<uint8_t>(DSTupleParseMode(input.GetNonNullConstant(Identifier("mode")).GetValue<string>()));
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
            DSThetaRecordConstants(input, *result, HAS_LG_K ? 1 : 0, HAS_LG_K ? 3 : 2);
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
            DSThetaRecordConstants(input, *result, HAS_LG_K ? 1 : 0, HAS_LG_K ? 2 : 1);
            return std::move(result);
        }

        unique_ptr<FunctionData> DSTupleBindIntersect(BindAggregateFunctionInput &input)
        {
            auto result = make_uniq<DSTupleBindData>();
            result->mode = DSTupleBindMode(input);
            DSThetaRecordConstants(input, *result, 0, 1);
            return std::move(result);
        }

        void DSTupleThrowSeedMismatch(const char *function_name, uint16_t a, uint16_t b)
        {
            throw InvalidInputException("%s: cannot combine Tuple sketches built with different seeds (seed hashes %d and %d)", function_name, a, b);
        }

        // ============================================================
        // Aggregate states (see datasketches_theta_arena.hpp)
        // ============================================================

        struct DSTupleTraits
        {
            using Alloc = DSTupleAllocator;
            using Entry = std::pair<uint64_t, double>;
            using Update = DSTupleUpdate;
            using Compact = DSTupleCompact;
            using Union = DSTupleUnion;
            using Intersection = DSTupleIntersection;
            static constexpr bool SET_ONLY = false;
            static constexpr bool HAS_NUM_VALUES = false;
            static constexpr const char *KIND = "Tuple";
            static constexpr const char *BUILD_NAME = "datasketch_tuple";
            static constexpr const char *INTERSECT_NAME = "datasketch_tuple_intersect_agg";

            static DSTupleDoublePolicy Policy(const DSThetaParams &params)
            {
                return DSTupleDoublePolicy(static_cast<DSTupleMode>(params.mode));
            }

            static uint64_t Key(const Entry &entry) { return entry.first; }
            static idx_t SummarySize(const DSThetaParams &) { return sizeof(double); }
            static void WriteSummary(const Entry &entry, data_ptr_t data, const DSThetaParams &)
            {
                memcpy(data, &entry.second, sizeof(double));
            }
            static Entry MakeEntry(uint64_t key, const_data_ptr_t data, const DSThetaParams &)
            {
                double value;
                memcpy(&value, data, sizeof(double));
                return Entry(key, value);
            }
            static void Fold(data_ptr_t data, const Entry &incoming, const DSThetaParams &params)
            {
                double value;
                memcpy(&value, data, sizeof(double));
                Policy(params)(value, incoming.second);
                memcpy(data, &value, sizeof(double));
            }
            static idx_t PayloadSize(const DSThetaParams &) { return sizeof(double); }
            static uint8_t NumValues(const Compact &) { return 1; }

            template <class KEY>
            static void Apply(Update &sketch, const_data_ptr_t key, uint32_t length, const_data_ptr_t payload, const DSThetaParams &)
            {
                double value;
                memcpy(&value, payload, sizeof(double));
                if constexpr (std::is_same<KEY, string_t>::value)
                {
                    sketch.update(key, length, value);
                }
                else
                {
                    KEY typed;
                    memcpy(&typed, key, sizeof(KEY));
                    sketch.update(typed, value);
                }
            }

            static Update NewUpdate(const DSTupleBindData &bind, const DSThetaParams &params)
            {
                DSTupleUpdate::builder b{Policy(params), Alloc(DSArenaMode())};
                b.set_lg_k(bind.lg_k);
                b.set_seed(bind.seed);
                b.set_p(bind.p);
                return b.build();
            }

            static Union NewUnion(uint8_t lg_k, uint64_t seed, const DSThetaParams &params)
            {
                DSTupleUnion::builder b{Policy(params), Alloc(DSArenaMode())};
                b.set_lg_k(lg_k);
                b.set_seed(seed);
                return b.build();
            }

            static Intersection NewIntersection(uint64_t seed, const DSThetaParams &params)
            {
                return Intersection(seed, Policy(params), Alloc(DSArenaMode()));
            }

            static Compact Deserialize(const string_t &blob)
            {
                auto seed = DSTupleSeedFor(blob);
                return ThetaGuard([&]()
                                  { return Compact::deserialize(blob.GetData(), blob.GetSize(), seed, datasketches::serde<double>(), Alloc(DSArenaMode())); },
                                  KIND);
            }

            static Compact Empty(uint8_t lg_k, uint64_t seed, const DSThetaParams &)
            {
                DSTupleUpdate::builder b{DSTupleDoublePolicy(), Alloc(DSArenaMode())};
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
                DSTupleThrowSeedMismatch(function_name, a, b);
            }

            static void ThrowNumValues(const char *, idx_t, idx_t) {}
        };

        template <class KEY>
        using DSTupleState = DSThetaFamilyState<DSTupleTraits, KEY>;
        using DSTupleMergeState = DSTupleState<string_t>;
        using DSTupleIntersectState = DSThetaFamilyIntersectState<DSTupleTraits>;
        // merging the same sketch repeatedly is not idempotent for mode sum
        using DSTupleMergeOperation = DSThetaFamilyOperation<false>;
        using DSTupleIntersectOperation = DSThetaFamilyIntersectOperation<false>;

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
            return DSDescription(description, examples, names, types, DSTupleCategories());
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
                using STATE = DSTupleState<T>;
                using OP = DSThetaFamilyCreatePairOperation;
                auto fun = AggregateFunction::BinaryAggregate<STATE, T, double, string_t, OP>(key_type, LogicalType::DOUBLE, sketch_type);
                DSThetaSetStateExport<STATE>(fun);
                auto signature = DSTupleSignature(with_lg_k, {key_type, LogicalType::DOUBLE}, sketch_type);
                DSTupleAddMode(signature);
                DSTupleAddSeedAndP(signature);
                fun.GetSignature() = std::move(signature);
                if (with_lg_k)
                {
                    fun.SetBindCallback(DSTupleBindBuild<true>);
                    DSReadDataArgumentsAt<1, 2, STATE, T, double, OP>(fun);
                }
                else
                {
                    fun.SetBindCallback(DSTupleBindBuild<false>);
                    DSReadDataArgumentsAt<0, 1, STATE, T, double, OP>(fun);
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
                auto fun = AggregateFunction::UnaryAggregate<DSTupleMergeState, string_t, string_t, DSTupleMergeOperation>(sketch_type, sketch_type);
                DSThetaSetStateExport<DSTupleMergeState>(fun);
                auto signature = DSTupleSignature(with_lg_k, {sketch_type}, sketch_type);
                DSTupleAddMode(signature);
                fun.GetSignature() = std::move(signature);
                if (with_lg_k)
                {
                    fun.SetBindCallback(DSTupleBindMerge<true>);
                    DSReadDataArgumentAt<1, DSTupleMergeState, string_t, DSTupleMergeOperation>(fun);
                    descs.push_back(DSTupleDescription("Merges (unions) Tuple sketches into one with the given log2 of nominal entries (lg_k); the "
                                                       "summaries of a key are combined by mode := 'sum' (default), 'min', 'max' or 'one'",
                                                       {ex.QueryPartials("datasketch_tuple(16, sketch)"), ex.QueryPartials("datasketch_tuple(16, sketch, mode := 'max')")},
                                                       {"lg_k", "sketch", "mode"}, {LogicalType::INTEGER, sketch_type, LogicalType::VARCHAR}));
                }
                else
                {
                    fun.SetBindCallback(DSTupleBindMerge<false>);
                    DSReadDataArgumentAt<0, DSTupleMergeState, string_t, DSTupleMergeOperation>(fun);
                    descs.push_back(DSTupleDescription("Merges (unions) Tuple sketches into one, keeping as many entries as the largest input "
                                                       "(at least lg_k 12); the summaries of a key are combined by mode := 'sum' (default), 'min', 'max' or 'one'",
                                                       {ex.QueryPartials("datasketch_tuple(sketch)"), ex.QueryPartials("datasketch_tuple(sketch, mode := 'max')")},
                                                       {"sketch", "mode"}, {sketch_type, LogicalType::VARCHAR}));
                }
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                set.AddFunction(fun);
            }
            DSRegisterAggregates(loader, std::move(set), std::move(descs));
        }

        // --- datasketch_tuple_intersect_agg
        {
            AggregateFunctionSet set("datasketch_tuple_intersect_agg");
            auto fun = AggregateFunction::UnaryAggregate<DSTupleIntersectState, string_t, string_t, DSTupleIntersectOperation>(sketch_type, sketch_type);
            DSThetaSetStateExport<DSTupleIntersectState>(fun);
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
            DSRegisterAggregates(loader, std::move(set), std::move(descs));
        }

        // --- set operations
        {
            ScalarFunctionSet set("datasketch_tuple_union");
            set.AddFunction(DSTupleVariadic(ScalarFunction("datasketch_tuple_union", {sketch_type, sketch_type}, sketch_type, DSTupleUnionFunction),
                                            {"sketch_a", "sketch_b"}, sketch_type));
            set.AddFunction(DSTupleVariadic(ScalarFunction("datasketch_tuple_union", {LogicalType::INTEGER, sketch_type, sketch_type}, sketch_type, DSTupleUnionWithKFunction),
                                            {"lg_k", "sketch_a", "sketch_b"}, sketch_type));
            DSRegisterScalars(loader, std::move(set),
                                {DSTupleDescription("Returns the union of two or more Tuple sketches, keeping as many entries as the largest input (at least lg_k 12); "
                                                    "the summaries of a key are combined by mode := 'sum' (default), 'min', 'max' or 'one'",
                                                    {ex.Query("datasketch_tuple_union(sketch_a, sketch_b)"), ex.Query("datasketch_tuple_union(sketch_a, sketch_b, sketch_c, mode := 'max')")},
                                                    {"sketch_a", "sketch_b", "mode"}, {sketch_type, sketch_type, LogicalType::VARCHAR}),
                                 DSTupleDescription("Returns the union of two or more Tuple sketches with the given log2 of nominal entries (lg_k, 4-26); "
                                                    "the summaries of a key are combined by mode := 'sum' (default), 'min', 'max' or 'one'",
                                                    {ex.Query("datasketch_tuple_union(16, sketch_a, sketch_b)")},
                                                    {"lg_k", "sketch_a", "sketch_b", "mode"}, {LogicalType::INTEGER, sketch_type, sketch_type, LogicalType::VARCHAR})});
        }
        DSRegisterScalar(loader, DSTupleVariadic(ScalarFunction("datasketch_tuple_intersect", {sketch_type, sketch_type}, sketch_type, DSTupleIntersectFunction), {"sketch_a", "sketch_b"}, sketch_type),
                            DSTupleDescription("Returns the intersection of two or more Tuple sketches: the keys present in every sketch, with their summaries "
                                               "combined by mode := 'sum' (default), 'min', 'max' or 'one'",
                                               {ex.Query("datasketch_tuple_intersect(sketch_a, sketch_b)"), ex.Query("datasketch_tuple_intersect(sketch_a, sketch_b, mode := 'min')")},
                                               {"sketch_a", "sketch_b", "mode"}, {sketch_type, sketch_type, LogicalType::VARCHAR}));

        // --- summaries
        {
            child_list_t<LogicalType> entry;
            entry.emplace_back("hash", LogicalType::UBIGINT);
            entry.emplace_back("summary", LogicalType::DOUBLE);
            DSRegisterScalar(loader, ScalarFunction("datasketch_tuple_summaries", {sketch_type}, LogicalType::LIST(LogicalType::STRUCT(std::move(entry))), DSTupleSummaries),
                                DSTupleDescription("Returns the retained entries of the Tuple sketch as a list of {hash, summary} structs (the key hash and its summary)",
                                                   {ex.Query("datasketch_tuple_summaries(sketch)")}, {"sketch"}, {sketch_type}));
        }
        DSRegisterScalar(loader, ScalarFunction("datasketch_tuple_sum", {sketch_type}, LogicalType::DOUBLE, DSTupleSum),
                            DSTupleDescription("Returns the estimated total of the summaries over all distinct keys: the sum of the retained summaries divided by "
                                               "theta (with mode 'sum', the estimated sum of the values; exact when the sketch is not in estimation mode)",
                                               {ex.Query("datasketch_tuple_sum(sketch)")}, {"sketch"}, {sketch_type}));
        DSRegisterScalar(loader, ScalarFunction("datasketch_tuple_mean", {sketch_type}, LogicalType::DOUBLE, DSTupleMean),
                            DSTupleDescription("Returns the mean summary per distinct key (the mean of the retained summaries), or NULL if the sketch retains no entries",
                                               {ex.Query("datasketch_tuple_mean(sketch)")}, {"sketch"}, {sketch_type}));

        DSTupleScalars::Register(loader, sketch_type, theta_type, "Tuple sketch", DSTupleCategories(), ex);
    }
}
