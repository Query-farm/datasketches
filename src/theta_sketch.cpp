#include "datasketches_extension.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/parsed_data/create_aggregate_function_info.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/string_vector.hpp"
#include "duckdb/common/vector/struct_vector.hpp"
#include "duckdb/common/vector/unified_vector_format.hpp"
#include "datasketches_theta_common.hpp"
#include "datasketches_theta_arena.hpp"

// Apache DataSketches Headers
#include <DataSketches/theta_sketch.hpp>
#include <DataSketches/theta_union.hpp>
#include <DataSketches/theta_intersection.hpp>
#include <DataSketches/theta_a_not_b.hpp>
#include <DataSketches/theta_jaccard_similarity.hpp>

#include <cstring>

namespace duckdb
{
    namespace
    {
        // The library's classes, allocating through DuckDB's allocator (see DSAllocator).
        using ThetaAllocator = DSAllocator<uint64_t>;
        using compact_theta_sketch = datasketches::compact_theta_sketch_alloc<ThetaAllocator>;
        using theta_a_not_b = datasketches::theta_a_not_b_alloc<ThetaAllocator>;
        using theta_intersection = datasketches::theta_intersection_alloc<ThetaAllocator>;
        using theta_jaccard_similarity = datasketches::theta_jaccard_similarity_alloc<ThetaAllocator>;
        using theta_union = datasketches::theta_union_alloc<ThetaAllocator>;
        using update_theta_sketch = datasketches::update_theta_sketch_alloc<ThetaAllocator>;
        using wrapped_compact_theta_sketch = datasketches::wrapped_compact_theta_sketch_alloc<ThetaAllocator>;
        using ThetaSketchBase = datasketches::base_theta_sketch_alloc<ThetaAllocator>;


        // ============================================================
        // 1. Seeds, (de)serialization and sketch access
        // ============================================================


        compact_theta_sketch ThetaDeserialize(const string_t &blob)
        {
            auto data = blob.GetData();
            auto size = blob.GetSize();
            auto seed = ThetaSeedForHash(ThetaPeekSeedHash(data, size));
            return ThetaGuard([&]()
                              { return compact_theta_sketch::deserialize(data, size, seed); });
        }

        // Read-only access that does not copy the retained hashes: a wrapped sketch reads the serialized bytes (also
        // compressed ones) in place. It reads them as 64-bit words, so a blob that is not 8-byte aligned (DuckDB does
        // not align string data) is copied into an aligned buffer first.
        template <class FUNC>
        auto ThetaWithWrapped(const string_t &blob, FUNC &&fun) -> decltype(fun(std::declval<const wrapped_compact_theta_sketch &>()))
        {
            const char *data = blob.GetData();
            const idx_t size = blob.GetSize();
            std::vector<uint64_t> aligned;
            if (size > 0 && reinterpret_cast<uintptr_t>(data) % alignof(uint64_t) != 0)
            {
                aligned.resize((size + sizeof(uint64_t) - 1) / sizeof(uint64_t));
                memcpy(aligned.data(), data, size);
                data = reinterpret_cast<const char *>(aligned.data());
            }
            auto seed = ThetaSeedForHash(ThetaPeekSeedHash(data, size));
            return ThetaGuard([&]()
                              {
                                  const auto sketch = wrapped_compact_theta_sketch::wrap(data, size, seed);
                                  return fun(sketch); });
        }

        // Stored sketches use the compressed (serial version 4) format whenever it applies; every reader accepts
        // serial versions 1-4, so sketches stored by earlier releases stay readable.
        string_t ThetaSerialize(Vector &result, const compact_theta_sketch &sketch)
        {
            auto bytes = sketch.serialize_compressed();
            return StringVector::AddStringOrBlob(result, reinterpret_cast<const char *>(bytes.data()), bytes.size());
        }

        // ============================================================
        // 2. Bind Data
        // ============================================================

        using DSThetaBindData = DSThetaFamilyBindData;

        uint8_t DSThetaBindLgK(BindAggregateFunctionInput &input, idx_t index)
        {
            auto value = input.GetNonNullConstant(index);
            auto lg_k = value.GetValue<int64_t>();
            if (lg_k < datasketches::theta_constants::MIN_LG_K || lg_k > datasketches::theta_constants::MAX_LG_K)
            {
                throw BinderException("Theta sketch lg_k must be between %d and %d, got %lld",
                                      datasketches::theta_constants::MIN_LG_K, datasketches::theta_constants::MAX_LG_K, lg_k);
            }
            return static_cast<uint8_t>(lg_k);
        }

        // Building from values: optional leading lg_k, keyword-only seed and p (these stay in the argument list).
        template <bool HAS_LG_K>
        unique_ptr<FunctionData> DSThetaBindBuild(BindAggregateFunctionInput &input)
        {
            auto result = make_uniq<DSThetaBindData>();
            result->lg_k = HAS_LG_K ? DSThetaBindLgK(input, 0) : DS_THETA_DEFAULT_LG_K;
            result->seed = static_cast<uint64_t>(input.GetNonNullConstant(Identifier("seed")).GetValue<int64_t>());
            auto p = input.GetNonNullConstant(Identifier("p")).GetValue<double>();
            if (!(p > 0.0 && p <= 1.0))
            {
                throw BinderException("Theta sketch sampling probability p must be in (0, 1], got %f", p);
            }
            result->p = static_cast<float>(p);
            DSThetaRecordConstants(input, *result, HAS_LG_K ? 1 : 0, HAS_LG_K ? 2 : 1);
            return std::move(result);
        }

        // Merging sketches with an explicit lg_k. The lg_k stays in the argument list (the update reads the sketch
        // at index 1): bind data is not part of the serialized plan, so the optimizer would otherwise treat
        // aggregates that differ only in lg_k as identical.
        unique_ptr<FunctionData> DSThetaBindMergeWithK(BindAggregateFunctionInput &input)
        {
            auto result = make_uniq<DSThetaBindData>();
            result->lg_k = DSThetaBindLgK(input, 0);
            DSThetaRecordConstants(input, *result, 1, 2);
            return std::move(result);
        }

        // Merging sketches: the union grows to the lg_k of the sketches merged.
        unique_ptr<FunctionData> DSThetaBindMergeDefault(BindAggregateFunctionInput &input)
        {
            auto result = make_uniq<DSThetaBindData>();
            result->infer_lg_k = true;
            return std::move(result);
        }

        // ============================================================
        // 3. Aggregate states (see datasketches_theta_arena.hpp)
        // ============================================================

        struct DSThetaTraits
        {
            using Alloc = ThetaAllocator;
            using Entry = uint64_t;
            using Update = update_theta_sketch;
            using Compact = compact_theta_sketch;
            using Union = theta_union;
            using Intersection = theta_intersection;
            static constexpr bool SET_ONLY = true;
            static constexpr bool HAS_NUM_VALUES = false;
            static constexpr const char *KIND = "Theta";
            static constexpr const char *BUILD_NAME = "datasketch_theta";
            static constexpr const char *INTERSECT_NAME = "datasketch_theta_intersect_agg";

            static uint64_t Key(const Entry &entry) { return entry; }
            static idx_t SummarySize(const DSThetaParams &) { return 0; }
            static void WriteSummary(const Entry &, data_ptr_t, const DSThetaParams &) {}
            static Entry MakeEntry(uint64_t key, const_data_ptr_t, const DSThetaParams &) { return key; }
            static void Fold(data_ptr_t, const Entry &, const DSThetaParams &) {}
            static idx_t PayloadSize(const DSThetaParams &) { return 0; }
            static uint8_t NumValues(const Compact &) { return 1; }

            template <class KEY>
            static void Apply(Update &sketch, const_data_ptr_t key, uint32_t length, const_data_ptr_t, const DSThetaParams &)
            {
                if constexpr (std::is_same<KEY, string_t>::value)
                {
                    sketch.update(key, length);
                }
                else
                {
                    KEY value;
                    memcpy(&value, key, sizeof(KEY));
                    sketch.update(value);
                }
            }

            static Update NewUpdate(const DSThetaBindData &bind, const DSThetaParams &)
            {
                update_theta_sketch::builder b{Alloc(DSArenaMode())};
                b.set_lg_k(bind.lg_k);
                b.set_seed(bind.seed);
                b.set_p(bind.p);
                return b.build();
            }

            static Union NewUnion(uint8_t lg_k, uint64_t seed, const DSThetaParams &)
            {
                theta_union::builder b{Alloc(DSArenaMode())};
                b.set_lg_k(lg_k);
                b.set_seed(seed);
                return b.build();
            }

            static Intersection NewIntersection(uint64_t seed, const DSThetaParams &)
            {
                return Intersection(seed, Alloc(DSArenaMode()));
            }

            static Compact Deserialize(const string_t &blob)
            {
                auto data = blob.GetData();
                auto size = blob.GetSize();
                auto seed = ThetaSeedForHash(ThetaPeekSeedHash(data, size));
                return ThetaGuard([&]()
                                  { return Compact::deserialize(data, size, seed, Alloc(DSArenaMode())); });
            }

            static Compact Empty(uint8_t lg_k, uint64_t seed, const DSThetaParams &)
            {
                update_theta_sketch::builder b{Alloc(DSArenaMode())};
                b.set_lg_k(lg_k);
                b.set_seed(seed);
                return b.build().compact();
            }

            static auto Serialize(const Compact &sketch) -> decltype(sketch.serialize_compressed())
            {
                return sketch.serialize_compressed();
            }

            static void ThrowSeedMismatch(const char *function_name, uint16_t a, uint16_t b)
            {
                if (string(function_name) == BUILD_NAME)
                {
                    throw InvalidInputException("datasketch_theta: cannot merge Theta sketches built with different seeds (seed hashes %d and %d)", a, b);
                }
                throw InvalidInputException("%s: cannot combine Theta sketches built with different seeds (seed hashes %d and %d)", function_name, a, b);
            }

            static void ThrowNumValues(const char *, idx_t, idx_t) {}
        };

        template <class KEY>
        using DSThetaState = DSThetaFamilyState<DSThetaTraits, KEY>;
        using DSThetaIntersectState = DSThetaFamilyIntersectState<DSThetaTraits>;
        using DSThetaMergeOperation = DSThetaFamilyOperation<true>;
        using DSThetaIntersectOperation = DSThetaFamilyIntersectOperation<true>;

        // ============================================================
        // 5. Scalar Functions
        // ============================================================


        compact_theta_sketch ThetaUnionOf(const vector<string_t> &blobs, optional_idx explicit_lg_k)
        {
            vector<compact_theta_sketch> sketches;
            sketches.reserve(blobs.size());
            ThetaSeedTracker seeds("datasketch_theta_union");
            uint8_t lg_k = DS_THETA_DEFAULT_LG_K;
            for (auto &blob : blobs)
            {
                sketches.push_back(ThetaDeserialize(blob));
                seeds.Add(sketches.back());
                lg_k = MaxValue(lg_k, ThetaInferLgK(sketches.back()));
            }
            if (explicit_lg_k.IsValid())
            {
                lg_k = static_cast<uint8_t>(explicit_lg_k.GetIndex());
            }
            return ThetaGuard([&]()
                              {
                                  theta_union::builder b;
                                  b.set_lg_k(lg_k);
                                  b.set_seed(seeds.Seed());
                                  auto union_obj = b.build();
                                  for (auto &sketch : sketches)
                                  {
                                      union_obj.update(sketch);
                                  }
                                  return union_obj.get_result(); });
        }

        void DSThetaUnion(DataChunk &args, ExpressionState &state, Vector &result)
        {
            auto result_data = FlatVector::GetDataMutable<string_t>(result);
            ThetaForEachRow(args, result, 0, 0, [&](idx_t row, const ThetaRowArgs &row_args)
                            { result_data[row] = ThetaSerialize(result, ThetaUnionOf(row_args.sketches, optional_idx())); });
        }

        void DSThetaUnionWithK(DataChunk &args, ExpressionState &state, Vector &result)
        {
            auto result_data = FlatVector::GetDataMutable<string_t>(result);
            ThetaForEachRow(args, result, 1, 0, [&](idx_t row, const ThetaRowArgs &row_args)
                            {
                                auto lg_k = row_args.Get<int32_t>(0);
                                ThetaCheckLgK(lg_k, "datasketch_theta_union");
                                result_data[row] = ThetaSerialize(result, ThetaUnionOf(row_args.sketches, optional_idx(static_cast<idx_t>(lg_k)))); });
        }

        void DSThetaIntersect(DataChunk &args, ExpressionState &state, Vector &result)
        {
            auto result_data = FlatVector::GetDataMutable<string_t>(result);
            ThetaForEachRow(args, result, 0, 0, [&](idx_t row, const ThetaRowArgs &row_args)
                            {
                                vector<compact_theta_sketch> sketches;
                                sketches.reserve(row_args.sketches.size());
                                ThetaSeedTracker seeds("datasketch_theta_intersect");
                                for (auto &blob : row_args.sketches)
                                {
                                    sketches.push_back(ThetaDeserialize(blob));
                                    seeds.Add(sketches.back());
                                }
                                auto intersected = ThetaGuard([&]()
                                                              {
                                                                  theta_intersection intersection(seeds.Seed());
                                                                  for (auto &sketch : sketches)
                                                                  {
                                                                      intersection.update(sketch);
                                                                  }
                                                                  return intersection.get_result(); });
                                result_data[row] = ThetaSerialize(result, intersected); });
        }

        void DSThetaANotB(DataChunk &args, ExpressionState &state, Vector &result)
        {
            auto result_data = FlatVector::GetDataMutable<string_t>(result);
            ThetaForEachRow(args, result, 0, 2, [&](idx_t row, const ThetaRowArgs &row_args)
                            {
                                auto sketch_a = ThetaDeserialize(row_args.sketches[0]);
                                auto sketch_b = ThetaDeserialize(row_args.sketches[1]);
                                ThetaSeedTracker seeds("datasketch_theta_a_not_b");
                                seeds.Add(sketch_a);
                                seeds.Add(sketch_b);
                                auto difference = ThetaGuard([&]()
                                                             {
                                                                 theta_a_not_b a_not_b(seeds.Seed());
                                                                 return a_not_b.compute(sketch_a, sketch_b); });
                                result_data[row] = ThetaSerialize(result, difference); });
        }

        // --- SIMILARITY (Jaccard), on wrapped sketches

        // Runs fun(sketch_a, sketch_b, seed) on two wrapped sketches built with the same seed.
        template <class FUNC>
        auto ThetaWithPair(const string_t &a_blob, const string_t &b_blob, const char *function_name, FUNC &&fun)
            -> decltype(fun(std::declval<const wrapped_compact_theta_sketch &>(), std::declval<const wrapped_compact_theta_sketch &>(), uint64_t(0)))
        {
            return ThetaWithWrapped(a_blob, [&](const wrapped_compact_theta_sketch &sketch_a)
                                    { return ThetaWithWrapped(b_blob, [&](const wrapped_compact_theta_sketch &sketch_b)
                                                              {
                                                                  ThetaSeedTracker seeds(function_name);
                                                                  seeds.Add(sketch_a);
                                                                  seeds.Add(sketch_b);
                                                                  return fun(sketch_a, sketch_b, seeds.Seed()); }); });
        }

        void DSThetaJaccard(DataChunk &args, ExpressionState &state, Vector &result)
        {
            auto &children = StructVector::GetEntries(result);
            auto lower = FlatVector::GetDataMutable<double>(children[0]);
            auto estimate = FlatVector::GetDataMutable<double>(children[1]);
            auto upper = FlatVector::GetDataMutable<double>(children[2]);
            ThetaForEachRow(args, result, 0, 2, [&](idx_t row, const ThetaRowArgs &row_args)
                            {
                                auto bounds = ThetaWithPair(row_args.sketches[0], row_args.sketches[1], "datasketch_theta_jaccard",
                                                            [&](const wrapped_compact_theta_sketch &a, const wrapped_compact_theta_sketch &b, uint64_t seed)
                                                            { return theta_jaccard_similarity::jaccard(a, b, seed); });
                                lower[row] = bounds[0];
                                estimate[row] = bounds[1];
                                upper[row] = bounds[2]; });
        }

        void DSThetaExactlyEqual(DataChunk &args, ExpressionState &state, Vector &result)
        {
            auto result_data = FlatVector::GetDataMutable<bool>(result);
            ThetaForEachRow(args, result, 0, 2, [&](idx_t row, const ThetaRowArgs &row_args)
                            { result_data[row] = ThetaWithPair(row_args.sketches[0], row_args.sketches[1], "datasketch_theta_exactly_equal",
                                                               [&](const wrapped_compact_theta_sketch &a, const wrapped_compact_theta_sketch &b, uint64_t seed)
                                                               { return theta_jaccard_similarity::exactly_equal(a, b, seed); }); });
        }

        template <bool SIMILARITY>
        void DSThetaSimilarityTest(DataChunk &args, ExpressionState &state, Vector &result)
        {
            const char *function_name = SIMILARITY ? "datasketch_theta_similarity_test" : "datasketch_theta_dissimilarity_test";
            auto result_data = FlatVector::GetDataMutable<bool>(result);
            ThetaForEachRow(args, result, 0, 2, [&](idx_t row, const ThetaRowArgs &row_args)
                            {
                                auto threshold = row_args.Get<double>(2);
                                result_data[row] = ThetaWithPair(row_args.sketches[0], row_args.sketches[1], function_name,
                                                                 [&](const wrapped_compact_theta_sketch &actual, const wrapped_compact_theta_sketch &expected, uint64_t seed)
                                                                 {
                                                                     return SIMILARITY ? theta_jaccard_similarity::similarity_test(actual, expected, threshold, seed)
                                                                                       : theta_jaccard_similarity::dissimilarity_test(actual, expected, threshold, seed); }); });
        }

        // --- READ-ONLY FUNCTIONS, on wrapped sketches

        template <class RESULT_TYPE, class FUNC>
        void ThetaUnaryRead(DataChunk &args, Vector &result, FUNC &&fun)
        {
            UnaryExecutor::Execute<string_t, RESULT_TYPE>(args.data[0], result, [&](string_t sketch_blob)
                                                          { return ThetaWithWrapped(sketch_blob, fun); });
        }

        void DSThetaEstimate(DataChunk &args, ExpressionState &state, Vector &result)
        {
            ThetaUnaryRead<double>(args, result, [](const wrapped_compact_theta_sketch &sketch)
                                   { return sketch.get_estimate(); });
        }

        template <bool LOWER>
        void DSThetaBound(DataChunk &args, ExpressionState &state, Vector &result)
        {
            BinaryExecutor::Execute<string_t, int32_t, double>(args.data[0], args.data[1], result, [&](string_t sketch_blob, int32_t num_std_devs)
                                                               {
                                                                   auto std_devs = DSCheckNumStdDevs(num_std_devs, LOWER ? "datasketch_theta_lower_bound" : "datasketch_theta_upper_bound");
                                                                   return ThetaWithWrapped(sketch_blob, [&](const wrapped_compact_theta_sketch &sketch)
                                                                                           { return LOWER ? sketch.get_lower_bound(std_devs) : sketch.get_upper_bound(std_devs); }); });
        }

        void DSThetaDescribe(DataChunk &args, ExpressionState &state, Vector &result)
        {
            ThetaUnaryRead<string_t>(args, result, [&](const wrapped_compact_theta_sketch &sketch)
                                     {
                                         auto description = sketch.to_string(false);
                                         return StringVector::AddString(result, description.data(), description.size()); });
        }

        void DSThetaIsEmpty(DataChunk &args, ExpressionState &state, Vector &result)
        {
            ThetaUnaryRead<bool>(args, result, [](const wrapped_compact_theta_sketch &sketch)
                                 { return sketch.is_empty(); });
        }

        void DSThetaIsEstimation(DataChunk &args, ExpressionState &state, Vector &result)
        {
            ThetaUnaryRead<bool>(args, result, [](const wrapped_compact_theta_sketch &sketch)
                                 { return sketch.is_estimation_mode(); });
        }

        void DSThetaGetTheta(DataChunk &args, ExpressionState &state, Vector &result)
        {
            ThetaUnaryRead<double>(args, result, [](const wrapped_compact_theta_sketch &sketch)
                                   { return sketch.get_theta(); });
        }

        void DSThetaNumRetained(DataChunk &args, ExpressionState &state, Vector &result)
        {
            ThetaUnaryRead<int64_t>(args, result, [](const wrapped_compact_theta_sketch &sketch)
                                    { return static_cast<int64_t>(sketch.get_num_retained()); });
        }

        void DSThetaSeedHash(DataChunk &args, ExpressionState &state, Vector &result)
        {
            ThetaUnaryRead<int64_t>(args, result, [](const wrapped_compact_theta_sketch &sketch)
                                    { return static_cast<int64_t>(sketch.get_seed_hash()); });
        }

        // ============================================================
        // 6. Type Creation & Registration Helpers
        // ============================================================

        FunctionDescription ThetaDescription(string description, vector<string> examples, vector<string> parameter_names,
                                             vector<LogicalType> parameter_types)
        {
            return DSDescription(std::move(description), std::move(examples), std::move(parameter_names),
                                 std::move(parameter_types), {"sketch", "theta"});
        }


        const char *ThetaBuildDescription(bool with_lg_k)
        {
            return with_lg_k ? "Creates a Theta sketch with the given log2 of nominal entries (lg_k, 4-26) for estimating the number of distinct "
                               "values and for set operations. Optional keyword arguments: seed (hash seed; sketches can only be combined "
                               "with sketches built with the same seed) and p (sampling probability in (0, 1], to pre-sample the input)"
                             : "Creates a Theta sketch (lg_k 12) for estimating the number of distinct values and for set operations. Optional "
                               "keyword arguments: seed (hash seed; sketches can only be combined with sketches built with the same seed) "
                               "and p (sampling probability in (0, 1], to pre-sample the input)";
        }

        // Adds datasketch_theta([lg_k,] data, seed := ..., p := ...) for one input type.
        template <typename T>
        void RegisterThetaBuild(AggregateFunctionSet &set, vector<FunctionDescription> &descs, const string &name,
                                const LogicalType &input_type, const LogicalType &sketch_type)
        {
            for (bool with_lg_k : {false, true})
            {
                using STATE = DSThetaState<T>;
                auto fun = AggregateFunction::UnaryAggregate<STATE, T, string_t, DSThetaFamilyCreateOperation>(input_type, sketch_type);
                DSThetaSetStateExport<STATE>(fun);
                FunctionSignature signature(vector<LogicalType>(), sketch_type);
                if (with_lg_k)
                {
                    signature.AddParameter(LogicalType::INTEGER);
                    DSReadDataArgumentAt<1, STATE, T, DSThetaFamilyCreateOperation>(fun);
                    fun.SetBindCallback(DSThetaBindBuild<true>);
                }
                else
                {
                    fun.SetBindCallback(DSThetaBindBuild<false>);
                }
                signature.AddParameter(input_type);
                signature.AddKeywordOnly("seed", LogicalType::BIGINT, Value::BIGINT(static_cast<int64_t>(DS_THETA_DEFAULT_SEED)));
                signature.AddKeywordOnly("p", LogicalType::DOUBLE, Value::DOUBLE(1.0));
                fun.GetSignature() = std::move(signature);
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                set.AddFunction(fun);

                // Runnable examples over inline data (BLOB input hashes the bytes of each value)
                const string value = input_type.id() == LogicalTypeId::BLOB ? "i::VARCHAR::BLOB" : "i";
                const string rows = ") FROM range(1000) t(i)";
                if (with_lg_k)
                {
                    descs.push_back(ThetaDescription(ThetaBuildDescription(true),
                                                     {"SELECT " + name + "(14, " + value + rows, "SELECT " + name + "(14, " + value + ", seed := 42, p := 0.5" + rows},
                                                     {"lg_k", "data", "seed", "p"},
                                                     {LogicalType::INTEGER, input_type, LogicalType::BIGINT, LogicalType::DOUBLE}));
                }
                else
                {
                    descs.push_back(ThetaDescription(ThetaBuildDescription(false),
                                                     {"SELECT " + name + "(" + value + rows, "SELECT " + name + "(" + value + ", seed := 42" + rows},
                                                     {"data", "seed", "p"},
                                                     {input_type, LogicalType::BIGINT, LogicalType::DOUBLE}));
                }
            }
        }
    }

    // ============================================================
    // 7. Main Loader
    // ============================================================

    void LoadThetaSketch(ExtensionLoader &loader)
    {
        auto sketch_type = DSCreateSketchType(loader, "sketch_theta", "Sketch type for Theta Sketch");
        const DSExampleSources ex("datasketch_theta(i)");

        // --- datasketch_theta: build from values, or merge (union) sketches
        {
            AggregateFunctionSet sketch_agg("datasketch_theta");
            vector<FunctionDescription> agg_descs;

            // IMPORTANT: do not register BLOB input here: a plain BLOB would then be hashed instead of merged as a
            // serialized sketch (e.g. sketches read back from Parquet). BLOB values are hashed by datasketch_theta_blob.
            RegisterThetaBuild<int8_t>(sketch_agg, agg_descs, "datasketch_theta", LogicalType::TINYINT, sketch_type);
            RegisterThetaBuild<int16_t>(sketch_agg, agg_descs, "datasketch_theta", LogicalType::SMALLINT, sketch_type);
            RegisterThetaBuild<int32_t>(sketch_agg, agg_descs, "datasketch_theta", LogicalType::INTEGER, sketch_type);
            RegisterThetaBuild<int64_t>(sketch_agg, agg_descs, "datasketch_theta", LogicalType::BIGINT, sketch_type);
            RegisterThetaBuild<uint8_t>(sketch_agg, agg_descs, "datasketch_theta", LogicalType::UTINYINT, sketch_type);
            RegisterThetaBuild<uint16_t>(sketch_agg, agg_descs, "datasketch_theta", LogicalType::USMALLINT, sketch_type);
            RegisterThetaBuild<uint32_t>(sketch_agg, agg_descs, "datasketch_theta", LogicalType::UINTEGER, sketch_type);
            RegisterThetaBuild<uint64_t>(sketch_agg, agg_descs, "datasketch_theta", LogicalType::UBIGINT, sketch_type);
            RegisterThetaBuild<float>(sketch_agg, agg_descs, "datasketch_theta", LogicalType::FLOAT, sketch_type);
            RegisterThetaBuild<double>(sketch_agg, agg_descs, "datasketch_theta", LogicalType::DOUBLE, sketch_type);
            RegisterThetaBuild<string_t>(sketch_agg, agg_descs, "datasketch_theta", LogicalType::VARCHAR, sketch_type);

            using MERGE_STATE = DSThetaState<string_t>;
            auto fun_merge = AggregateFunction::UnaryAggregate<MERGE_STATE, string_t, string_t, DSThetaMergeOperation>(sketch_type, sketch_type);
            DSThetaSetStateExport<MERGE_STATE>(fun_merge);
            fun_merge.SetBindCallback(DSThetaBindMergeDefault);
            fun_merge.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
            DSSetArguments(fun_merge, {sketch_type});
            sketch_agg.AddFunction(fun_merge);
            agg_descs.push_back(ThetaDescription("Merges (unions) Theta sketches into a single sketch. The result keeps as many entries as the "
                                                 "largest input sketch (inferred from the sketches, at least lg_k 12)",
                                                 {ex.QueryPartials("datasketch_theta(sketch)")}, {"sketch"}, {sketch_type}));

            auto fun_merge_k = AggregateFunction::UnaryAggregate<MERGE_STATE, string_t, string_t, DSThetaMergeOperation>(sketch_type, sketch_type);
            DSThetaSetStateExport<MERGE_STATE>(fun_merge_k);
            fun_merge_k.SetBindCallback(DSThetaBindMergeWithK);
            fun_merge_k.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
            DSSetArguments(fun_merge_k, {LogicalType::INTEGER, sketch_type});
            DSReadDataArgumentAt<1, MERGE_STATE, string_t, DSThetaMergeOperation>(fun_merge_k);
            sketch_agg.AddFunction(fun_merge_k);
            agg_descs.push_back(ThetaDescription("Merges (unions) Theta sketches into a single sketch with the given log2 of nominal entries (lg_k)",
                                                 {ex.QueryPartials("datasketch_theta(16, sketch)")}, {"lg_k", "sketch"}, {LogicalType::INTEGER, sketch_type}));

            DSRegisterAggregates(loader, std::move(sketch_agg), std::move(agg_descs));
        }

        // --- datasketch_theta_blob: build from raw BLOB values
        {
            AggregateFunctionSet blob_agg("datasketch_theta_blob");
            vector<FunctionDescription> blob_descs;
            RegisterThetaBuild<string_t>(blob_agg, blob_descs, "datasketch_theta_blob", LogicalType::BLOB, sketch_type);
            for (auto &desc : blob_descs)
            {
                desc.description = "Hashes raw BLOB values (their bytes) into a Theta sketch, like datasketch_theta does for other types "
                                   "(datasketch_theta(BLOB) merges serialized sketches instead). " +
                                   desc.description.substr(desc.description.find("Optional"));
            }
            DSRegisterAggregates(loader, std::move(blob_agg), std::move(blob_descs));
        }

        // --- datasketch_theta_intersect_agg: intersect sketches across rows
        {
            AggregateFunctionSet intersect_agg("datasketch_theta_intersect_agg");
            auto fun = AggregateFunction::UnaryAggregate<DSThetaIntersectState, string_t, string_t, DSThetaIntersectOperation>(sketch_type, sketch_type);
            DSThetaSetStateExport<DSThetaIntersectState>(fun);
            fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
            DSSetArguments(fun, {sketch_type});
            intersect_agg.AddFunction(fun);
            vector<FunctionDescription> descs;
            descs.push_back(ThetaDescription("Intersects Theta sketches across rows: the result estimates the number of distinct values present "
                                             "in every input sketch. NULL sketches are skipped; returns NULL if there are no sketches",
                                             {ex.QueryOverlapping("datasketch_theta_intersect_agg(sketch)")}, {"sketch"}, {sketch_type}));
            DSRegisterAggregates(loader, std::move(intersect_agg), std::move(descs));
        }

        // --- SET OPERATIONS ---
        {
            ScalarFunctionSet set("datasketch_theta_union");
            set.AddFunction(ThetaVariadic(ScalarFunction("datasketch_theta_union", {sketch_type, sketch_type}, sketch_type, DSThetaUnion),
                                          {"sketch_a", "sketch_b"}, sketch_type));
            set.AddFunction(ThetaVariadic(ScalarFunction("datasketch_theta_union", {LogicalType::INTEGER, sketch_type, sketch_type}, sketch_type, DSThetaUnionWithK),
                                          {"lg_k", "sketch_a", "sketch_b"}, sketch_type));
            DSRegisterScalars(loader, std::move(set),
                                {ThetaDescription("Returns a new Theta sketch representing the union of two or more sketches. The result keeps "
                                                  "as many entries as the largest input sketch (inferred from the sketches, at least lg_k 12)",
                                                  {ex.Query("datasketch_theta_union(sketch_a, sketch_b)"), ex.Query("datasketch_theta_union(sketch_a, sketch_b, sketch_c)")},
                                                  {"sketch_a", "sketch_b"}, {sketch_type, sketch_type}),
                                 ThetaDescription("Returns a new Theta sketch representing the union of two or more sketches, with the given "
                                                  "log2 of nominal entries (lg_k, 4-26)",
                                                  {ex.Query("datasketch_theta_union(16, sketch_a, sketch_b)"), ex.Query("datasketch_theta_union(16, sketch_a, sketch_b, sketch_c)")},
                                                  {"lg_k", "sketch_a", "sketch_b"}, {LogicalType::INTEGER, sketch_type, sketch_type})});
        }
        {
            auto fun = ThetaVariadic(ScalarFunction("datasketch_theta_intersect", {sketch_type, sketch_type}, sketch_type, DSThetaIntersect),
                                     {"sketch_a", "sketch_b"}, sketch_type);
            DSRegisterScalar(loader, std::move(fun),
                                ThetaDescription("Returns a new Theta sketch representing the intersection of two or more sketches",
                                                 {ex.Query("datasketch_theta_intersect(sketch_a, sketch_b)"), ex.Query("datasketch_theta_intersect(sketch_a, sketch_b, sketch_c)")},
                                                 {"sketch_a", "sketch_b"}, {sketch_type, sketch_type}));
        }
        DSRegisterScalar(loader, ScalarFunction("datasketch_theta_a_not_b", {sketch_type, sketch_type}, sketch_type, DSThetaANotB),
                            ThetaDescription("Returns a new Theta sketch representing elements in sketch A but not in sketch B (set difference)",
                                             {ex.Query("datasketch_theta_a_not_b(sketch_a, sketch_b)")}, {"sketch_a", "sketch_b"}, {sketch_type, sketch_type}));

        // --- SIMILARITY ---
        {
            child_list_t<LogicalType> bounds;
            bounds.emplace_back("lower", LogicalType::DOUBLE);
            bounds.emplace_back("estimate", LogicalType::DOUBLE);
            bounds.emplace_back("upper", LogicalType::DOUBLE);
            DSRegisterScalar(loader, ScalarFunction("datasketch_theta_jaccard", {sketch_type, sketch_type}, LogicalType::STRUCT(std::move(bounds)), DSThetaJaccard),
                                ThetaDescription("Returns the Jaccard similarity |A ∩ B| / |A ∪ B| of two sketches as a struct of its estimate "
                                                 "and approximate lower and upper bounds (about 2 standard deviations). Two empty sketches "
                                                 "are identical (1.0)",
                                                 {ex.Query("datasketch_theta_jaccard(sketch_a, sketch_b)"), ex.Query("datasketch_theta_jaccard(sketch_a, sketch_b).estimate")},
                                                 {"sketch_a", "sketch_b"}, {sketch_type, sketch_type}));
        }
        DSRegisterScalar(loader, ScalarFunction("datasketch_theta_exactly_equal", {sketch_type, sketch_type}, LogicalType::BOOLEAN, DSThetaExactlyEqual),
                            ThetaDescription("Returns true if the two sketches retain exactly the same hashes with the same theta, i.e. they "
                                             "represent the same set as far as the sketches can tell",
                                             {ex.Query("datasketch_theta_exactly_equal(sketch_a, sketch_b)")}, {"sketch_a", "sketch_b"}, {sketch_type, sketch_type}));
        DSRegisterScalar(loader, ScalarFunction("datasketch_theta_similarity_test", {sketch_type, sketch_type, LogicalType::DOUBLE}, LogicalType::BOOLEAN, DSThetaSimilarityTest<true>),
                            ThetaDescription("Returns true if the Jaccard similarity of the two sketches is at least the threshold, with "
                                             "high confidence (its lower bound is >= threshold)",
                                             {ex.Query("datasketch_theta_similarity_test(actual, expected, 0.9)")}, {"actual", "expected", "threshold"},
                                             {sketch_type, sketch_type, LogicalType::DOUBLE}));
        DSRegisterScalar(loader, ScalarFunction("datasketch_theta_dissimilarity_test", {sketch_type, sketch_type, LogicalType::DOUBLE}, LogicalType::BOOLEAN, DSThetaSimilarityTest<false>),
                            ThetaDescription("Returns true if the Jaccard similarity of the two sketches is at most the threshold, with "
                                             "high confidence (its upper bound is <= threshold)",
                                             {ex.Query("datasketch_theta_dissimilarity_test(actual, expected, 0.1)")}, {"actual", "expected", "threshold"},
                                             {sketch_type, sketch_type, LogicalType::DOUBLE}));

        // --- ESTIMATES ---
        DSRegisterScalar(loader, ScalarFunction("datasketch_theta_estimate", {sketch_type}, LogicalType::DOUBLE, DSThetaEstimate),
                            ThetaDescription("Returns the estimated number of distinct values in the Theta sketch",
                                             {ex.Query("datasketch_theta_estimate(sketch)")}, {"sketch"}, {sketch_type}));
        DSRegisterScalar(loader, ScalarFunction("datasketch_theta_lower_bound", {sketch_type, LogicalType::INTEGER}, LogicalType::DOUBLE, DSThetaBound<true>),
                            ThetaDescription("Returns the lower bound estimate at the given number of standard deviations (1, 2, or 3)",
                                             {ex.Query("datasketch_theta_lower_bound(sketch, 2)")}, {"sketch", "num_std_devs"}, {sketch_type, LogicalType::INTEGER}));
        DSRegisterScalar(loader, ScalarFunction("datasketch_theta_upper_bound", {sketch_type, LogicalType::INTEGER}, LogicalType::DOUBLE, DSThetaBound<false>),
                            ThetaDescription("Returns the upper bound estimate at the given number of standard deviations (1, 2, or 3)",
                                             {ex.Query("datasketch_theta_upper_bound(sketch, 2)")}, {"sketch", "num_std_devs"}, {sketch_type, LogicalType::INTEGER}));
        DSRegisterScalar(loader, ScalarFunction("datasketch_theta_describe", {sketch_type}, LogicalType::VARCHAR, DSThetaDescribe),
                            ThetaDescription("Returns a human-readable description of the Theta sketch",
                                             {ex.Query("datasketch_theta_describe(sketch)")}, {"sketch"}, {sketch_type}));

        // --- METADATA ---
        DSRegisterScalar(loader, ScalarFunction("datasketch_theta_is_empty", {sketch_type}, LogicalType::BOOLEAN, DSThetaIsEmpty),
                            ThetaDescription("Returns true if the Theta sketch is empty",
                                             {ex.Query("datasketch_theta_is_empty(sketch)")}, {"sketch"}, {sketch_type}));
        DSRegisterScalar(loader, ScalarFunction("datasketch_theta_is_estimation_mode", {sketch_type}, LogicalType::BOOLEAN, DSThetaIsEstimation),
                            ThetaDescription("Returns true if the sketch is in estimation mode (has exceeded exact counting capacity)",
                                             {ex.Query("datasketch_theta_is_estimation_mode(sketch)")}, {"sketch"}, {sketch_type}));
        DSRegisterScalar(loader, ScalarFunction("datasketch_theta_get_theta", {sketch_type}, LogicalType::DOUBLE, DSThetaGetTheta),
                            ThetaDescription("Returns the theta value of the sketch (sampling probability)",
                                             {ex.Query("datasketch_theta_get_theta(sketch)")}, {"sketch"}, {sketch_type}));
        DSRegisterScalar(loader, ScalarFunction("datasketch_theta_num_retained", {sketch_type}, LogicalType::BIGINT, DSThetaNumRetained),
                            ThetaDescription("Returns the number of hash values retained in the sketch",
                                             {ex.Query("datasketch_theta_num_retained(sketch)")}, {"sketch"}, {sketch_type}));
        DSRegisterScalar(loader, ScalarFunction("datasketch_theta_seed_hash", {sketch_type}, LogicalType::BIGINT, DSThetaSeedHash),
                            ThetaDescription("Returns the 16-bit hash of the seed the sketch was built with (compact sketches store only "
                                             "this hash, not the seed); sketches can only be combined if their seed hashes match",
                                             {ex.Query("datasketch_theta_seed_hash(sketch)")}, {"sketch"}, {sketch_type}));
        DSRegisterScalar(loader, ScalarFunction("datasketch_theta_get_seed", {sketch_type}, LogicalType::BIGINT, DSThetaSeedHash),
                            ThetaDescription("Alias of datasketch_theta_seed_hash: returns the 16-bit hash of the seed the sketch was built "
                                             "with, not the seed itself (compact sketches do not store the seed)",
                                             {ex.Query("datasketch_theta_get_seed(sketch)")}, {"sketch"}, {sketch_type}));
    }

}
