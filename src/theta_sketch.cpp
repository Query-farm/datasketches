#include "datasketches_extension.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/parsed_data/create_aggregate_function_info.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/string_vector.hpp"
#include "duckdb/common/vector/struct_vector.hpp"
#include "duckdb/common/vector/unified_vector_format.hpp"

// Apache DataSketches Headers
#include <DataSketches/theta_sketch.hpp>
#include <DataSketches/theta_union.hpp>
#include <DataSketches/theta_intersection.hpp>
#include <DataSketches/theta_a_not_b.hpp>
#include <DataSketches/theta_jaccard_similarity.hpp>

#include <cstring>
#include <mutex>
#include <unordered_map>

namespace duckdb
{
    namespace
    {
        using datasketches::compact_theta_sketch;
        using datasketches::theta_a_not_b;
        using datasketches::theta_intersection;
        using datasketches::theta_jaccard_similarity;
        using datasketches::theta_union;
        using datasketches::update_theta_sketch;
        using datasketches::wrapped_compact_theta_sketch;
        using ThetaSketchBase = datasketches::base_theta_sketch_alloc<std::allocator<uint64_t>>;

        // The default nominal size used when building, and the minimum used when combining sketches.
        constexpr uint8_t DS_THETA_DEFAULT_LG_K = 12;
        constexpr uint64_t DS_THETA_DEFAULT_SEED = datasketches::DEFAULT_SEED;

        // ============================================================
        // 1. Seeds, (de)serialization and sketch access
        // ============================================================

        uint16_t ThetaSeedHash(uint64_t seed)
        {
            return compute_seed_hash(seed);
        }

        // A compact Theta sketch only stores a 16-bit hash of the seed it was built with, and every operation on
        // compact sketches (deserialize, wrap, union, intersection, A-not-B, Jaccard) uses the seed only to compare
        // that hash. Only building a sketch from raw values needs the real seed. So that sketches built with a custom
        // seed can be read and combined without repeating the seed in every function call, the readers use a seed
        // with the same hash as the one stored in the sketch: found by a search on first use of each distinct hash
        // (about 65k hash computations), then cached.
        uint64_t ThetaSeedForHash(uint16_t seed_hash)
        {
            static const uint16_t default_seed_hash = ThetaSeedHash(DS_THETA_DEFAULT_SEED);
            if (seed_hash == default_seed_hash)
            {
                return DS_THETA_DEFAULT_SEED;
            }
            static std::mutex lock;
            static std::unordered_map<uint16_t, uint64_t> cache;
            std::lock_guard<std::mutex> guard(lock);
            auto entry = cache.find(seed_hash);
            if (entry != cache.end())
            {
                return entry->second;
            }
            for (uint64_t candidate = 0; candidate < (uint64_t(1) << 32); candidate++)
            {
                if (ThetaSeedHash(candidate) == seed_hash)
                {
                    cache.emplace(seed_hash, candidate);
                    return candidate;
                }
            }
            throw InvalidInputException("Invalid Theta sketch: unsupported seed hash %d", seed_hash);
        }

        // Serial versions 2-4 store the seed hash in bytes 6-7; version 1 has none (and is read with the default seed).
        uint16_t ThetaPeekSeedHash(const char *data, idx_t size)
        {
            if (size >= 8 && static_cast<uint8_t>(data[1]) >= 2)
            {
                uint16_t seed_hash;
                memcpy(&seed_hash, data + 6, sizeof(seed_hash));
                return seed_hash;
            }
            return ThetaSeedHash(DS_THETA_DEFAULT_SEED);
        }

        // Runs fun, reporting errors raised by the DataSketches library (std::exception) as invalid input.
        template <class FUNC>
        auto ThetaGuard(FUNC &&fun) -> decltype(fun())
        {
            try
            {
                return fun();
            }
            catch (Exception &)
            {
                throw;
            }
            catch (std::exception &ex)
            {
                throw InvalidInputException("Theta sketch error: %s", ex.what());
            }
        }

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

        compact_theta_sketch ThetaEmptySketch(uint8_t lg_k, uint64_t seed)
        {
            update_theta_sketch::builder b;
            b.set_lg_k(lg_k);
            b.set_seed(seed);
            return b.build().compact();
        }

        // Tracks the seed of the sketches being combined: empty sketches hold no hashes, so their seed is ignored.
        struct ThetaSeedTracker
        {
            explicit ThetaSeedTracker(const char *function_name) : function_name(function_name) {}

            void Add(const ThetaSketchBase &sketch)
            {
                if (sketch.is_empty())
                {
                    return;
                }
                if (!has_seed_hash)
                {
                    has_seed_hash = true;
                    seed_hash = sketch.get_seed_hash();
                }
                else if (seed_hash != sketch.get_seed_hash())
                {
                    throw InvalidInputException("%s: cannot combine Theta sketches built with different seeds (seed hashes %d and %d)",
                                                function_name, seed_hash, sketch.get_seed_hash());
                }
            }

            uint64_t Seed() const
            {
                return has_seed_hash ? ThetaSeedForHash(seed_hash) : DS_THETA_DEFAULT_SEED;
            }

            const char *function_name;
            bool has_seed_hash = false;
            uint16_t seed_hash = 0;
        };

        void ThetaCheckLgK(int64_t lg_k, const char *function_name)
        {
            if (lg_k < datasketches::theta_constants::MIN_LG_K || lg_k > datasketches::theta_constants::MAX_LG_K)
            {
                throw InvalidInputException("%s: lg_k must be between %d and %d, got %lld", function_name,
                                            datasketches::theta_constants::MIN_LG_K, datasketches::theta_constants::MAX_LG_K, lg_k);
            }
        }

        // Compact sketches do not record the lg_k they were built with. A sketch in estimation mode retains between
        // k and 2k hashes (exactly k after a union), so floor(log2(retained)) recovers its lg_k; for a sketch in exact
        // mode it is a lower bound. Combining uses the largest of these, and at least the default lg_k (12), so the
        // result is never smaller than the inputs (an explicit lg_k overrides this).
        uint8_t ThetaInferLgK(const ThetaSketchBase &sketch)
        {
            uint32_t retained = sketch.get_num_retained();
            uint8_t lg_k = 0;
            while (retained > 1)
            {
                retained >>= 1;
                lg_k++;
            }
            return MinValue<uint8_t>(MaxValue<uint8_t>(lg_k, DS_THETA_DEFAULT_LG_K), datasketches::theta_constants::MAX_LG_K);
        }

        // ============================================================
        // 2. Bind Data
        // ============================================================

        struct DSThetaBindData : public FunctionData
        {
            DSThetaBindData() = default;
            DSThetaBindData(uint8_t lg_k, bool infer_lg_k, uint64_t seed, float p)
                : lg_k(lg_k), infer_lg_k(infer_lg_k), seed(seed), p(p) {}

            unique_ptr<FunctionData> Copy() const override
            {
                return make_uniq<DSThetaBindData>(lg_k, infer_lg_k, seed, p);
            }

            bool Equals(const FunctionData &other_p) const override
            {
                auto &other = other_p.Cast<DSThetaBindData>();
                return lg_k == other.lg_k && infer_lg_k == other.infer_lg_k && seed == other.seed && p == other.p;
            }

            //! lg_k of the sketches built (or the minimum lg_k of the union when infer_lg_k is set)
            uint8_t lg_k = DS_THETA_DEFAULT_LG_K;
            //! Merging: grow the union to the lg_k of the sketches merged
            bool infer_lg_k = false;
            //! Building: the hash seed and the sampling probability
            uint64_t seed = DS_THETA_DEFAULT_SEED;
            float p = 1.0f;
        };

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
            uint8_t lg_k = HAS_LG_K ? DSThetaBindLgK(input, 0) : DS_THETA_DEFAULT_LG_K;
            auto seed = static_cast<uint64_t>(input.GetNonNullConstant(Identifier("seed")).GetValue<int64_t>());
            auto p = input.GetNonNullConstant(Identifier("p")).GetValue<double>();
            if (!(p > 0.0 && p <= 1.0))
            {
                throw BinderException("Theta sketch sampling probability p must be in (0, 1], got %f", p);
            }
            return make_uniq<DSThetaBindData>(lg_k, false, seed, static_cast<float>(p));
        }

        // Merging sketches with an explicit lg_k.
        unique_ptr<FunctionData> DSThetaBindMergeWithK(BindAggregateFunctionInput &input)
        {
            auto lg_k = DSThetaBindLgK(input, 0);
            DSEraseAggregateArgument(input, 0);
            return make_uniq<DSThetaBindData>(lg_k, false, DS_THETA_DEFAULT_SEED, 1.0f);
        }

        // Merging sketches: the union grows to the lg_k of the sketches merged.
        unique_ptr<FunctionData> DSThetaBindMergeDefault(BindAggregateFunctionInput &input)
        {
            return make_uniq<DSThetaBindData>(DS_THETA_DEFAULT_LG_K, true, DS_THETA_DEFAULT_SEED, 1.0f);
        }

        // ============================================================
        // 3. Build / Union Aggregate State & Operations
        // ============================================================

        struct DSThetaState
        {
            update_theta_sketch *update_sketch = nullptr;
            theta_union *union_sketch = nullptr;
            uint8_t union_lg_k = 0;
            uint64_t union_seed = DS_THETA_DEFAULT_SEED;

            ~DSThetaState()
            {
                if (update_sketch)
                    delete update_sketch;
                if (union_sketch)
                    delete union_sketch;
            }

            void CreateUpdateSketch(const DSThetaBindData &bind_data)
            {
                if (!update_sketch)
                {
                    update_theta_sketch::builder b;
                    b.set_lg_k(bind_data.lg_k);
                    b.set_seed(bind_data.seed);
                    b.set_p(bind_data.p);
                    update_sketch = new update_theta_sketch(b.build());
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
                theta_union::builder b;
                b.set_lg_k(lg_k);
                b.set_seed(seed);
                auto grown = new theta_union(b.build());
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

            uint64_t Seed(const DSThetaBindData &bind_data) const
            {
                return union_sketch ? union_seed : bind_data.seed;
            }

            uint8_t LgK(const DSThetaBindData &bind_data) const
            {
                return union_sketch ? union_lg_k : bind_data.lg_k;
            }
        };

        struct DSThetaOperationBase
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
                if (state.update_sketch)
                    delete state.update_sketch;
                if (state.union_sketch)
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
                auto &bind_data = aggr_input_data.bind_data->template Cast<DSThetaBindData>();
                auto source_seed = source.Seed(bind_data);
                if (target.HasData() && ThetaSeedHash(source_seed) != ThetaSeedHash(target.Seed(bind_data)))
                {
                    throw InvalidInputException("datasketch_theta: cannot merge Theta sketches built with different seeds (seed hashes %d and %d)",
                                                ThetaSeedHash(target.Seed(bind_data)), ThetaSeedHash(source_seed));
                }
                auto lg_k = MaxValue(source.LgK(bind_data), target.LgK(bind_data));
                target.EnsureUnion(lg_k, target.HasData() ? target.Seed(bind_data) : source_seed);
                if (target.update_sketch)
                {
                    target.union_sketch->update(*target.update_sketch);
                    delete target.update_sketch;
                    target.update_sketch = nullptr;
                }
                if (source.update_sketch)
                    target.union_sketch->update(*source.update_sketch);
                if (source.union_sketch)
                    target.union_sketch->update(source.union_sketch->get_result());
            }

            template <class T, class STATE>
            static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data)
            {
                if (state.union_sketch)
                {
                    target = ThetaSerialize(finalize_data.result, state.union_sketch->get_result());
                }
                else if (state.update_sketch)
                {
                    target = ThetaSerialize(finalize_data.result, state.update_sketch->compact());
                }
                else
                {
                    auto &bind_data = finalize_data.input.bind_data->template Cast<DSThetaBindData>();
                    target = ThetaSerialize(finalize_data.result, ThetaEmptySketch(bind_data.lg_k, bind_data.seed));
                }
            }
        };

        struct DSThetaCreateOperation : DSThetaOperationBase
        {
            template <class A_TYPE, class STATE, class OP>
            static void Operation(STATE &state, const A_TYPE &a_data, AggregateUnaryInput &idata)
            {
                auto &bind_data = idata.input.bind_data->template Cast<DSThetaBindData>();
                state.CreateUpdateSketch(bind_data);
                if constexpr (std::is_same_v<A_TYPE, duckdb::string_t>)
                {
                    state.update_sketch->update(a_data.GetData(), a_data.GetSize());
                }
                else
                {
                    state.update_sketch->update(a_data);
                }
            }

            template <class INPUT_TYPE, class STATE, class OP>
            static void ConstantOperation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input, idx_t count)
            {
                // Adding the same value again does not change the sketch.
                Operation<INPUT_TYPE, STATE, OP>(state, input, unary_input);
            }
        };

        struct DSThetaMergeOperation : DSThetaOperationBase
        {
            template <class A_TYPE, class STATE, class OP>
            static void Operation(STATE &state, const A_TYPE &a_data, AggregateUnaryInput &idata)
            {
                auto &bind_data = idata.input.bind_data->template Cast<DSThetaBindData>();
                auto sketch = ThetaDeserialize(a_data);
                if (sketch.is_empty())
                {
                    // an empty sketch adds nothing to a union
                    return;
                }
                if (state.union_sketch && sketch.get_seed_hash() != ThetaSeedHash(state.union_seed))
                {
                    throw InvalidInputException("datasketch_theta: cannot merge Theta sketches built with different seeds (seed hashes %d and %d)",
                                                ThetaSeedHash(state.union_seed), sketch.get_seed_hash());
                }
                auto lg_k = bind_data.infer_lg_k ? MaxValue(bind_data.lg_k, ThetaInferLgK(sketch)) : bind_data.lg_k;
                state.EnsureUnion(lg_k, ThetaSeedForHash(sketch.get_seed_hash()));
                ThetaGuard([&]()
                           { state.union_sketch->update(sketch); });
            }

            template <class INPUT_TYPE, class STATE, class OP>
            static void ConstantOperation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input, idx_t count)
            {
                // A union is idempotent: merging the same sketch again does not change it.
                Operation<INPUT_TYPE, STATE, OP>(state, input, unary_input);
            }
        };

        // ============================================================
        // 4. Intersection Aggregate
        // ============================================================

        struct DSThetaIntersectState
        {
            theta_intersection *intersection = nullptr;
            uint64_t seed = DS_THETA_DEFAULT_SEED;
            //! An empty sketch was intersected: the result is empty
            bool saw_empty = false;

            ~DSThetaIntersectState()
            {
                if (intersection)
                    delete intersection;
            }

            void Add(const compact_theta_sketch &sketch)
            {
                if (sketch.is_empty())
                {
                    saw_empty = true;
                    return;
                }
                if (!intersection)
                {
                    seed = ThetaSeedForHash(sketch.get_seed_hash());
                    intersection = new theta_intersection(seed);
                }
                else if (sketch.get_seed_hash() != ThetaSeedHash(seed))
                {
                    throw InvalidInputException("datasketch_theta_intersect_agg: cannot combine Theta sketches built with different seeds (seed hashes %d and %d)",
                                                ThetaSeedHash(seed), sketch.get_seed_hash());
                }
                ThetaGuard([&]()
                           { intersection->update(sketch); });
            }
        };

        struct DSThetaIntersectOperation
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
                if (state.intersection)
                    delete state.intersection;
                state.intersection = nullptr;
            }

            static bool IgnoreNull() { return true; }

            template <class A_TYPE, class STATE, class OP>
            static void Operation(STATE &state, const A_TYPE &a_data, AggregateUnaryInput &)
            {
                state.Add(ThetaDeserialize(a_data));
            }

            template <class INPUT_TYPE, class STATE, class OP>
            static void ConstantOperation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input, idx_t count)
            {
                // An intersection is idempotent: intersecting the same sketch again does not change it.
                Operation<INPUT_TYPE, STATE, OP>(state, input, unary_input);
            }

            template <class STATE, class OP>
            static void Combine(const STATE &source, STATE &target, AggregateInputData &)
            {
                target.saw_empty = target.saw_empty || source.saw_empty;
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
                    target = ThetaSerialize(finalize_data.result, ThetaEmptySketch(DS_THETA_DEFAULT_LG_K, state.seed));
                }
                else if (state.intersection)
                {
                    target = ThetaSerialize(finalize_data.result, state.intersection->get_result());
                }
                else
                {
                    // no sketches: the intersection of nothing is undefined
                    finalize_data.ReturnNull();
                }
            }
        };


        // ============================================================
        // 5. Scalar Functions
        // ============================================================

        // The arguments of one row of a scalar function call.
        struct ThetaRowArgs
        {
            const vector<UnifiedVectorFormat> &formats;
            const vector<idx_t> &indexes;
            //! The sketch arguments of the row
            const vector<string_t> &sketches;

            template <class T>
            T Get(idx_t column) const
            {
                return UnifiedVectorFormat::GetData<T>(formats[column])[indexes[column]];
            }
        };

        // Calls fun(row, row_args) for every row whose arguments are all non-NULL (a NULL argument gives a NULL
        // result). The sketch arguments are the columns [first_sketch, end_sketch); end_sketch = 0 means "to the last
        // column" (variadic functions).
        template <class FUNC>
        void ThetaForEachRow(DataChunk &args, Vector &result, idx_t first_sketch, idx_t end_sketch, FUNC &&fun)
        {
            const idx_t count = args.size();
            const idx_t column_count = args.ColumnCount();
            if (end_sketch == 0)
            {
                end_sketch = column_count;
            }
            vector<UnifiedVectorFormat> formats(column_count);
            for (idx_t col = 0; col < column_count; col++)
            {
                args.data[col].ToUnifiedFormat(formats[col]);
            }
            vector<string_t> sketches(end_sketch - first_sketch);
            vector<idx_t> indexes(column_count);
            ThetaRowArgs row_args{formats, indexes, sketches};
            for (idx_t row = 0; row < count; row++)
            {
                bool valid = true;
                for (idx_t col = 0; col < column_count; col++)
                {
                    indexes[col] = formats[col].sel->get_index(row);
                    if (!formats[col].validity.RowIsValid(indexes[col]))
                    {
                        valid = false;
                    }
                }
                if (!valid)
                {
                    FlatVector::SetNull(result, row, true);
                    continue;
                }
                for (idx_t col = first_sketch; col < end_sketch; col++)
                {
                    sketches[col - first_sketch] = row_args.Get<string_t>(col);
                }
                fun(row, row_args);
            }
        }

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
                                                                   if (num_std_devs < 1 || num_std_devs > 3)
                                                                   {
                                                                       throw InvalidInputException("%s: num_std_devs must be 1, 2 or 3, got %d",
                                                                                                   LOWER ? "datasketch_theta_lower_bound" : "datasketch_theta_upper_bound", num_std_devs);
                                                                   }
                                                                   return ThetaWithWrapped(sketch_blob, [&](const wrapped_compact_theta_sketch &sketch)
                                                                                           { return LOWER ? sketch.get_lower_bound(static_cast<uint8_t>(num_std_devs))
                                                                                                          : sketch.get_upper_bound(static_cast<uint8_t>(num_std_devs)); }); });
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

        LogicalType CreateThetaSketchType(ExtensionLoader &loader)
        {
            auto new_type = LogicalType(LogicalTypeId::BLOB);
            auto new_type_name = "sketch_theta";
            auto type_info = CreateTypeInfo(new_type_name, LogicalType::BLOB);
            type_info.temporary = false;
            type_info.internal = true;
            type_info.comment = "Sketch type for Theta Sketch";
            new_type = new_type.WithAlias(new_type_name);

            auto &system_catalog = Catalog::GetSystemCatalog(loader.GetDatabaseInstance());
            auto data = CatalogTransaction::GetSystemTransaction(loader.GetDatabaseInstance());
            system_catalog.CreateType(data, type_info);

            loader.RegisterCastFunction(LogicalType::BLOB, new_type, DefaultCasts::ReinterpretCast, 1);
            loader.RegisterCastFunction(new_type, LogicalType::BLOB, DefaultCasts::ReinterpretCast, 1);
            return new_type;
        }

        FunctionDescription ThetaDescription(string description, vector<string> examples, vector<string> parameter_names,
                                             vector<LogicalType> parameter_types)
        {
            FunctionDescription desc;
            desc.description = std::move(description);
            desc.examples = std::move(examples);
            desc.parameter_names = std::move(parameter_names);
            desc.parameter_types = std::move(parameter_types);
            desc.categories = {"sketch", "theta"};
            return desc;
        }

        void ThetaRegisterScalar(ExtensionLoader &loader, ScalarFunctionSet set, vector<FunctionDescription> descriptions)
        {
            set.SetFallible();
            CreateScalarFunctionInfo info(std::move(set));
            for (auto &desc : descriptions)
            {
                info.descriptions.push_back(std::move(desc));
            }
            DSRegister(loader, info);
        }

        void ThetaRegisterScalar(ExtensionLoader &loader, ScalarFunction fun, FunctionDescription description)
        {
            ScalarFunctionSet set(fun.GetName());
            set.AddFunction(std::move(fun));
            ThetaRegisterScalar(loader, std::move(set), {std::move(description)});
        }

        // Names the parameters of a variadic function: DuckDB matches descriptions to overloads by their fixed
        // parameters, so the descriptions list those only and DSRegister cannot name the variadic parameter.
        ScalarFunction ThetaVariadic(ScalarFunction fun, const vector<string> &names, const LogicalType &sketch_type)
        {
            fun.GetSignature().AddArgs("sketches", sketch_type);
            for (idx_t i = 0; i < names.size(); i++)
            {
                fun.GetSignature().GetParameter(i).SetName(Identifier(names[i]));
            }
            return fun;
        }

        void ThetaRegisterAggregate(ExtensionLoader &loader, AggregateFunctionSet &set, vector<FunctionDescription> &descriptions)
        {
            set.ApplyToFunctions([](AggregateFunction &fun)
                                 { fun.SetFallible(); });
            CreateAggregateFunctionInfo info(set);
            for (auto &desc : descriptions)
            {
                info.descriptions.push_back(std::move(desc));
            }
            DSRegister(loader, info);
        }

        // The update callback of an aggregate whose value follows LEADING constant arguments (e.g. lg_k): the
        // constant arguments stay in the argument list on DuckDB 2.0 when the function also has keyword arguments.
        template <idx_t LEADING, class STATE, class INPUT_TYPE, class OP>
        void ThetaSkipLeadingScatterUpdate(Vector inputs[], AggregateInputData &aggr_input_data, idx_t input_count,
                                           Vector &states, idx_t count)
        {
            D_ASSERT(input_count > LEADING);
            AggregateExecutor::UnaryScatter<STATE, INPUT_TYPE, OP>(inputs[LEADING], states, aggr_input_data, count);
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
                auto fun = AggregateFunction::UnaryAggregate<DSThetaState, T, string_t, DSThetaCreateOperation, AggregateDestructorType::LEGACY>(
                    input_type, sketch_type);
                FunctionSignature signature(vector<LogicalType>(), sketch_type);
                if (with_lg_k)
                {
                    signature.AddParameter(LogicalType::INTEGER);
                    fun.SetStateUpdateCallback(ThetaSkipLeadingScatterUpdate<1, DSThetaState, T, DSThetaCreateOperation>);
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

                if (with_lg_k)
                {
                    descs.push_back(ThetaDescription(ThetaBuildDescription(true),
                                                     {name + "(14, column)", name + "(14, column, seed := 42, p := 0.5)"},
                                                     {"lg_k", "data", "seed", "p"},
                                                     {LogicalType::INTEGER, input_type, LogicalType::BIGINT, LogicalType::DOUBLE}));
                }
                else
                {
                    descs.push_back(ThetaDescription(ThetaBuildDescription(false),
                                                     {name + "(column)", name + "(column, seed := 42)"},
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
        auto sketch_type = CreateThetaSketchType(loader);

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

            auto fun_merge = AggregateFunction::UnaryAggregate<DSThetaState, string_t, string_t, DSThetaMergeOperation, AggregateDestructorType::LEGACY>(
                sketch_type, sketch_type);
            fun_merge.SetBindCallback(DSThetaBindMergeDefault);
            fun_merge.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
            DSSetArguments(fun_merge, {sketch_type});
            sketch_agg.AddFunction(fun_merge);
            agg_descs.push_back(ThetaDescription("Merges (unions) Theta sketches into a single sketch. The result keeps as many entries as the "
                                                 "largest input sketch (inferred from the sketches, at least lg_k 12)",
                                                 {"datasketch_theta(sketch)"}, {"sketch"}, {sketch_type}));

            auto fun_merge_k = AggregateFunction::UnaryAggregate<DSThetaState, string_t, string_t, DSThetaMergeOperation, AggregateDestructorType::LEGACY>(
                sketch_type, sketch_type);
            fun_merge_k.SetBindCallback(DSThetaBindMergeWithK);
            fun_merge_k.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
            DSSetArguments(fun_merge_k, {LogicalType::INTEGER, sketch_type});
            sketch_agg.AddFunction(fun_merge_k);
            agg_descs.push_back(ThetaDescription("Merges (unions) Theta sketches into a single sketch with the given log2 of nominal entries (lg_k)",
                                                 {"datasketch_theta(16, sketch)"}, {"lg_k", "sketch"}, {LogicalType::INTEGER, sketch_type}));

            ThetaRegisterAggregate(loader, sketch_agg, agg_descs);
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
            ThetaRegisterAggregate(loader, blob_agg, blob_descs);
        }

        // --- datasketch_theta_intersect_agg: intersect sketches across rows
        {
            AggregateFunctionSet intersect_agg("datasketch_theta_intersect_agg");
            auto fun = AggregateFunction::UnaryAggregate<DSThetaIntersectState, string_t, string_t, DSThetaIntersectOperation, AggregateDestructorType::LEGACY>(
                sketch_type, sketch_type);
            fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
            DSSetArguments(fun, {sketch_type});
            intersect_agg.AddFunction(fun);
            vector<FunctionDescription> descs;
            descs.push_back(ThetaDescription("Intersects Theta sketches across rows: the result estimates the number of distinct values present "
                                             "in every input sketch. NULL sketches are skipped; returns NULL if there are no sketches",
                                             {"datasketch_theta_intersect_agg(sketch)"}, {"sketch"}, {sketch_type}));
            ThetaRegisterAggregate(loader, intersect_agg, descs);
        }

        // --- SET OPERATIONS ---
        {
            ScalarFunctionSet set("datasketch_theta_union");
            set.AddFunction(ThetaVariadic(ScalarFunction("datasketch_theta_union", {sketch_type, sketch_type}, sketch_type, DSThetaUnion),
                                          {"sketch_a", "sketch_b"}, sketch_type));
            set.AddFunction(ThetaVariadic(ScalarFunction("datasketch_theta_union", {LogicalType::INTEGER, sketch_type, sketch_type}, sketch_type, DSThetaUnionWithK),
                                          {"lg_k", "sketch_a", "sketch_b"}, sketch_type));
            ThetaRegisterScalar(loader, std::move(set),
                                {ThetaDescription("Returns a new Theta sketch representing the union of two or more sketches. The result keeps "
                                                  "as many entries as the largest input sketch (inferred from the sketches, at least lg_k 12)",
                                                  {"datasketch_theta_union(sketch_a, sketch_b)", "datasketch_theta_union(sketch_a, sketch_b, sketch_c)"},
                                                  {"sketch_a", "sketch_b"}, {sketch_type, sketch_type}),
                                 ThetaDescription("Returns a new Theta sketch representing the union of two or more sketches, with the given "
                                                  "log2 of nominal entries (lg_k, 4-26)",
                                                  {"datasketch_theta_union(16, sketch_a, sketch_b)", "datasketch_theta_union(16, sketch_a, sketch_b, sketch_c)"},
                                                  {"lg_k", "sketch_a", "sketch_b"}, {LogicalType::INTEGER, sketch_type, sketch_type})});
        }
        {
            auto fun = ThetaVariadic(ScalarFunction("datasketch_theta_intersect", {sketch_type, sketch_type}, sketch_type, DSThetaIntersect),
                                     {"sketch_a", "sketch_b"}, sketch_type);
            ThetaRegisterScalar(loader, std::move(fun),
                                ThetaDescription("Returns a new Theta sketch representing the intersection of two or more sketches",
                                                 {"datasketch_theta_intersect(sketch_a, sketch_b)", "datasketch_theta_intersect(sketch_a, sketch_b, sketch_c)"},
                                                 {"sketch_a", "sketch_b"}, {sketch_type, sketch_type}));
        }
        ThetaRegisterScalar(loader, ScalarFunction("datasketch_theta_a_not_b", {sketch_type, sketch_type}, sketch_type, DSThetaANotB),
                            ThetaDescription("Returns a new Theta sketch representing elements in sketch A but not in sketch B (set difference)",
                                             {"datasketch_theta_a_not_b(sketch_a, sketch_b)"}, {"sketch_a", "sketch_b"}, {sketch_type, sketch_type}));

        // --- SIMILARITY ---
        {
            child_list_t<LogicalType> bounds;
            bounds.emplace_back("lower", LogicalType::DOUBLE);
            bounds.emplace_back("estimate", LogicalType::DOUBLE);
            bounds.emplace_back("upper", LogicalType::DOUBLE);
            ThetaRegisterScalar(loader, ScalarFunction("datasketch_theta_jaccard", {sketch_type, sketch_type}, LogicalType::STRUCT(std::move(bounds)), DSThetaJaccard),
                                ThetaDescription("Returns the Jaccard similarity |A ∩ B| / |A ∪ B| of two sketches as a struct of its estimate "
                                                 "and approximate lower and upper bounds (about 2 standard deviations). Two empty sketches "
                                                 "are identical (1.0)",
                                                 {"datasketch_theta_jaccard(sketch_a, sketch_b)", "datasketch_theta_jaccard(sketch_a, sketch_b).estimate"},
                                                 {"sketch_a", "sketch_b"}, {sketch_type, sketch_type}));
        }
        ThetaRegisterScalar(loader, ScalarFunction("datasketch_theta_exactly_equal", {sketch_type, sketch_type}, LogicalType::BOOLEAN, DSThetaExactlyEqual),
                            ThetaDescription("Returns true if the two sketches retain exactly the same hashes with the same theta, i.e. they "
                                             "represent the same set as far as the sketches can tell",
                                             {"datasketch_theta_exactly_equal(sketch_a, sketch_b)"}, {"sketch_a", "sketch_b"}, {sketch_type, sketch_type}));
        ThetaRegisterScalar(loader, ScalarFunction("datasketch_theta_similarity_test", {sketch_type, sketch_type, LogicalType::DOUBLE}, LogicalType::BOOLEAN, DSThetaSimilarityTest<true>),
                            ThetaDescription("Returns true if the Jaccard similarity of the two sketches is at least the threshold, with "
                                             "high confidence (its lower bound is >= threshold)",
                                             {"datasketch_theta_similarity_test(actual, expected, 0.9)"}, {"actual", "expected", "threshold"},
                                             {sketch_type, sketch_type, LogicalType::DOUBLE}));
        ThetaRegisterScalar(loader, ScalarFunction("datasketch_theta_dissimilarity_test", {sketch_type, sketch_type, LogicalType::DOUBLE}, LogicalType::BOOLEAN, DSThetaSimilarityTest<false>),
                            ThetaDescription("Returns true if the Jaccard similarity of the two sketches is at most the threshold, with "
                                             "high confidence (its upper bound is <= threshold)",
                                             {"datasketch_theta_dissimilarity_test(actual, expected, 0.1)"}, {"actual", "expected", "threshold"},
                                             {sketch_type, sketch_type, LogicalType::DOUBLE}));

        // --- ESTIMATES ---
        ThetaRegisterScalar(loader, ScalarFunction("datasketch_theta_estimate", {sketch_type}, LogicalType::DOUBLE, DSThetaEstimate),
                            ThetaDescription("Returns the estimated number of distinct values in the Theta sketch",
                                             {"datasketch_theta_estimate(sketch)"}, {"sketch"}, {sketch_type}));
        ThetaRegisterScalar(loader, ScalarFunction("datasketch_theta_lower_bound", {sketch_type, LogicalType::INTEGER}, LogicalType::DOUBLE, DSThetaBound<true>),
                            ThetaDescription("Returns the lower bound estimate at the given number of standard deviations (1, 2, or 3)",
                                             {"datasketch_theta_lower_bound(sketch, 2)"}, {"sketch", "num_std_devs"}, {sketch_type, LogicalType::INTEGER}));
        ThetaRegisterScalar(loader, ScalarFunction("datasketch_theta_upper_bound", {sketch_type, LogicalType::INTEGER}, LogicalType::DOUBLE, DSThetaBound<false>),
                            ThetaDescription("Returns the upper bound estimate at the given number of standard deviations (1, 2, or 3)",
                                             {"datasketch_theta_upper_bound(sketch, 2)"}, {"sketch", "num_std_devs"}, {sketch_type, LogicalType::INTEGER}));
        ThetaRegisterScalar(loader, ScalarFunction("datasketch_theta_describe", {sketch_type}, LogicalType::VARCHAR, DSThetaDescribe),
                            ThetaDescription("Returns a human-readable description of the Theta sketch",
                                             {"datasketch_theta_describe(sketch)"}, {"sketch"}, {sketch_type}));

        // --- METADATA ---
        ThetaRegisterScalar(loader, ScalarFunction("datasketch_theta_is_empty", {sketch_type}, LogicalType::BOOLEAN, DSThetaIsEmpty),
                            ThetaDescription("Returns true if the Theta sketch is empty",
                                             {"datasketch_theta_is_empty(sketch)"}, {"sketch"}, {sketch_type}));
        ThetaRegisterScalar(loader, ScalarFunction("datasketch_theta_is_estimation_mode", {sketch_type}, LogicalType::BOOLEAN, DSThetaIsEstimation),
                            ThetaDescription("Returns true if the sketch is in estimation mode (has exceeded exact counting capacity)",
                                             {"datasketch_theta_is_estimation_mode(sketch)"}, {"sketch"}, {sketch_type}));
        ThetaRegisterScalar(loader, ScalarFunction("datasketch_theta_get_theta", {sketch_type}, LogicalType::DOUBLE, DSThetaGetTheta),
                            ThetaDescription("Returns the theta value of the sketch (sampling probability)",
                                             {"datasketch_theta_get_theta(sketch)"}, {"sketch"}, {sketch_type}));
        ThetaRegisterScalar(loader, ScalarFunction("datasketch_theta_num_retained", {sketch_type}, LogicalType::BIGINT, DSThetaNumRetained),
                            ThetaDescription("Returns the number of hash values retained in the sketch",
                                             {"datasketch_theta_num_retained(sketch)"}, {"sketch"}, {sketch_type}));
        ThetaRegisterScalar(loader, ScalarFunction("datasketch_theta_seed_hash", {sketch_type}, LogicalType::BIGINT, DSThetaSeedHash),
                            ThetaDescription("Returns the 16-bit hash of the seed the sketch was built with (compact sketches store only "
                                             "this hash, not the seed); sketches can only be combined if their seed hashes match",
                                             {"datasketch_theta_seed_hash(sketch)"}, {"sketch"}, {sketch_type}));
        ThetaRegisterScalar(loader, ScalarFunction("datasketch_theta_get_seed", {sketch_type}, LogicalType::BIGINT, DSThetaSeedHash),
                            ThetaDescription("Alias of datasketch_theta_seed_hash: returns the 16-bit hash of the seed the sketch was built "
                                             "with, not the seed itself (compact sketches do not store the seed)",
                                             {"datasketch_theta_get_seed(sketch)"}, {"sketch"}, {sketch_type}));
    }

}
