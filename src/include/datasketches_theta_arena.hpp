#pragma once

// Arena states of the Theta-family aggregates (theta_sketch.cpp, tuple_sketch.cpp, aod_sketch.cpp).
//
// The states keep everything in the aggregate arena (DSAllocator's arena mode), have no destructor and export
// themselves as BLOBs, so DuckDB counts them toward memory_limit and can spill them. Every sketch object here
// allocates in arena mode; where its memory lands is decided by the arena of the scope it allocates in:
// - the callback's arena (DSArenaScope), for what a state keeps;
// - a per-thread scratch arena (DSScratchScope), for temporaries: input sketches, results, serialized bytes.
// Copies made by the library use the source object's allocator, which in arena mode means "the current arena", so a
// temporary in the scratch arena copied while the callback's arena is current lands in the state, and a state copied
// while the scratch arena is current lands in the scratch arena. Nothing of a temporary may outlive its scratch
// scope, and nothing a state keeps may be created under one.
//
// DuckDB creates a state per input row when grouping reduces little, and a Theta update sketch or union allocates a
// hash table up front (about 1 KB at lg_k 12). So the states buffer small inputs:
// - the build aggregates keep their first raw input rows and replay them, in order, into an update sketch when more
//   arrive (or into a temporary one when a result is needed), so the result is the one an update sketch of all rows
//   gives;
// - unions keep up to BUFFERED distinct keys with their combined summaries while that is no more than the union's
//   nominal size k. A union's result is the k smallest keys below its theta (the minimum theta of its inputs), each
//   with the summaries of its inputs combined in the order they arrived; with at most k keys nothing is trimmed, so
//   the buffer holds exactly what the union would hold, and replaying it into a union gives the same union.
// Intersections keep their current result (an intersection only shrinks) and intersect it with each input in a
// temporary intersection, overwriting the result in place: the library's intersection reallocates its table on every
// update, which in an arena would never be freed.
//
// A TRAITS class describes a sketch family:
//   using Entry, Alloc (DSAllocator), Update, Compact, Union, Intersection;
//   using BindData;                      // DSThetaFamilyBindData
//   static constexpr bool SET_ONLY;      // no summaries: raw buffers can be concatenated
//   static constexpr const char *KIND, *BUILD_NAME, *INTERSECT_NAME;
//   static uint64_t Key(const Entry &);
//   static idx_t SummarySize(const DSThetaParams &);
//   static void WriteSummary(const Entry &, data_ptr_t, const DSThetaParams &);
//   static Entry MakeEntry(uint64_t key, const_data_ptr_t summary, const DSThetaParams &);
//   static void Fold(data_ptr_t summary, const Entry &incoming, const DSThetaParams &);    // union policy
//   static idx_t PayloadSize(const DSThetaParams &);                                       // raw row value bytes
//   template <class KEY> static void Apply(Update &, const_data_ptr_t key, uint32_t length, const_data_ptr_t payload,
//                                          const DSThetaParams &);                         // update with a raw row
//   static Update NewUpdate(const BindData &, const DSThetaParams &);
//   static Union NewUnion(uint8_t lg_k, uint64_t seed, const DSThetaParams &);
//   static Intersection NewIntersection(uint64_t seed, const DSThetaParams &);
//   static Compact Deserialize(const string_t &blob);
//   static Compact Empty(uint8_t lg_k, uint64_t seed, const DSThetaParams &);
//   static auto Serialize(const Compact &);  // the stored format
//   static void ThrowSeedMismatch(const char *function_name, uint16_t a, uint16_t b);
//   static void ThrowNumValues(const char *function_name, idx_t a, idx_t b);              // AoD only

#include "datasketches_theta_common.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/string_vector.hpp"
#include "duckdb/storage/arena_allocator.hpp"

#include <DataSketches/theta_constants.hpp>

#include <cstring>
#include <vector>

namespace duckdb
{
        // ============================================================
        // Scratch arena and arena buffers
        // ============================================================

        // Makes the arena allocators allocate from this thread's scratch arena while the scope lasts. The arena is
        // reset when the outermost scope ends, so temporaries cost a pointer bump instead of a malloc and free.
        struct DSScratchScope
        {
            DSScratchScope() : scope(Arena())
            {
                depth++;
            }
            ~DSScratchScope()
            {
                if (--depth == 0)
                {
                    auto &arena = Arena();
                    if (arena.SizeInBytes() > MAX_RETAINED)
                    {
                        arena.Destroy();
                    }
                    else
                    {
                        arena.Reset();
                    }
                }
            }
            DSScratchScope(const DSScratchScope &) = delete;
            DSScratchScope &operator=(const DSScratchScope &) = delete;

            static ArenaAllocator &Arena()
            {
                static thread_local ArenaAllocator arena(Allocator::DefaultAllocator());
                return arena;
            }

        private:
            static constexpr idx_t MAX_RETAINED = idx_t(1) << 20;
            static inline thread_local idx_t depth = 0;
            DSArenaScope scope;
        };

        // Bytes in the aggregate arena, in an array that doubles as it fills. Memory it gives up stays in the arena.
        struct DSArenaBytes
        {
            data_ptr_t data;
            uint32_t size;
            uint32_t capacity;

            void Initialize()
            {
                data = nullptr;
                size = 0;
                capacity = 0;
            }

            // Appends n bytes, returning where they go.
            data_ptr_t Grow(idx_t n, ArenaAllocator &arena)
            {
                const idx_t needed = idx_t(size) + n;
                if (needed > NumericLimits<uint32_t>::Maximum())
                {
                    throw InvalidInputException("Theta sketch aggregate state is too large");
                }
                if (needed > capacity)
                {
                    const idx_t new_capacity = MinValue<idx_t>(MaxValue<idx_t>(idx_t(capacity) * 2, needed), NumericLimits<uint32_t>::Maximum());
                    auto new_data = arena.AllocateAligned(new_capacity);
                    if (size)
                    {
                        memcpy(new_data, data, size);
                    }
                    data = new_data;
                    capacity = static_cast<uint32_t>(new_capacity);
                }
                auto result = data + size;
                size = static_cast<uint32_t>(needed);
                return result;
            }

            void Append(const void *source, idx_t n, ArenaAllocator &arena)
            {
                if (n)
                {
                    memcpy(Grow(n, arena), source, n);
                }
            }
        };

        // Writes and reads the exported states.
        struct DSThetaWriter
        {
            std::vector<uint8_t> bytes;

            template <class T>
            void Put(const T &value)
            {
                auto p = reinterpret_cast<const uint8_t *>(&value);
                bytes.insert(bytes.end(), p, p + sizeof(T));
            }
            void PutBytes(const void *data, idx_t size)
            {
                auto p = reinterpret_cast<const uint8_t *>(data);
                bytes.insert(bytes.end(), p, p + size);
            }
            // A length-prefixed block.
            template <class BYTES>
            void PutBlock(const BYTES &block)
            {
                Put<uint32_t>(static_cast<uint32_t>(block.size()));
                PutBytes(block.data(), block.size());
            }
        };

        struct DSThetaReader
        {
            DSThetaReader(const string_t &blob, const char *kind)
                : data(const_data_ptr_cast(blob.GetData())), size(blob.GetSize()), kind(kind) {}

            const_data_ptr_t data;
            idx_t size;
            idx_t pos = 0;
            const char *kind;

            void Check(idx_t n) const
            {
                if (n > size - pos)
                {
                    throw InvalidInputException("Invalid %s sketch aggregate state", kind);
                }
            }
            template <class T>
            T Get()
            {
                Check(sizeof(T));
                T value;
                memcpy(&value, data + pos, sizeof(T));
                pos += sizeof(T);
                return value;
            }
            const_data_ptr_t GetBytes(idx_t n)
            {
                Check(n);
                auto result = data + pos;
                pos += n;
                return result;
            }
            string_t GetBlock()
            {
                auto n = Get<uint32_t>();
                auto p = GetBytes(n);
                return string_t(const_char_ptr_cast(p), n);
            }
            void Fail() const
            {
                throw InvalidInputException("Invalid %s sketch aggregate state", kind);
            }
            void End() const
            {
                if (pos != size)
                {
                    Fail();
                }
            }
        };

        // ============================================================
        // Bind data and parameters
        // ============================================================

        // The bind data of every Theta-family aggregate. `constants` are the constant arguments (lg_k, seed, p, mode)
        // by argument index, which the exported state records so that re-binding it gets the same bind data.
        struct DSThetaFamilyBindData : public FunctionData
        {
            unique_ptr<FunctionData> Copy() const override
            {
                return make_uniq<DSThetaFamilyBindData>(*this);
            }

            bool Equals(const FunctionData &other_p) const override
            {
                auto &other = other_p.Cast<DSThetaFamilyBindData>();
                return lg_k == other.lg_k && infer_lg_k == other.infer_lg_k && seed == other.seed && p == other.p && mode == other.mode;
            }

            //! lg_k of the sketches built (or the minimum lg_k of the union when infer_lg_k is set)
            uint8_t lg_k = DS_THETA_DEFAULT_LG_K;
            //! Merging: grow the union to the lg_k of the sketches merged
            bool infer_lg_k = false;
            //! Building: the hash seed and the sampling probability
            uint64_t seed = DS_THETA_DEFAULT_SEED;
            float p = 1.0f;
            //! Tuple sketches: how summaries combine (DSTupleMode)
            uint8_t mode = 0;
            vector<std::pair<idx_t, Value>> constants;
        };

        // Records the constant arguments of a call: every argument but the data arguments [data_begin, data_end).
        inline void DSThetaRecordConstants(BindAggregateFunctionInput &input, DSThetaFamilyBindData &bind_data, idx_t data_begin, idx_t data_end)
        {
            auto &arguments = input.GetArguments();
            for (idx_t i = 0; i < arguments.size(); i++)
            {
                if (i >= data_begin && i < data_end)
                {
                    continue;
                }
                bind_data.constants.emplace_back(i, input.GetNonNullConstant(i));
            }
        }

        inline const DSThetaFamilyBindData &DSThetaBind(optional_ptr<FunctionData> bind_data)
        {
            return bind_data->Cast<DSThetaFamilyBindData>();
        }

        inline uint8_t DSThetaMode(optional_ptr<FunctionData> bind_data)
        {
            return bind_data ? bind_data->Cast<DSThetaFamilyBindData>().mode : 0;
        }

        // What the sketch operations need besides the bind data: the summary mode and the number of values (AoD).
        struct DSThetaParams
        {
            uint8_t mode = 0;
            uint8_t num_values = 1;
        };

        // A sketch over entries held in a vector, in the shape the library's unions and intersections read.
        template <class TRAITS>
        struct DSThetaEntriesView
        {
            using Entry = typename TRAITS::Entry;
            using EntryAlloc = typename std::allocator_traits<typename TRAITS::Alloc>::template rebind_alloc<Entry>;
            using Entries = std::vector<Entry, EntryAlloc>;

            DSThetaEntriesView(bool empty, uint64_t seed, uint64_t theta)
                : entries(EntryAlloc(DSArenaMode())), empty(empty), seed_hash(ThetaSeedHash(seed)), theta(theta) {}

            // Makes the entries from count buffered entries (key, then summary).
            void Load(const_data_ptr_t data, idx_t count, const DSThetaParams &params)
            {
                const idx_t stride = sizeof(uint64_t) + TRAITS::SummarySize(params);
                entries.reserve(count);
                for (idx_t i = 0; i < count; i++)
                {
                    uint64_t key;
                    memcpy(&key, data + i * stride, sizeof(key));
                    entries.push_back(TRAITS::MakeEntry(key, data + i * stride + sizeof(uint64_t), params));
                }
            }

            bool is_empty() const { return empty; }
            bool is_ordered() const { return false; }
            uint16_t get_seed_hash() const { return seed_hash; }
            uint64_t get_theta64() const { return theta; }
            uint32_t get_num_retained() const { return static_cast<uint32_t>(entries.size()); }
            typename Entries::const_iterator begin() const { return entries.begin(); }
            typename Entries::const_iterator end() const { return entries.end(); }

            Entries entries;
            bool empty;
            uint16_t seed_hash;
            uint64_t theta;
        };

        // Writes the entries of a sketch (key, then summary).
        template <class TRAITS, class SKETCH>
        void DSThetaWriteEntries(const SKETCH &sketch, DSThetaWriter &writer, const DSThetaParams &params)
        {
            vector<uint8_t> summary(TRAITS::SummarySize(params));
            for (const auto &entry : sketch)
            {
                writer.Put<uint64_t>(TRAITS::Key(entry));
                TRAITS::WriteSummary(entry, summary.data(), params);
                writer.PutBytes(summary.data(), summary.size());
            }
        }

        // ============================================================
        // Unions
        // ============================================================

        // A union in the arena: buffered keys while it holds few, then the library's union.
        template <class TRAITS>
        struct DSThetaUnionPart
        {
            using Union = typename TRAITS::Union;
            using Compact = typename TRAITS::Compact;
            using View = DSThetaEntriesView<TRAITS>;
            static constexpr uint32_t BUFFERED = 32;

            Union *sketch;
            //! The buffered keys (distinct, each followed by its summary) while there is no sketch
            DSArenaBytes entries;
            uint32_t count;
            uint64_t theta;
            uint64_t seed;
            uint8_t lg_k;
            bool active;
            //! A non-empty sketch was added
            bool nonempty;

            void Initialize()
            {
                sketch = nullptr;
                entries.Initialize();
                count = 0;
                theta = datasketches::theta_constants::MAX_THETA;
                seed = DS_THETA_DEFAULT_SEED;
                lg_k = 0;
                active = false;
                nonempty = false;
            }

            uint32_t Limit() const
            {
                return MinValue<uint32_t>(BUFFERED, uint32_t(1) << lg_k);
            }

            static idx_t Stride(const DSThetaParams &params)
            {
                return sizeof(uint64_t) + TRAITS::SummarySize(params);
            }

            uint64_t KeyAt(idx_t i, const DSThetaParams &params) const
            {
                uint64_t key;
                memcpy(&key, entries.data + i * Stride(params), sizeof(key));
                return key;
            }

            idx_t Find(uint64_t key, const DSThetaParams &params) const
            {
                for (idx_t i = 0; i < count; i++)
                {
                    if (KeyAt(i, params) == key)
                    {
                        return i;
                    }
                }
                return DConstants::INVALID_INDEX;
            }

            // The buffered keys as a sketch (allocated in the current, scratch, arena).
            View Buffered(const DSThetaParams &params) const
            {
                View view(false, seed, theta);
                view.Load(entries.data, count, params);
                return view;
            }

            // The library's EnsureUnion: creates the union, or grows it to a larger lg_k.
            void Ensure(uint8_t new_lg_k, uint64_t new_seed, const DSThetaParams &params, ArenaAllocator &arena)
            {
                if (!active)
                {
                    Initialize();
                    active = true;
                    lg_k = new_lg_k;
                    seed = new_seed;
                    return;
                }
                if (new_lg_k <= lg_k)
                {
                    return;
                }
                lg_k = new_lg_k;
                if (sketch)
                {
                    // re-add what the union holds to a larger one
                    DSScratchScope scratch;
                    auto held = sketch->get_result();
                    DSArenaScope back(arena);
                    auto grown = arena.Make<Union>(TRAITS::NewUnion(lg_k, seed, params));
                    grown->update(held);
                    sketch = grown;
                }
            }

            // Buffers a sketch's entries if the buffer stays within its limit.
            template <class SKETCH>
            bool TryBuffer(const SKETCH &incoming, const DSThetaParams &params, ArenaAllocator &arena)
            {
                const uint64_t new_theta = MinValue<uint64_t>(theta, incoming.get_theta64());
                const uint32_t limit = Limit();
                idx_t kept = 0;
                for (idx_t i = 0; i < count; i++)
                {
                    kept += KeyAt(i, params) < new_theta;
                }
                idx_t added = 0;
                for (const auto &entry : incoming)
                {
                    const auto key = TRAITS::Key(entry);
                    if (key < new_theta && Find(key, params) == DConstants::INVALID_INDEX && kept + ++added > limit)
                    {
                        return false;
                    }
                }
                // drop the keys at or above the new theta: the union's result leaves them out
                const idx_t stride = Stride(params);
                if (new_theta < theta)
                {
                    uint32_t write = 0;
                    for (idx_t i = 0; i < count; i++)
                    {
                        if (KeyAt(i, params) < new_theta)
                        {
                            if (write != i)
                            {
                                memmove(entries.data + write * stride, entries.data + i * stride, stride);
                            }
                            write++;
                        }
                    }
                    count = write;
                    entries.size = static_cast<uint32_t>(count * stride);
                    theta = new_theta;
                }
                for (const auto &entry : incoming)
                {
                    const auto key = TRAITS::Key(entry);
                    if (key >= theta)
                    {
                        continue;
                    }
                    auto index = Find(key, params);
                    if (index != DConstants::INVALID_INDEX)
                    {
                        TRAITS::Fold(entries.data + index * stride + sizeof(uint64_t), entry, params);
                        continue;
                    }
                    auto slot = entries.Grow(stride, arena);
                    memcpy(slot, &key, sizeof(key));
                    TRAITS::WriteSummary(entry, slot + sizeof(uint64_t), params);
                    count++;
                }
                return true;
            }

            // Replaces the buffer with the library's union.
            void Promote(const DSThetaParams &params, ArenaAllocator &arena)
            {
                D_ASSERT(!sketch);
                auto promoted = arena.Make<Union>(TRAITS::NewUnion(lg_k, seed, params));
                if (nonempty)
                {
                    DSScratchScope scratch;
                    auto buffered = Buffered(params);
                    DSArenaScope back(arena);
                    promoted->update(buffered);
                }
                sketch = promoted;
                entries.Initialize();
                count = 0;
            }

            // EnsureUnion(lg_k, seed), then the union's update with `incoming`, which is read only. It may live in the
            // scratch arena or another state's arena: what the union keeps of it is copied into `arena`.
            template <class SKETCH>
            void Add(const SKETCH &incoming, uint8_t new_lg_k, uint64_t new_seed, const DSThetaParams &params, ArenaAllocator &arena)
            {
                DSArenaScope scope(arena);
                Ensure(new_lg_k, new_seed, params, arena);
                if (incoming.is_empty())
                {
                    return;
                }
                if (!sketch)
                {
                    if (TryBuffer(incoming, params, arena))
                    {
                        nonempty = true;
                        return;
                    }
                    Promote(params, arena);
                }
                nonempty = true;
                ThetaGuard([&]()
                           { sketch->update(incoming); },
                           TRAITS::KIND);
            }

            // Adds another union (EnsureUnion(lg_k, seed), then the update with its result).
            void AddUnion(const DSThetaUnionPart &source, uint8_t new_lg_k, uint64_t new_seed, const DSThetaParams &params, ArenaAllocator &arena)
            {
                D_ASSERT(source.active);
                if (!active && source.sketch && source.lg_k == new_lg_k)
                {
                    // a copy allocates through the source's allocator, which in arena mode is the current (target's) arena
                    DSArenaScope scope(arena);
                    Initialize();
                    active = true;
                    lg_k = source.lg_k;
                    seed = source.seed;
                    nonempty = source.nonempty;
                    sketch = arena.Make<Union>(*source.sketch);
                    return;
                }
                DSScratchScope scratch;
                if (source.sketch)
                {
                    auto result = source.sketch->get_result();
                    Add(result, new_lg_k, new_seed, params, arena);
                }
                else if (source.nonempty)
                {
                    Add(source.Buffered(params), new_lg_k, new_seed, params, arena);
                }
                else
                {
                    DSArenaScope back(arena);
                    Ensure(new_lg_k, new_seed, params, arena);
                }
            }

            // The union's result, in the current (scratch) arena.
            Compact Result(const DSThetaParams &params) const
            {
                if (sketch)
                {
                    return sketch->get_result();
                }
                auto temporary = TRAITS::NewUnion(lg_k, seed, params);
                if (nonempty)
                {
                    temporary.update(Buffered(params));
                }
                return temporary.get_result();
            }

            // lg_k, seed and flags, then the buffered entries or the union's result.
            void Export(DSThetaWriter &writer, const DSThetaParams &params) const
            {
                writer.Put<uint8_t>(sketch ? 2 : 1);
                writer.Put<uint8_t>(lg_k);
                writer.Put<uint64_t>(seed);
                writer.Put<uint8_t>(nonempty);
                if (sketch)
                {
                    DSScratchScope scratch;
                    writer.PutBlock(TRAITS::Serialize(sketch->get_result()));
                    return;
                }
                writer.Put<uint64_t>(theta);
                writer.Put<uint32_t>(count);
                writer.PutBytes(entries.data, count * Stride(params));
            }

            void Import(DSThetaReader &reader, uint8_t kind, const DSThetaParams &params, ArenaAllocator &arena)
            {
                const auto new_lg_k = reader.Get<uint8_t>();
                const auto new_seed = reader.Get<uint64_t>();
                const auto new_nonempty = reader.Get<uint8_t>();
                if (new_lg_k < datasketches::theta_constants::MIN_LG_K || new_lg_k > datasketches::theta_constants::MAX_LG_K || new_nonempty > 1)
                {
                    reader.Fail();
                }
                if (kind == 2)
                {
                    // the union's result: a union of it is the union (buffered again if it is small)
                    auto block = reader.GetBlock();
                    DSScratchScope scratch;
                    auto result = TRAITS::Deserialize(block);
                    {
                        DSArenaScope back(arena);
                        Ensure(new_lg_k, new_seed, params, arena);
                    }
                    if (new_nonempty)
                    {
                        Add(result, new_lg_k, new_seed, params, arena);
                    }
                    return;
                }
                const auto new_theta = reader.Get<uint64_t>();
                const auto new_count = reader.Get<uint32_t>();
                if (new_count > BUFFERED || new_count > (uint32_t(1) << new_lg_k) || (new_count && !new_nonempty))
                {
                    reader.Fail();
                }
                auto data = reader.GetBytes(new_count * Stride(params));
                Initialize();
                active = true;
                lg_k = new_lg_k;
                seed = new_seed;
                nonempty = new_nonempty;
                theta = new_theta;
                count = new_count;
                entries.Append(data, new_count * Stride(params), arena);
            }
        };

        // ============================================================
        // Build and union aggregates
        // ============================================================

        // The state of the aggregates that build a sketch from rows (an update sketch) and of those that merge
        // sketches (a union); combining states makes a union, as before. KEY is the type of the rows' keys.
        template <class TRAITS, class KEY>
        struct DSThetaFamilyState
        {
            using Update = typename TRAITS::Update;
            using Compact = typename TRAITS::Compact;
            using BindData = DSThetaFamilyBindData;
            //! Raw rows kept before an update sketch is built
            static constexpr uint32_t RAW_ROWS = 32;
            static constexpr uint32_t RAW_BYTES = 1024;

            DSThetaUnionPart<TRAITS> part;
            //! The update sketch of the rows
            Update *update;
            //! An update sketch's compact form, from an imported state
            Compact *compact;
            //! The first rows (key length, key, value) while there is no update sketch
            DSArenaBytes raw;
            uint32_t raw_rows;
            //! The number of values per key of AoD sketches (0 while unknown)
            uint8_t num_values;
            //! A merged input sketch was empty: with no other input, the result is an empty sketch rather than NULL
            bool saw_empty_sketch;

            void Initialize()
            {
                part.Initialize();
                update = nullptr;
                compact = nullptr;
                raw.Initialize();
                raw_rows = 0;
                num_values = 0;
                saw_empty_sketch = false;
            }

            DSThetaParams Params(const BindData &bind) const
            {
                DSThetaParams params;
                params.mode = bind.mode;
                params.num_values = MaxValue<uint8_t>(num_values, 1);
                return params;
            }

            bool HasData() const
            {
                return update || compact || raw_rows || part.active;
            }
            bool IsEmpty() const
            {
                return !HasData() && !saw_empty_sketch;
            }
            bool OnlyRaw() const
            {
                return raw_rows && !update && !compact && !part.active;
            }
            uint64_t Seed(const BindData &bind) const
            {
                return part.active ? part.seed : bind.seed;
            }
            uint8_t LgK(const BindData &bind) const
            {
                return part.active ? part.lg_k : bind.lg_k;
            }

            void CheckNumValues(idx_t count, const char *function_name)
            {
                if (num_values == 0)
                {
                    num_values = static_cast<uint8_t>(count);
                }
                else if (num_values != count)
                {
                    TRAITS::ThrowNumValues(function_name, num_values, count);
                }
            }

            // Replays raw rows (as written by AddRow) into an update sketch.
            static void Replay(Update &sketch, const_data_ptr_t data, idx_t size, const DSThetaParams &params)
            {
                const idx_t payload = TRAITS::PayloadSize(params);
                idx_t pos = 0;
                while (pos < size)
                {
                    uint32_t length;
                    memcpy(&length, data + pos, sizeof(length));
                    pos += sizeof(length);
                    TRAITS::template Apply<KEY>(sketch, data + pos, length, data + pos + length, params);
                    pos += length + payload;
                }
            }

            // The update sketch of the rows: the one built so far, or one replayed from the raw rows in the current
            // (scratch) arena.
            template <class FUNC>
            void WithRowsSketch(const BindData &bind, FUNC &&fun) const
            {
                if (update)
                {
                    fun(update->compact());
                    return;
                }
                auto params = Params(bind);
                auto temporary = TRAITS::NewUpdate(bind, params);
                Replay(temporary, raw.data, raw.size, params);
                fun(temporary.compact());
            }

            // Adds a row: key bytes (the value of a numeric key, or a string's bytes) and value bytes.
            void AddRow(const_data_ptr_t key, uint32_t length, const_data_ptr_t payload, const BindData &bind, ArenaAllocator &arena)
            {
                auto params = Params(bind);
                if (update)
                {
                    TRAITS::template Apply<KEY>(*update, key, length, payload, params);
                    return;
                }
                const idx_t payload_size = TRAITS::PayloadSize(params);
                const idx_t row_size = sizeof(uint32_t) + length + payload_size;
                if (raw_rows < RAW_ROWS && raw.size + row_size <= RAW_BYTES)
                {
                    auto slot = raw.Grow(row_size, arena);
                    memcpy(slot, &length, sizeof(length));
                    memcpy(slot + sizeof(length), key, length);
                    memcpy(slot + sizeof(length) + length, payload, payload_size);
                    raw_rows++;
                    return;
                }
                update = arena.Make<Update>(TRAITS::NewUpdate(bind, params));
                Replay(*update, raw.data, raw.size, params);
                raw.Initialize();
                raw_rows = 0;
                TRAITS::template Apply<KEY>(*update, key, length, payload, params);
            }

            // Merges a serialized sketch.
            void Merge(const string_t &blob, const BindData &bind, ArenaAllocator &arena)
            {
                DSScratchScope scratch;
                auto sketch = TRAITS::Deserialize(blob);
                if (sketch.is_empty())
                {
                    // an empty sketch adds nothing to a union
                    saw_empty_sketch = true;
                    return;
                }
                if (part.active && sketch.get_seed_hash() != ThetaSeedHash(part.seed))
                {
                    TRAITS::ThrowSeedMismatch(TRAITS::BUILD_NAME, ThetaSeedHash(part.seed), sketch.get_seed_hash());
                }
                if (TRAITS::HAS_NUM_VALUES)
                {
                    CheckNumValues(TRAITS::NumValues(sketch), TRAITS::BUILD_NAME);
                }
                auto lg_k = bind.infer_lg_k ? MaxValue(bind.lg_k, ThetaInferLgK(sketch)) : bind.lg_k;
                part.Add(sketch, lg_k, ThetaSeedForHash(sketch.get_seed_hash()), Params(bind), arena);
            }

            // Moves what the rows built into the union.
            void FlushRows(uint8_t lg_k, uint64_t seed, const BindData &bind, ArenaAllocator &arena)
            {
                auto params = Params(bind);
                if (update || raw_rows)
                {
                    DSScratchScope scratch;
                    WithRowsSketch(bind, [&](const Compact &rows)
                                   { part.Add(rows, lg_k, seed, params, arena); });
                }
                else if (compact)
                {
                    part.Add(*compact, lg_k, seed, params, arena);
                }
                else
                {
                    DSArenaScope scope(arena);
                    part.Ensure(lg_k, seed, params, arena);
                }
                update = nullptr;
                compact = nullptr;
                raw.Initialize();
                raw_rows = 0;
            }

            // Combines a partial state, which may live in another thread's arena: nothing of it is taken over.
            void Combine(const DSThetaFamilyState &source, const BindData &bind, ArenaAllocator &arena)
            {
                saw_empty_sketch = saw_empty_sketch || source.saw_empty_sketch;
                if (!source.HasData())
                {
                    return;
                }
                auto source_seed = source.Seed(bind);
                if (HasData() && ThetaSeedHash(source_seed) != ThetaSeedHash(Seed(bind)))
                {
                    TRAITS::ThrowSeedMismatch(TRAITS::BUILD_NAME, ThetaSeedHash(Seed(bind)), ThetaSeedHash(source_seed));
                }
                if (TRAITS::HAS_NUM_VALUES)
                {
                    CheckNumValues(source.num_values, TRAITS::BUILD_NAME);
                }
                const uint32_t k = uint32_t(1) << bind.lg_k;
                if (source.OnlyRaw() && source.raw_rows <= k)
                {
                    // The rows' update sketch is exact (at most k keys), so the union of it holds just its keys: keeping
                    // the rows gives the same result. Without summaries, the rows of two such states give the union of
                    // both (at most k keys in all).
                    const bool fresh = !HasData();
                    const bool concatenate = TRAITS::SET_ONLY && OnlyRaw() && raw_rows + source.raw_rows <= MinValue(RAW_ROWS, k) &&
                                             raw.size + source.raw.size <= RAW_BYTES;
                    if (fresh || concatenate)
                    {
                        raw.Append(source.raw.data, source.raw.size, arena);
                        raw_rows += source.raw_rows;
                        return;
                    }
                }
                auto params = Params(bind);
                auto lg_k = MaxValue(source.LgK(bind), LgK(bind));
                auto seed = HasData() ? Seed(bind) : source_seed;
                if (!HasData() && !source.update && !source.raw_rows && !source.compact)
                {
                    // only a union: copied if it is the library's
                    part.AddUnion(source.part, lg_k, seed, params, arena);
                    return;
                }
                FlushRows(lg_k, seed, bind, arena);
                if (source.update || source.raw_rows)
                {
                    DSScratchScope scratch;
                    source.WithRowsSketch(bind, [&](const Compact &rows)
                                          { part.Add(rows, lg_k, seed, params, arena); });
                }
                if (source.compact)
                {
                    part.Add(*source.compact, lg_k, seed, params, arena);
                }
                if (source.part.active)
                {
                    part.AddUnion(source.part, lg_k, seed, params, arena);
                }
            }

            // The aggregate's result.
            string_t Finalize(Vector &result, const BindData &bind) const
            {
                auto params = Params(bind);
                DSScratchScope scratch;
                if (part.active)
                {
                    return Store(result, part.Result(params));
                }
                if (update || raw_rows)
                {
                    string_t stored;
                    WithRowsSketch(bind, [&](const Compact &rows)
                                   { stored = Store(result, rows); });
                    return stored;
                }
                if (compact)
                {
                    return Store(result, *compact);
                }
                return Store(result, TRAITS::Empty(bind.lg_k, bind.seed, params));
            }

            static string_t Store(Vector &result, const Compact &sketch)
            {
                auto bytes = TRAITS::Serialize(sketch);
                return StringVector::AddStringOrBlob(result, const_char_ptr_cast(bytes.data()), bytes.size());
            }

            // The exported state: a version, flags and num_values, then the rows (raw, or the update sketch's compact
            // form) and the union. Raw rows are exported as they are, so importing a small state keeps it small.
            static constexpr uint8_t EXPORT_VERSION = 1;
            static constexpr uint8_t ROWS_NONE = 0;
            static constexpr uint8_t ROWS_RAW = 1;
            static constexpr uint8_t ROWS_COMPACT = 2;

            std::vector<uint8_t> Export(optional_ptr<FunctionData> bind_data) const
            {
                const auto &bind = bind_data->Cast<BindData>();
                auto params = Params(bind);
                DSThetaWriter writer;
                writer.Put<uint8_t>(EXPORT_VERSION);
                writer.Put<uint8_t>(saw_empty_sketch);
                writer.Put<uint8_t>(num_values);
                if (update)
                {
                    DSScratchScope scratch;
                    writer.Put<uint8_t>(ROWS_COMPACT);
                    writer.PutBlock(TRAITS::Serialize(update->compact()));
                }
                else if (compact)
                {
                    DSScratchScope scratch;
                    writer.Put<uint8_t>(ROWS_COMPACT);
                    writer.PutBlock(TRAITS::Serialize(*compact));
                }
                else if (raw_rows)
                {
                    writer.Put<uint8_t>(ROWS_RAW);
                    writer.Put<uint32_t>(raw_rows);
                    writer.Put<uint32_t>(raw.size);
                    writer.PutBytes(raw.data, raw.size);
                }
                else
                {
                    writer.Put<uint8_t>(ROWS_NONE);
                }
                if (part.active)
                {
                    part.Export(writer, params);
                }
                else
                {
                    writer.Put<uint8_t>(0);
                }
                return std::move(writer.bytes);
            }

            void Import(const string_t &blob, ArenaAllocator &arena)
            {
                DSThetaReader reader(blob, TRAITS::KIND);
                if (reader.Get<uint8_t>() != EXPORT_VERSION)
                {
                    reader.Fail();
                }
                const auto flags = reader.Get<uint8_t>();
                if (flags > 1)
                {
                    reader.Fail();
                }
                saw_empty_sketch = flags;
                num_values = reader.Get<uint8_t>();
                DSThetaParams params;
                params.num_values = MaxValue<uint8_t>(num_values, 1);
                // mode is not needed: imported states only copy summaries
                const auto rows = reader.Get<uint8_t>();
                if (rows == ROWS_COMPACT)
                {
                    auto block = reader.GetBlock();
                    DSScratchScope scratch;
                    auto sketch = TRAITS::Deserialize(block);
                    DSArenaScope back(arena);
                    compact = arena.Make<Compact>(sketch);
                }
                else if (rows == ROWS_RAW)
                {
                    const auto count = reader.Get<uint32_t>();
                    const auto size = reader.Get<uint32_t>();
                    auto data = reader.GetBytes(size);
                    // check the rows' framing, which Replay trusts
                    const idx_t payload = TRAITS::PayloadSize(params);
                    idx_t pos = 0;
                    uint32_t found = 0;
                    while (pos < size)
                    {
                        if (size - pos < sizeof(uint32_t))
                        {
                            reader.Fail();
                        }
                        uint32_t length;
                        memcpy(&length, data + pos, sizeof(length));
                        pos += sizeof(length);
                        if ((!std::is_same<KEY, string_t>::value && length != sizeof(KEY)) || size - pos < idx_t(length) + payload)
                        {
                            reader.Fail();
                        }
                        pos += length + payload;
                        found++;
                    }
                    if (found != count || count == 0 || count > RAW_ROWS || size > RAW_BYTES)
                    {
                        reader.Fail();
                    }
                    raw.Append(data, size, arena);
                    raw_rows = count;
                }
                else if (rows != ROWS_NONE)
                {
                    reader.Fail();
                }
                const auto union_kind = reader.Get<uint8_t>();
                if (union_kind == 1 || union_kind == 2)
                {
                    part.Import(reader, union_kind, params, arena);
                }
                else if (union_kind != 0)
                {
                    reader.Fail();
                }
                reader.End();
            }
        };

        // ============================================================
        // Intersection aggregates
        // ============================================================

        // Intersects sketches across rows. The state is the current result: each input is intersected with it in a
        // temporary intersection, which gives what the library's intersection would hold.
        template <class TRAITS>
        struct DSThetaFamilyIntersectState
        {
            using Compact = typename TRAITS::Compact;
            using View = DSThetaEntriesView<TRAITS>;

            //! The result's entries (key, then summary)
            DSArenaBytes entries;
            uint32_t count;
            uint64_t theta;
            uint64_t seed;
            bool has_result;
            bool result_empty;
            //! An empty sketch was intersected: the result is empty
            bool saw_empty;
            uint8_t num_values;

            void Initialize()
            {
                entries.Initialize();
                count = 0;
                theta = datasketches::theta_constants::MAX_THETA;
                seed = DS_THETA_DEFAULT_SEED;
                has_result = false;
                result_empty = false;
                saw_empty = false;
                num_values = 0;
            }

            bool IsEmpty() const
            {
                return !has_result && !saw_empty;
            }

            DSThetaParams Params(uint8_t mode) const
            {
                DSThetaParams params;
                params.mode = mode;
                params.num_values = MaxValue<uint8_t>(num_values, 1);
                return params;
            }

            // The current result as a sketch (allocated in the current, scratch, arena).
            View Current(const DSThetaParams &params) const
            {
                View view(result_empty, seed, theta);
                view.Load(entries.data, count, params);
                return view;
            }

            // Intersects a sketch (read only; it may be another state's) with `sketch_num_values` values per key.
            template <class SKETCH>
            void Add(const SKETCH &sketch, uint8_t sketch_num_values, uint8_t mode, ArenaAllocator &arena)
            {
                if (sketch.is_empty())
                {
                    saw_empty = true;
                    if (num_values == 0)
                    {
                        num_values = sketch_num_values;
                    }
                    return;
                }
                if (!has_result)
                {
                    seed = ThetaSeedForHash(sketch.get_seed_hash());
                    num_values = sketch_num_values;
                }
                else
                {
                    if (sketch.get_seed_hash() != ThetaSeedHash(seed))
                    {
                        TRAITS::ThrowSeedMismatch(TRAITS::INTERSECT_NAME, ThetaSeedHash(seed), sketch.get_seed_hash());
                    }
                    if (TRAITS::HAS_NUM_VALUES && sketch_num_values != num_values)
                    {
                        TRAITS::ThrowNumValues(TRAITS::INTERSECT_NAME, num_values, sketch_num_values);
                    }
                }
                auto params = Params(mode);
                DSScratchScope scratch;
                auto intersection = TRAITS::NewIntersection(seed, params);
                ThetaGuard([&]()
                           {
                               if (has_result)
                               {
                                   intersection.update(Current(params));
                               }
                               intersection.update(sketch); },
                           TRAITS::KIND);
                auto result = intersection.get_result(false);
                // an intersection only shrinks, so the result usually fits where the previous one was
                DSThetaWriter writer;
                DSThetaWriteEntries<TRAITS>(result, writer, params);
                entries.size = 0;
                entries.Append(writer.bytes.data(), writer.bytes.size(), arena);
                count = result.get_num_retained();
                theta = result.get_theta64();
                result_empty = result.is_empty();
                has_result = true;
            }

            void AddBlob(const string_t &blob, uint8_t mode, ArenaAllocator &arena)
            {
                DSScratchScope scratch;
                auto sketch = TRAITS::Deserialize(blob);
                Add(sketch, TRAITS::NumValues(sketch), mode, arena);
            }

            void Combine(const DSThetaFamilyIntersectState &source, uint8_t mode, ArenaAllocator &arena)
            {
                saw_empty = saw_empty || source.saw_empty;
                if (num_values == 0)
                {
                    num_values = source.num_values;
                }
                if (source.has_result)
                {
                    DSScratchScope scratch;
                    Add(source.Current(source.Params(mode)), MaxValue<uint8_t>(source.num_values, 1), mode, arena);
                }
            }

            string_t Finalize(Vector &result, uint8_t mode) const
            {
                auto params = Params(mode);
                DSScratchScope scratch;
                if (saw_empty)
                {
                    return Store(result, TRAITS::Empty(DS_THETA_DEFAULT_LG_K, seed, params));
                }
                auto intersection = TRAITS::NewIntersection(seed, params);
                intersection.update(Current(params));
                return Store(result, intersection.get_result());
            }

            static string_t Store(Vector &result, const Compact &sketch)
            {
                auto bytes = TRAITS::Serialize(sketch);
                return StringVector::AddStringOrBlob(result, const_char_ptr_cast(bytes.data()), bytes.size());
            }

            static constexpr uint8_t EXPORT_VERSION = 1;

            std::vector<uint8_t> Export(optional_ptr<FunctionData> bind_data) const
            {
                const auto mode = DSThetaMode(bind_data);
                auto params = Params(mode);
                DSThetaWriter writer;
                writer.Put<uint8_t>(EXPORT_VERSION);
                writer.Put<uint8_t>(uint8_t(has_result) | uint8_t(result_empty) << 1 | uint8_t(saw_empty) << 2);
                writer.Put<uint8_t>(num_values);
                writer.Put<uint64_t>(theta);
                writer.Put<uint64_t>(seed);
                writer.Put<uint32_t>(count);
                writer.PutBytes(entries.data, count * (sizeof(uint64_t) + TRAITS::SummarySize(params)));
                return std::move(writer.bytes);
            }

            void Import(const string_t &blob, ArenaAllocator &arena)
            {
                DSThetaReader reader(blob, TRAITS::KIND);
                if (reader.Get<uint8_t>() != EXPORT_VERSION)
                {
                    reader.Fail();
                }
                const auto flags = reader.Get<uint8_t>();
                if (flags > 7)
                {
                    reader.Fail();
                }
                has_result = flags & 1;
                result_empty = flags & 2;
                saw_empty = flags & 4;
                num_values = reader.Get<uint8_t>();
                theta = reader.Get<uint64_t>();
                seed = reader.Get<uint64_t>();
                count = reader.Get<uint32_t>();
                const idx_t size = idx_t(count) * (sizeof(uint64_t) + TRAITS::SummarySize(Params(0)));
                entries.Append(reader.GetBytes(size), size, arena);
                reader.End();
            }

        };

        // ============================================================
        // Aggregate operations and state export
        // ============================================================

        // Operations of the build and merge aggregates. IDEMPOTENT: merging the same sketch twice is merging it once.
        template <bool IDEMPOTENT>
        struct DSThetaFamilyOperation
        {
            template <class STATE>
            static void Initialize(STATE &state)
            {
                state.Initialize();
            }

            static bool IgnoreNull() { return true; }

            template <class STATE, class OP>
            static void Combine(const STATE &source, STATE &target, AggregateInputData &aggr_input_data)
            {
                if (source.IsEmpty())
                {
                    return;
                }
                DSArenaScope scope(aggr_input_data.allocator);
                target.Combine(source, DSThetaBind(aggr_input_data.bind_data), aggr_input_data.allocator);
            }

            template <class T, class STATE>
            static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data)
            {
                if (state.IsEmpty())
                {
                    // no input rows: NULL, as for the other sketches' aggregates
                    finalize_data.ReturnNull();
                    return;
                }
                DSArenaScope scope(finalize_data.input.allocator);
                target = state.Finalize(finalize_data.result, DSThetaBind(finalize_data.input.bind_data));
            }

            // Merging serialized sketches.
            template <class A_TYPE, class STATE, class OP>
            static void Operation(STATE &state, const A_TYPE &blob, AggregateUnaryInput &idata)
            {
                DSArenaScope scope(idata.input.allocator);
                state.Merge(blob, DSThetaBind(idata.input.bind_data), idata.input.allocator);
            }

            template <class INPUT_TYPE, class STATE, class OP>
            static void ConstantOperation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input, idx_t count)
            {
                for (idx_t i = 0; i < (IDEMPOTENT ? 1 : count); i++)
                {
                    Operation<INPUT_TYPE, STATE, OP>(state, input, unary_input);
                }
            }
        };

        // The key of a row as bytes: the value of a numeric key, the bytes of a string.
        template <class KEY>
        inline std::pair<const_data_ptr_t, uint32_t> DSThetaKeyBytes(const KEY &key)
        {
            if constexpr (std::is_same<KEY, string_t>::value)
            {
                return {const_data_ptr_cast(key.GetData()), static_cast<uint32_t>(key.GetSize())};
            }
            else
            {
                return {const_data_ptr_cast(&key), static_cast<uint32_t>(sizeof(KEY))};
            }
        }

        // Builds from keys (Theta): adding the same key again does not change the sketch.
        struct DSThetaFamilyCreateOperation : DSThetaFamilyOperation<true>
        {
            template <class A_TYPE, class STATE, class OP>
            static void Operation(STATE &state, const A_TYPE &key, AggregateUnaryInput &idata)
            {
                DSArenaScope scope(idata.input.allocator);
                auto bytes = DSThetaKeyBytes(key);
                state.AddRow(bytes.first, bytes.second, nullptr, DSThetaBind(idata.input.bind_data), idata.input.allocator);
            }

            template <class INPUT_TYPE, class STATE, class OP>
            static void ConstantOperation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input, idx_t count)
            {
                Operation<INPUT_TYPE, STATE, OP>(state, input, unary_input);
            }
        };

        // Builds from (key, DOUBLE value) rows (Tuple).
        struct DSThetaFamilyCreatePairOperation : DSThetaFamilyOperation<false>
        {
            template <class KEY_TYPE, class VALUE_TYPE, class STATE, class OP>
            static void Operation(STATE &state, const KEY_TYPE &key, const VALUE_TYPE &value, AggregateBinaryInput &idata)
            {
                DSArenaScope scope(idata.input.allocator);
                auto bytes = DSThetaKeyBytes(key);
                state.AddRow(bytes.first, bytes.second, const_data_ptr_cast(&value), DSThetaBind(idata.input.bind_data), idata.input.allocator);
            }
        };

        template <bool IDEMPOTENT>
        struct DSThetaFamilyIntersectOperation
        {
            template <class STATE>
            static void Initialize(STATE &state)
            {
                state.Initialize();
            }

            static bool IgnoreNull() { return true; }

            template <class A_TYPE, class STATE, class OP>
            static void Operation(STATE &state, const A_TYPE &blob, AggregateUnaryInput &idata)
            {
                DSArenaScope scope(idata.input.allocator);
                state.AddBlob(blob, DSThetaMode(idata.input.bind_data), idata.input.allocator);
            }

            template <class INPUT_TYPE, class STATE, class OP>
            static void ConstantOperation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input, idx_t count)
            {
                for (idx_t i = 0; i < (IDEMPOTENT ? 1 : count); i++)
                {
                    Operation<INPUT_TYPE, STATE, OP>(state, input, unary_input);
                }
            }

            template <class STATE, class OP>
            static void Combine(const STATE &source, STATE &target, AggregateInputData &aggr_input_data)
            {
                if (source.IsEmpty())
                {
                    return;
                }
                DSArenaScope scope(aggr_input_data.allocator);
                target.Combine(source, DSThetaMode(aggr_input_data.bind_data), aggr_input_data.allocator);
            }

            template <class T, class STATE>
            static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data)
            {
                if (state.IsEmpty())
                {
                    // no sketches: the intersection of nothing is undefined
                    finalize_data.ReturnNull();
                    return;
                }
                DSArenaScope scope(finalize_data.input.allocator);
                target = state.Finalize(finalize_data.result, DSThetaMode(finalize_data.input.bind_data));
            }
        };

        // Exports each state (NULL for a state that saw no rows) for spilling.
        template <class STATE>
        static void DSThetaExportState(Vector &state_vector, AggregateFinalizeInputData &aggr_input_data, Vector &result,
                                       idx_t count, idx_t offset)
        {
            auto states = state_vector.Values<STATE *>();
            auto results = FlatVector::GetDataMutable<string_t>(result);
            DSArenaScope scope(aggr_input_data.allocator);
            for (idx_t i = 0; i < count; i++)
            {
                auto &state = *states[i].GetValue();
                if (state.IsEmpty())
                {
                    FlatVector::SetNull(result, offset + i, true);
                    continue;
                }
                auto bytes = state.Export(aggr_input_data.bind_data);
                results[offset + i] = StringVector::AddStringOrBlob(result, const_char_ptr_cast(bytes.data()), bytes.size());
            }
        }

        // Rebuilds exported states in the arena.
        template <class STATE>
        static void DSThetaImportState(AggregateImportInputData &input)
        {
            const auto count = input.input_vec.size();
            auto entries = input.input_vec.Values<string_t>();
            DSArenaScope scope(input.allocator);
            for (idx_t i = 0; i < count; i++)
            {
                auto &state = *reinterpret_cast<STATE *>(input.dest_buffer + i * input.layout.total_state_size);
                state.Initialize();
                const auto entry = entries[i];
                if (entry.IsValid())
                {
                    state.Import(entry.GetValue(), input.allocator);
                }
            }
        }

        // The exported state is a BLOB; the constant arguments are recorded so that re-binding it gets them.
        template <class STATE>
        static AggregateStateLayout DSThetaGetStateType(AggregateLayoutInput &input)
        {
            AggregateStateLayout layout(LogicalType::BLOB, AlignValue<idx_t>(sizeof(STATE)));
            if (input.bind_data)
            {
                for (auto &constant : input.bind_data->template Cast<DSThetaFamilyBindData>().constants)
                {
                    layout.constant_parameters.emplace(constant.first, constant.second);
                }
            }
            return layout;
        }

        template <class STATE>
        static void DSThetaSetStateExport(AggregateFunction &fun)
        {
            fun.SetStateExportCallbacks(DSThetaGetStateType<STATE>, DSThetaExportState<STATE>, DSThetaImportState<STATE>);
        }

} // namespace duckdb
