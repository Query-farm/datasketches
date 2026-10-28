#include "datasketches_arena_buffer.hpp"

#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/string_vector.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/parser/parsed_data/create_aggregate_function_info.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"

#include <DataSketches/bloom_filter.hpp>
#include <DataSketches/common_defs.hpp>

#include <cmath>

namespace duckdb
{
    namespace
    {
        using BloomFilter = datasketches::bloom_filter_alloc<DSAllocator<uint8_t>>;

        const vector<string> DS_BLOOM_CATEGORIES = {"sketch", "bloom_filter"};

        // Every filter is built with the same seed: filters can only be combined, and queried with the same hashes,
        // if they share it (the library otherwise picks a random seed per filter).
        constexpr uint64_t DS_BLOOM_SEED = datasketches::DEFAULT_SEED;

        // The largest filter the library builds (it stores the bit array's length in 32-bit words).
        constexpr uint64_t DS_BLOOM_MAX_BITS = (static_cast<uint64_t>(NumericLimits<int32_t>::Maximum()) - 32) * 64;

        const DSExampleSources &DSBloomExamples()
        {
            static const DSExampleSources examples("datasketch_bloom_filter(1000, 0.01, i)");
            return examples;
        }

        BloomFilter DSBloomDeserialize(const string_t &blob)
        {
            return DSDeserialize<BloomFilter>(blob, "Bloom filter");
        }

        // Counts the bits set first: the header caches the count, and a filter that was updated since it was last
        // counted stores "unknown" instead. Counting makes equal filters serialize to equal bytes however they were
        // built (e.g. in one thread or merged from several), and spares readers the count.
        string_t DSBloomSerialize(Vector &result, BloomFilter &filter)
        {
            filter.get_bits_used();
            auto bytes = filter.serialize();
            return StringVector::AddStringOrBlob(result, const_char_ptr_cast(bytes.data()), bytes.size());
        }

        // Adds `value` to the filter, or tests whether it may be in it (see DSHashKey for how values are hashed).
        template <bool IS_UUID, class T>
        void DSBloomUpdate(BloomFilter &filter, const T &value)
        {
            DSHashKey<IS_UUID>(value, [&](auto... key) { filter.update(key...); });
        }

        template <bool IS_UUID, class T>
        bool DSBloomQuery(const BloomFilter &filter, const T &value)
        {
            return DSHashKey<IS_UUID>(value, [&](auto... key) { return filter.query(key...); });
        }

        // Combines `source` into `target`, reporting filters of different sizes as invalid input.
        void DSBloomCombine(BloomFilter &target, const BloomFilter &source, const char *function_name)
        {
            if (!target.is_compatible(source))
            {
                throw InvalidInputException("%s: cannot combine Bloom filters with different capacities, numbers of hashes "
                                            "or seeds (%llu bits, %u hashes and %llu bits, %u hashes)",
                                            function_name, target.get_capacity(), target.get_num_hashes(), source.get_capacity(),
                                            source.get_num_hashes());
            }
            target.union_with(source);
        }

        // ============================================================
        // 1. Bind Data
        // ============================================================
        struct DSBloomBindData : public FunctionData
        {
            // The merge aggregate's: its filters carry their own sizes.
            DSBloomBindData() : max_distinct_items(0), false_positive_rate(0), filter_bytes(0)
            {
            }
            DSBloomBindData(uint64_t max_distinct_items, double false_positive_rate)
                : max_distinct_items(max_distinct_items), false_positive_rate(false_positive_rate),
                  filter_bytes(((BloomFilter::builder::suggest_num_filter_bits(max_distinct_items, false_positive_rate) + 63) / 64) * 8)
            {
            }
            unique_ptr<FunctionData> Copy() const override
            {
                return make_uniq<DSBloomBindData>(*this);
            }
            bool Equals(const FunctionData &other_p) const override
            {
                auto &other = other_p.Cast<DSBloomBindData>();
                return max_distinct_items == other.max_distinct_items && false_positive_rate == other.false_positive_rate;
            }
            BloomFilter Create(const DSAllocator<uint8_t> &allocator = DSAllocator<uint8_t>()) const
            {
                return BloomFilter::builder::create_by_accuracy(max_distinct_items, false_positive_rate, DS_BLOOM_SEED, allocator);
            }
            uint64_t max_distinct_items;
            double false_positive_rate;
            // The size of the filter's bit array.
            idx_t filter_bytes;
        };

        // datasketch_bloom_filter(max_distinct_items, false_positive_rate, data): both constants size the filter.
        unique_ptr<FunctionData> DSBloomBind(BindAggregateFunctionInput &input)
        {
            auto &context = input.GetClientContext();
            auto &arguments = input.GetArguments();
            auto max_items = DSBindConstant(context, *arguments[0], "Bloom filter max_distinct_items must be a constant",
                                            "Bloom filter max_distinct_items cannot be NULL")
                                 .GetValue<int64_t>();
            auto rate = DSBindConstant(context, *arguments[1], "Bloom filter false_positive_rate must be a constant",
                                       "Bloom filter false_positive_rate cannot be NULL")
                            .GetValue<double>();
            if (max_items < 1)
            {
                throw BinderException("Bloom filter max_distinct_items must be at least 1, got: %lld", (long long)max_items);
            }
            if (!(rate > 0.0 && rate < 1.0))
            {
                throw BinderException("Bloom filter false_positive_rate must be between 0 and 1 (exclusive), got: %f", rate);
            }
            auto bits = BloomFilter::builder::suggest_num_filter_bits(static_cast<uint64_t>(max_items), rate);
            if (bits > DS_BLOOM_MAX_BITS)
            {
                throw BinderException("Bloom filter for %lld items at a false positive rate of %f would need %llu bits, more "
                                      "than the maximum of %llu",
                                      (long long)max_items, rate, bits, DS_BLOOM_MAX_BITS);
            }
            return make_uniq<DSBloomBindData>(static_cast<uint64_t>(max_items), rate);
        }

        unique_ptr<FunctionData> DSBloomUnionBind(BindAggregateFunctionInput &)
        {
            return make_uniq<DSBloomBindData>();
        }

        // The constants an exported state is re-bound with.
        void DSBloomConstants(const DSBloomBindData &bind, unordered_map<idx_t, Value> &constants)
        {
            constants.emplace(0, Value::BIGINT(static_cast<int64_t>(bind.max_distinct_items)));
            constants.emplace(1, Value::DOUBLE(bind.false_positive_rate));
        }

        // ============================================================
        // 2. State & Operations
        // ============================================================
        // The aggregates' state, in the aggregate arena (see datasketches_arena_buffer.hpp): the first values, or the
        // filter. Values of every type are buffered as they are (strings copied into the arena): a filter never keeps
        // them, only their hashes.
        template <class T, bool IS_UUID>
        struct DSBloomPolicy
        {
            using BIND = DSBloomBindData;
            using VALUE = T;
            using WEIGHT = void;
            using SKETCH = BloomFilter;
            using ENTRY = DSBufferedEntry<T, void>;
            static constexpr const char *NAME = "Bloom filter";
            static constexpr uint32_t BUFFERED = 32;

            static DSAllocator<uint8_t> ArenaMode()
            {
                return DSAllocator<uint8_t>(DSArenaMode());
            }
            static idx_t Budget(const BIND &bind) { return bind.filter_bytes; }
            static ENTRY Entry(const T &value) { return ENTRY {value}; }
            static BloomFilter *Create(const BIND &bind, ArenaAllocator &arena)
            {
                return arena.Make<BloomFilter>(bind.Create(ArenaMode()));
            }
            static BloomFilter Temporary(const BIND &bind) { return bind.Create(); }
            static void Update(BloomFilter &filter, const ENTRY &entry) { DSBloomUpdate<IS_UUID>(filter, entry.value); }
            // See DSBloomSerialize.
            static auto Serialize(BloomFilter &filter)
            {
                filter.get_bits_used();
                return filter.serialize();
            }
            static BloomFilter *Deserialize(const string_t &blob, ArenaAllocator &arena)
            {
                return arena.Make<BloomFilter>(DSDeserialize<BloomFilter>(blob, "Bloom filter", ArenaMode()));
            }
            // The copy allocates through the source's allocator, which in arena mode is the current (target's) arena.
            static BloomFilter *Copy(const BloomFilter &source, ArenaAllocator &arena)
            {
                return arena.Make<BloomFilter>(source);
            }
            static void Combine(BloomFilter &target, const BloomFilter &source, const BIND &, ArenaAllocator &)
            {
                DSBloomCombine(target, source, "datasketch_bloom_filter");
            }
            // An input filter is read on the heap and freed once combined, except the first, which becomes the state's.
            static void Merge(BloomFilter *&filter, const string_t &blob, const BIND &, ArenaAllocator &arena)
            {
                if (!filter)
                {
                    filter = Deserialize(blob, arena);
                    return;
                }
                DSBloomCombine(*filter, DSBloomDeserialize(blob), "datasketch_bloom_filter");
            }
        };

        // Adding a value (or a filter) again does not change the filter.
        template <class BASE>
        struct DSBloomIdempotent : BASE
        {
            template <class INPUT_TYPE, class STATE, class OP>
            static void ConstantOperation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input, idx_t)
            {
                BASE::template Operation<INPUT_TYPE, STATE, OP>(state, input, unary_input);
            }
        };

        // ============================================================
        // 3. Scalar Functions
        // ============================================================
        template <class MARKER>
        void DSBloomContains(DataChunk &args, ExpressionState &, Vector &result)
        {
            using INPUT = DSCountingInput<MARKER>;
            DSSketchCache<BloomFilter> cache;
            BinaryExecutor::Execute<string_t, typename INPUT::physical_type, bool>(
                args.data[0], args.data[1], result, [&](string_t blob, const typename INPUT::physical_type &item)
                { return DSBloomQuery<INPUT::is_uuid>(cache.Get(blob, "Bloom filter"), item); });
        }

        template <class RESULT_TYPE, class FUNC>
        void DSBloomFilterFunction(DataChunk &args, Vector &result, FUNC &&fun)
        {
            UnaryExecutor::Execute<string_t, RESULT_TYPE>(args.data[0], result, [&](string_t blob)
                                                          { return static_cast<RESULT_TYPE>(fun(DSBloomDeserialize(blob))); });
        }

        void DSBloomIsEmpty(DataChunk &args, ExpressionState &, Vector &result)
        {
            DSBloomFilterFunction<bool>(args, result, [](const BloomFilter &filter) { return filter.is_empty(); });
        }

        void DSBloomCapacity(DataChunk &args, ExpressionState &, Vector &result)
        {
            DSBloomFilterFunction<uint64_t>(args, result, [](const BloomFilter &filter) { return filter.get_capacity(); });
        }

        void DSBloomNumHashes(DataChunk &args, ExpressionState &, Vector &result)
        {
            DSBloomFilterFunction<uint16_t>(args, result, [](const BloomFilter &filter) { return filter.get_num_hashes(); });
        }

        void DSBloomBitsUsed(DataChunk &args, ExpressionState &, Vector &result)
        {
            DSBloomFilterFunction<uint64_t>(args, result, [](BloomFilter filter) { return filter.get_bits_used(); });
        }

        // The probability that an item that was never added tests positive, given how full the filter is:
        // (bits set / capacity) ^ number of hashes.
        void DSBloomFalsePositiveRate(DataChunk &args, ExpressionState &, Vector &result)
        {
            DSBloomFilterFunction<double>(args, result, [](BloomFilter filter)
                                          {
                auto fill = static_cast<double>(filter.get_bits_used()) / static_cast<double>(filter.get_capacity());
                return std::pow(fill, filter.get_num_hashes()); });
        }

        void DSBloomDescribe(DataChunk &args, ExpressionState &, Vector &result)
        {
            UnaryExecutor::Execute<string_t, string_t>(args.data[0], result, [&](string_t blob)
                                                       {
                auto text = DSBloomDeserialize(blob).to_string();
                return StringVector::AddString(result, text.data(), text.size()); });
        }

        void DSBloomIsCompatible(DataChunk &args, ExpressionState &, Vector &result)
        {
            BinaryExecutor::Execute<string_t, string_t, bool>(args.data[0], args.data[1], result, [&](string_t a, string_t b)
                                                              { return DSBloomDeserialize(a).is_compatible(DSBloomDeserialize(b)); });
        }

        template <bool INTERSECT>
        void DSBloomSetOperation(DataChunk &args, ExpressionState &, Vector &result)
        {
            const char *function_name = INTERSECT ? "datasketch_bloom_filter_intersect" : "datasketch_bloom_filter_union";
            BinaryExecutor::Execute<string_t, string_t, string_t>(args.data[0], args.data[1], result, [&](string_t a, string_t b)
                                                                  {
                auto filter = DSBloomDeserialize(a);
                auto other = DSBloomDeserialize(b);
                if (INTERSECT)
                {
                    if (!filter.is_compatible(other))
                    {
                        // Reuse the union's message for incompatible filters.
                        DSBloomCombine(filter, other, function_name);
                    }
                    filter.intersect(other);
                }
                else
                {
                    DSBloomCombine(filter, other, function_name);
                }
                return DSBloomSerialize(result, filter); });
        }
    } // namespace

    // ============================================================
    // 4. Main Loader
    // ============================================================
    void LoadBloomFilter(ExtensionLoader &loader)
    {
        auto sketch_type = DSCreateSketchType(loader, "sketch_bloom_filter", "Sketch type for Bloom filters");
        const auto &ex = DSBloomExamples();

        // --- datasketch_bloom_filter(max_distinct_items, false_positive_rate, data). The constants stay in the
        // argument list (see DSReadDataArgumentAt); the data follows them.
        {
            AggregateFunctionSet set("datasketch_bloom_filter");
            vector<FunctionDescription> descs;
            DSForEachHashedType([&](auto marker, const LogicalType &item_type)
                                {
                using INPUT = DSCountingInput<decltype(marker)>;
                using T = typename INPUT::physical_type;
                using STATE = DSBufferedArenaState<DSBloomPolicy<T, INPUT::is_uuid>>;
                using OP = DSBloomIdempotent<DSArenaCreateOperation<DSBloomBindData, INPUT::is_uuid>>;
                auto fun = AggregateFunction::UnaryAggregate<STATE, T, string_t, OP>(item_type, sketch_type);
                DSReadDataArgumentAt<2, STATE, T, OP>(fun);
                DSSetBufferedStateExport<STATE, DSBloomBindData, DSBloomConstants>(fun);
                DSAddAggregate(set, descs, fun, DSBloomBind, {LogicalType::BIGINT, LogicalType::DOUBLE, item_type},
                               {"max_distinct_items", "false_positive_rate", "data"},
                               "Creates a Bloom filter sized to hold max_distinct_items distinct values with the given false positive rate. "
                               "Equal values of different types (e.g. 5 and 5::BIGINT) test alike, as do -0.0 and 0.0",
                               {"SELECT datasketch_bloom_filter(1000, 0.01, i) FROM range(100) t(i)"}); });

            // datasketch_bloom_filter(filter): combines filters.
            using MERGE_STATE = DSBufferedArenaState<DSBloomPolicy<int64_t, false>>;
            using MERGE = DSBloomIdempotent<DSArenaMergeOperation<DSBloomBindData>>;
            auto merge = AggregateFunction::UnaryAggregate<MERGE_STATE, string_t, string_t, MERGE>(sketch_type, sketch_type);
            DSSetBufferedStateExport<MERGE_STATE, DSBloomBindData, DSNoConstants<DSBloomBindData>>(merge);
            DSAddAggregate(set, descs, merge, DSBloomUnionBind, {sketch_type}, {"filter"},
                           "Combines Bloom filters built with the same max_distinct_items and false_positive_rate into one that "
                           "contains the values of each",
                           {ex.QueryPartials("datasketch_bloom_filter(sketch)")});
            DSRegisterAggregates(loader, std::move(set), std::move(descs), DS_BLOOM_CATEGORIES);
        }

        // --- datasketch_bloom_filter_contains(filter, item), for every input type
        {
            ScalarFunctionSet set("datasketch_bloom_filter_contains");
            vector<FunctionDescription> descs;
            DSForEachHashedType([&](auto marker, const LogicalType &item_type)
                                { DSAddScalar(set, descs, ScalarFunction({sketch_type, item_type}, LogicalType::BOOLEAN, DSBloomContains<decltype(marker)>),
                                              {"filter", "item"},
                                              "Returns false if the item was certainly not added to the filter, true if it probably was "
                                              "(a false positive happens at about the filter's false positive rate)",
                                              {"SELECT datasketch_bloom_filter_contains(sketch, 42) FROM " + ex.Sketch("sketch", 0, 1000) + " s"}); });
            DSRegisterScalars(loader, std::move(set), std::move(descs), DS_BLOOM_CATEGORIES);
        }

        // --- Filter properties
        auto register_filter_function = [&](const string &name, const LogicalType &return_type, scalar_function_t function,
                                            const string &description)
        {
            DSRegisterScalar(loader, ScalarFunction(Identifier(name), {sketch_type}, return_type, std::move(function)),
                             DSDescription(description, {ex.Query(name + "(sketch)")}, {"filter"}, {sketch_type}), DS_BLOOM_CATEGORIES);
        };
        register_filter_function("datasketch_bloom_filter_is_empty", LogicalType::BOOLEAN, DSBloomIsEmpty,
                                 "Returns true if no value was added to the filter");
        register_filter_function("datasketch_bloom_filter_capacity", LogicalType::UBIGINT, DSBloomCapacity,
                                 "Returns the number of bits in the filter");
        register_filter_function("datasketch_bloom_filter_num_hashes", LogicalType::USMALLINT, DSBloomNumHashes,
                                 "Returns the number of hash functions the filter sets per value");
        register_filter_function("datasketch_bloom_filter_bits_used", LogicalType::UBIGINT, DSBloomBitsUsed,
                                 "Returns the number of bits set in the filter");
        register_filter_function("datasketch_bloom_filter_false_positive_rate", LogicalType::DOUBLE, DSBloomFalsePositiveRate,
                                 "Returns the filter's current false positive rate, (bits_used / capacity) ^ num_hashes: the probability "
                                 "that a value that was never added tests positive");
        register_filter_function("datasketch_bloom_filter_describe", LogicalType::VARCHAR, DSBloomDescribe,
                                 "Returns a description of the filter");

        // --- Two filters
        auto register_pair_function = [&](const string &name, const LogicalType &return_type, scalar_function_t function,
                                          const string &description)
        {
            DSRegisterScalar(loader, ScalarFunction(Identifier(name), {sketch_type, sketch_type}, return_type, std::move(function)),
                             DSDescription(description, {ex.Query(name + "(sketch_a, sketch_b)")}, {"filter_a", "filter_b"},
                                           {sketch_type, sketch_type}),
                             DS_BLOOM_CATEGORIES);
        };
        register_pair_function("datasketch_bloom_filter_union", sketch_type, DSBloomSetOperation<false>,
                               "Returns a filter containing the values of both filters, which must have the same capacity and number of hashes");
        register_pair_function("datasketch_bloom_filter_intersect", sketch_type, DSBloomSetOperation<true>,
                               "Returns a filter that tests positive for the values both filters test positive for (their bitwise AND); "
                               "the filters must have the same capacity and number of hashes");
        register_pair_function("datasketch_bloom_filter_is_compatible", LogicalType::BOOLEAN, DSBloomIsCompatible,
                               "Returns true if the filters have the same capacity, number of hashes and seed, so they can be combined");
    }
} // namespace duckdb
