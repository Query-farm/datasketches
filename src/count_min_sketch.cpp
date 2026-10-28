#include "datasketches_arena_buffer.hpp"

#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/string_vector.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/parser/parsed_data/create_aggregate_function_info.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"

#include <DataSketches/count_min.hpp>

#include <cmath>

namespace duckdb
{
    namespace
    {
        // Signed weights, so rows can also subtract from an item's count.
        using CountMinSketch = datasketches::count_min_sketch<int64_t, DSAllocator<int64_t>>;

        const vector<string> DS_COUNT_MIN_CATEGORIES = {"sketch", "count_min"};

        // The library's limit on the number of counters (num_hashes * num_buckets).
        constexpr uint64_t DS_COUNT_MIN_MAX_COUNTERS = uint64_t(1) << 30;

        const DSExampleSources &DSCountMinExamples()
        {
            static const DSExampleSources examples("datasketch_count_min(3, 128, i % 10)");
            return examples;
        }

        CountMinSketch DSCountMinDeserialize(const string_t &blob)
        {
            return DSDeserialize<CountMinSketch>(blob, "Count-Min");
        }

        string_t DSCountMinSerialize(Vector &result, const CountMinSketch &sketch)
        {
            auto bytes = sketch.serialize();
            return StringVector::AddStringOrBlob(result, const_char_ptr_cast(bytes.data()), bytes.size());
        }

        // Passes a hash key (see DSHashKey) to fun as the library's item arguments: it hashes integers and bytes only,
        // so a DOUBLE is its canonical bits.
        template <class FUNC>
        auto DSCountMinKey(int64_t key, FUNC &&fun) { return fun(key); }
        template <class FUNC>
        auto DSCountMinKey(double key, FUNC &&fun) { return fun(DSCanonicalDoubleBits(key)); }
        template <class FUNC>
        auto DSCountMinKey(const void *data, size_t size, FUNC &&fun) { return fun(data, size); }

        // Adds `weight` to the item's count.
        template <bool IS_UUID, class T>
        void DSCountMinUpdate(CountMinSketch &sketch, const T &value, int64_t weight)
        {
            DSHashKey<IS_UUID>(value, [&](auto... key)
                               { DSCountMinKey(key..., [&](auto... item) { sketch.update(item..., weight); }); });
        }

        // fun(sketch, item...) for the item, hashed as DSCountMinUpdate hashes it.
        template <bool IS_UUID, class T, class FUNC>
        int64_t DSCountMinQuery(const CountMinSketch &sketch, const T &value, FUNC &&fun)
        {
            return DSHashKey<IS_UUID>(value, [&](auto... key)
                                      { return DSCountMinKey(key..., [&](auto... item) { return fun(sketch, item...); }); });
        }

        void DSCountMinMerge(CountMinSketch &target, const CountMinSketch &source, const char *function_name)
        {
            if (target.get_num_hashes() != source.get_num_hashes() || target.get_num_buckets() != source.get_num_buckets())
            {
                throw InvalidInputException("%s: cannot merge Count-Min sketches of different sizes (%u hashes x %u buckets "
                                            "and %u hashes x %u buckets)",
                                            function_name, target.get_num_hashes(), target.get_num_buckets(),
                                            source.get_num_hashes(), source.get_num_buckets());
            }
            try
            {
                target.merge(source);
            }
            catch (std::invalid_argument &e)
            {
                throw InvalidInputException("%s: %s", function_name, e.what());
            }
        }

        // ============================================================
        // 1. Bind Data
        // ============================================================
        struct DSCountMinBindData : public FunctionData
        {
            DSCountMinBindData(uint8_t num_hashes, uint32_t num_buckets) : num_hashes(num_hashes), num_buckets(num_buckets) {}
            unique_ptr<FunctionData> Copy() const override { return make_uniq<DSCountMinBindData>(num_hashes, num_buckets); }
            bool Equals(const FunctionData &other_p) const override
            {
                auto &other = other_p.Cast<DSCountMinBindData>();
                return num_hashes == other.num_hashes && num_buckets == other.num_buckets;
            }
            uint8_t num_hashes;
            uint32_t num_buckets;
        };

        void DSCountMinCheckSize(int64_t num_hashes, int64_t num_buckets)
        {
            if (num_hashes < 1 || num_hashes > 255)
            {
                throw BinderException("Count-Min num_hashes must be between 1 and 255, got: %lld", (long long)num_hashes);
            }
            if (num_buckets < 3)
            {
                throw BinderException("Count-Min num_buckets must be at least 3, got: %lld", (long long)num_buckets);
            }
            if (static_cast<uint64_t>(num_hashes) * static_cast<uint64_t>(num_buckets) >= DS_COUNT_MIN_MAX_COUNTERS)
            {
                throw BinderException("Count-Min num_hashes * num_buckets must be less than 2^30, got: %lld * %lld",
                                      (long long)num_hashes, (long long)num_buckets);
            }
        }

        // datasketch_count_min(num_hashes, num_buckets, data[, weight])
        unique_ptr<FunctionData> DSCountMinBind(BindAggregateFunctionInput &input)
        {
            auto &context = input.GetClientContext();
            auto &arguments = input.GetArguments();
            auto num_hashes = DSBindConstant(context, *arguments[0], "Count-Min num_hashes must be a constant",
                                             "Count-Min num_hashes cannot be NULL")
                                  .GetValue<int64_t>();
            auto num_buckets = DSBindConstant(context, *arguments[1], "Count-Min num_buckets must be a constant",
                                              "Count-Min num_buckets cannot be NULL")
                                   .GetValue<int64_t>();
            DSCountMinCheckSize(num_hashes, num_buckets);
            return make_uniq<DSCountMinBindData>(static_cast<uint8_t>(num_hashes), static_cast<uint32_t>(num_buckets));
        }

        // The merge aggregate's bind data: its sketches carry their own sizes.
        unique_ptr<FunctionData> DSCountMinMergeBind(BindAggregateFunctionInput &)
        {
            return make_uniq<DSCountMinBindData>(0, 0);
        }

        // The constants an exported state is re-bound with.
        void DSCountMinConstants(const DSCountMinBindData &bind, unordered_map<idx_t, Value> &constants)
        {
            constants.emplace(0, Value::INTEGER(bind.num_hashes));
            constants.emplace(1, Value::INTEGER(static_cast<int32_t>(bind.num_buckets)));
        }

        // ============================================================
        // 2. State & Operations
        // ============================================================
        // The aggregates' state, in the aggregate arena (see datasketches_arena_buffer.hpp): the first values with
        // their weights, or the sketch. Values of every type are buffered as they are (strings copied into the arena):
        // a sketch never keeps them, only their hashes. Counters are sums, so feeding the buffered values later gives
        // the same sketch.
        template <class T, bool IS_UUID>
        struct DSCountMinPolicy
        {
            using BIND = DSCountMinBindData;
            using VALUE = T;
            using WEIGHT = int64_t;
            using SKETCH = CountMinSketch;
            using ENTRY = DSBufferedEntry<T, int64_t>;
            static constexpr const char *NAME = "Count-Min";
            static constexpr uint32_t BUFFERED = 32;

            static DSAllocator<int64_t> ArenaMode()
            {
                return DSAllocator<int64_t>(DSArenaMode());
            }
            static idx_t Budget(const BIND &bind)
            {
                return static_cast<idx_t>(bind.num_hashes) * bind.num_buckets * sizeof(int64_t);
            }
            static ENTRY Entry(const T &value, int64_t weight = 1) { return ENTRY {value, weight}; }
            static CountMinSketch *Create(const BIND &bind, ArenaAllocator &arena)
            {
                return arena.Make<CountMinSketch>(bind.num_hashes, bind.num_buckets, datasketches::DEFAULT_SEED, ArenaMode());
            }
            static CountMinSketch Temporary(const BIND &bind) { return CountMinSketch(bind.num_hashes, bind.num_buckets); }
            static void Update(CountMinSketch &sketch, const ENTRY &entry)
            {
                DSCountMinUpdate<IS_UUID>(sketch, entry.value, entry.weight);
            }
            static auto Serialize(const CountMinSketch &sketch) { return sketch.serialize(); }
            static CountMinSketch *Deserialize(const string_t &blob, ArenaAllocator &arena)
            {
                return arena.Make<CountMinSketch>(DSDeserialize<CountMinSketch>(blob, "Count-Min", datasketches::DEFAULT_SEED, ArenaMode()));
            }
            // The copy allocates through the source's allocator, which in arena mode is the current (target's) arena
            // (with vcpkg-overlay/apache-datasketches/count-min-hash-seeds-allocator.patch).
            static CountMinSketch *Copy(const CountMinSketch &source, ArenaAllocator &arena)
            {
                return arena.Make<CountMinSketch>(source);
            }
            static void Combine(CountMinSketch &target, const CountMinSketch &source, const BIND &, ArenaAllocator &)
            {
                DSCountMinMerge(target, source, "datasketch_count_min");
            }
            // An input sketch is read on the heap and freed once merged, except the first, which becomes the state's.
            static void Merge(CountMinSketch *&sketch, const string_t &blob, const BIND &, ArenaAllocator &arena)
            {
                if (!sketch)
                {
                    sketch = Deserialize(blob, arena);
                    return;
                }
                DSCountMinMerge(*sketch, DSCountMinDeserialize(blob), "datasketch_count_min");
            }
        };

        // Each row counts once; a repeated value counts as many times as it repeats.
        template <bool IS_UUID>
        struct DSCountMinOperation : DSArenaCreateOperation<DSCountMinBindData, IS_UUID>
        {
            template <class INPUT_TYPE, class STATE, class OP>
            static void ConstantOperation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input, idx_t count)
            {
                DSArenaScope scope(unary_input.input.allocator);
                state.UpdateWeighted(input, static_cast<int64_t>(count), unary_input.input.bind_data->template Cast<DSCountMinBindData>(),
                                     unary_input.input.allocator);
            }
        };

        // ============================================================
        // 3. Scalar Functions
        // ============================================================
        template <class MARKER, class FUNC>
        void DSCountMinItemFunction(DataChunk &args, Vector &result, FUNC &&fun)
        {
            using INPUT = DSCountingInput<MARKER>;
            DSSketchCache<CountMinSketch> cache;
            BinaryExecutor::Execute<string_t, typename INPUT::physical_type, int64_t>(
                args.data[0], args.data[1], result, [&](string_t blob, const typename INPUT::physical_type &item)
                { return DSCountMinQuery<INPUT::is_uuid>(cache.Get(blob, "Count-Min"), item, fun); });
        }

        struct DSCountMinEstimate
        {
            template <class... KEY>
            int64_t operator()(const CountMinSketch &sketch, KEY... key) const { return sketch.get_estimate(key...); }
        };
        struct DSCountMinLowerBound
        {
            template <class... KEY>
            int64_t operator()(const CountMinSketch &sketch, KEY... key) const { return sketch.get_lower_bound(key...); }
        };
        struct DSCountMinUpperBound
        {
            template <class... KEY>
            int64_t operator()(const CountMinSketch &sketch, KEY... key) const { return sketch.get_upper_bound(key...); }
        };

        template <class MARKER, class FUNC>
        void DSCountMinItem(DataChunk &args, ExpressionState &, Vector &result)
        {
            DSCountMinItemFunction<MARKER>(args, result, FUNC());
        }

        template <class RESULT_TYPE, class FUNC>
        void DSCountMinSketchFunction(DataChunk &args, Vector &result, FUNC &&fun)
        {
            UnaryExecutor::Execute<string_t, RESULT_TYPE>(args.data[0], result, [&](string_t blob)
                                                          { return static_cast<RESULT_TYPE>(fun(DSCountMinDeserialize(blob))); });
        }

        void DSCountMinIsEmpty(DataChunk &args, ExpressionState &, Vector &result)
        {
            DSCountMinSketchFunction<bool>(args, result, [](const CountMinSketch &sketch) { return sketch.is_empty(); });
        }

        void DSCountMinTotalWeight(DataChunk &args, ExpressionState &, Vector &result)
        {
            DSCountMinSketchFunction<int64_t>(args, result, [](const CountMinSketch &sketch) { return sketch.get_total_weight(); });
        }

        void DSCountMinNumHashes(DataChunk &args, ExpressionState &, Vector &result)
        {
            DSCountMinSketchFunction<uint8_t>(args, result, [](const CountMinSketch &sketch) { return sketch.get_num_hashes(); });
        }

        void DSCountMinNumBuckets(DataChunk &args, ExpressionState &, Vector &result)
        {
            DSCountMinSketchFunction<uint32_t>(args, result, [](const CountMinSketch &sketch) { return sketch.get_num_buckets(); });
        }

        void DSCountMinRelativeError(DataChunk &args, ExpressionState &, Vector &result)
        {
            DSCountMinSketchFunction<double>(args, result, [](const CountMinSketch &sketch) { return sketch.get_relative_error(); });
        }

        void DSCountMinDescribe(DataChunk &args, ExpressionState &, Vector &result)
        {
            UnaryExecutor::Execute<string_t, string_t>(args.data[0], result, [&](string_t blob)
                                                       {
                auto text = DSCountMinDeserialize(blob).to_string();
                return StringVector::AddString(result, text.data(), text.size()); });
        }

        void DSCountMinMergeScalar(DataChunk &args, ExpressionState &, Vector &result)
        {
            BinaryExecutor::Execute<string_t, string_t, string_t>(args.data[0], args.data[1], result, [&](string_t a, string_t b)
                                                                  {
                auto sketch = DSCountMinDeserialize(a);
                DSCountMinMerge(sketch, DSCountMinDeserialize(b), "datasketch_count_min_merge");
                return DSCountMinSerialize(result, sketch); });
        }

        // The number of buckets that keeps an estimate within relative_error * total_weight of the true count.
        void DSCountMinSuggestNumBuckets(DataChunk &args, ExpressionState &, Vector &result)
        {
            UnaryExecutor::Execute<double, int64_t>(args.data[0], result, [&](double relative_error)
                                                    {
                if (!(relative_error > 0.0) || std::isinf(relative_error))
                {
                    throw InvalidInputException("datasketch_count_min_suggest_num_buckets: relative_error must be a positive number, got: %f", relative_error);
                }
                return static_cast<int64_t>(std::ceil(std::exp(1.0) / relative_error)); });
        }

        // The number of hashes that keeps an estimate within its error bound with the given probability.
        void DSCountMinSuggestNumHashes(DataChunk &args, ExpressionState &, Vector &result)
        {
            UnaryExecutor::Execute<double, int64_t>(args.data[0], result, [&](double confidence)
                                                    {
                if (!(confidence >= 0.0 && confidence < 1.0))
                {
                    throw InvalidInputException("datasketch_count_min_suggest_num_hashes: confidence must be at least 0 and less than 1, got: %f", confidence);
                }
                return static_cast<int64_t>(MinValue<double>(std::ceil(std::log(1.0 / (1.0 - confidence))), 255.0)); });
        }
    } // namespace

    // ============================================================
    // 4. Main Loader
    // ============================================================
    void LoadCountMinSketch(ExtensionLoader &loader)
    {
        auto sketch_type = DSCreateSketchType(loader, "sketch_count_min", "Sketch type for Count-Min sketches");
        const auto &ex = DSCountMinExamples();
        const string rows = " FROM (SELECT i % 10 AS item, i AS weight FROM range(1000) t(i)) t";

        // --- datasketch_count_min(num_hashes, num_buckets, data), and datasketch_count_min(sketch) to merge. The
        // constants stay in the argument list (see DSReadDataArgumentAt); the data follows them.
        {
            AggregateFunctionSet set("datasketch_count_min");
            vector<FunctionDescription> descs;
            DSForEachHashedType([&](auto marker, const LogicalType &item_type)
                                {
                using INPUT = DSCountingInput<decltype(marker)>;
                using OP = DSCountMinOperation<INPUT::is_uuid>;
                using T = typename INPUT::physical_type;
                using STATE = DSBufferedArenaState<DSCountMinPolicy<T, INPUT::is_uuid>>;
                auto fun = AggregateFunction::UnaryAggregate<STATE, T, string_t, OP>(item_type, sketch_type);
                DSReadDataArgumentAt<2, STATE, T, OP>(fun);
                DSSetBufferedStateExport<STATE, DSCountMinBindData, DSCountMinConstants>(fun);
                DSAddAggregate(set, descs, fun, DSCountMinBind, {LogicalType::INTEGER, LogicalType::INTEGER, item_type},
                               {"num_hashes", "num_buckets", "data"},
                               "Creates a Count-Min sketch that estimates how often each value occurs. An estimate exceeds the true "
                               "count by at most e / num_buckets of the total count, with probability 1 - e^-num_hashes "
                               "(see datasketch_count_min_suggest_num_buckets and _suggest_num_hashes)",
                               {"SELECT datasketch_count_min(3, 128, item)" + rows}); });
            using MERGE_STATE = DSBufferedArenaState<DSCountMinPolicy<int64_t, false>>;
            using MERGE = DSArenaMergeOperation<DSCountMinBindData>;
            auto merge = AggregateFunction::UnaryAggregate<MERGE_STATE, string_t, string_t, MERGE>(sketch_type, sketch_type);
            DSSetBufferedStateExport<MERGE_STATE, DSCountMinBindData, DSNoConstants<DSCountMinBindData>>(merge);
            DSAddAggregate(set, descs, merge, DSCountMinMergeBind, {sketch_type}, {"sketch"},
                           "Merges Count-Min sketches of the same size",
                           {ex.QueryPartials("datasketch_count_min(sketch)")});
            DSRegisterAggregates(loader, std::move(set), std::move(descs), DS_COUNT_MIN_CATEGORIES);
        }

        // --- datasketch_count_min_weighted(num_hashes, num_buckets, data, weight)
        {
            AggregateFunctionSet set("datasketch_count_min_weighted");
            vector<FunctionDescription> descs;
            DSForEachHashedType([&](auto marker, const LogicalType &item_type)
                                {
                using INPUT = DSCountingInput<decltype(marker)>;
                using OP = DSArenaWeightedOperation<DSCountMinBindData>;
                using T = typename INPUT::physical_type;
                using STATE = DSBufferedArenaState<DSCountMinPolicy<T, INPUT::is_uuid>>;
                auto fun = AggregateFunction::BinaryAggregate<STATE, T, int64_t, string_t, OP>(item_type, LogicalType::BIGINT, sketch_type);
                DSReadDataArgumentsAt<2, 3, STATE, T, int64_t, OP>(fun);
                DSSetBufferedStateExport<STATE, DSCountMinBindData, DSCountMinConstants>(fun);
                DSAddAggregate(set, descs, fun, DSCountMinBind, {LogicalType::INTEGER, LogicalType::INTEGER, item_type, LogicalType::BIGINT},
                               {"num_hashes", "num_buckets", "data", "weight"},
                               "Creates a Count-Min sketch of the total weight of each value: each row adds its weight (which may be "
                               "negative) to the value's count",
                               {"SELECT datasketch_count_min_weighted(3, 128, item, weight)" + rows}); });
            DSRegisterAggregates(loader, std::move(set), std::move(descs), DS_COUNT_MIN_CATEGORIES);
        }

        // --- An item's estimate and bounds, for every input type
        auto register_item_function = [&](const string &name, const string &description, auto fun)
        {
            using FUNC = decltype(fun);
            ScalarFunctionSet set {Identifier(name)};
            vector<FunctionDescription> descs;
            DSForEachHashedType([&](auto marker, const LogicalType &item_type)
                                { DSAddScalar(set, descs, ScalarFunction({sketch_type, item_type}, LogicalType::BIGINT, DSCountMinItem<decltype(marker), FUNC>),
                                              {"sketch", "item"}, description, {ex.Query(name + "(sketch, 3)")}); });
            DSRegisterScalars(loader, std::move(set), std::move(descs), DS_COUNT_MIN_CATEGORIES);
        };
        register_item_function("datasketch_count_min_estimate",
                               "Returns the estimated count of an item: never less than its true count, and more by at most "
                               "relative_error * total_weight with high probability (for non-negative weights)",
                               DSCountMinEstimate());
        register_item_function("datasketch_count_min_lower_bound",
                               "Returns the library's lower bound of an item's count, which is the estimate itself",
                               DSCountMinLowerBound());
        register_item_function("datasketch_count_min_upper_bound",
                               "Returns the library's upper bound of an item's count: estimate + relative_error * total_weight",
                               DSCountMinUpperBound());

        // --- Sketch properties
        auto register_sketch_function = [&](const string &name, const LogicalType &return_type, scalar_function_t function,
                                            const string &description)
        {
            DSRegisterScalar(loader, ScalarFunction(Identifier(name), {sketch_type}, return_type, std::move(function)),
                             DSDescription(description, {ex.Query(name + "(sketch)")}, {"sketch"}, {sketch_type}),
                             DS_COUNT_MIN_CATEGORIES);
        };
        register_sketch_function("datasketch_count_min_is_empty", LogicalType::BOOLEAN, DSCountMinIsEmpty,
                                 "Returns true if no value was added to the sketch");
        register_sketch_function("datasketch_count_min_total_weight", LogicalType::BIGINT, DSCountMinTotalWeight,
                                 "Returns the total weight of every value added to the sketch: the number of rows, or the sum of the absolute "
                                 "values of their weights");
        register_sketch_function("datasketch_count_min_num_hashes", LogicalType::UTINYINT, DSCountMinNumHashes,
                                 "Returns the sketch's number of hash functions (rows of counters)");
        register_sketch_function("datasketch_count_min_num_buckets", LogicalType::UINTEGER, DSCountMinNumBuckets,
                                 "Returns the sketch's number of buckets (counters per hash function)");
        register_sketch_function("datasketch_count_min_relative_error", LogicalType::DOUBLE, DSCountMinRelativeError,
                                 "Returns the sketch's relative error, e / num_buckets: an estimate exceeds the true count by at most "
                                 "this fraction of the total weight, with high probability");
        register_sketch_function("datasketch_count_min_describe", LogicalType::VARCHAR, DSCountMinDescribe,
                                 "Returns a description of the sketch");

        DSRegisterScalar(loader,
                         ScalarFunction("datasketch_count_min_merge", {sketch_type, sketch_type}, sketch_type, DSCountMinMergeScalar),
                         DSDescription("Returns the merge of two Count-Min sketches of the same size",
                                       {ex.Query("datasketch_count_min_merge(sketch_a, sketch_b)")}, {"sketch_a", "sketch_b"},
                                       {sketch_type, sketch_type}),
                         DS_COUNT_MIN_CATEGORIES);

        // --- Sizing
        DSRegisterScalar(loader,
                         ScalarFunction("datasketch_count_min_suggest_num_buckets", {LogicalType::DOUBLE}, LogicalType::BIGINT,
                                        DSCountMinSuggestNumBuckets),
                         DSDescription("Returns the number of buckets for which estimates exceed true counts by at most relative_error "
                                       "times the total weight: ceil(e / relative_error)",
                                       {"SELECT datasketch_count_min_suggest_num_buckets(0.01)"}, {"relative_error"}, {LogicalType::DOUBLE}),
                         DS_COUNT_MIN_CATEGORIES);
        DSRegisterScalar(loader,
                         ScalarFunction("datasketch_count_min_suggest_num_hashes", {LogicalType::DOUBLE}, LogicalType::BIGINT,
                                        DSCountMinSuggestNumHashes),
                         DSDescription("Returns the number of hashes for which estimates stay within their error bound with probability "
                                       "confidence: ceil(ln(1 / (1 - confidence)))",
                                       {"SELECT datasketch_count_min_suggest_num_hashes(0.99)"}, {"confidence"}, {LogicalType::DOUBLE}),
                         DS_COUNT_MIN_CATEGORIES);
    }
} // namespace duckdb
