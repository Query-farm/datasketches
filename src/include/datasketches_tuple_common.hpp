#pragma once

// Functions shared by the Tuple sketch types (tuple_sketch.cpp: sketch_tuple_double, aod_sketch.cpp: sketch_aod).
//
// A TRAITS class describes one sketch type:
//   using Compact = ...;                                   // the compact sketch class
//   static constexpr const char *KIND;                     // "Tuple", "Array of doubles" (for messages)
//   static constexpr const char *PREFIX;                   // "datasketch_tuple", "datasketch_aod"
//   static Compact Deserialize(const string_t &blob);
//   static string_t Serialize(Vector &result, const Compact &sketch);
//   static Compact ANotB(uint64_t seed, const Compact &a, const Compact &b);
//   static void CheckCompatible(const Compact &a, const Compact &b, const char *function_name);

#include "datasketches_theta_common.hpp"
#include "duckdb/common/vector/string_vector.hpp"
#include "duckdb/common/vector/struct_vector.hpp"
#include "duckdb/common/vector/list_vector.hpp"

#include <DataSketches/theta_sketch.hpp>
#include <DataSketches/theta_jaccard_similarity.hpp>

namespace duckdb
{
        // Tuple sketches (serial versions 1 and 3, and array-of-doubles sketches) store the seed hash in bytes 6-7.
        inline uint16_t DSTuplePeekSeedHash(const char *data, idx_t size)
        {
            if (size >= 8)
            {
                uint16_t seed_hash;
                memcpy(&seed_hash, data + 6, sizeof(seed_hash));
                return seed_hash;
            }
            return ThetaSeedHash(DS_THETA_DEFAULT_SEED);
        }

        inline uint64_t DSTupleSeedFor(const string_t &blob)
        {
            return ThetaSeedForHash(DSTuplePeekSeedHash(blob.GetData(), blob.GetSize()));
        }

        // The keys of a tuple sketch, in the shape compact_theta_sketch's converting constructor expects.
        struct DSTupleKeysView
        {
            std::vector<uint64_t> keys;
            bool empty = true;
            bool ordered = true;
            uint16_t seed_hash = 0;
            uint64_t theta = 0;

            std::allocator<uint64_t> get_allocator() const { return std::allocator<uint64_t>(); }
            bool is_empty() const { return empty; }
            bool is_ordered() const { return ordered; }
            uint16_t get_seed_hash() const { return seed_hash; }
            uint64_t get_theta64() const { return theta; }
            uint32_t get_num_retained() const { return static_cast<uint32_t>(keys.size()); }
            std::vector<uint64_t>::const_iterator begin() const { return keys.begin(); }
            std::vector<uint64_t>::const_iterator end() const { return keys.end(); }
        };

        // Drops the summaries: a Theta sketch of the same keys (same theta and seed hash).
        template <class SKETCH>
        datasketches::compact_theta_sketch DSTupleToTheta(const SKETCH &sketch)
        {
            DSTupleKeysView view;
            view.empty = sketch.is_empty();
            view.ordered = sketch.is_ordered();
            view.seed_hash = sketch.get_seed_hash();
            view.theta = sketch.get_theta64();
            view.keys.reserve(sketch.get_num_retained());
            for (const auto &entry : sketch)
            {
                view.keys.push_back(entry.first);
            }
            return datasketches::compact_theta_sketch(view, true);
        }

        template <class TRAITS>
        struct DSTupleFunctions
        {
            using Compact = typename TRAITS::Compact;

            template <class RESULT_TYPE, class FUNC>
            static void UnaryRead(DataChunk &args, Vector &result, FUNC &&fun)
            {
                UnaryExecutor::Execute<string_t, RESULT_TYPE>(args.data[0], result, [&](string_t blob)
                                                              {
                                                                  auto sketch = TRAITS::Deserialize(blob);
                                                                  return ThetaGuard([&]() { return fun(sketch); }, TRAITS::KIND); });
            }

            static void Estimate(DataChunk &args, ExpressionState &, Vector &result)
            {
                UnaryRead<double>(args, result, [](const Compact &sketch)
                                  { return sketch.get_estimate(); });
            }

            template <bool LOWER>
            static void Bound(DataChunk &args, ExpressionState &, Vector &result)
            {
                BinaryExecutor::Execute<string_t, int32_t, double>(args.data[0], args.data[1], result, [&](string_t blob, int32_t num_std_devs)
                                                                   {
                                                                       if (num_std_devs < 1 || num_std_devs > 3)
                                                                       {
                                                                           throw InvalidInputException("%s_%s: num_std_devs must be 1, 2 or 3, got %d", TRAITS::PREFIX,
                                                                                                       LOWER ? "lower_bound" : "upper_bound", num_std_devs);
                                                                       }
                                                                       auto sketch = TRAITS::Deserialize(blob);
                                                                       return LOWER ? sketch.get_lower_bound(static_cast<uint8_t>(num_std_devs))
                                                                                    : sketch.get_upper_bound(static_cast<uint8_t>(num_std_devs)); });
            }

            static void NumRetained(DataChunk &args, ExpressionState &, Vector &result)
            {
                UnaryRead<int64_t>(args, result, [](const Compact &sketch)
                                   { return static_cast<int64_t>(sketch.get_num_retained()); });
            }

            static void GetTheta(DataChunk &args, ExpressionState &, Vector &result)
            {
                UnaryRead<double>(args, result, [](const Compact &sketch)
                                  { return sketch.get_theta(); });
            }

            static void SeedHash(DataChunk &args, ExpressionState &, Vector &result)
            {
                UnaryRead<int64_t>(args, result, [](const Compact &sketch)
                                   { return static_cast<int64_t>(sketch.get_seed_hash()); });
            }

            static void IsEmpty(DataChunk &args, ExpressionState &, Vector &result)
            {
                UnaryRead<bool>(args, result, [](const Compact &sketch)
                                { return sketch.is_empty(); });
            }

            static void IsEstimationMode(DataChunk &args, ExpressionState &, Vector &result)
            {
                UnaryRead<bool>(args, result, [](const Compact &sketch)
                                { return sketch.is_estimation_mode(); });
            }

            static void Describe(DataChunk &args, ExpressionState &, Vector &result)
            {
                UnaryRead<string_t>(args, result, [&](const Compact &sketch)
                                    {
                                        auto description = sketch.to_string(false);
                                        return StringVector::AddString(result, description.data(), description.size()); });
            }

            static void ToTheta(DataChunk &args, ExpressionState &, Vector &result)
            {
                UnaryRead<string_t>(args, result, [&](const Compact &sketch)
                                    {
                                        auto bytes = DSTupleToTheta(sketch).serialize_compressed();
                                        return StringVector::AddStringOrBlob(result, reinterpret_cast<const char *>(bytes.data()), bytes.size()); });
            }

            static void ANotB(DataChunk &args, ExpressionState &, Vector &result)
            {
                auto result_data = FlatVector::GetDataMutable<string_t>(result);
                ThetaForEachRow(args, result, 0, 2, [&](idx_t row, const ThetaRowArgs &row_args)
                                {
                                    auto a = TRAITS::Deserialize(row_args.sketches[0]);
                                    auto b = TRAITS::Deserialize(row_args.sketches[1]);
                                    string name = string(TRAITS::PREFIX) + "_a_not_b";
                                    ThetaSeedTracker seeds(name.c_str(), TRAITS::KIND);
                                    seeds.Add(a);
                                    seeds.Add(b);
                                    TRAITS::CheckCompatible(a, b, name.c_str());
                                    auto difference = ThetaGuard([&]() { return TRAITS::ANotB(seeds.Seed(), a, b); }, TRAITS::KIND);
                                    result_data[row] = TRAITS::Serialize(result, difference); });
            }

            // Runs fun(theta_a, theta_b, seed) on the keys of two sketches built with the same seed.
            template <class FUNC>
            static auto WithKeyPair(const ThetaRowArgs &row_args, const char *suffix, FUNC &&fun)
                -> decltype(fun(std::declval<const datasketches::compact_theta_sketch &>(), std::declval<const datasketches::compact_theta_sketch &>(), uint64_t(0)))
            {
                auto a = TRAITS::Deserialize(row_args.sketches[0]);
                auto b = TRAITS::Deserialize(row_args.sketches[1]);
                string name = string(TRAITS::PREFIX) + suffix;
                ThetaSeedTracker seeds(name.c_str(), TRAITS::KIND);
                seeds.Add(a);
                seeds.Add(b);
                auto theta_a = DSTupleToTheta(a);
                auto theta_b = DSTupleToTheta(b);
                return ThetaGuard([&]() { return fun(theta_a, theta_b, seeds.Seed()); }, TRAITS::KIND);
            }

            static void Jaccard(DataChunk &args, ExpressionState &, Vector &result)
            {
                auto &children = StructVector::GetEntries(result);
                auto lower = FlatVector::GetDataMutable<double>(children[0]);
                auto estimate = FlatVector::GetDataMutable<double>(children[1]);
                auto upper = FlatVector::GetDataMutable<double>(children[2]);
                ThetaForEachRow(args, result, 0, 2, [&](idx_t row, const ThetaRowArgs &row_args)
                                {
                                    auto bounds = WithKeyPair(row_args, "_jaccard", [](const datasketches::compact_theta_sketch &a, const datasketches::compact_theta_sketch &b, uint64_t seed)
                                                              { return datasketches::theta_jaccard_similarity::jaccard(a, b, seed); });
                                    lower[row] = bounds[0];
                                    estimate[row] = bounds[1];
                                    upper[row] = bounds[2]; });
            }

            static void ExactlyEqual(DataChunk &args, ExpressionState &, Vector &result)
            {
                auto result_data = FlatVector::GetDataMutable<bool>(result);
                ThetaForEachRow(args, result, 0, 2, [&](idx_t row, const ThetaRowArgs &row_args)
                                { result_data[row] = WithKeyPair(row_args, "_exactly_equal", [](const datasketches::compact_theta_sketch &a, const datasketches::compact_theta_sketch &b, uint64_t seed)
                                                                 { return datasketches::theta_jaccard_similarity::exactly_equal(a, b, seed); }); });
            }

            template <bool SIMILARITY>
            static void SimilarityTest(DataChunk &args, ExpressionState &, Vector &result)
            {
                auto result_data = FlatVector::GetDataMutable<bool>(result);
                ThetaForEachRow(args, result, 0, 2, [&](idx_t row, const ThetaRowArgs &row_args)
                                {
                                    auto threshold = row_args.Get<double>(2);
                                    result_data[row] = WithKeyPair(row_args, SIMILARITY ? "_similarity_test" : "_dissimilarity_test",
                                                                   [&](const datasketches::compact_theta_sketch &actual, const datasketches::compact_theta_sketch &expected, uint64_t seed)
                                                                   {
                                                                       return SIMILARITY ? datasketches::theta_jaccard_similarity::similarity_test(actual, expected, threshold, seed)
                                                                                         : datasketches::theta_jaccard_similarity::dissimilarity_test(actual, expected, threshold, seed); }); });
            }

            // Registers the functions common to all tuple sketch types.
            // `ex` builds the sketches the examples refer to, so each example is a runnable query.
            static void Register(ExtensionLoader &loader, const LogicalType &sketch_type, const LogicalType &theta_type,
                                 const string &noun, const vector<string> &categories, const DSExampleSources &ex)
            {
                const string p = TRAITS::PREFIX;
                auto desc = [&](const string &description, const vector<string> &examples, const vector<string> &names, const vector<LogicalType> &types)
                {
                    vector<string> queries;
                    for (auto &example : examples)
                    {
                        queries.push_back(ex.Query(example));
                    }
                    return DSSketchDescription(description, std::move(queries), names, types, categories);
                };
                const vector<string> one = {"sketch"};
                const vector<LogicalType> one_type = {sketch_type};

                ThetaRegisterScalar(loader, ScalarFunction(Identifier(p + "_a_not_b"), {sketch_type, sketch_type}, sketch_type, ANotB),
                                    desc("Returns a new " + noun + " with the keys (and their summaries) of sketch A that are not in sketch B (set difference)",
                                         {p + "_a_not_b(sketch_a, sketch_b)"}, {"sketch_a", "sketch_b"}, {sketch_type, sketch_type}));
                ThetaRegisterScalar(loader, ScalarFunction(Identifier(p + "_estimate"), one_type, LogicalType::DOUBLE, Estimate),
                                    desc("Returns the estimated number of distinct keys in the " + noun, {p + "_estimate(sketch)"}, one, one_type));
                ThetaRegisterScalar(loader, ScalarFunction(Identifier(p + "_lower_bound"), {sketch_type, LogicalType::INTEGER}, LogicalType::DOUBLE, Bound<true>),
                                    desc("Returns the lower bound of the estimated number of distinct keys at the given number of standard deviations (1, 2, or 3)",
                                         {p + "_lower_bound(sketch, 2)"}, {"sketch", "num_std_devs"}, {sketch_type, LogicalType::INTEGER}));
                ThetaRegisterScalar(loader, ScalarFunction(Identifier(p + "_upper_bound"), {sketch_type, LogicalType::INTEGER}, LogicalType::DOUBLE, Bound<false>),
                                    desc("Returns the upper bound of the estimated number of distinct keys at the given number of standard deviations (1, 2, or 3)",
                                         {p + "_upper_bound(sketch, 2)"}, {"sketch", "num_std_devs"}, {sketch_type, LogicalType::INTEGER}));
                ThetaRegisterScalar(loader, ScalarFunction(Identifier(p + "_num_retained"), one_type, LogicalType::BIGINT, NumRetained),
                                    desc("Returns the number of entries (keys with their summaries) retained in the " + noun, {p + "_num_retained(sketch)"}, one, one_type));
                ThetaRegisterScalar(loader, ScalarFunction(Identifier(p + "_get_theta"), one_type, LogicalType::DOUBLE, GetTheta),
                                    desc("Returns the theta value of the " + noun + " (the fraction of the key hash space it retains)", {p + "_get_theta(sketch)"}, one, one_type));
                ThetaRegisterScalar(loader, ScalarFunction(Identifier(p + "_seed_hash"), one_type, LogicalType::BIGINT, SeedHash),
                                    desc("Returns the 16-bit hash of the seed the " + noun + " was built with; sketches can only be combined if their seed hashes match",
                                         {p + "_seed_hash(sketch)"}, one, one_type));
                ThetaRegisterScalar(loader, ScalarFunction(Identifier(p + "_is_empty"), one_type, LogicalType::BOOLEAN, IsEmpty),
                                    desc("Returns true if the " + noun + " is empty", {p + "_is_empty(sketch)"}, one, one_type));
                ThetaRegisterScalar(loader, ScalarFunction(Identifier(p + "_is_estimation_mode"), one_type, LogicalType::BOOLEAN, IsEstimationMode),
                                    desc("Returns true if the " + noun + " is in estimation mode (it has sampled the keys)", {p + "_is_estimation_mode(sketch)"}, one, one_type));
                ThetaRegisterScalar(loader, ScalarFunction(Identifier(p + "_describe"), one_type, LogicalType::VARCHAR, Describe),
                                    desc("Returns a human-readable description of the " + noun, {p + "_describe(sketch)"}, one, one_type));
                ThetaRegisterScalar(loader, ScalarFunction(Identifier(p + "_to_theta"), one_type, theta_type, ToTheta),
                                    desc("Converts the " + noun + " to a Theta sketch of its keys (dropping the summaries), usable with the datasketch_theta functions",
                                         {p + "_to_theta(sketch)"}, one, one_type));

                child_list_t<LogicalType> bounds;
                bounds.emplace_back("lower", LogicalType::DOUBLE);
                bounds.emplace_back("estimate", LogicalType::DOUBLE);
                bounds.emplace_back("upper", LogicalType::DOUBLE);
                ThetaRegisterScalar(loader, ScalarFunction(Identifier(p + "_jaccard"), {sketch_type, sketch_type}, LogicalType::STRUCT(std::move(bounds)), Jaccard),
                                    desc("Returns the Jaccard similarity of the key sets of two sketches as a struct of its estimate and approximate lower and upper "
                                         "bounds (about 2 standard deviations). Two empty sketches are identical (1.0)",
                                         {p + "_jaccard(sketch_a, sketch_b)", p + "_jaccard(sketch_a, sketch_b).estimate"}, {"sketch_a", "sketch_b"}, {sketch_type, sketch_type}));
                ThetaRegisterScalar(loader, ScalarFunction(Identifier(p + "_exactly_equal"), {sketch_type, sketch_type}, LogicalType::BOOLEAN, ExactlyEqual),
                                    desc("Returns true if the two sketches retain exactly the same keys with the same theta (summaries are not compared)",
                                         {p + "_exactly_equal(sketch_a, sketch_b)"}, {"sketch_a", "sketch_b"}, {sketch_type, sketch_type}));
                ThetaRegisterScalar(loader, ScalarFunction(Identifier(p + "_similarity_test"), {sketch_type, sketch_type, LogicalType::DOUBLE}, LogicalType::BOOLEAN, SimilarityTest<true>),
                                    desc("Returns true if the Jaccard similarity of the key sets is at least the threshold, with high confidence (its lower bound is >= threshold)",
                                         {p + "_similarity_test(actual, expected, 0.9)"}, {"actual", "expected", "threshold"}, {sketch_type, sketch_type, LogicalType::DOUBLE}));
                ThetaRegisterScalar(loader, ScalarFunction(Identifier(p + "_dissimilarity_test"), {sketch_type, sketch_type, LogicalType::DOUBLE}, LogicalType::BOOLEAN, SimilarityTest<false>),
                                    desc("Returns true if the Jaccard similarity of the key sets is at most the threshold, with high confidence (its upper bound is <= threshold)",
                                         {p + "_dissimilarity_test(actual, expected, 0.1)"}, {"actual", "expected", "threshold"}, {sketch_type, sketch_type, LogicalType::DOUBLE}));
            }
        };

        // Binds a constant lg_k argument.
        inline uint8_t DSTupleBindLgK(BindAggregateFunctionInput &input, idx_t index)
        {
            auto lg_k = input.GetNonNullConstant(index).GetValue<int64_t>();
            if (lg_k < datasketches::theta_constants::MIN_LG_K || lg_k > datasketches::theta_constants::MAX_LG_K)
            {
                throw BinderException("lg_k must be between %d and %d, got %lld", datasketches::theta_constants::MIN_LG_K,
                                      datasketches::theta_constants::MAX_LG_K, lg_k);
            }
            return static_cast<uint8_t>(lg_k);
        }

        // Binds the seed := and p := keyword arguments of a build aggregate.
        inline void DSTupleBindSeedAndP(BindAggregateFunctionInput &input, uint64_t &seed, float &p)
        {
            seed = static_cast<uint64_t>(input.GetNonNullConstant(Identifier("seed")).GetValue<int64_t>());
            auto p_value = input.GetNonNullConstant(Identifier("p")).GetValue<double>();
            if (!(p_value > 0.0 && p_value <= 1.0))
            {
                throw BinderException("sampling probability p must be in (0, 1], got %f", p_value);
            }
            p = static_cast<float>(p_value);
        }

        // Makes the signature [lg_k,] data..., then the keyword-only arguments; the constants stay in the argument
        // list so that aggregates differing only in them are not merged by the optimizer.
        inline FunctionSignature DSTupleSignature(bool with_lg_k, const vector<LogicalType> &data_types, const LogicalType &return_type)
        {
            FunctionSignature signature(vector<LogicalType>(), return_type);
            if (with_lg_k)
            {
                signature.AddParameter(LogicalType::INTEGER);
            }
            for (auto &type : data_types)
            {
                signature.AddParameter(type);
            }
            return signature;
        }

        inline void DSTupleAddSeedAndP(FunctionSignature &signature)
        {
            signature.AddKeywordOnly("seed", LogicalType::BIGINT, Value::BIGINT(static_cast<int64_t>(DS_THETA_DEFAULT_SEED)));
            signature.AddKeywordOnly("p", LogicalType::DOUBLE, Value::DOUBLE(1.0));
        }
} // namespace duckdb
