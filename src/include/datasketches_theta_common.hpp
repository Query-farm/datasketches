#pragma once

// Helpers shared by the Theta and Tuple sketch functions (theta_sketch.cpp, tuple_sketch.cpp, aod_sketch.cpp).

#include "datasketches_extension.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/parsed_data/create_aggregate_function_info.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/unified_vector_format.hpp"

#include <DataSketches/theta_constants.hpp>
#include <DataSketches/common_defs.hpp>
#include <DataSketches/MurmurHash3.h>

#include <cstring>
#include <mutex>
#include <unordered_map>

namespace duckdb
{
        // The default nominal size used when building, and the minimum used when combining sketches.
        constexpr uint8_t DS_THETA_DEFAULT_LG_K = 12;
        constexpr uint64_t DS_THETA_DEFAULT_SEED = datasketches::DEFAULT_SEED;

        inline uint16_t ThetaSeedHash(uint64_t seed)
        {
            return compute_seed_hash(seed);
        }

        // A compact Theta sketch only stores a 16-bit hash of the seed it was built with, and every operation on
        // compact sketches (deserialize, wrap, union, intersection, A-not-B, Jaccard) uses the seed only to compare
        // that hash. Only building a sketch from raw values needs the real seed. So that sketches built with a custom
        // seed can be read and combined without repeating the seed in every function call, the readers use a seed
        // with the same hash as the one stored in the sketch: found by a search on first use of each distinct hash
        // (about 65k hash computations), then cached.
        inline uint64_t ThetaSeedForHash(uint16_t seed_hash)
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
        inline uint16_t ThetaPeekSeedHash(const char *data, idx_t size)
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
        auto ThetaGuard(FUNC &&fun, const char *kind = "Theta") -> decltype(fun())
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
                throw InvalidInputException("%s sketch error: %s", kind, ex.what());
            }
        }

        // Tracks the seed of the sketches being combined: empty sketches hold no hashes, so their seed is ignored.
        struct ThetaSeedTracker
        {
            explicit ThetaSeedTracker(const char *function_name, const char *kind = "Theta") : function_name(function_name), kind(kind) {}

            template <class SKETCH>
            void Add(const SKETCH &sketch)
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
                    throw InvalidInputException("%s: cannot combine %s sketches built with different seeds (seed hashes %d and %d)",
                                                function_name, kind, seed_hash, sketch.get_seed_hash());
                }
            }

            uint64_t Seed() const
            {
                return has_seed_hash ? ThetaSeedForHash(seed_hash) : DS_THETA_DEFAULT_SEED;
            }

            const char *function_name;
            const char *kind;
            bool has_seed_hash = false;
            uint16_t seed_hash = 0;
        };

        inline void ThetaCheckLgK(int64_t lg_k, const char *function_name)
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
        template <class SKETCH>
        uint8_t ThetaInferLgK(const SKETCH &sketch)
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

        inline void ThetaRegisterScalar(ExtensionLoader &loader, ScalarFunctionSet set, vector<FunctionDescription> descriptions)
        {
            set.SetFallible();
            CreateScalarFunctionInfo info(std::move(set));
            for (auto &desc : descriptions)
            {
                info.descriptions.push_back(std::move(desc));
            }
            DSRegister(loader, info);
        }

        inline void ThetaRegisterScalar(ExtensionLoader &loader, ScalarFunction fun, FunctionDescription description)
        {
            ScalarFunctionSet set(fun.GetName());
            set.AddFunction(std::move(fun));
            ThetaRegisterScalar(loader, std::move(set), {std::move(description)});
        }

        // Names the parameters of a variadic function: DuckDB matches descriptions to overloads by their fixed
        // parameters, so the descriptions list those only and DSRegister cannot name the variadic parameter.
        inline ScalarFunction ThetaVariadic(ScalarFunction fun, const vector<string> &names, const LogicalType &sketch_type)
        {
            fun.GetSignature().AddArgs("sketches", sketch_type);
            for (idx_t i = 0; i < names.size(); i++)
            {
                fun.GetSignature().GetParameter(i).SetName(Identifier(names[i]));
            }
            return fun;
        }

        inline void ThetaRegisterAggregate(ExtensionLoader &loader, AggregateFunctionSet &set, vector<FunctionDescription> &descriptions)
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

        inline FunctionDescription DSSketchDescription(string description, vector<string> examples, vector<string> parameter_names,
                                                       vector<LogicalType> parameter_types, vector<string> categories)
        {
            FunctionDescription desc;
            desc.description = std::move(description);
            desc.examples = std::move(examples);
            desc.parameter_names = std::move(parameter_names);
            desc.parameter_types = std::move(parameter_types);
            desc.categories = std::move(categories);
            return desc;
        }

        // Registers a BLOB alias type for a sketch, castable from and to BLOB.
        inline LogicalType DSCreateSketchType(ExtensionLoader &loader, const string &name, const string &comment)
        {
            auto type_info = CreateTypeInfo(name, LogicalType::BLOB);
            type_info.temporary = false;
            type_info.internal = true;
            type_info.comment = comment;
            auto new_type = LogicalType(LogicalTypeId::BLOB).WithAlias(name);

            auto &system_catalog = Catalog::GetSystemCatalog(loader.GetDatabaseInstance());
            auto data = CatalogTransaction::GetSystemTransaction(loader.GetDatabaseInstance());
            system_catalog.CreateType(data, type_info);

            loader.RegisterCastFunction(LogicalType::BLOB, new_type, DefaultCasts::ReinterpretCast, 1);
            loader.RegisterCastFunction(new_type, LogicalType::BLOB, DefaultCasts::ReinterpretCast, 1);
            return new_type;
        }
} // namespace duckdb
