#pragma once

#include "duckdb.hpp"
#include "duckdb/function/aggregate_function.hpp"
#include "duckdb/parser/parsed_data/create_function_info.hpp"
#include "duckdb/parser/parsed_data/create_type_info.hpp"
#include "duckdb/catalog/catalog.hpp"

namespace duckdb
{

	class DatasketchesExtension : public Extension
	{
	public:
		void Load(ExtensionLoader &loader) override;
		std::string Name() override;
	};

	// Constant arguments that a bind folds into its bind data (K, lg_max_k) stay in the aggregate's argument list
	// instead of being erased. Plan serialization, which the common subplan optimizer uses to find identical
	// subplans, records only the arguments, so erasing them made e.g. datasketch_kll(12, x) and
	// datasketch_kll(16, x) look identical and one result was reused for both. These update callbacks read the
	// data (and weight) arguments at fixed positions and skip the constants.
	template <idx_t DATA_INDEX, class STATE, class INPUT_TYPE, class OP>
	static void DSUnaryScatterUpdateAt(Vector inputs[], AggregateInputData &aggr_input_data, idx_t input_count,
	                                   Vector &states, idx_t count)
	{
		D_ASSERT(input_count > DATA_INDEX);
		AggregateExecutor::UnaryScatter<STATE, INPUT_TYPE, OP>(inputs[DATA_INDEX], states, aggr_input_data, count);
	}

	template <idx_t DATA_INDEX, class STATE, class INPUT_TYPE, class OP>
	static void DSUnaryUpdateAt(Vector inputs[], AggregateInputData &aggr_input_data, idx_t input_count,
	                            data_ptr_t state, idx_t count)
	{
		D_ASSERT(input_count > DATA_INDEX);
		AggregateExecutor::UnaryUpdate<STATE, INPUT_TYPE, OP>(inputs[DATA_INDEX], aggr_input_data, state, count);
	}

	template <idx_t A_INDEX, idx_t B_INDEX, class STATE, class A_TYPE, class B_TYPE, class OP>
	static void DSBinaryScatterUpdateAt(Vector inputs[], AggregateInputData &aggr_input_data, idx_t input_count,
	                                    Vector &states, idx_t count)
	{
		D_ASSERT(input_count > A_INDEX && input_count > B_INDEX);
		AggregateExecutor::BinaryScatter<STATE, A_TYPE, B_TYPE, OP>(aggr_input_data, inputs[A_INDEX], inputs[B_INDEX],
		                                                            states, count);
	}

	template <idx_t A_INDEX, idx_t B_INDEX, class STATE, class A_TYPE, class B_TYPE, class OP>
	static void DSBinaryUpdateAt(Vector inputs[], AggregateInputData &aggr_input_data, idx_t input_count,
	                             data_ptr_t state, idx_t count)
	{
		D_ASSERT(input_count > A_INDEX && input_count > B_INDEX);
		AggregateExecutor::BinaryUpdate<STATE, A_TYPE, B_TYPE, OP>(aggr_input_data, inputs[A_INDEX], inputs[B_INDEX],
		                                                           state, count);
	}

	// Makes a unary aggregate (built with AggregateFunction::UnaryAggregate*) read its input at DATA_INDEX.
	template <idx_t DATA_INDEX, class STATE, class INPUT_TYPE, class OP>
	inline void DSReadDataArgumentAt(AggregateFunction &fun)
	{
		fun.update = DSUnaryScatterUpdateAt<DATA_INDEX, STATE, INPUT_TYPE, OP>;
		fun.simple_update = DSUnaryUpdateAt<DATA_INDEX, STATE, INPUT_TYPE, OP>;
	}

	// Makes a binary aggregate (built with AggregateFunction::BinaryAggregate) read its inputs at A_INDEX and B_INDEX.
	template <idx_t A_INDEX, idx_t B_INDEX, class STATE, class A_TYPE, class B_TYPE, class OP>
	inline void DSReadDataArgumentsAt(AggregateFunction &fun)
	{
		fun.update = DSBinaryScatterUpdateAt<A_INDEX, B_INDEX, STATE, A_TYPE, B_TYPE, OP>;
		fun.simple_update = DSBinaryUpdateAt<A_INDEX, B_INDEX, STATE, A_TYPE, B_TYPE, OP>;
	}


	// Gives every description of a function the given categories (e.g. {"sketch", "kll"}) unless it already has
	// some. A function registered without descriptions gets one carrying only the categories, so duckdb_functions()
	// reports them for every overload.
	template <class INFO>
	inline void DSSetCategories(INFO &info, const vector<string> &categories)
	{
		if (info.descriptions.empty())
		{
			info.descriptions.emplace_back();
		}
		for (auto &desc : info.descriptions)
		{
			if (desc.categories.empty())
			{
				desc.categories = categories;
			}
		}
	}

	// Registers a BLOB-backed sketch type with casts to and from BLOB. The catalog entry carries the alias, so a cast
	// to the type (e.g. NULL::sketch_theta) has the sketch type rather than BLOB.
	inline LogicalType DSCreateSketchType(ExtensionLoader &loader, const string &name, const string &comment)
	{
		auto new_type = LogicalType(LogicalTypeId::BLOB);
		new_type.SetAlias(name);
		auto type_info = CreateTypeInfo(name, new_type);
		type_info.temporary = false;
		type_info.internal = true;
		type_info.comment = comment;
		auto &system_catalog = Catalog::GetSystemCatalog(loader.GetDatabaseInstance());
		auto data = CatalogTransaction::GetSystemTransaction(loader.GetDatabaseInstance());
		system_catalog.CreateType(data, type_info);
		loader.RegisterCastFunction(LogicalType::BLOB, new_type, DefaultCasts::ReinterpretCast, 1);
		loader.RegisterCastFunction(new_type, LogicalType::BLOB, DefaultCasts::ReinterpretCast, 1);
		return new_type;
	}

	// Deserializes a sketch, reporting a corrupt or foreign blob as invalid input.
	template <class SKETCH>
	SKETCH DSDeserialize(const string_t &data, const char *sketch_name)
	{
		try
		{
			return SKETCH::deserialize(data.GetDataUnsafe(), data.GetSize());
		}
		catch (const std::exception &e)
		{
			throw InvalidInputException("Failed to deserialize %s sketch: %s", sketch_name, e.what());
		}
	}

	// Checks the number of standard deviations of a bound: the libraries accept 1, 2 or 3.
	inline uint8_t DSCheckNumStdDevs(int64_t num_std_devs, const char *function_name)
	{
		if (num_std_devs < 1 || num_std_devs > 3)
		{
			throw InvalidInputException("%s: num_std_devs must be 1, 2 or 3, got %lld", function_name, (long long)num_std_devs);
		}
		return static_cast<uint8_t>(num_std_devs);
	}

} // namespace duckdb
