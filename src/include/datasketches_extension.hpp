#pragma once

#include "duckdb.hpp"
#include "duckdb/function/aggregate_function.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/parser/parsed_data/create_type_info.hpp"

namespace duckdb
{

	class DatasketchesExtension : public Extension
	{
	public:
		void Load(ExtensionLoader &loader) override;
		std::string Name() override;
	};

	// Removes a constant argument (e.g. K) that an aggregate bind callback consumed into its bind data, so the
	// update callbacks receive the data argument first.
	inline void DSEraseAggregateArgument(BindAggregateFunctionInput &input, idx_t index)
	{
		input.GetArguments().erase_at(index);
		input.GetBoundFunction().GetArguments().erase_at(index);
	}

	// Prepends an argument (e.g. K) to an aggregate function's signature.
	inline void DSPrependArgument(AggregateFunction &fun, const LogicalType &type)
	{
		vector<LogicalType> arguments;
		arguments.push_back(type);
		for (auto &param : fun.GetSignature().GetParameters())
		{
			arguments.push_back(param.GetType());
		}
		fun.GetSignature() = FunctionSignature(std::move(arguments), fun.GetReturnType());
	}

	// Marks a scalar function as fallible: every sketch function deserializes its input, which can throw.
	inline ScalarFunction DSFallible(ScalarFunction fun)
	{
		fun.SetFallible();
		return fun;
	}

	// DuckDB 2.0 reports parameter names from the function signature (duckdb_functions()), not from the
	// descriptions, so copy the documented names onto the matching overloads' (positional-only) parameters.
	template <class INFO>
	inline void DSApplyParameterNames(INFO &info)
	{
		auto descriptions = info.descriptions;
		info.functions.ApplyToFunctions([&](SimpleFunction &fun) {
			auto &signature = fun.GetSignature();
			for (auto &desc : descriptions)
			{
				if (desc.parameter_types.size() != signature.GetParameterCount() ||
				    desc.parameter_names.size() != signature.GetParameterCount())
				{
					continue;
				}
				bool match = true;
				for (idx_t i = 0; i < signature.GetParameterCount(); i++)
				{
					if (desc.parameter_types[i] != signature.GetParameter(i).GetType())
					{
						match = false;
						break;
					}
				}
				if (!match)
				{
					continue;
				}
				for (idx_t i = 0; i < signature.GetParameterCount(); i++)
				{
					signature.GetParameter(i).SetName(Identifier(desc.parameter_names[i]));
				}
				return;
			}
		});
	}

	template <class INFO>
	inline void DSRegister(ExtensionLoader &loader, INFO &info)
	{
		DSApplyParameterNames(info);
		loader.RegisterFunction(info);
	}

	// DuckDB 2.0: constant arguments that a bind folds into its bind data (K, lg_max_k, build options) must stay in
	// the aggregate's argument list. Plan serialization, which the common subplan optimizer uses to find identical
	// subplans, records only the arguments, so erasing them makes e.g. datasketch_kll(12, x) and
	// datasketch_kll(16, x) look identical and one result is reused for both. These update callbacks read the
	// data (and weight) arguments at fixed positions and skip the constants.
	template <idx_t DATA_INDEX, class STATE, class INPUT_TYPE, class OP>
	static void DSUnaryScatterUpdateAt(Vector inputs[], AggregateInputData &aggr_input_data, idx_t input_count,
	                                   Vector &states, idx_t count)
	{
		D_ASSERT(input_count > DATA_INDEX);
		AggregateExecutor::UnaryScatter<STATE, INPUT_TYPE, OP>(inputs[DATA_INDEX], states, aggr_input_data, count);
	}

	template <idx_t A_INDEX, idx_t B_INDEX, class STATE, class A_TYPE, class B_TYPE, class OP>
	static void DSBinaryScatterUpdateAt(Vector inputs[], AggregateInputData &aggr_input_data, idx_t input_count,
	                                    Vector &states, idx_t count)
	{
		D_ASSERT(input_count > A_INDEX && input_count > B_INDEX);
		AggregateExecutor::BinaryScatter<STATE, A_TYPE, B_TYPE, OP>(aggr_input_data, inputs[A_INDEX], inputs[B_INDEX],
		                                                            states, count);
	}

	// Makes a unary aggregate (built with AggregateFunction::UnaryAggregate) read its input at DATA_INDEX.
	template <idx_t DATA_INDEX, class STATE, class INPUT_TYPE, class OP>
	inline void DSReadDataArgumentAt(AggregateFunction &fun)
	{
		D_ASSERT(!fun.HasStateClusterUpdateCallback());
		fun.SetStateUpdateCallback(DSUnaryScatterUpdateAt<DATA_INDEX, STATE, INPUT_TYPE, OP>);
	}

	// Makes a binary aggregate (built with AggregateFunction::BinaryAggregate) read its inputs at A_INDEX and B_INDEX.
	template <idx_t A_INDEX, idx_t B_INDEX, class STATE, class A_TYPE, class B_TYPE, class OP>
	inline void DSReadDataArgumentsAt(AggregateFunction &fun)
	{
		D_ASSERT(!fun.HasStateClusterUpdateCallback());
		fun.SetStateUpdateCallback(DSBinaryScatterUpdateAt<A_INDEX, B_INDEX, STATE, A_TYPE, B_TYPE, OP>);
	}

	// Replaces an aggregate function's argument list.
	inline void DSSetArguments(AggregateFunction &fun, vector<LogicalType> arguments)
	{
		fun.GetSignature() = FunctionSignature(std::move(arguments), fun.GetReturnType());
	}

} // namespace duckdb
