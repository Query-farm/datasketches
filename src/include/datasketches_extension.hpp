#pragma once

#include "duckdb.hpp"
#include <string>
#include "duckdb/function/aggregate_function.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/parser/parsed_data/create_type_info.hpp"
#include "duckdb/parser/parsed_data/create_aggregate_function_info.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"

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
		auto new_type = LogicalType(LogicalTypeId::BLOB).WithAlias(name);
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

	inline FunctionDescription DSDescription(string description, vector<string> examples, vector<string> parameter_names,
	                                         vector<LogicalType> parameter_types, vector<string> categories = {})
	{
		FunctionDescription desc;
		desc.description = std::move(description);
		desc.examples = std::move(examples);
		desc.parameter_names = std::move(parameter_names);
		desc.parameter_types = std::move(parameter_types);
		desc.categories = std::move(categories);
		return desc;
	}

	// Registers a scalar function set with its descriptions. Every sketch function deserializes its input, which can
	// throw, so the functions are fallible. `categories` fills in descriptions that have none.
	inline void DSRegisterScalars(ExtensionLoader &loader, ScalarFunctionSet set, vector<FunctionDescription> descriptions,
	                              const vector<string> &categories = {})
	{
		set.SetFallible();
		CreateScalarFunctionInfo info(std::move(set));
		for (auto &desc : descriptions)
		{
			info.descriptions.push_back(std::move(desc));
		}
		if (!categories.empty())
		{
			DSSetCategories(info, categories);
		}
		DSRegister(loader, info);
	}

	inline void DSRegisterScalar(ExtensionLoader &loader, ScalarFunction fun, FunctionDescription description,
	                             const vector<string> &categories = {})
	{
		ScalarFunctionSet set(fun.GetName());
		set.AddFunction(std::move(fun));
		DSRegisterScalars(loader, std::move(set), {std::move(description)}, categories);
	}

	// Registers an aggregate function set with its descriptions; see DSRegisterScalars.
	inline void DSRegisterAggregates(ExtensionLoader &loader, AggregateFunctionSet set, vector<FunctionDescription> descriptions,
	                                 const vector<string> &categories = {})
	{
		set.ApplyToFunctions([](AggregateFunction &fun) { fun.SetFallible(); });
		CreateAggregateFunctionInfo info(std::move(set));
		for (auto &desc : descriptions)
		{
			info.descriptions.push_back(std::move(desc));
		}
		if (!categories.empty())
		{
			DSSetCategories(info, categories);
		}
		DSRegister(loader, info);
	}

	// Adds an aggregate overload taking `arguments` (its constants included) to `set`, with its description.
	inline void DSAddAggregate(AggregateFunctionSet &set, vector<FunctionDescription> &descriptions, AggregateFunction fun,
	                           bind_aggregate_function_t bind, vector<LogicalType> arguments, vector<string> parameter_names,
	                           string description, vector<string> examples)
	{
		fun.SetBindCallback(bind);
		fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
		DSSetArguments(fun, arguments);
		set.AddFunction(fun);
		descriptions.push_back(DSDescription(std::move(description), std::move(examples), std::move(parameter_names),
		                                     std::move(arguments)));
	}

	// Adds a scalar overload to `set`, with its description.
	inline void DSAddScalar(ScalarFunctionSet &set, vector<FunctionDescription> &descriptions, ScalarFunction fun,
	                        vector<string> parameter_names, string description, vector<string> examples)
	{
		vector<LogicalType> parameter_types;
		for (auto &param : fun.GetSignature().GetParameters())
		{
			parameter_types.push_back(param.GetType());
		}
		set.AddFunction(std::move(fun));
		descriptions.push_back(DSDescription(std::move(description), std::move(examples), std::move(parameter_names),
		                                     std::move(parameter_types)));
	}

	// The value of a constant argument (K or an option) of an aggregate.
	inline Value DSBindConstant(ClientContext &context, Expression &argument, const string &not_constant_message,
	                            const string &null_message)
	{
		if (argument.HasParameter())
		{
			throw ParameterNotResolvedException();
		}
		if (!argument.IsFoldable())
		{
			throw BinderException(not_constant_message);
		}
		auto value = ExpressionExecutor::EvaluateScalar(context, argument);
		if (value.IsNull())
		{
			throw BinderException(null_message);
		}
		return value;
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

	// Example queries for function descriptions. Each example is a complete query that runs as-is: the sketches it
	// refers to (sketch, sketch_a, sketch_b, sketch_c, actual, expected) are built from inline data with `build`, an
	// aggregate over the BIGINT column i of range().
	struct DSExampleSources
	{
		explicit DSExampleSources(string build_p) : build(std::move(build_p))
		{
		}

		string build;

		// A subquery producing one sketch named `alias` over range(lo, hi).
		string Sketch(const string &alias, idx_t lo, idx_t hi) const
		{
			return "(SELECT " + build + " AS " + alias + " FROM range(" + std::to_string(lo) + ", " + std::to_string(hi) + ") t(i))";
		}

		// True if `name` appears in `call` as a whole argument (not as part of a function name such as datasketch_aod).
		static bool HasArgument(const string &call, const string &name)
		{
			for (auto pos = call.find(name); pos != string::npos; pos = call.find(name, pos + 1))
			{
				auto end = pos + name.size();
				bool starts = pos > 0 && (call[pos - 1] == '(' || call[pos - 1] == ' ');
				bool ends = end < call.size() && (call[end] == ',' || call[end] == ')');
				if (starts && ends)
				{
					return true;
				}
			}
			return false;
		}

		// SELECT <call> FROM the sketches the call refers to. Overlapping inputs keep set operations meaningful.
		string Query(const string &call) const
		{
			string from;
			if (HasArgument(call, "sketch_a"))
			{
				from = Sketch("sketch_a", 0, 1000) + " a, " + Sketch("sketch_b", 500, 1500) + " b";
				if (HasArgument(call, "sketch_c"))
				{
					from += ", " + Sketch("sketch_c", 250, 1250) + " c";
				}
			}
			else if (HasArgument(call, "actual"))
			{
				from = Sketch("actual", 0, 1000) + " a, " + Sketch("expected", 100, 1100) + " e";
			}
			else
			{
				from = Sketch("sketch", 0, 1000) + " s";
			}
			return "SELECT " + call + " FROM " + from;
		}

		// SELECT <call> FROM several rows of `sketch` (partial sketches of disjoint groups), for aggregates that merge sketches.
		string QueryPartials(const string &call) const
		{
			return "SELECT " + call + " FROM (SELECT " + build + " AS sketch FROM range(1000) t(i) GROUP BY i % 4) s";
		}

		// SELECT <call> FROM two rows of overlapping `sketch`es, for aggregates that intersect sketches.
		string QueryOverlapping(const string &call) const
		{
			return "SELECT " + call + " FROM (SELECT " + build + " AS sketch FROM range(0, 1000) t(i) UNION ALL SELECT " + build +
			       " FROM range(500, 1500) t(i)) s";
		}
	};

} // namespace duckdb

