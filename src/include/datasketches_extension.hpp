#pragma once

#include "duckdb.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/parsed_data/create_aggregate_function_info.hpp"
#include "duckdb/function/scalar_function.hpp"

using namespace duckdb;

namespace duckdb
{

	class DatasketchesExtension : public Extension
	{
	public:
		void Load(DuckDB &db) override;
		std::string Name() override;
		std::string Version() const override;
	};

} // namespace duckdb
