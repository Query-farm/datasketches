#pragma once

#include "duckdb.hpp"
#include "duckdb/main/extension_util.hpp"

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
