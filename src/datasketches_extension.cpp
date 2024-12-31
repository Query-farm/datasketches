#define DUCKDB_EXTENSION_MAIN

#include "datasketches_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/extra_type_info.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include <duckdb/parser/parsed_data/create_aggregate_function_info.hpp>

#include <optional>

#include "generated.h"

namespace duckdb
{

    static void LoadInternal(DatabaseInstance &instance)
    {
        LoadQuantilesSketch(instance);
        LoadKLLSketch(instance);
        LoadREQSketch(instance);
        LoadTDigestSketch(instance);
        LoadHLLSketch(instance);
        LoadCPCSketch(instance);
    }

    void DatasketchesExtension::Load(DuckDB &db)
    {
        LoadInternal(*db.instance);
    }
    std::string DatasketchesExtension::Name()
    {
        return "datasketches";
    }

    std::string DatasketchesExtension::Version() const
    {
        return "0.0.1";
    }

} // namespace duckdb

extern "C"
{

    DUCKDB_EXTENSION_API void datasketches_init(duckdb::DatabaseInstance &db)
    {
        duckdb::DuckDB db_wrapper(db);
        db_wrapper.LoadExtension<duckdb::DatasketchesExtension>();
    }

    DUCKDB_EXTENSION_API const char *datasketches_version()
    {
        return duckdb::DuckDB::LibraryVersion();
    }
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
