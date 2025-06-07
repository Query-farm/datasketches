#define DUCKDB_EXTENSION_MAIN

#include "datasketches_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/extra_type_info.hpp"
#include "duckdb/function/scalar_function.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include <duckdb/parser/parsed_data/create_aggregate_function_info.hpp>

#include <optional>

#include "generated.h"

namespace duckdb
{

    static void LoadInternal(ExtensionLoader &loader)
    {
        duckdb_datasketches::LoadQuantilesSketch(loader);
        duckdb_datasketches::LoadKLLSketch(loader);
        duckdb_datasketches::LoadREQSketch(loader);
        duckdb_datasketches::LoadTDigestSketch(loader);
        duckdb_datasketches::LoadHLLSketch(loader);
        duckdb_datasketches::LoadCPCSketch(loader);
    }

    void DatasketchesExtension::Load(ExtensionLoader &loader)
    {
        LoadInternal(loader);
    }
    std::string DatasketchesExtension::Name()
    {
        return "datasketches";
    }

} // namespace duckdb

extern "C"
{
    DUCKDB_CPP_EXTENSION_ENTRY(datasketches, loader)
    {
        duckdb::LoadInternal(loader);
    }
}
