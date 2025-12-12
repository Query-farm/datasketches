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

#include "query_farm_telemetry.hpp"

namespace duckdb
{

    void LoadQuantilesSketch(ExtensionLoader &loader);
    void LoadKLLSketch(ExtensionLoader &loader);
    void LoadREQSketch(ExtensionLoader &loader);
    void LoadTDigestSketch(ExtensionLoader &loader);
    void LoadHLLSketch(ExtensionLoader &loader);
    void LoadCPCSketch(ExtensionLoader &loader);
    void LoadThetaSketch(ExtensionLoader &loader);
    void LoadFrequentItemsSketch(ExtensionLoader &loader);

    static void LoadInternal(ExtensionLoader &loader)
    {
        LoadQuantilesSketch(loader);
        LoadKLLSketch(loader);
        LoadREQSketch(loader);
        LoadTDigestSketch(loader);
        LoadHLLSketch(loader);
        LoadCPCSketch(loader);
        LoadThetaSketch(loader);
        LoadFrequentItemsSketch(loader);
        QueryFarmSendTelemetry(loader, "datasketches", "2025121201");
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
