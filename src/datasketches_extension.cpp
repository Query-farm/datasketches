#define DUCKDB_EXTENSION_MAIN

#include "datasketches_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "query_farm_telemetry.hpp"

namespace duckdb
{
    static void LoadInternal(ExtensionLoader &loader)
    {
        // Use :: (global scope) to access the namespace
        ::duckdb_datasketches::LoadQuantilesSketch(loader);
        ::duckdb_datasketches::LoadKLLSketch(loader);
        ::duckdb_datasketches::LoadREQSketch(loader);
        ::duckdb_datasketches::LoadTDigestSketch(loader);
        ::duckdb_datasketches::LoadHLLSketch(loader);
        ::duckdb_datasketches::LoadCPCSketch(loader);

        // Load the new sketches
        ::duckdb_datasketches::LoadThetaSketch(loader);
        ::duckdb_datasketches::LoadFrequentItemsSketch(loader);

        QueryFarmSendTelemetry(loader, "datasketches", "2025100901");
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
