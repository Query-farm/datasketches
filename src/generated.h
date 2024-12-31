#include "datasketches_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/extra_type_info.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include <duckdb/parser/parsed_data/create_aggregate_function_info.hpp>

#include <DataSketches/quantiles_sketch.hpp>
#include <optional>

namespace duckdb
{

  void LoadQuantilesSketch(DatabaseInstance &instance);
  void LoadKLLSketch(DatabaseInstance &instance);
  void LoadREQSketch(DatabaseInstance &instance);
  void LoadTDigestSketch(DatabaseInstance &instance);
  void LoadHLLSketch(DatabaseInstance &instance);
  void LoadCPCSketch(DatabaseInstance &instance);

}