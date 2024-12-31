#pragma once

#include "datasketches_extension.hpp"

using namespace duckdb;
namespace duckdb_datasketches
{
  void LoadQuantilesSketch(DatabaseInstance &instance);
  void LoadKLLSketch(DatabaseInstance &instance);
  void LoadREQSketch(DatabaseInstance &instance);
  void LoadTDigestSketch(DatabaseInstance &instance);
  void LoadHLLSketch(DatabaseInstance &instance);
  void LoadCPCSketch(DatabaseInstance &instance);
}