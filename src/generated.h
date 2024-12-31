#pragma once

#include "datasketches_extension.hpp"

namespace duckdb
{
  void LoadQuantilesSketch(DatabaseInstance &instance);
  void LoadKLLSketch(DatabaseInstance &instance);
  void LoadREQSketch(DatabaseInstance &instance);
  void LoadTDigestSketch(DatabaseInstance &instance);
  void LoadHLLSketch(DatabaseInstance &instance);
  void LoadCPCSketch(DatabaseInstance &instance);
}