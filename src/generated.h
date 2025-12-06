#pragma once

#include "datasketches_extension.hpp"

using namespace duckdb;
namespace duckdb_datasketches
{
  void LoadQuantilesSketch(ExtensionLoader &loader);
  void LoadKLLSketch(ExtensionLoader &loader);
  void LoadREQSketch(ExtensionLoader &loader);
  void LoadTDigestSketch(ExtensionLoader &loader);
  void LoadHLLSketch(ExtensionLoader &loader);
  void LoadCPCSketch(ExtensionLoader &loader);
}