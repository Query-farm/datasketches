#pragma once

#include "duckdb.hpp"

namespace duckdb
{

	class DatasketchesExtension : public Extension
	{
	public:
		void Load(ExtensionLoader &loader) override;
		std::string Name() override;
	};


} // namespace duckdb
  //


namespace duckdb_datasketches {
        // Existing Sketches
        void LoadQuantilesSketch(duckdb::ExtensionLoader &loader);
        void LoadKLLSketch(duckdb::ExtensionLoader &loader);
        void LoadREQSketch(duckdb::ExtensionLoader &loader);
        void LoadTDigestSketch(duckdb::ExtensionLoader &loader);
        void LoadHLLSketch(duckdb::ExtensionLoader &loader);
        void LoadCPCSketch(duckdb::ExtensionLoader &loader);

        // Your New Sketches
        void LoadThetaSketch(duckdb::ExtensionLoader &loader);
        void LoadFrequentItemsSketch(duckdb::ExtensionLoader &loader);
    }
