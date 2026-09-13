<p align="center">
  <a href="https://query.farm">
    <picture>
      <source media="(prefers-color-scheme: dark)" srcset="https://query.farm/media-kit/logo/wordmark-dark.svg">
      <img alt="Query.Farm" src="https://query.farm/media-kit/logo/wordmark-light.svg" height="64">
    </picture>
  </a>
</p>

# Apache DataSketches for DuckDB

[![DuckDB](https://img.shields.io/badge/DuckDB-community_extension-fdf1e0?logo=duckdb&logoColor=fff000)](https://duckdb.org/community_extensions/extensions/datasketches.html)
[![v1.5 build](https://github.com/Query-farm/datasketches/actions/workflows/MainDistributionPipeline.yml/badge.svg?branch=v1.5)](https://github.com/Query-farm/datasketches/actions/workflows/MainDistributionPipeline.yml?query=branch%3Av1.5)

The DuckDB DataSketches Extension is an extension for [DuckDB](https://duckdb.org) that provides an interface to the [Apache DataSketches](https://datasketches.apache.org/) library. This extension enables users to efficiently compute approximate results for large datasets directly within DuckDB, using state-of-the-art streaming algorithms for distinct counting, quantile estimation, and more.

## Documentation

Full documentation, including installation, usage, the function reference, and cookbook examples, is available at:

**[https://query.farm/products/extensions/datasketches](https://query.farm/products/extensions/datasketches)**

## Installation

```sql
INSTALL datasketches FROM community;
LOAD datasketches;
```

## Weighted Frequent Items

This extension provides `datasketch_frequent_items_weighted([lg_k,] value, weight)`,
which calls Apache DataSketches `frequent_items_sketch::update(item, weight)`.
It is useful for approximate TopK by metric, where each row contributes a
weight instead of a count of one:

```sql
WITH weighted_sketch AS (
    SELECT datasketch_frequent_items_weighted(
        10,
        item_name,
        metric_value
    ) AS sketch
    FROM input_items
)
SELECT f.item, f.estimate, f.lower_bound, f.upper_bound
FROM weighted_sketch,
     UNNEST(datasketch_frequent_items_get_frequent(sketch, 'NO_FALSE_POSITIVES')) AS t(f)
ORDER BY f.estimate DESC
LIMIT 100;
```

Weights are accepted as `BIGINT` or `UBIGINT`; negative signed weights are
rejected.
