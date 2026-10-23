# Apache DataSketches repository notes

For installation and the full user documentation, see the
[Query Farm Apache DataSketches documentation](https://query.farm/products/extensions/datasketches/).

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
