# Apache DataSketches for DuckDB

![Ducks creating sketches](./duck-sketches-2.jpg)

The DuckDB DataSketches Extension is an extension for [DuckDB](https://duckdb.org) that provides an interface to the [Apache DataSketches](https://datasketches.apache.org/) library. This extension enables users to efficiently compute approximate results for large datasets directly within DuckDB, using state-of-the-art streaming algorithms for distinct counting, quantile estimation, and more.

## Why use this extension?

DuckDB already has great implementations of [HyperLogLog](https://en.wikipedia.org/wiki/HyperLogLog)  via `approx_count_distinct(x)`  and [TDigest](https://arxiv.org/abs/1902.04023) via `approx_quantile(x, pos)`, but it doesn't expose the internal state of the aggregates nor allow the the user to tune all of the parameters of the sketches.  This extension allows data sketches to be serialized as `BLOB`s which can be stored and shared across different systems, processes, and environments without loss of fidelity. This makes data sketches highly useful in distributed data processing pipelines.

This extension has implemented these sketches from Apache DataSketches.

- Quantile Estimation
  - [TDigest](https://datasketches.apache.org/docs/tdigest/tdigest.html)
  - [Classic Quantile](https://datasketches.apache.org/docs/Quantiles/ClassicQuantilesSketch.html)
  - [Relative Error Quantile (REQ)](https://datasketches.apache.org/docs/REQ/ReqSketch.html)
  - [KLL](https://datasketches.apache.org/docs/KLL/KLLSketch.html)
- Approximate Distinct Count
  - [Compressed Probability Counting (CPC)](https://datasketches.apache.org/docs/CPC/CpcSketches.html)
  - [HyperLogLog (HLL)](https://datasketches.apache.org/docs/HLL/HllSketches.html)
  - [Theta Sketch](https://datasketches.apache.org/docs/Theta/ThetaSketches.html)
- Frequent Items / Heavy Hitters
  - [Frequent Items](https://datasketches.apache.org/docs/Frequency/FrequentItemsOverview.html)

Additional sketch types can be implemented as needed.

## Installation

You can install this extension by executing this SQL:

```sql
install datasketches from community;
load datasketches;
```

## Which Sketch Should I Use?

### Quantile Estimation

| Sketch | Best For | K Range | Notes |
|--------|----------|---------|-------|
| **KLL** | General purpose | 1-32768 | Best balance of accuracy, speed, and size. Start here. |
| **TDigest** | Extreme quantiles (p99, p999) | 10-1000 | More accurate at distribution tails. Only supports FLOAT/DOUBLE. |
| **REQ** | Relative error guarantees | 4-1024 | Error relative to rank, not fixed. Good for skewed data. |
| **Quantiles** | Compatibility | 1-32768 | Classic algorithm. Use KLL instead for new projects. |

**Recommendation**: Use **KLL** unless you specifically need TDigest's tail accuracy or REQ's relative error guarantees.

### Distinct Counting

| Sketch | Best For | lg_k Range | Notes |
|--------|----------|------------|-------|
| **CPC** | Storage efficiency | 4-26 | ~40% smaller than HLL at same accuracy. Slower to serialize. |
| **HLL** | Speed | 4-21 | Faster serialization/deserialization. Industry standard. |

**Recommendation**: Use **CPC** if storage is a concern, **HLL** if you need faster operations or broader compatibility.

## Quick Reference

### Aggregate Functions

| Function | Description |
|----------|-------------|
| `datasketch_tdigest(k, value)` | Create TDigest sketch from values |
| `datasketch_quantiles(k, value)` | Create Quantiles sketch from values |
| `datasketch_kll(k, value)` | Create KLL sketch from values |
| `datasketch_req(k, value)` | Create REQ sketch from values |
| `datasketch_hll(lg_k, value)` | Create HLL sketch from values |
| `datasketch_hll_union(lg_k, sketch)` | Merge HLL sketches |
| `datasketch_cpc(lg_k, value)` | Create CPC sketch from values |
| `datasketch_cpc_union(lg_k, sketch)` | Merge CPC sketches |

### Quantile Sketch Functions (TDigest, Quantiles, KLL, REQ)

| Function | Description |
|----------|-------------|
| `datasketch_*_quantile(sketch, rank, ...)` | Get value at normalized rank (0-1) |
| `datasketch_*_rank(sketch, value, ...)` | Get normalized rank of value |
| `datasketch_*_cdf(sketch, split_points, ...)` | Cumulative distribution function |
| `datasketch_*_pmf(sketch, split_points, ...)` | Probability mass function |
| `datasketch_*_k(sketch)` | Get K parameter |
| `datasketch_*_is_empty(sketch)` | Check if sketch is empty |
| `datasketch_*_describe(sketch, ...)` | Human-readable summary |
| `datasketch_*_n(sketch)` | Count of items (not TDigest) |
| `datasketch_*_min_item(sketch)` | Minimum value (not TDigest) |
| `datasketch_*_max_item(sketch)` | Maximum value (not TDigest) |
| `datasketch_*_num_retained(sketch)` | Items retained in sketch (not TDigest) |
| `datasketch_*_is_estimation_mode(sketch)` | Whether sketch is estimating (not TDigest) |
| `datasketch_*_normalized_rank_error(sketch, is_pmf)` | Get error bound (Quantiles, KLL only) |
| `datasketch_tdigest_total_weight(sketch)` | Total weight (TDigest only) |

### Counting Sketch Functions (HLL, CPC)

| Function | Description |
|----------|-------------|
| `datasketch_*_estimate(sketch)` | Get distinct count estimate |
| `datasketch_*_lower_bound(sketch, std_dev)` | Lower confidence bound |
| `datasketch_*_upper_bound(sketch, std_dev)` | Upper confidence bound |
| `datasketch_*_is_empty(sketch)` | Check if sketch is empty |
| `datasketch_*_describe(sketch, ...)` | Human-readable summary |
| `datasketch_hll_lg_config_k(sketch)` | Get lg_k parameter (HLL only) |
| `datasketch_hll_is_compact(sketch)` | Check if compact form (HLL only) |

## Features

### Quantile Estimation

These sketches estimate a specific quantile (or percentile) and ranks of a distribution or dataset while being memory-efficient.

#### TDigest - "`tdigest`"

This implementation is based on the following paper:
Ted Dunning, Otmar Ertl. Extremely Accurate Quantiles Using t-Digests and the following implementation in Java
[https://github.com/tdunning/t-digest](https://github.com/tdunning/t-digest). This implementation is similar to MergingDigest in the above Java implementation

The values that can be aggregated by this sketch are: `DOUBLE` and `FLOAT`.

The TDigest sketch is returned as a type `sketch_tdigest_[type]` which is equal to a BLOB.

```sql
-- Lets simulate a temperature sensor
CREATE TABLE readings(temp integer);

INSERT INTO readings(temp) select unnest(generate_series(1, 10));

-- Create a sketch by aggregating id over the readings table.
SELECT datasketch_tdigest_rank(datasketch_tdigest(10, temp), 5) from readings;
┌────────────────────────────────────────────────────────────┐
│ datasketch_tdigest_rank(datasketch_tdigest(10, "temp"), 5) │
│                           double                           │
├────────────────────────────────────────────────────────────┤
│                                                       0.45 │
└────────────────────────────────────────────────────────────┘

-- Put some more readings in at the high end.
INSERT INTO readings(temp) values (10), (10), (10), (10);

-- Now the rank of 5 is moved down.
SELECT datasketch_tdigest_rank(datasketch_tdigest(10, temp), 5) from readings;
┌────────────────────────────────────────────────────────────┐
│ datasketch_tdigest_rank(datasketch_tdigest(10, "temp"), 5) │
│                           double                           │
├────────────────────────────────────────────────────────────┤
│                                        0.32142857142857145 │
└────────────────────────────────────────────────────────────┘

-- Lets get the cumulative distribution function from the sketch.
SELECT datasketch_tdigest_cdf(datasketch_tdigest(10, temp), [1,5,9]) from readings;
┌──────────────────────────────────────────────────────────────────────────────────┐
│ datasketch_tdigest_cdf(datasketch_tdigest(10, "temp"), main.list_value(1, 5, 9)) │
│                                     double[]                                     │
├──────────────────────────────────────────────────────────────────────────────────┤
│ [0.03571428571428571, 0.32142857142857145, 0.6071428571428571, 1.0]              │
└──────────────────────────────────────────────────────────────────────────────────┘


-- The sketch can be persisted and updated later when more data
-- arrives without having to rescan the previously aggregated data.
SELECT datasketch_tdigest(10, temp) from readings;
datasketch_tdigest(10, "temp") = \x02\x01\x14\x0A\x00\x04\x00...
```

##### Aggregate Functions

**`datasketch_tdigest(INTEGER, DOUBLE | FLOAT | sketch_tdigest) -> sketch_tdigest_float | sketch_tdigest_double`**

The first argument is the compression parameter (K), which controls the tradeoff between accuracy and memory usage. Higher values provide better accuracy but use more memory. Typical values range from 100 to 300. The second parameter is the value to aggregate into the sketch.

This same aggregate function can perform a union of multiple sketches.

##### Scalar Functions

**`datasketch_tdigest_rank(sketch_tdigest, value) -> DOUBLE`**

Compute approximate normalized rank of the given value.

-----

**`datasketch_tdigest_quantile(sketch_tdigest, DOUBLE) -> DOUBLE`**

Compute approximate quantile value corresponding to the given normalized rank

-----


**`datasketch_tdigest_pmf(sketch_tdigest, value[]) -> double[]`**

Returns an approximation to the Probability Mass Function (PMF) of the input stream given a set of split points.

The returned value is a list of <i>m</i>+1 doubles each of which is an approximation to the fraction of the input stream values (the mass) that fall into one of those intervals.

-----

**`datasketch_tdigest_cdf(sketch_tdigest, value[]) -> double[]`**

Returns an approximation to the Cumulative Distribution Function (CDF), which is the cumulative analog of the PMF, of the input stream given a set of split points.

The second argument is a list of of <i>m</i> unique, monotonically increasing values that divide the input domain into <i>m+1</i> consecutive disjoint intervals.

The returned value is a list of <i>m</i>+1 doubles, which are a consecutive approximation to the CDF of the input stream given the split_points. The value at array position j of the returned CDF array is the sum of the returned values in positions 0 through j of the returned PMF array. This can be viewed as array of ranks of the given split points plus
one more value that is always 1.

-----

**`datasketch_tdigest_k(sketch_tdigest) -> USMALLINT`**

Return the value of K for the passed sketch.

-----

**`datasketch_tdigest_is_empty(sketch_tdigest) -> BOOLEAN`**

Returns if the sketch is empty.

-----

**`datasketch_tdigest_describe(sketch_tdigest, BOOLEAN) -> VARCHAR`**

Returns a human-readable string representation of the sketch. The second argument controls whether to include centroid details in the output.

-----

**`datasketch_tdigest_total_weight(sketch_tdigest) -> UBIGINT`**

Returns the total weight (number of items) that have been added to the sketch.


#### Quantile - "`quantile`"

This is an implementation of the Low Discrepancy Mergeable Quantiles Sketch
described in section 3.2 of the journal version of the paper ["Mergeable Summaries"
by Agarwal, Cormode, Huang, Phillips, Wei, and Yi](http://dblp.org/rec/html/journals/tods/AgarwalCHPWY13).

The values that can be aggregated by this sketch are:

* `TINYINT`, `SMALLINT`, `INTEGER`, `BIGINT`, `FLOAT`, `DOUBLE`, `UTINYINT`, `USMALLINT`, `UINTEGER`, `UBIGINT`

The Quantile sketch is returned as a type `sketch_quantiles_[type]` which is equal to a BLOB.

This algorithm is independent of the distribution of items and requires only that the items be comparable.

This algorithm intentionally inserts randomness into the sampling process for items that
ultimately get retained in the sketch. The results produced by this algorithm are not
deterministic. For example, if the same stream is inserted into two different instances
of this sketch, the answers obtained from the two sketches may not be identical.

Similarly, there may be directional inconsistencies. For example, the result
obtained from `datasketches_quantile_quantile` input into the reverse directional query
`datasketches_quantile_rank` may not result in the original item.

The accuracy of this sketch is a function of the configured value <i>k</i>, which also affects
the overall size of the sketch. Accuracy of this quantile sketch is always with respect to
the normalized rank. A <i>k</i> of 128 produces a normalized, rank error of about 1.7%.
For example, the median item returned from `datasketches_quantile_quantile(0.5)` will be between the actual items
from the hypothetically sorted array of input items at normalized ranks of 0.483 and 0.517, with
a confidence of about 99%.

<pre>
Table Guide for DoublesSketch Size in Bytes and Approximate Error:
          K =&gt; |      16      32      64     128     256     512   1,024
    ~ Error =&gt; | 12.145%  6.359%  3.317%  1.725%  0.894%  0.463%  0.239%
             N | Size in Bytes -&gt;
------------------------------------------------------------------------
             0 |       8       8       8       8       8       8       8
             1 |      72      72      72      72      72      72      72
             3 |      72      72      72      72      72      72      72
             7 |     104     104     104     104     104     104     104
            15 |     168     168     168     168     168     168     168
            31 |     296     296     296     296     296     296     296
            63 |     424     552     552     552     552     552     552
           127 |     552     808   1,064   1,064   1,064   1,064   1,064
           255 |     680   1,064   1,576   2,088   2,088   2,088   2,088
           511 |     808   1,320   2,088   3,112   4,136   4,136   4,136
         1,023 |     936   1,576   2,600   4,136   6,184   8,232   8,232
         2,047 |   1,064   1,832   3,112   5,160   8,232  12,328  16,424
         4,095 |   1,192   2,088   3,624   6,184  10,280  16,424  24,616
         8,191 |   1,320   2,344   4,136   7,208  12,328  20,520  32,808
        16,383 |   1,448   2,600   4,648   8,232  14,376  24,616  41,000
        32,767 |   1,576   2,856   5,160   9,256  16,424  28,712  49,192
        65,535 |   1,704   3,112   5,672  10,280  18,472  32,808  57,384
       131,071 |   1,832   3,368   6,184  11,304  20,520  36,904  65,576
       262,143 |   1,960   3,624   6,696  12,328  22,568  41,000  73,768
       524,287 |   2,088   3,880   7,208  13,352  24,616  45,096  81,960
     1,048,575 |   2,216   4,136   7,720  14,376  26,664  49,192  90,152
     2,097,151 |   2,344   4,392   8,232  15,400  28,712  53,288  98,344
     4,194,303 |   2,472   4,648   8,744  16,424  30,760  57,384 106,536
     8,388,607 |   2,600   4,904   9,256  17,448  32,808  61,480 114,728
    16,777,215 |   2,728   5,160   9,768  18,472  34,856  65,576 122,920
    33,554,431 |   2,856   5,416  10,280  19,496  36,904  69,672 131,112
    67,108,863 |   2,984   5,672  10,792  20,520  38,952  73,768 139,304
   134,217,727 |   3,112   5,928  11,304  21,544  41,000  77,864 147,496
   268,435,455 |   3,240   6,184  11,816  22,568  43,048  81,960 155,688
   536,870,911 |   3,368   6,440  12,328  23,592  45,096  86,056 163,880
 1,073,741,823 |   3,496   6,696  12,840  24,616  47,144  90,152 172,072
 2,147,483,647 |   3,624   6,952  13,352  25,640  49,192  94,248 180,264
 4,294,967,295 |   3,752   7,208  13,864  26,664  51,240  98,344 188,456
</pre>

```sql
-- Lets simulate a temperature sensor
CREATE TABLE readings(temp integer);

INSERT INTO readings(temp) select unnest(generate_series(1, 10));

-- Create a sketch by aggregating id over the readings table.
SELECT datasketch_quantiles_rank(datasketch_quantiles(16, temp), 5, true) from readings;
┌──────────────────────────────────────────────────────────────────────────────────────┐
│ datasketch_quantiles_rank(datasketch_quantiles(16, "temp"), 5, CAST('t' AS BOOLEAN)) │
│                                        double                                        │
├──────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  0.5 │
└──────────────────────────────────────────────────────────────────────────────────────┘

-- Put some more readings in at the high end.
INSERT INTO readings(temp) values (10), (10), (10), (10);

-- Now the rank of 5 is moved down.
SELECT datasketch_quantiles_rank(datasketch_quantiles(16, temp), 5, true) from readings;
┌──────────────────────────────────────────────────────────────────────────────────────┐
│ datasketch_quantiles_rank(datasketch_quantiles(16, "temp"), 5, CAST('t' AS BOOLEAN)) │
│                                        double                                        │
├──────────────────────────────────────────────────────────────────────────────────────┤
│                                                                  0.35714285714285715 │
└──────────────────────────────────────────────────────────────────────────────────────┘

-- Lets get the cumulative distribution function from the sketch.
SELECT datasketch_quantiles_cdf(datasketch_quantiles(16, temp), [1,5,9], true) from readings;
┌────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ datasketch_quantiles_cdf(datasketch_quantiles(16, "temp"), main.list_value(1, 5, 9), CAST('t' AS BOOLEAN)) │
│                                                  int32[]                                                   │
├────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ [0, 0, 0, 1]                                                                                               │
└────────────────────────────────────────────────────────────────────────────────────────────────────────────┘

-- The sketch can be persisted and updated later when more data
-- arrives without having to rescan the previously aggregated data.
SELECT datasketch_quantiles(16, temp) from readings;
datasketch_quantiles(16, "temp") = \x02\x03\x08\x18\x10\x00\x00...
```

##### Aggregate Functions

**`datasketch_quantiles(INTEGER, DOUBLE | FLOAT | sketch_quantiles) -> sketch_quantiles_[type]`**

The first argument is K, which controls the accuracy and size of the sketch. Valid range is 1 to 32768. A K of 128 yields about 1.7% normalized rank error. See the size/error table above for guidance. The second parameter is the value to aggregate into the sketch.

This same aggregate function can perform a union of multiple sketches.

##### Scalar Functions

**`datasketch_quantiles_rank(sketch_quantiles, value, BOOLEAN) -> DOUBLE`**

Returns an approximation to the normalized rank of the given item from 0 to 1, inclusive.

The third argument if true means the weight of the given item is included into the rank.

-----

**`datasketch_quantiles_quantile(sketch_quantiles, DOUBLE) -> DOUBLE`**

Returns an approximation to the data item associated with the given rank
of a hypothetical sorted version of the input stream so far.

The third argument if true means the weight of the given item is included into the rank.

-----


**`datasketch_quantiles_pmf(sketch_quantiles, value[], BOOLEAN) -> double[]`**

Returns an approximation to the Probability Mass Function (PMF) of the input stream
given a set of split points (items).

The resulting approximations have a probabilistic guarantee that can be obtained from the
`datasketch_quantiles_normalized_rank_error(sketch, true)` function.

The second argument is a list of <i>m</i> unique, monotonically increasing items
that divide the input domain into <i>m+1</i> consecutive disjoint intervals (bins).

The third argument if true the rank of an item includes its own weight, and therefore
if the sketch contains items equal to a split point, then in PMF such items are
included into the interval to the left of split point. Otherwise they are included into the interval
to the right of split point.

-----

**`datasketch_quantiles_cdf(sketch_quantiles, value[], BOOLEAN) -> double[]`**

Returns an approximation to the Cumulative Distribution Function (CDF), which is the
cumulative analog of the PMF, of the input stream given a set of split points (items).

The second argument is a list of <i>m</i> unique, monotonically increasing items
that divide the input domain into <i>m+1</i> consecutive disjoint intervals.

The third argument if true the rank of an item includes its own weight, and therefore
if the sketch contains items equal to a split point, then in PMF such items are
included into the interval to the left of split point. Otherwise they are included into the interval
to the right of split point.

The result is an array of m+1 double values, which are a consecutive approximation to the CDF
of the input stream given the split_points. The value at array position j of the returned
CDF array is the sum of the returned values in positions 0 through j of the returned PMF
array. This can be viewed as array of ranks of the given split points plus one more value
that is always 1.

-----

**`datasketch_quantiles_k(sketch_quantiles) -> USMALLINT`**

Return the value of K for the passed sketch.

-----

**`datasketch_quantiles_is_empty(sketch_quantiles) -> BOOLEAN`**

Returns if the sketch is empty.

-----

**`datasketch_quantiles_n(sketch_quantiles) -> UBIGINT`**

Return the length of the input stream

-----

**`datasketch_quantiles_is_estimation_mode(sketch_quantiles) -> BOOLEAN`**

Return if the sketch is in estimation mode

-----

**`datasketch_quantiles_num_retained(sketch_quantiles) -> BOOLEAN`**

Return the number of items in the sketch

-----

**`datasketch_quantiles_min_item(sketch_quantiles) -> value`**

Return the smallest item in the sketch.

-----

**`datasketch_quantiles_max_item(sketch_quantiles) -> value`**

Return the largest item in the sketch.

-----

**`datasketch_quantiles_normalized_rank_error(sketch_quantiles, BOOLEAN) -> DOUBLE`**

Gets the normalized rank error for this sketch. Constants were derived as the best fit to 99 percentile
empirically measured max error in thousands of trials.

The second argument if true returns the "double-sided" normalized rank error.
Otherwise, it is the "single-sided" normalized rank error for all the other queries.


#### KLL - "`kll`"

Implementation of a very compact quantiles sketch with lazy compaction scheme
and nearly optimal accuracy per retained item.
See [Optimal Quantile Approximation in Streams](https://arxiv.org/abs/1603.05346v2).

This is a stochastic streaming sketch that enables near real-time analysis of the
approximate distribution of items from a very large stream in a single pass, requiring only
that the items are comparable.

The values that can be aggregated by this sketch are:

* `TINYINT`, `SMALLINT`, `INTEGER`, `BIGINT`, `FLOAT`, `DOUBLE`, `UTINYINT`, `USMALLINT`, `UINTEGER`, `UBIGINT`

The KLL sketch is returned as a type `sketch_kll_[type]` which is equal to a BLOB.

This sketch is configured with a parameter <i>k</i>, which affects the size of the sketch and
its estimation error.

The estimation error is commonly called <i>epsilon</i> (or <i>eps</i>) and is a fraction
between zero and one. Larger values of <i>k</i> result in smaller values of epsilon.
Epsilon is always with respect to the rank and cannot be applied to the
corresponding items.

The <i>k</i> of 200 yields a "single-sided" epsilon of about 1.33% and a
"double-sided" (PMF) epsilon of about 1.65%.

```sql
-- Lets simulate a temperature sensor
CREATE TABLE readings(temp integer);

INSERT INTO readings(temp) select unnest(generate_series(1, 10));

-- Create a sketch by aggregating id over the readings table.
SELECT datasketch_kll_rank(datasketch_kll(16, temp), 5, true) from readings;
┌──────────────────────────────────────────────────────────────────────────┐
│ datasketch_kll_rank(datasketch_kll(16, "temp"), 5, CAST('t' AS BOOLEAN)) │
│                                  double                                  │
├──────────────────────────────────────────────────────────────────────────┤
│                                                                      0.5 │
└──────────────────────────────────────────────────────────────────────────┘

-- Put some more readings in at the high end.
INSERT INTO readings(temp) values (10), (10), (10), (10);

-- Now the rank of 5 is moved down.
SELECT datasketch_kll_rank(datasketch_kll(16, temp), 5, true) from readings;
┌──────────────────────────────────────────────────────────────────────────────────────┐
│ datasketch_kll_rank(datasketch_kll(16, "temp"), 5, CAST('t' AS BOOLEAN)) │
│                                        double                                        │
├──────────────────────────────────────────────────────────────────────────────────────┤
│                                                                  0.35714285714285715 │
└──────────────────────────────────────────────────────────────────────────────────────┘

-- Lets get the cumulative distribution function from the sketch.
SELECT datasketch_kll_cdf(datasketch_kll(16, temp), [1,5,9], true) from readings;
┌────────────────────────────────────────────────────────────────────────────────────────────────┐
│ datasketch_kll_cdf(datasketch_kll(16, "temp"), main.list_value(1, 5, 9), CAST('t' AS BOOLEAN)) │
│                                            int32[]                                             │
├────────────────────────────────────────────────────────────────────────────────────────────────┤
│ [0, 0, 0, 1]                                                                                   │
└────────────────────────────────────────────────────────────────────────────────────────────────┘

-- The sketch can be persisted and updated later when more data
-- arrives without having to rescan the previously aggregated data.
SELECT datasketch_kll(16, temp) from readings;
datasketch_kll(16, "temp") = \x05\x01\x0F\x00\x10\x00\x08\x00\x0E\x00\x00\x0
```

##### Aggregate Functions

**`datasketch_kll(INTEGER, DOUBLE | FLOAT | sketch_kll) -> sketch_kll_[type]`**

The first argument is K, which controls the accuracy and size of the sketch. Valid range is 1 to 32768. A K of 200 yields about 1.33% single-sided or 1.65% double-sided (PMF) normalized rank error. The second parameter is the value to aggregate into the sketch.

This same aggregate function can perform a union of multiple sketches.

##### Scalar Functions

**`datasketch_kll_rank(sketch_kll, value, BOOLEAN) -> DOUBLE`**

Returns an approximation to the normalized rank of the given item from 0 to 1, inclusive.

The third argument if true means the weight of the given item is included into the rank.

-----

**`datasketch_kll_quantile(sketch_kll, DOUBLE, BOOLEAN) -> value`**

Returns an approximation to the data item associated with the given rank
of a hypothetical sorted version of the input stream so far.

The third argument if true means the weight of the given item is included into the rank.

-----


**`datasketch_kll_pmf(sketch_kll, value[], BOOLEAN) -> double[]`**

Returns an approximation to the Probability Mass Function (PMF) of the input stream
given a set of split points (items).

The resulting approximations have a probabilistic guarantee that can be obtained from the
`datasketch_kll_normalized_rank_error(sketch, true)` function.

The second argument is a list of <i>m</i> unique, monotonically increasing items
that divide the input domain into <i>m+1</i> consecutive disjoint intervals (bins).

The third argument if true the rank of an item includes its own weight, and therefore
if the sketch contains items equal to a split point, then in PMF such items are
included into the interval to the left of split point. Otherwise they are included into the interval
to the right of split point.

-----

**`datasketch_kll_cdf(sketch_kll, value[], BOOLEAN) -> double[]`**

Returns an approximation to the Cumulative Distribution Function (CDF), which is the
cumulative analog of the PMF, of the input stream given a set of split points (items).

The second argument is a list of <i>m</i> unique, monotonically increasing items
that divide the input domain into <i>m+1</i> consecutive disjoint intervals.

The third argument if true the rank of an item includes its own weight, and therefore
if the sketch contains items equal to a split point, then in PMF such items are
included into the interval to the left of split point. Otherwise they are included into the interval
to the right of split point.

The result is an array of m+1 double values, which are a consecutive approximation to the CDF
of the input stream given the split_points. The value at array position j of the returned
CDF array is the sum of the returned values in positions 0 through j of the returned PMF
array. This can be viewed as array of ranks of the given split points plus one more value
that is always 1.

-----

**`datasketch_kll_k(sketch_kll) -> USMALLINT`**

Return the value of K for the passed sketch.

-----

**`datasketch_kll_is_empty(sketch_kll) -> BOOLEAN`**

Returns if the sketch is empty.

-----

**`datasketch_kll_n(sketch_kll) -> UBIGINT`**

Return the length of the input stream

-----

**`datasketch_kll_is_estimation_mode(sketch_kll) -> BOOLEAN`**

Return if the sketch is in estimation mode

-----

**`datasketch_kll_num_retained(sketch_kll) -> BOOLEAN`**

Return the number of items in the sketch

-----

**`datasketch_kll_min_item(sketch_kll) -> value`**

Return the smallest item in the sketch.

-----

**`datasketch_kll_max_item(sketch_kll) -> value`**

Return the largest item in the sketch.

-----

**`datasketch_kll_normalized_rank_error(sketch_kll, BOOLEAN) -> DOUBLE`**

Gets the normalized rank error for this sketch. Constants were derived as the best fit to 99 percentile
empirically measured max error in thousands of trials.

The second argument if true returns the "double-sided" normalized rank error.
Otherwise, it is the "single-sided" normalized rank error for all the other queries.



#### Relative Error Quantile - "`req`"

This is an implementation based on the paper
["Relative Error Streaming Quantiles" by Graham Cormode, Zohar Karnin, Edo Liberty,
Justin Thaler, Pavel Veselý](https://arxiv.org/abs/2004.01668), and loosely derived from a Python prototype written by Pavel Veselý.

This implementation differs from the algorithm described in the paper in the following:

<ul>
 <li>The algorithm requires no upper bound on the stream length.
 Instead, each relative-compactor counts the number of compaction operations performed
 so far (via variable state). Initially, the relative-compactor starts with INIT_NUMBER_OF_SECTIONS.
 Each time the number of compactions (variable state) exceeds 2^{numSections - 1}, we double
 numSections. Note that after merging the sketch with another one variable state may not correspond
 to the number of compactions performed at a particular level, however, since the state variable
 never exceeds the number of compactions, the guarantees of the sketch remain valid.</li>

 <li>The size of each section (variable k and section_size in the code and parameter k in
 the paper) is initialized with a number set by the user via variable k.
 When the number of sections doubles, we decrease section_size by a factor of sqrt(2).
 This is applied at each level separately. Thus, when we double the number of sections, the
 nominal compactor size increases by a factor of approx. sqrt(2) (+/- rounding).</li>

 <li>The merge operation here does not perform "special compactions", which are used in the paper
 to allow for a tight mathematical analysis of the sketch.</li>
 </ul>

The values that can be aggregated by this sketch are:

* `TINYINT`, `SMALLINT`, `INTEGER`, `BIGINT`, `FLOAT`, `DOUBLE`, `UTINYINT`, `USMALLINT`, `UINTEGER`, `UBIGINT`

The REQ sketch is returned as a type `sketch_req_[type]` which is equal to a BLOB.



This sketch is configured with a parameter <i>k</i>, which controls the size and error of the sketch.
It must be even and in the range [4, 1024], inclusive. Value of 12 roughly corresponds to 1% relative
error guarantee at 95% confidence.


```sql
-- Lets simulate a temperature sensor
CREATE TABLE readings(temp integer);

INSERT INTO readings(temp) select unnest(generate_series(1, 10));

-- Create a sketch by aggregating id over the readings table.
SELECT datasketch_req_rank(datasketch_req(16, temp), 5, true) from readings;
┌──────────────────────────────────────────────────────────────────────────┐
│ datasketch_req_rank(datasketch_req(16, "temp"), 5, CAST('t' AS BOOLEAN)) │
│                                  double                                  │
├──────────────────────────────────────────────────────────────────────────┤
│                                                                      0.5 │
└──────────────────────────────────────────────────────────────────────────┘

-- Put some more readings in at the high end.
INSERT INTO readings(temp) values (10), (10), (10), (10);

-- Now the rank of 5 is moved down.
SELECT datasketch_req_rank(datasketch_req(16, temp), 5, true) from readings;
┌──────────────────────────────────────────────────────────────────────────────────────┐
│ datasketch_req_rank(datasketch_req(16, "temp"), 5, CAST('t' AS BOOLEAN)) │
│                                        double                                        │
├──────────────────────────────────────────────────────────────────────────────────────┤
│                                                                  0.35714285714285715 │
└──────────────────────────────────────────────────────────────────────────────────────┘

-- Lets get the cumulative distribution function from the sketch.
SELECT datasketch_req_cdf(datasketch_req(16, temp), [1,5,9], true) from readings;
┌────────────────────────────────────────────────────────────────────────────────────────────────┐
│ datasketch_req_cdf(datasketch_req(16, "temp"), main.list_value(1, 5, 9), CAST('t' AS BOOLEAN)) │
│                                            int32[]                                             │
├────────────────────────────────────────────────────────────────────────────────────────────────┤
│ [0, 0, 0, 1]                                                                                   │
└────────────────────────────────────────────────────────────────────────────────────────────────┘

-- The sketch can be persisted and updated later when more data
-- arrives without having to rescan the previously aggregated data.
SELECT datasketch_req(16, temp) from readings;
datasketch_req(16, "temp") =  \x02\x01\x11\x08\x10\x00\x01\x00\x00\x00\x00\x00\x...
```

##### Aggregate Functions

**`datasketch_req(INTEGER, DOUBLE | FLOAT | sketch_req) -> sketch_req_[type]`**

The first argument is K, which controls the accuracy and size of the sketch. Must be even and in the range 4 to 1024. A K of 12 corresponds to about 1% relative error at 95% confidence. The second parameter is the value to aggregate into the sketch.

This same aggregate function can perform a union of multiple sketches.

##### Scalar Functions

**`datasketch_req_rank(sketch_req, value, BOOLEAN) -> DOUBLE`**

Returns an approximation to the normalized rank of the given item from 0 to 1, inclusive.

The third argument if true means the weight of the given item is included into the rank.

-----

**`datasketch_req_quantile(sketch_req, DOUBLE) -> DOUBLE`**

Returns an approximation to the data item associated with the given rank
of a hypothetical sorted version of the input stream so far.

The third argument if true means the weight of the given item is included into the rank.

-----


**`datasketch_req_pmf(sketch_req, value[], BOOLEAN) -> double[]`**

Returns an approximation to the Probability Mass Function (PMF) of the input stream
given a set of split points (items).

The resulting approximations have a probabilistic guarantee that can be obtained from the
`datasketch_req_normalized_rank_error(sketch, true)` function.

The second argument is a list of <i>m</i> unique, monotonically increasing items
that divide the input domain into <i>m+1</i> consecutive disjoint intervals (bins).

The third argument if true the rank of an item includes its own weight, and therefore
if the sketch contains items equal to a split point, then in PMF such items are
included into the interval to the left of split point. Otherwise they are included into the interval
to the right of split point.

-----

**`datasketch_req_cdf(sketch_req, value[], BOOLEAN) -> double[]`**

Returns an approximation to the Cumulative Distribution Function (CDF), which is the
cumulative analog of the PMF, of the input stream given a set of split points (items).

The second argument is a list of <i>m</i> unique, monotonically increasing items
that divide the input domain into <i>m+1</i> consecutive disjoint intervals.

The third argument if true the rank of an item includes its own weight, and therefore
if the sketch contains items equal to a split point, then in PMF such items are
included into the interval to the left of split point. Otherwise they are included into the interval
to the right of split point.

The result is an array of m+1 double values, which are a consecutive approximation to the CDF
of the input stream given the split_points. The value at array position j of the returned
CDF array is the sum of the returned values in positions 0 through j of the returned PMF
array. This can be viewed as array of ranks of the given split points plus one more value
that is always 1.

-----

**`datasketch_req_k(sketch_req) -> USMALLINT`**

Return the value of K for the passed sketch.

-----

**`datasketch_req_is_empty(sketch_req) -> BOOLEAN`**

Returns if the sketch is empty.

-----

**`datasketch_req_n(sketch_req) -> UBIGINT`**

Return the length of the input stream

-----

**`datasketch_req_is_estimation_mode(sketch_req) -> BOOLEAN`**

Return if the sketch is in estimation mode

-----

**`datasketch_req_num_retained(sketch_req) -> BOOLEAN`**

Return the number of items in the sketch

-----

**`datasketch_req_min_item(sketch_req) -> value`**

Return the smallest item in the sketch.

-----

**`datasketch_req_max_item(sketch_req) -> value`**

Return the largest item in the sketch.

-----

**`datasketch_req_normalized_rank_error(sketch_req, BOOLEAN) -> DOUBLE`**

Gets the normalized rank error for this sketch. Constants were derived as the best fit to 99 percentile
empirically measured max error in thousands of trials.

The second argument if true returns the "double-sided" normalized rank error.
Otherwise, it is the "single-sided" normalized rank error for all the other queries.

### Approximate Distinct Count

These sketch type provide fast and memory-efficient cardinality estimation.

#### HyperLogLog - "`hll`"

This sketch type contains a set of very compact implementations of Phillipe Flajolet’s HyperLogLog (HLL) but with significantly improved error behavior and excellent speed performance.

If the use case for sketching is primarily counting uniques and merging, the HyperLogLog sketch is the 2nd highest performing in terms of accuracy for storage space consumed (the new CPC sketch developed by Kevin J. Lang now beats it).

Neither HLL nor CPC sketches provide means for set intersections or set differences.

The values that can be aggregated by the HLL sketch are:

* `TINYINT`, `SMALLINT`, `INTEGER`, `BIGINT`, `FLOAT`, `DOUBLE`, `UTINYINT`, `USMALLINT`, `UINTEGER`, `UBIGINT`, `VARCHAR`, `BLOB`

The HLL sketch is returned as a type `sketch_hll` which is equal to a BLOB.

##### Example

```sql
-- This table will contain the items where we are interested in knowing
-- how many unique item ids there are.
CREATE TABLE items(id integer);

-- Insert the same ids twice to demonstrate the sketch only counts distinct items.
INSERT INTO items(id) select unnest(generate_series(1, 100000));
INSERT INTO items(id) select unnest(generate_series(1, 100000));

-- Create a sketch by aggregating id over the items table,
-- use the smallest possible sketch by setting K to 8, better
-- accuracy comes with larger values of K
SELECT datasketch_hll_estimate(datasketch_hll(8, id)) from items;
┌────────────────────────────────────────────────┐
│ datasketch_hll_estimate(datasketch_hll(8, id)) │
│                     double                     │
├────────────────────────────────────────────────┤
│                              99156.23039496985 │
└────────────────────────────────────────────────┘

-- The sketch can be persisted and updated later when more data
-- arrives without having to rescan the previously aggregated data.
SELECT datasketch_hll(8, id) from items;
datasketch_hll(8, id) = \x0A\x01\x07\x08\x00\x10\x06\x02\x00\x00\x00\x00\x00\x00...
```

##### Aggregate Functions

**`datasketch_hll(INTEGER, HLL_SUPPORTED_TYPE) -> sketch_hll`**

The first argument is lg_k (log base 2 of K), which controls the accuracy and size of the sketch. Valid range is 4 to 21. Higher values provide better accuracy but use more memory. A lg_k of 12 (4096 bins) is a common choice. The second parameter is the value to aggregate into the sketch.

-----

**`datasketch_hll_union(INTEGER, sketch_hll) -> sketch_hll`**

The first argument is lg_k (log base 2 of K). Valid range is 4 to 21. The second parameter is the sketch to aggregate via a union operation.

##### Scalar Functions

**`datasketch_hll_estimate(sketch_hll) -> DOUBLE`**

Get the estimated number of distinct elements seen by this sketch

-----

**`datasketch_hll_lower_bound(sketch_hll, integer) -> DOUBLE`**

Returns the approximate lower error bound given the specified number of standard deviations.

-----

**`datasketch_hll_upper_bound(sketch_hll, integer) -> DOUBLE`**

Returns the approximate lower error bound given the specified number of standard deviations.

-----

**`datasketch_hll_describe(sketch_hll) -> VARCHAR`**

Returns a human readable summary of the sketch.

-----

**`datasketch_hll_is_empty(sketch_hll) -> BOOLEAN`**

Returns if the sketch is empty.

-----

**`datasketch_hll_lg_config_k(sketch_hll) -> UTINYINT`**

Returns the base two logarithm for the number of bins in the sketch.

#### Compressed Probability Counting - "`cpc`"

This is an implementations of [Kevin J. Lang’s CPC sketch1](https://arxiv.org/abs/1708.06839). The stored CPC sketch can consume about 40% less space than a HyperLogLog sketch of comparable accuracy. Nonetheless, the HLL and CPC sketches have been intentially designed to offer different tradeoffs so that, in fact, they complement each other in many ways.

The CPC sketch has better accuracy for a given stored size then HyperLogLog, HyperLogLog has faster serialization and deserialization times than CPC.

Similar to the HyperLogLog sketch, the primary use-case for the CPC sketch is for counting distinct values as a stream, and then merging multiple sketches together for a total distinct count

Neither HLL nor CPC sketches provide means for set intersections or set differences.

The values that can be aggregated by the CPC sketch are:

* `TINYINT`, `SMALLINT`, `INTEGER`, `BIGINT`, `FLOAT`, `DOUBLE`, `UTINYINT`, `USMALLINT`, `UINTEGER`, `UBIGINT`, `VARCHAR`, `BLOB`

The CPC sketch is returned as a type `sketch_cpc` which is equal to a BLOB.

##### Example

```sql
-- This table will contain the items where we are interested in knowing
-- how many unique item ids there are.
CREATE TABLE items(id integer);

-- Insert the same ids twice to demonstrate the sketch only counts distinct items.
INSERT INTO items(id) select unnest(generate_series(1, 100000));
INSERT INTO items(id) select unnest(generate_series(1, 100000));

-- Create a sketch by aggregating id over the items table,
-- use the smallest possible sketch by setting K to 4, better
-- accuracy comes with larger values of K
SELECT datasketch_cpc_estimate(datasketch_cpc(4, id)) from items;
┌────────────────────────────────────────────────┐
│ datasketch_cpc_estimate(datasketch_cpc(4, id)) │
│                     double                     │
├────────────────────────────────────────────────┤
│                             104074.26344655872 │
└────────────────────────────────────────────────┘

-- The sketch can be persisted and updated later when more data
-- arrives without having to rescan the previously aggregated data.
SELECT datasketch_cpc(4, id) from items;
datasketch_cpc(4, id) = \x04\x01\x10\x04\x0A\x12\xCC\x93\xD0\x00\x00\x00\x03\x00\x00\x00\x0C]\xAD\x019\x9B\xFA\x04+\x00\x00\x00
```

##### Aggregate Functions

**`datasketch_cpc(INTEGER, CPC_SUPPORTED_TYPE) -> sketch_cpc`**

The first argument is lg_k (log base 2 of K), which controls the accuracy and size of the sketch. Valid range is 4 to 26. Higher values provide better accuracy but use more memory. A lg_k of 11 is a common choice. The second parameter is the value to aggregate into the sketch.

-----

**`datasketch_cpc_union(INTEGER, sketch_cpc) -> sketch_cpc`**

The first argument is lg_k (log base 2 of K). Valid range is 4 to 26. The second parameter is the sketch to aggregate via a union operation.

##### Scalar Functions

**`datasketch_cpc_estimate(sketch_cpc) -> DOUBLE`**

Get the estimated number of distinct elements seen by this sketch

-----

**`datasketch_cpc_lower_bound(sketch_cpc, integer kappa) -> DOUBLE`**

Returns the approximate lower error bound given a parameter kappa (1, 2 or 3).
This parameter is similar to the number of standard deviations of the normal distribution and corresponds to approximately 67%, 95% and 99% confidence intervals.

-----

**`datasketch_cpc_upper_bound(sketch_cpc, integer kappa) -> DOUBLE`**

Returns the approximate upper error bound given a parameter kappa (1, 2 or 3).
This parameter is similar to the number of standard deviations of the normal distribution and corresponds to approximately 67%, 95% and 99% confidence intervals.

-----

**`datasketch_cpc_describe(sketch_cpc) -> VARCHAR`**

Returns a human readable summary of the sketch.

-----

**`datasketch_cpc_is_empty(sketch_cpc) -> BOOLEAN`**

Returns if the sketch is empty.


#### **Theta Sketch \- "theta"**

The Theta Sketch framework provides projections of a set of unique identifiers to a set of random values (theta) between 0 and 1\. These sketches allow for set operations (Union, Intersection, Difference) while maintaining high accuracy for distinct counting.

The values that can be aggregated by the Theta sketch are:

* INTEGER, BIGINT, VARCHAR, BLOB, and other standard types.

The Theta sketch is returned as a type sketch\_theta which is equal to a BLOB.

##### **Aggregate Functions**

**datasketch\_theta(lg\_k, value) \-\> sketch\_theta**

Builds a Theta sketch. The first argument (lg\_k) is the log base 2 of the nominal entries (e.g., 12 for 4096 entries). The second argument is the value to add.

```sql

-- Build a sketch from a range of numbers with 2^10 bins
SELECT datasketch_theta(10, i) FROM range(0, 100) t(i);

┌────────────────────────────────────────────────────────────────────────────────────────────┐
│                                  datasketch_theta(10, i)                                   │
│                                        sketch_theta                                        │
├────────────────────────────────────────────────────────────────────────────────────────────┤
│ \x02\x03\x03\x00\x00\x1A\xCC\x93d\x00\x00\x00\x00\x00\x00\x00\x0A\x22\xDB\xB7\x1C\x87_\x…  │
└────────────────────────────────────────────────────────────────────────────────────────────┘


```

**datasketch\_theta\_union(sketch\_theta, sketch\_theta) \-\> sketch\_theta**

Computes the union of two Theta sketches. This is an aggregate function that can also merge multiple sketches.

```sql

-- Union of {1,2,3} and {3,4,5} \-\> {1,2,3,4,5} (Count: 5\)
SELECT datasketch_theta_estimate(
    datasketch_theta_union(
        (SELECT datasketch_theta(10, i) FROM range(1, 4) t(i)),
        (SELECT datasketch_theta(10, i) FROM range(3, 6) t(i))
    )
);
┌────────────────────────────────────────────────────────────────────────────────────────────┐
│ datasketch_theta_estimate(datasketch_theta_union((SELECT datasketch_theta(10, i) FROM "r…  │
│                                           double                                           │
├────────────────────────────────────────────────────────────────────────────────────────────┤
│                                            5.0                                             │
└────────────────────────────────────────────────────────────────────────────────────────────┘
```

**datasketch\_theta\_intersect(sketch\_theta, sketch\_theta) \-\> sketch\_theta**

Computes the intersection of two Theta sketches.

```sql
-- Intersection of {1,2,3} and {3,4,5} \-\> {3} (Count: 1\)
SELECT datasketch_theta_estimate(
    datasketch_theta_intersect(
        (SELECT datasketch_theta(10, i) FROM range(1, 4) t(i)),
        (SELECT datasketch_theta(10, i) FROM range(3, 6) t(i))
    )
);
┌────────────────────────────────────────────────────────────────────────────────────────────┐
│ datasketch_theta_estimate(datasketch_theta_intersect((SELECT datasketch_theta(10, i) FRO…  │
│                                           double                                           │
├────────────────────────────────────────────────────────────────────────────────────────────┤
│                                            1.0                                             │
└────────────────────────────────────────────────────────────────────────────────────────────┘
```

**datasketch\_theta\_a\_not\_b(sketch\_theta, sketch\_theta) \-\> sketch\_theta**

Computes the set difference (A \\ B) of two Theta sketches. Note that this operation is not commutative.

```sql

-- {1,2,3} NOT {3,4,5} \-\> {1,2} (Count: 2\)
SELECT datasketch_theta_estimate(
    datasketch_theta_a_not_b(
        (SELECT datasketch_theta(10, i) FROM range(1, 4) t(i)),
        (SELECT datasketch_theta(10, i) FROM range(3, 6) t(i))
    )
);
┌────────────────────────────────────────────────────────────────────────────────────────────┐
│ datasketch_theta_estimate(datasketch_theta_a_not_b((SELECT datasketch_theta(10, i) FROM …  │
│                                           double                                           │
├────────────────────────────────────────────────────────────────────────────────────────────┤
│                                            2.0                                             │
└────────────────────────────────────────────────────────────────────────────────────────────┘
```



##### **Scalar Functions**

**datasketch\_theta\_estimate(sketch\_theta) \-\> DOUBLE**

Get the estimated number of distinct elements seen by this sketch.

```sql

SELECT datasketch_theta_estimate(datasketch_theta(10, i))
FROM range(0, 1000) t(i);
┌────────────────────────────────────────────────────┐
│ datasketch_theta_estimate(datasketch_theta(10, i)) │
│                       double                       │
├────────────────────────────────────────────────────┤
│                       1000.0                       │
└────────────────────────────────────────────────────┘
```

**datasketch\_theta\_lower\_bound(sketch\_theta, num\_std\_devs) \-\> DOUBLE**

Returns the approximate lower error bound given the specified number of standard deviations (1, 2, or 3).

```sql
-- Get the lower bound with 2 standard deviations (approx 95% confidence)
SELECT datasketch_theta_lower_bound(sketch, 2)
FROM (SELECT datasketch_theta(10, i) as sketch FROM range(0, 1000) t(i));
┌─────────────────────────────────────────┐
│ datasketch_theta_lower_bound(sketch, 2) │
│                 double                  │
├─────────────────────────────────────────┤
│                 1000.0                  │
└─────────────────────────────────────────┘
```

**datasketch\_theta\_upper\_bound(sketch\_theta, num\_std\_devs) \-\> DOUBLE**

Returns the approximate upper error bound given the specified number of standard deviations (1, 2, or 3).

```sql
-- Get the upper bound with 2 standard deviations
SELECT datasketch_theta_upper_bound(sketch, 2)
FROM (SELECT datasketch_theta(10, i) as sketch FROM range(0, 1000) t(i));
┌─────────────────────────────────────────┐
│ datasketch_theta_upper_bound(sketch, 2) │
│                 double                  │
├─────────────────────────────────────────┤
│                 1000.0                  │
└─────────────────────────────────────────┘
```

**datasketch\_theta\_get\_theta(sketch\_theta) \-\> DOUBLE**

Returns theta (0.0 to 1.0), which acts as a sampling rate for the sketch. If the sketch is in exact mode, theta is 1.0. If it is in estimation mode, theta \< 1.0.

```sql
SELECT datasketch_theta_get_theta(datasketch_theta(5, i))
FROM range(0, 100) t(i);
-- Returns \< 1.0 because K=32 (2^5) is too small to hold 100 items exactly.
┌────────────────────────────────────────────────────┐
│ datasketch_theta_get_theta(datasketch_theta(5, i)) │
│                       double                       │
├────────────────────────────────────────────────────┤
│                 0.2457577112224644                 │
└────────────────────────────────────────────────────┘
```

**datasketch\_theta\_is\_empty(sketch\_theta) \-\> BOOLEAN**

Returns true if the sketch is empty (has seen no items).

```sql
SELECT datasketch_theta_is_empty(datasketch_theta(10, i))
FROM (SELECT NULL as i WHERE 1=0) t;
┌────────────────────────────────────────────────────┐
│ datasketch_theta_is_empty(datasketch_theta(10, i)) │
│                      boolean                       │
├────────────────────────────────────────────────────┤
│ true                                               │
└────────────────────────────────────────────────────┘
```

**datasketch\_theta\_is\_estimation\_mode(sketch\_theta) \-\> BOOLEAN**

Returns true if the sketch is in estimation mode (has discarded some data to save space), false if it is exact.

```sql
SELECT datasketch_theta_is_estimation_mode(datasketch_theta(10, i))
FROM range(0, 5000) t(i);
-- Returns true (2^10 = 1024 bins, which is < 5000 items)
┌──────────────────────────────────────────────────────────────┐
│ datasketch_theta_is_estimation_mode(datasketch_theta(10, i)) │
│                           boolean                            │
├──────────────────────────────────────────────────────────────┤
│ true                                                         │
└──────────────────────────────────────────────────────────────┘
```

**datasketch\_theta\_num\_retained(sketch\_theta) \-\> INTEGER**

Returns the number of entries currently retained in the sketch.

```sql
SELECT datasketch_theta_num_retained(datasketch_theta(10, i))
FROM range(0, 50) t(i);
┌────────────────────────────────────────────────────────┐
│ datasketch_theta_num_retained(datasketch_theta(10, i)) │
│                         int64                          │
├────────────────────────────────────────────────────────┤
│                           50                           │
└────────────────────────────────────────────────────────┘
```

**datasketch\_theta\_get\_seed(sketch\_theta) \-\> BIGINT**

Returns the hash seed used for this sketch. Sketches must have the same seed to be merged or intersected.

```sql
SELECT datasketch_theta_get_seed(datasketch_theta(10, 1));
┌────────────────────────────────────────────────────┐
│ datasketch_theta_get_seed(datasketch_theta(10, 1)) │
│                       int64                        │
├────────────────────────────────────────────────────┤
│                       37836                        │
└────────────────────────────────────────────────────┘
```

**datasketch\_theta\_describe(sketch\_theta) \-\> VARCHAR**

Returns a human-readable summary of the sketch, including internal parameters.

```sql
SELECT datasketch_theta_describe(datasketch_theta(10, i))
FROM range(0, 5) t(i);
┌────────────────────────────────────────────────────────────────────────────────────────────┐
│                     datasketch_theta_describe(datasketch_theta(10, i))                     │
│                                          varchar                                           │
├────────────────────────────────────────────────────────────────────────────────────────────┤
│ ### Theta sketch summary:\n   num retained entries : 5\n   seed hash            : 37836\…  │
└────────────────────────────────────────────────────────────────────────────────────────────┘
```

### Frequent Items / Heavy Hitters

These sketches identify the most frequent items in a data stream (the "Heavy Hitters").

#### Frequent Items - "`frequent_items`"

This is an implementation of the Frequent Items sketch, which finds items that occur more than a threshold frequency. It can operate in "Exact Mode" for small maps or "Approximate Mode" (using an epsilon error factor) for large datasets.

The values that can be aggregated by this sketch are:

  * **Signed Integers:** `TINYINT`, `SMALLINT`, `INTEGER`, `BIGINT`
  * **Unsigned Integers:** `UTINYINT`, `USMALLINT`, `UINTEGER`, `UBIGINT`
  * **Floating Point:** `FLOAT`, `DOUBLE`
  * **Strings:** `VARCHAR`

The Frequent Items sketch is returned as a type `sketch_frequent_items` which is equal to a BLOB.

##### Example

```sql
-- Create a table with some heavy hitters
CREATE TABLE stream(item VARCHAR);
INSERT INTO stream SELECT 'heavy_hitter' FROM range(0, 100); -- Appears 100 times
INSERT INTO stream SELECT 'rare_' || i::VARCHAR FROM range(0, 2000) t(i); -- Noise

-- Create the sketch (lg_k = 6)
-- We use UNNEST on the result of get_frequent to view the list of structs
SELECT
    f.item,
    f.estimate,
    f.lower_bound,
    f.upper_bound
FROM (SELECT datasketch_frequent_items(6, item) as sketch FROM stream),
     UNNEST(datasketch_frequent_items_get_frequent(sketch, 'NO_FALSE_POSITIVES')) as t(f);
```



```
┌──────────────┬──────────┬─────────────┬─────────────┐
│     item     │ estimate │ lower_bound │ upper_bound │
│   varchar    │  int64   │    int64    │    int64    │
├──────────────┼──────────┼─────────────┼─────────────┤
│ heavy_hitter │   100    │     59      │     100     │
└──────────────┴──────────┴─────────────┴─────────────┘
```

##### Aggregate Functions

**`datasketch_frequent_items([lg_k,] value) -> sketch_frequent_items`**

Builds a Frequent Items sketch. The optional first argument (`lg_k`) is the log base 2 of the map size (default: 10). The second argument is the item to count.

```sql
-- Track frequent strings
SELECT datasketch_frequent_items(name) FROM customers;

-- Track frequent integers with custom lg_k
SELECT datasketch_frequent_items(6, user_id) FROM events;

-- Track frequent floating point values
SELECT datasketch_frequent_items(8, price) FROM transactions;
```

**Merging Sketches:**

You can merge multiple sketches by passing a `sketch_frequent_items` column:

```sql
-- Create partial sketches per partition
CREATE TABLE partial_sketches AS
SELECT region, datasketch_frequent_items(product_id) as sketch
FROM sales GROUP BY region;

-- Merge all partial sketches into one
SELECT datasketch_frequent_items(sketch) FROM partial_sketches;
```

##### Scalar Functions

**`datasketch_frequent_items_estimate(sketch, item) -> BIGINT`**

Returns the estimated count for a specific item. If the item is not found or tracked, returns 0.

```sql
-- Estimate for a string item
SELECT datasketch_frequent_items_estimate(sketch, 'apple') FROM fruit_sketch;

-- Estimate for an integer item
SELECT datasketch_frequent_items_estimate(sketch, 101) FROM user_sketch;

-- Estimate for a double item
SELECT datasketch_frequent_items_estimate(sketch, 9.99) FROM price_sketch;
```

-----

**`datasketch_frequent_items_get_frequent(sketch, error_type) -> LIST(STRUCT)`**

Returns a list of structs containing the frequent items. The `error_type` argument must be either:
- `'NO_FALSE_POSITIVES'`: Returns items guaranteed to be heavy hitters (stricter, fewer results)
- `'NO_FALSE_NEGATIVES'`: Returns all items that *might* be heavy hitters (includes potential false positives)

The returned struct contains: `item` (VARCHAR), `estimate` (BIGINT), `lower_bound` (BIGINT), and `upper_bound` (BIGINT).

> **Note:** Items are always returned as VARCHAR strings, regardless of the original input type.

```sql
-- Unnest the result to see items as a table
SELECT f.item, f.estimate, f.lower_bound, f.upper_bound
FROM my_sketch,
     UNNEST(datasketch_frequent_items_get_frequent(sketch, 'NO_FALSE_POSITIVES')) as t(f)
ORDER BY f.estimate DESC;
```

-----

**`datasketch_frequent_items_lower_bound(sketch, item) -> BIGINT`**

Returns the lower bound of the frequency estimate for a specific item.

```sql
SELECT datasketch_frequent_items_lower_bound(sketch, 'apple') FROM fruit_sketch;
SELECT datasketch_frequent_items_lower_bound(sketch, 101) FROM user_sketch;
```

-----

**`datasketch_frequent_items_upper_bound(sketch, item) -> BIGINT`**

Returns the upper bound of the frequency estimate for a specific item.

```sql
SELECT datasketch_frequent_items_upper_bound(sketch, 'apple') FROM fruit_sketch;
SELECT datasketch_frequent_items_upper_bound(sketch, 101) FROM user_sketch;
```

-----

**`datasketch_frequent_items_epsilon(sketch) -> DOUBLE`**

Returns epsilon, the maximum error width of the sketch. This is determined by the `lg_k` parameter used during creation. Smaller epsilon means higher accuracy.

```sql
SELECT datasketch_frequent_items_epsilon(sketch) FROM my_sketch;
-- Example output: 0.00341796875 (for lg_k=10)
```

-----

**`datasketch_frequent_items_total_weight(sketch) -> BIGINT`**

Returns the total count of all items inserted into the sketch.

```sql
SELECT datasketch_frequent_items_total_weight(sketch) FROM my_sketch;
-- Returns the number of items processed
```

-----

**`datasketch_frequent_items_num_active(sketch) -> BIGINT`**

Returns the number of active (distinct) items currently retained in the sketch.

```sql
SELECT datasketch_frequent_items_num_active(sketch) FROM my_sketch;
```

-----

**`datasketch_frequent_items_is_empty(sketch) -> BOOLEAN`**

Returns true if the sketch is empty (no items have been added).

```sql
SELECT datasketch_frequent_items_is_empty(sketch) FROM my_sketch;
```

##### Supported Type Examples

```sql
-- VARCHAR
SELECT datasketch_frequent_items_estimate(
    datasketch_frequent_items(name), 'Alice'
) FROM users;

-- INTEGER
SELECT datasketch_frequent_items_estimate(
    datasketch_frequent_items(age), 25
) FROM users;

-- BIGINT
SELECT datasketch_frequent_items_estimate(
    datasketch_frequent_items(big_id), 9223372036854775807
) FROM large_ids;

-- DOUBLE
SELECT datasketch_frequent_items_estimate(
    datasketch_frequent_items(price), 19.99
) FROM products;

-- TINYINT
SELECT datasketch_frequent_items_estimate(
    datasketch_frequent_items(status_code), 1::TINYINT
) FROM events;

-- Unsigned types work similarly
SELECT datasketch_frequent_items_estimate(
    datasketch_frequent_items(port), 443::USMALLINT
) FROM network_logs;
```



