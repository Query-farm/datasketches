# Datasketches for DuckDB

The DuckDB DataSketches Extension is a plugin for [DuckDB](https://duckdb.org) that provides an interface to the [Apache DataSketches](https://datasketches.apache.org/) library. This extension enables users to efficiently compute approximate results for large datasets directly within DuckDB, using state-of-the-art streaming algorithms for distinct counting, quantile estimation, and more.

This repository is based on https://github.com/duckdb/extension-template, check it out if you want to build and ship your own DuckDB extension.

## Features

### Quantile Estimation

These sketches estimatate a specific quantile (or percentile) and ranks of a distribution or dataset while being memory-efficient.

#### TDigest - "`tdigest`"

This implementation is based on the following paper:
Ted Dunning, Otmar Ertl. Extremely Accurate Quantiles Using t-Digests and the following implementation in Java
[https://github.com/tdunning/t-digest](https://github.com/tdunning/t-digest). This implementation is similar to MergingDigest in the above Java implementation

The values that can be aggregated by the CPC sketch are: `DOUBLE` and `FLOAT`.

The TDigest sketch is returned as a type `sketch_tdigest` which is equal to a BLOB.

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

The first argument is the base two logarithm of the number of bins in the sketch, which affects memory used. The second parameter is the value to aggregate into the sketch.

This same aggregate function can perform a union of multiple sketches.

##### Scalar Functions

**`datasketch_tdigest_rank(sketch_tdigest, value) -> DOUBLE`**

Compute approximate normalized rank of the given value.

**`datasketch_tdigest_quantile(sketch_tdigest, DOUBLE) -> DOUBLE`**

Compute approximate quantile value corresponding to the given normalized rank


**`datasketch_tdigest_pmf(sketch_tdigest, value[]) -> double[]`**

Returns an approximation to the Probability Mass Function (PMF) of the input stream given a set of split points.

The returned value is a list of <i>m</i>+1 doubles each of which is an approximation to the fraction of the input stream values (the mass) that fall into one of those intervals.

**`datasketch_tdigest_cdf(sketch_tdigest, value[]) -> double[]`**

Returns an approximation to the Cumulative Distribution Function (CDF), which is the cumulative analog of the PMF, of the input stream given a set of split points.

The second argument is a list of of <i>m</i> unique, monotonically increasing values that divide the input domain into <i>m+1</i> consecutive disjoint intervals.

The returned value is a list of <i>m</i>+1 doubles, which are a consecutive approximation to the CDF of the input stream given the split_points. The value at array position j of the returned CDF array is the sum of the returned values in positions 0 through j of the returned PMF array. This can be viewed as array of ranks of the given split points plus
one more value that is always 1.

-----

**`datasketch_tdigest_get_k(sketch_tdigest) -> USMALLINT`**

Return the value of K for the passed sketch.

-----

**`datasketch_tdigest_is_empty(sketch_tdigest) -> BOOLEAN`**

Returns if the sketch is empty.

### Approximate Distinct Count

These sketch type provide fast and memory-efficient cardinality estimation.

#### HyperLogLog - "`hll`"

This sketch type contains a set of very compact implementations of Phillipe Flajolet’s HyperLogLog (HLL) but with significantly improved error behavior and excellent speed performance.

If the use case for sketching is primarily counting uniques and merging, the HyperLogLog sketch is the 2nd highest performing in terms of accuracy for storage space consumed (the new CPC sketch developed by Kevin J. Lang now beats it).

Neither HLL nor CPC sketches provide means for set intersections or set differences.

The values that can be aggregated by the CPC sketch are:

* `TINYINT`, `SMALLINT`, `INTEGER`, `BIGINT`, `FLOAT`, `DOUBLE`, `UTINYINT`, `USMALLINT`, `UINTEGER`, `UBIGINT`, `VARCHAR`, `BLOB`

The HLL sketch is returned as a type `sketch_hll` which is equal to a BLOB.

##### Example

```sql
-- This table will contain the items where we are interested in knowing
-- how many unique item id there are.
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

The first argument is the base two logarithm of the number of bins in the sketch, which affects memory used. The second parameter is the value to aggregate into the sketch.

**`datasketch_hll_union(INTEGER, sketch_hll) -> sketch_hll`**

The first argument is the base two logarithm of the number of bins in the sketch, which affects memory used. The second parameter is the sketch to aggregate via a union operation.

##### Scalar Functions

**`datasketch_hll_estimate(sketch_hll) -> DOUBLE`**

Get the estimated number of distinct elements seen by this sketch

**`datasketch_hll_lower_bound(sketch_hll, integer) -> DOUBLE`**

Returns the approximate lower error bound given the specified number of standard deviations.

**`datasketch_hll_upper_bound(sketch_hll, integer) -> DOUBLE`**

Returns the approximate lower error bound given the specified number of standard deviations.

**`datasketch_hll_describe(sketch_hll) -> VARCHAR`**

Returns a human readable summary of the sketch.

**`datasketch_hll_is_empty(sketch_hll) -> BOOLEAN`**

Returns if the sketch is empty.

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
-- how many unique item id there are.
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

The first argument is the base two logarithm of the number of bins in the sketch, which affects memory used. The second parameter is the value to aggregate into the sketch.

**`datasketch_cpc_union(INTEGER, sketch_cpc) -> sketch_cpc`**

The first argument is the base two logarithm of the number of bins in the sketch, which affects memory used. The second parameter is the sketch to aggregate via a union operation.

##### Scalar Functions

**`datasketch_cpc_estimate(sketch_cpc) -> DOUBLE`**

Get the estimated number of distinct elements seen by this sketch

**`datasketch_cpc_lower_bound(sketch_cpc, integer kappa) -> DOUBLE`**

Returns the approximate lower error bound given a parameter kappa (1, 2 or 3).
This parameter is similar to the number of standard deviations of the normal distribution and corresponds to approximately 67%, 95% and 99% confidence intervals.

**`datasketch_cpc_upper_bound(sketch_cpc, integer kappa) -> DOUBLE`**

Returns the approximate upper error bound given a parameter kappa (1, 2 or 3).
This parameter is similar to the number of standard deviations of the normal distribution and corresponds to approximately 67%, 95% and 99% confidence intervals.

**`datasketch_cpc_describe(sketch_cpc) -> VARCHAR`**

Returns a human readable summary of the sketch.

**`datasketch_cpc_is_empty(sketch_cpc) -> BOOLEAN`**

Returns if the sketch is empty.




## Building
### Managing dependencies
DuckDB extensions uses VCPKG for dependency management. Enabling VCPKG is very simple: follow the [installation instructions](https://vcpkg.io/en/getting-started) or just run the following:
```shell
git clone https://github.com/Microsoft/vcpkg.git
./vcpkg/bootstrap-vcpkg.sh
export VCPKG_TOOLCHAIN_PATH=`pwd`/vcpkg/scripts/buildsystems/vcpkg.cmake
```

### Build steps
Now to build the extension, run:
```sh
make
```
The main binaries that will be built are:
```sh
./build/release/duckdb
./build/release/test/unittest
./build/release/extension/datasketches/datasketches.duckdb_extension
```
- `duckdb` is the binary for the duckdb shell with the extension code automatically loaded.
- `unittest` is the test runner of duckdb. Again, the extension is already linked into the binary.
- `datasketches.duckdb_extension` is the loadable binary as it would be distributed.

## Running the extension
To run the extension code, simply start the shell with `./build/release/duckdb`.


## Running the tests
Different tests can be created for DuckDB extensions. The primary way of testing DuckDB extensions should be the SQL tests in `./test/sql`. These SQL tests can be run using:
```sh
make test
```

### Installing the deployed binaries
To install your extension binaries from S3, you will need to do two things. Firstly, DuckDB should be launched with the
`allow_unsigned_extensions` option set to true. How to set this will depend on the client you're using. Some examples:

CLI:
```shell
duckdb -unsigned
```

Python:
```python
con = duckdb.connect(':memory:', config={'allow_unsigned_extensions' : 'true'})
```

NodeJS:
```js
db = new duckdb.Database(':memory:', {"allow_unsigned_extensions": "true"});
```

Secondly, you will need to set the repository endpoint in DuckDB to the HTTP url of your bucket + version of the extension
you want to install. To do this run the following SQL query in DuckDB:
```sql
SET custom_extension_repository='bucket.s3.eu-west-1.amazonaws.com/<your_extension_name>/latest';
```
Note that the `/latest` path will allow you to install the latest extension version available for your current version of
DuckDB. To specify a specific version, you can pass the version instead.

After running these steps, you can install and load your extension using the regular INSTALL/LOAD commands in DuckDB:
```sql
INSTALL datasketches
LOAD datasketches
```
