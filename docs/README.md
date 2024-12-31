# datasketches


The DuckDB DataSketches Extension is a plugin for [DuckDB](https://duckdb.org) that provides an interface to the [Apache DataSketches](https://datasketches.apache.org/) library. This extension enables users to efficiently compute approximate results for large datasets directly within DuckDB, using state-of-the-art streaming algorithms for distinct counting, quantile estimation, and more.

This repository is based on https://github.com/duckdb/extension-template, check it out if you want to build and ship your own DuckDB extension.

## Features

### Approximate Distinct Count

These sketches provide fast and memory-efficient cardinality estimation.

Sketch Types

#### HyperLogLog - "`HLL`"



#### Compressed Probability Counting - "`CPC`"

The is an implementations of [Kevin J. Lang’s CPC sketch1](https://arxiv.org/abs/1708.06839). The stored CPC sketch can consume about 40% less space than a HyperLogLog sketch of comparable accuracy. Nonetheless, the HLL and CPC sketches have been intentially designed to offer different tradeoffs so that, in fact, they complement each other in many ways.

The CPC sketch has better accuracy for a given stored size then HyperLogLog, HyperLogLog has faster serialization and deserialization times than CPC.

Similar to the HyperLogLog sketch, the primary use-case for the CPC sketch is for counting distinct values as a stream, and then merging multiple sketches together for a total distinct count

Neither HLL nor CPC sketches provide means for set intersections or set differences

The values that can be aggregated by the CPC sketch are:

* `TINYINT`, `SMALLINT`, `INTEGER`, `BIGINT`, `FLOAT`, `DOUBLE`, `UTINYINT`, `USMALLINT`, `UINTEGER`, `UBIGINT`, `VARCHAR`, `BLOB`

The CPC sketch is returned as a type `sketch_cpc` which is equal to a BLOB.

##### Example

```sql
-- This table will contain the items where we are intersted in knowing
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
