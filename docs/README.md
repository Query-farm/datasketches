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

##### Aggregate Functions

`datasketch_cpc(k, value) -> sketch_cpc`

###### Arguments:

| Argument | Type | Description |
|---------------|------|-------------|
| `k` | INTEGER | The K parameter for the sketch, determines the amount of memory the sketch will use |
| `value` | CPC_SUPPORTED_TYPE | The value to aggregate into the sketch |

###### Return type:

`sketch_cpc` a CPC sketch which is a logical BLOB type.

`datasketch_cpc_union(k, value) -> sketch_cpc`

###### Arguments:

| Argument | Type | Description |
|---------------|------|-------------|
| `k` | INTEGER | The K parameter for the sketch, determines the amount of memory the sketch will use |
| `value` | `sketch_cpc` | The CPC sketch to combine togehter |

###### Return type:

`sketch_cpc` a CPC sketch that is the union of all aggregated CPC sketches

##### Scalar Functions

`datasketch_cpc_estimate(sketch_cpc) -> DOUBLE`

Get the estimated number of distinct elements seen by this sketch

`datasketch_cpc_lower_bound(sketch_cpc, integer kappa) -> DOUBLE`

Returns the approximate lower error bound given a parameter kappa (1, 2 or 3).
This parameter is similar to the number of standard deviations of the normal distribution and corresponds to approximately 67%, 95% and 99% confidence intervals.

`datasketch_cpc_upper_bound(sketch_cpc, integer kappa) -> DOUBLE`

Returns the approximate upper error bound given a parameter kappa (1, 2 or 3).
This parameter is similar to the number of standard deviations of the normal distribution and corresponds to approximately 67%, 95% and 99% confidence intervals.

`datasketch_cpc_describe(sketch_cpc) -> VARCHAR`

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
