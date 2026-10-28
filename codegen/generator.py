"""Generates src/generated.cpp and test/sql/datasketch_generated_examples.test.

The generated sketches are described in SKETCHES and their scalar functions in scalar_functions();
codegen/generated.cpp.j2 renders them. Run it from anywhere: `python codegen/generator.py`.
"""

from __future__ import annotations

import textwrap
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Callable, Optional

from jinja2 import Environment, FileSystemLoader, StrictUndefined

CODEGEN_DIR = Path(__file__).resolve().parent
ROOT = CODEGEN_DIR.parent


# ---------------------------------------------------------------------------
# Types
# ---------------------------------------------------------------------------

# The C++ type each DuckDB logical type's values are read as. UUIDs are stored as hugeint_t; DSUUIDInput marks
# them so the counting sketches hash them as UUIDs (see DSCountingInput in the template).
CPP_TYPE = {
    "LogicalType::BOOLEAN": "bool",
    "LogicalType::TINYINT": "int8_t",
    "LogicalType::SMALLINT": "int16_t",
    "LogicalType::INTEGER": "int32_t",
    "LogicalType::BIGINT": "int64_t",
    "LogicalType::FLOAT": "float",
    "LogicalType::DOUBLE": "double",
    "LogicalType::UTINYINT": "uint8_t",
    "LogicalType::USMALLINT": "uint16_t",
    "LogicalType::UINTEGER": "uint32_t",
    "LogicalType::UBIGINT": "uint64_t",
    "LogicalType::HUGEINT": "hugeint_t",
    "LogicalType::UHUGEINT": "uhugeint_t",
    "LogicalType::UUID": "DSUUIDInput",
    "LogicalType::VARCHAR": "string_t",
    "LogicalType::BLOB": "string_t",
}

NUMERIC_TYPES = (
    "LogicalType::TINYINT",
    "LogicalType::SMALLINT",
    "LogicalType::INTEGER",
    "LogicalType::BIGINT",
    "LogicalType::FLOAT",
    "LogicalType::DOUBLE",
    "LogicalType::UTINYINT",
    "LogicalType::USMALLINT",
    "LogicalType::UINTEGER",
    "LogicalType::UBIGINT",
)

# Every sketch's K argument.
K_LOGICAL_TYPE = "LogicalType::INTEGER"


def to_type_id(logical_type: str) -> str:
    return logical_type.replace("LogicalType", "LogicalTypeId")


def type_name(logical_type: str) -> str:
    """The type's SQL name, e.g. DOUBLE for LogicalType::DOUBLE."""
    return logical_type.split("::")[1]


# ---------------------------------------------------------------------------
# Sketches
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Option:
    """An optional constant third argument of a sketch's aggregates, kept in its bind data."""

    name: str
    logical_type: str
    member: str
    """The bind data member's C++ declaration, with its default."""
    member_comment: str
    parse: str
    """C++ converting the constant's Value `option` to the member's type."""
    example: str
    create_note: str
    """What the option does, appended to the create aggregate's description."""
    union_note: str = ""
    """What the option does for the union aggregate; empty if the union does not take it."""


@dataclass(frozen=True)
class Sketch:
    name: str
    """The C++ identifier stem (DSKLLState, LoadKLLSketch, ...); lowercased, the SQL name."""
    cpp_class: str
    """The datasketches class; templated on the item type unless the sketch is a counting sketch."""
    item_types: tuple
    """The logical types of the values the sketch aggregates, in registration order."""
    example_k: int
    k_min: int
    k_max: int
    """The K values the library accepts (an invalid K would fail at runtime or be silently changed)."""
    k_power_of_two: bool = False
    k_label: str = "K"
    """How K is described in errors."""
    option: Optional[Option] = None
    counting: bool = False
    """Counting sketches have one sketch type for every input type; the others have one per item type."""
    union_accepts_values: bool = False
    """The library union also accepts raw values, so the create aggregate builds the union directly."""

    @property
    def sql(self) -> str:
        return self.name.lower()

    @property
    def union_class(self) -> str:
        return f"datasketches::{self.sql}_union"

    @property
    def state_class(self) -> str:
        """The class of the sketches the aggregates build, for the item type T."""
        return self.cpp_class if self.counting else f"{self.cpp_class}<DSItem<T>>"

    @property
    def constructor_args(self) -> str:
        """The state_class constructor's arguments, from the bind data `bind`."""
        return "bind.k, bind.hra" if self.option and self.option.name == "hra" else "bind.k"

    @property
    def overload_types(self) -> tuple:
        """The item types the scalar functions are registered for; a counting sketch's take no item type."""
        return (None,) if self.counting else self.item_types

    @property
    def k_check(self) -> str:
        """A C++ condition that is true for an invalid K."""
        return f"k < {self.k_min} || k > {self.k_max}" + (" || (k & (k - 1)) != 0" if self.k_power_of_two else "")

    @property
    def k_error(self) -> str:
        power_of_two = "a power of 2 " if self.k_power_of_two else ""
        return f"{self.name} {self.k_label} value must be {power_of_two}between {self.k_min} and {self.k_max}, got: "


HLL_TARGET_TYPE = Option(
    "target_type",
    "LogicalType::VARCHAR",
    "datasketches::target_hll_type target_type = datasketches::HLL_4;",
    "The register encoding of the sketches the aggregate returns.",
    "DSParseHLLTargetType(option.GetValue<string>())",
    "'HLL_8'",
    "target_type (a constant) selects the register encoding: 'HLL_4' (the default, smallest), 'HLL_6' or 'HLL_8' (largest, fastest)",
    "target_type (a constant) selects the result's register encoding: 'HLL_4' (the default), 'HLL_6' or 'HLL_8'",
)

REQ_HRA = Option(
    "hra",
    "LogicalType::BOOLEAN",
    "bool hra = true;",
    "true: high-rank accuracy (the library default), false: low-rank accuracy.",
    "option.GetValue<bool>()",
    "false",
    "hra (a constant) selects high-rank accuracy (true, the default) or low-rank accuracy (false)",
)

COUNTING_TYPES = NUMERIC_TYPES + (
    "LogicalType::HUGEINT",
    "LogicalType::UHUGEINT",
    "LogicalType::UUID",
    "LogicalType::VARCHAR",
    "LogicalType::BLOB",
)


SKETCHES = [
    Sketch("Quantiles", "datasketches::quantiles_sketch", NUMERIC_TYPES + ("LogicalType::VARCHAR",),
           example_k=16, k_min=2, k_max=32768, k_power_of_two=True),
    Sketch("KLL", "datasketches::kll_sketch", NUMERIC_TYPES + ("LogicalType::VARCHAR",),
           example_k=200, k_min=8, k_max=65535),
    # The library rounds an odd K down. (It kept K in 8 bits, so a larger K wrapped around: see
    # vcpkg-overlay/apache-datasketches/req-k-16-bit.patch.)
    Sketch("REQ", "datasketches::req_sketch", NUMERIC_TYPES,
           example_k=12, k_min=4, k_max=1024, option=REQ_HRA),
    # The library's K is a uint16_t.
    Sketch("TDigest", "datasketches::tdigest", ("LogicalType::FLOAT", "LogicalType::DOUBLE"),
           example_k=100, k_min=10, k_max=65535, k_label="K (compression)"),
    Sketch("HLL", "datasketches::hll_sketch", COUNTING_TYPES,
           example_k=12, k_min=4, k_max=21, k_label="K (lg_k)", option=HLL_TARGET_TYPE,
           counting=True, union_accepts_values=True),
    Sketch("CPC", "datasketches::cpc_sketch", COUNTING_TYPES,
           example_k=12, k_min=4, k_max=26, k_label="K (lg_k)", counting=True),
]

QUANTILE_SKETCHES = frozenset({"Quantiles", "KLL", "REQ", "TDigest"})
COUNTING_SKETCHES = frozenset({"HLL", "CPC"})
ALL_SKETCHES = QUANTILE_SKETCHES | COUNTING_SKETCHES
# The quantile sketches built on the library's common quantiles API (TDigest has its own).
CLASSIC_QUANTILE_SKETCHES = frozenset({"Quantiles", "KLL", "REQ"})


# ---------------------------------------------------------------------------
# Scalar function arguments and results
# ---------------------------------------------------------------------------
# A scalar function is registered once per item type of a quantile sketch (`item_type` below is that overload's
# logical type, and `T` its C++ type), and once for a counting sketch (`item_type` is None).


@dataclass(frozen=True)
class Arg:
    name: str
    """In the function body, the argument's value is `<name>_data` and its vector `<name>_vector`."""
    cpp: str
    """The C++ type the executor passes to the function body."""
    logical: Callable[[Optional[str]], str]
    """The argument's DuckDB type, given the overload's item type."""
    pre_executor: str = ""
    """C++ that runs once per chunk, before the executor."""
    process: str = ""
    """C++ that runs once per row, before the function body."""
    is_sketch: bool = False


def fixed_arg(name: str, logical_type: str) -> Arg:
    return Arg(name, CPP_TYPE[logical_type], lambda item_type: logical_type)


def item_arg(name: str) -> Arg:
    """An argument of the sketch's item type."""
    return Arg(name, "T", lambda item_type: item_type)


def sketch_arg(sketch: Sketch, name: str = "sketch") -> Arg:
    """A serialized sketch; the function body sees it deserialized as `<name>`, reused while rows repeat it."""
    if sketch.counting:
        sketch_class = sketch.cpp_class
        logical = lambda item_type: "sketch_type"
    else:
        sketch_class = f"{sketch.cpp_class}<DSItem<T>>"
        logical = lambda item_type: f"sketch_map_types[{to_type_id(item_type)}]"
    return Arg(
        name,
        "string_t",
        logical,
        pre_executor=f"DSSketchCache<{sketch_class}> {name}_cache;",
        process=f'const auto &{name} = {name}_cache.Get({name}_data, "{sketch.name}");',
        is_sketch=True,
    )


def list_arg(name: str, child_logical: Callable[[Optional[str]], str], child_cpp: str) -> Arg:
    """A LIST argument; the function body sees the row's list as the DSListView `<name>`."""
    return Arg(
        name,
        "list_entry_t",
        lambda item_type: f"LogicalType::LIST({child_logical(item_type)})",
        pre_executor=textwrap.dedent(f"""\
            UnifiedVectorFormat {name}_children_unified;
            ListVector::GetChild({name}_vector).ToUnifiedFormat({name}_children_unified);
            auto {name}_children_data = UnifiedVectorFormat::GetData<{child_cpp}>({name}_children_unified);"""),
        process=f"DSListView<{child_cpp}> {name} {{{name}_data, {name}_children_unified, {name}_children_data}};",
    )


# The split points of a CDF or PMF.
SPLIT_POINTS_ARG = list_arg("split_points", lambda item_type: item_type, "T")


@dataclass(frozen=True)
class Returns:
    cpp: str
    logical: Callable[[Optional[str]], str]
    """The result's DuckDB type, given the overload's item type."""


def returns(logical_type: str) -> Returns:
    return Returns(CPP_TYPE[logical_type], lambda item_type: logical_type)


RETURNS_ITEM = Returns("T", lambda item_type: item_type)
RETURNS_SKETCH = Returns("string_t", lambda item_type: "sketch_type")
RETURNS_DOUBLE_LIST = Returns("list_entry_t", lambda item_type: "LogicalType::LIST(LogicalType::DOUBLE)")
RETURNS_ITEM_LIST = Returns("list_entry_t", lambda item_type: f"LogicalType::LIST({item_type})")
RETURNS_SORTED_VIEW = Returns(
    "list_entry_t",
    lambda item_type: (
        "LogicalType::LIST(LogicalType::STRUCT(child_list_t<LogicalType>{"
        f'{{"item", {item_type}}}, '
        '{"weight", LogicalType::UBIGINT}, '
        '{"cumulative_weight", LogicalType::UBIGINT}}))'
    ),
)


# ---------------------------------------------------------------------------
# Scalar functions
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Function:
    name: str
    """The SQL name is datasketch_<sketch>_<name>."""
    sketches: frozenset
    """The sketches that have this function."""
    description: str
    args: tuple
    returns: Returns
    body: str
    """C++ computing the result; `sketch` is the deserialized sketch and `result` the result vector."""
    example_args: str = ""
    """The example call's arguments after its sketches."""
    example: str = ""
    """A runnable query using the function, filled in by scalar_functions()."""
    sql_name: str = ""
    """datasketch_<sketch>_<name>, filled in by scalar_functions()."""


def _distribution_body(sketch: Sketch, kind: str, inclusive: bool) -> str:
    """The body of a CDF or PMF: the probabilities at the split points, as a LIST(DOUBLE)."""
    extra = ", inclusive_data" if inclusive else ""
    items = f'DSItems(split_points, "datasketch_{sketch.sql}_{kind}")'
    return f"return DSAppendDoubles(result, sketch.get_{kind.upper()}({items}.data(), split_points.size(){extra}));"


def _all_functions(sketch: Sketch) -> list:
    """Every scalar function, as `sketch` would have it, in registration order."""
    s = sketch.sql
    sk = sketch_arg(sketch)
    inclusive = fixed_arg("inclusive", "LogicalType::BOOLEAN")
    num_std_devs = fixed_arg("num_std_devs", "LogicalType::INTEGER")
    tdigest = frozenset({"TDigest"})
    hll = frozenset({"HLL"})
    cpc = frozenset({"CPC"})
    req = frozenset({"REQ"})

    return [
        Function("is_empty", ALL_SKETCHES, "Return a boolean indicating if the sketch is empty",
                 (sk,), returns("LogicalType::BOOLEAN"), "return sketch.is_empty();"),

        # --- Quantile sketches ---
        Function("k", QUANTILE_SKETCHES, "Return the value of K for this sketch",
                 (sk,), returns("LogicalType::USMALLINT"), "return sketch.get_k();"),
        Function("cdf", CLASSIC_QUANTILE_SKETCHES,
                 "Return the Cumulative Distribution Function (CDF) of the sketch for a series of points",
                 (sk, SPLIT_POINTS_ARG, inclusive), RETURNS_DOUBLE_LIST, _distribution_body(sketch, "cdf", True),
                 "[25.0, 50.0, 75.0], true"),
        Function("cdf", tdigest,
                 "Return the Cumulative Distribution Function (CDF) of the sketch for a series of points",
                 (sk, SPLIT_POINTS_ARG), RETURNS_DOUBLE_LIST, _distribution_body(sketch, "cdf", False),
                 "[25.0, 50.0, 75.0]"),
        Function("pmf", CLASSIC_QUANTILE_SKETCHES,
                 "Return the Probability Mass Function (PMF) of the sketch for a series of points",
                 (sk, SPLIT_POINTS_ARG, inclusive), RETURNS_DOUBLE_LIST, _distribution_body(sketch, "pmf", True),
                 "[25.0, 50.0, 75.0], true"),
        Function("pmf", tdigest,
                 "Return the Probability Mass Function (PMF) of the sketch for a series of points",
                 (sk, SPLIT_POINTS_ARG), RETURNS_DOUBLE_LIST, _distribution_body(sketch, "pmf", False),
                 "[25.0, 50.0, 75.0]"),

        # --- HLL ---
        Function("describe", hll, "Return a string representation of the sketch",
                 (sk, fixed_arg("summary", "LogicalType::BOOLEAN"), fixed_arg("detail", "LogicalType::BOOLEAN")),
                 returns("LogicalType::VARCHAR"),
                 "return StringVector::AddString(result, sketch.to_string(summary_data, detail_data, false, false));",
                 "true, false"),
        Function("lg_config_k", hll, "Return the value of log base 2 K for this sketch",
                 (sk,), returns("LogicalType::UTINYINT"), "return sketch.get_lg_config_k();"),
        Function("is_compact", hll, "Return whether the sketch is in compact form",
                 (sk,), returns("LogicalType::BOOLEAN"), "return sketch.is_compact();"),
        Function("composite_estimate", hll,
                 "Return the composite estimate of the number of distinct items, computed from the registers alone (the raw HLL estimate blended with linear counting). datasketch_hll_estimate prefers the HIP estimator, which is only available for sketches that were not built by a union",
                 (sk,), returns("LogicalType::DOUBLE"), "return sketch.get_composite_estimate();"),
        Function("target_type", hll, "Return the sketch's register encoding: 'HLL_4', 'HLL_6' or 'HLL_8'",
                 (sk,), returns("LogicalType::VARCHAR"),
                 "return StringVector::AddString(result, DSHLLTargetTypeName(sketch.get_target_type()));"),
        Function("merge", hll,
                 "Return the union of two HLL sketches as a new sketch with the first sketch's target type. The union runs at the larger lg_config_k of the two; if either input is in HLL (dense) mode the result is reduced to the smaller lg_config_k",
                 (sketch_arg(sketch, "sketch_a"), sketch_arg(sketch, "sketch_b")), RETURNS_SKETCH,
                 "return DSHLLMerge(result, sketch_a, sketch_b);"),

        # --- CPC ---
        Function("merge", cpc,
                 "Return the union of two CPC sketches as a new sketch. The result uses the smaller lg_k of the two inputs",
                 (sketch_arg(sketch, "sketch_a"), sketch_arg(sketch, "sketch_b")), RETURNS_SKETCH,
                 "return DSCPCMerge(result, sketch_a, sketch_b);"),
        Function("describe", cpc, "Return a string representation of the sketch",
                 (sk,), returns("LogicalType::VARCHAR"), "return StringVector::AddString(result, sketch.to_string());"),

        # --- Counting sketches ---
        Function("estimate", COUNTING_SKETCHES, "Return the estimate of the number of distinct items seen by the sketch",
                 (sk,), returns("LogicalType::DOUBLE"), "return sketch.get_estimate();"),
        Function("lower_bound", COUNTING_SKETCHES,
                 "Return the lower bound of the number of distinct items seen by the sketch",
                 (sk, num_std_devs), returns("LogicalType::DOUBLE"), f'return sketch.get_lower_bound(DSCheckNumStdDevs(num_std_devs_data, "datasketch_{s}_lower_bound"));', "2"),
        Function("upper_bound", COUNTING_SKETCHES,
                 "Return the upper bound of the number of distinct items seen by the sketch",
                 (sk, num_std_devs), returns("LogicalType::DOUBLE"), f'return sketch.get_upper_bound(DSCheckNumStdDevs(num_std_devs_data, "datasketch_{s}_upper_bound"));', "2"),

        # --- TDigest ---
        Function("describe", tdigest, "Return a description of this sketch",
                 (sk, fixed_arg("include_centroids", "LogicalType::BOOLEAN")), returns("LogicalType::VARCHAR"),
                 "return StringVector::AddString(result, sketch.to_string(include_centroids_data));", "false"),
        Function("rank", tdigest, "Return the rank of an item in the sketch",
                 (sk, item_arg("item")), returns("LogicalType::DOUBLE"), "return sketch.get_rank(DSItemIn(item_data));",
                 "50.0"),
        Function("total_weight", tdigest, "Return the total weight of this sketch",
                 (sk,), returns("LogicalType::UBIGINT"), "return sketch.get_total_weight();"),
        Function("quantile", tdigest, "Return the quantile of a rank in the sketch",
                 (sk, fixed_arg("rank", "LogicalType::DOUBLE")), RETURNS_ITEM,
                 "DSCheckRank(rank_data); return DSItemOut<T>(result, sketch.get_quantile(rank_data));", "0.5"),

        # --- Classic quantile sketches ---
        Function("normalized_rank_error", frozenset({"Quantiles", "KLL"}), "Return the normalized rank error of the sketch",
                 (sk, fixed_arg("is_pmf", "LogicalType::BOOLEAN")), returns("LogicalType::DOUBLE"),
                 "return sketch.get_normalized_rank_error(is_pmf_data);", "false"),
        Function("describe", CLASSIC_QUANTILE_SKETCHES, "Return a description of this sketch",
                 (sk, fixed_arg("include_levels", "LogicalType::BOOLEAN"), fixed_arg("include_items", "LogicalType::BOOLEAN")),
                 returns("LogicalType::VARCHAR"),
                 "return StringVector::AddString(result, sketch.to_string(include_levels_data, include_items_data));",
                 "false, false"),
        Function("rank", CLASSIC_QUANTILE_SKETCHES, "Return the rank of an item in the sketch",
                 (sk, item_arg("item"), inclusive), returns("LogicalType::DOUBLE"),
                 "return sketch.get_rank(DSItemIn(item_data), inclusive_data);", "50.0, true"),
        Function("quantile", CLASSIC_QUANTILE_SKETCHES, "Return the quantile of a rank in the sketch",
                 (sk, fixed_arg("rank", "LogicalType::DOUBLE"), inclusive), RETURNS_ITEM,
                 "DSCheckRank(rank_data); return DSItemOut<T>(result, sketch.get_quantile(rank_data, inclusive_data));", "0.5, true"),
        Function("n", CLASSIC_QUANTILE_SKETCHES, "Return the number of items contained in the sketch",
                 (sk,), returns("LogicalType::UBIGINT"), "return sketch.get_n();"),
        Function("is_estimation_mode", CLASSIC_QUANTILE_SKETCHES, "Return a boolean indicating if the sketch is in estimation mode",
                 (sk,), returns("LogicalType::BOOLEAN"), "return sketch.is_estimation_mode();"),
        Function("num_retained", CLASSIC_QUANTILE_SKETCHES, "Return the number of retained items in the sketch",
                 (sk,), returns("LogicalType::UBIGINT"), "return sketch.get_num_retained();"),
        Function("min_item", CLASSIC_QUANTILE_SKETCHES, "Return the minimum item in the sketch",
                 (sk,), RETURNS_ITEM, "return DSItemOut<T>(result, sketch.get_min_item());"),
        Function("max_item", CLASSIC_QUANTILE_SKETCHES, "Return the maximum item in the sketch",
                 (sk,), RETURNS_ITEM, "return DSItemOut<T>(result, sketch.get_max_item());"),

        # --- REQ ---
        Function("rank_lower_bound", req,
                 "Return an approximate lower bound of the given normalized rank, with a confidence of num_std_dev standard deviations (1, 2 or 3)",
                 (sk, fixed_arg("rank", "LogicalType::DOUBLE"), fixed_arg("num_std_dev", "LogicalType::UTINYINT")),
                 returns("LogicalType::DOUBLE"),
                 "DSCheckRank(rank_data); return sketch.get_rank_lower_bound(rank_data, num_std_dev_data);", "0.5, 2"),
        Function("rank_upper_bound", req,
                 "Return an approximate upper bound of the given normalized rank, with a confidence of num_std_dev standard deviations (1, 2 or 3)",
                 (sk, fixed_arg("rank", "LogicalType::DOUBLE"), fixed_arg("num_std_dev", "LogicalType::UTINYINT")),
                 returns("LogicalType::DOUBLE"),
                 "DSCheckRank(rank_data); return sketch.get_rank_upper_bound(rank_data, num_std_dev_data);", "0.5, 2"),
        Function("is_hra", req,
                 "Return true if the sketch favors accuracy at high ranks (HRA), false if it favors low ranks (LRA)",
                 (sk,), returns("LogicalType::BOOLEAN"), "return sketch.is_HRA();"),

        # --- TDigest ---
        Function("min_value", tdigest, "Return the minimum value seen by the sketch",
                 (sk,), RETURNS_ITEM, "return sketch.get_min_value();"),
        Function("max_value", tdigest, "Return the maximum value seen by the sketch",
                 (sk,), RETURNS_ITEM, "return sketch.get_max_value();"),

        # --- Classic quantile sketches: list results ---
        Function("quantiles", CLASSIC_QUANTILE_SKETCHES,
                 "Return the quantiles (items) at a list of normalized ranks in [0, 1]. A NULL rank yields a NULL quantile",
                 (sk, list_arg("ranks", lambda item_type: "LogicalType::DOUBLE", "double"), inclusive), RETURNS_ITEM_LIST,
                 f'return DSQuantiles<T>(result, sketch, ranks, inclusive_data, "datasketch_{s}_quantiles");',
                 "[0.25, 0.5, 0.75], true"),
        Function("ranks", CLASSIC_QUANTILE_SKETCHES,
                 "Return the normalized ranks of a list of items. A NULL item yields a NULL rank",
                 (sk, list_arg("items", lambda item_type: item_type, "T"), inclusive),
                 RETURNS_DOUBLE_LIST, "return DSRanks<T>(result, sketch, items, inclusive_data);",
                 "[10.0, 20.0, 30.0], true"),
        Function("sorted_view", CLASSIC_QUANTILE_SKETCHES,
                 "Return the sketch's sorted view: one entry per retained item in ascending order (equal items can appear more than once), with the item's weight and the inclusive cumulative weight (the number of input items less than or equal to it; estimated once the sketch is in estimation mode). Divide cumulative_weight by datasketch_"
                 + s
                 + "_n(sketch) for the normalized rank. An empty sketch yields an empty list",
                 (sk,), RETURNS_SORTED_VIEW, "return DSSortedView<T>(result, sketch);"),
    ]


def scalar_functions(sketch: Sketch) -> list:
    """The sketch's scalar functions, in registration order, with their examples."""
    result = []
    for function in _all_functions(sketch):
        if sketch.name not in function.sketches:
            continue
        call_args = [arg.name for arg in function.args if arg.is_sketch]
        if function.example_args:
            call_args.append(function.example_args)
        sql_name = f"datasketch_{sketch.sql}_{function.name}"
        call = f"{sql_name}({', '.join(call_args)})"
        result.append(replace(function, example=example_query(sketch, call), sql_name=sql_name))
    return result


# ---------------------------------------------------------------------------
# Function description examples
# ---------------------------------------------------------------------------
# Every example is a complete, runnable query: it builds its sketches from inline data (range()) instead of
# referring to columns of a table the reader does not have. test/sql/datasketch_generated_examples.test, which
# this script also writes, runs each of them.


def example_value(sketch: Sketch) -> str:
    """The value aggregated into example sketches: integers for counting sketches, DOUBLE otherwise."""
    return "i" if sketch.counting else "i::DOUBLE"


def example_create(sketch: Sketch, extra_args: str = "") -> str:
    return f"datasketch_{sketch.sql}({sketch.example_k}, {example_value(sketch)}{extra_args})"


def example_sketch_source(sketch: Sketch, alias: str = "sketch", rows: str = "range(100)", group_by: str = "") -> str:
    return f"(SELECT {example_create(sketch)} AS {alias} FROM {rows} t(i){group_by})"


def example_query(sketch: Sketch, call: str) -> str:
    """Wrap a scalar call on `sketch` (or `sketch_a` and `sketch_b`) in a query that builds the sketches."""
    if "sketch_a" in call:
        return (
            f"SELECT {call} FROM {example_sketch_source(sketch, 'sketch_a')} a, "
            f"{example_sketch_source(sketch, 'sketch_b', 'range(50, 150)')} b"
        )
    return f"SELECT {call} FROM {example_sketch_source(sketch)} s"


def aggregate_examples(sketch: Sketch) -> dict:
    name = f"datasketch_{sketch.sql}"
    k = sketch.example_k
    partial_sketches = example_sketch_source(sketch, group_by=" GROUP BY i % 4")
    option = f", {sketch.option.example}" if sketch.option else ""
    return {
        "create": f"SELECT {example_create(sketch)} FROM range(100) t(i)",
        "create_option": f"SELECT {example_create(sketch, option)} FROM range(100) t(i)",
        "merge": f"SELECT {name}({k}, sketch) FROM {partial_sketches} s",
        "union": f"SELECT {name}_union({k}, sketch) FROM {partial_sketches} s",
        "union_option": f"SELECT {name}_union({k}, sketch{option}) FROM {partial_sketches} s",
    }


def all_examples(sketch: Sketch) -> list:
    """Every example the template registers for a sketch, in registration order."""
    examples = [function.example for function in scalar_functions(sketch)]
    aggregates = aggregate_examples(sketch)
    examples.append(aggregates["create"])
    if sketch.option:
        examples.append(aggregates["create_option"])
    if sketch.counting:
        examples.append(aggregates["union"])
        if sketch.option and sketch.option.union_note:
            examples.append(aggregates["union_option"])
    else:
        examples.append(aggregates["merge"])
    return examples


def sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def render_examples_test() -> str:
    examples = []
    for sketch in SKETCHES:
        for example in all_examples(sketch):
            if example not in examples:
                examples.append(example)
    family = "|".join(sketch.sql for sketch in SKETCHES)
    lines = [
        "# name: test/sql/datasketch_generated_examples.test",
        "# description: every example in the generated function descriptions runs",
        "# group: [datasketches]",
        "",
        "# Generated by codegen/generator.py. Do not edit; regenerate instead.",
        "",
        "require datasketches",
        "",
        "statement ok",
        "CREATE TABLE generated_examples(example VARCHAR);",
        "",
        "statement ok",
        "INSERT INTO generated_examples VALUES",
        ",\n".join(f"({sql_string(e)})" for e in examples) + ";",
        "",
        "# Every registered example of the generated sketch functions is one of the examples run below",
        "query II",
        "SELECT count(*), count(*) FILTER (WHERE example NOT IN (SELECT example FROM generated_examples))",
        "FROM (SELECT DISTINCT unnest(examples) AS example FROM duckdb_functions()",
        f"      WHERE regexp_matches(function_name, '^datasketch_({family})(_|$)'));",
        "----",
        f"{len(examples)}\t0",
        "",
    ]
    for example in examples:
        lines += ["statement ok", example + ";", ""]
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Rendering helpers used by the template
# ---------------------------------------------------------------------------


EXECUTORS = {1: "UnaryExecutor", 2: "BinaryExecutor", 3: "TernaryExecutor"}


def parameter_names(function: Function) -> str:
    return ", ".join(f'"{arg.name}"' for arg in function.args)


def parameter_types(function: Function, item_type: Optional[str]) -> str:
    return ", ".join(arg.logical(item_type) for arg in function.args)


def function_block(function: Function) -> str:
    """The C++ executing `function` over the argument vectors: one lambda call per row, guarded so that library
    errors are reported as invalid input."""
    if len(function.args) not in EXECUTORS:
        raise NotImplementedError(f"Unhandled number of arguments {len(function.args)}")
    cpp_types = ", ".join([arg.cpp for arg in function.args] + [function.returns.cpp])
    vectors = ", ".join(f"{arg.name}_vector" for arg in function.args)
    params = ", ".join(f"{arg.cpp} {arg.name}_data" for arg in function.args)
    pre_executor = [arg.pre_executor for arg in function.args if arg.pre_executor]
    body = [arg.process for arg in function.args if arg.process] + [function.body]
    lines = pre_executor + [
        f"{EXECUTORS[len(function.args)]}::Execute<{cpp_types}>({vectors}, result, [&]({params}) {{",
        f'    return DSGuard("{function.sql_name}", [&]() {{',
        textwrap.indent("\n".join(body), "        "),
        "    });",
        "});",
    ]
    return textwrap.indent("\n".join(lines), "    ")


def cpp_string(value: str) -> str:
    return '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'


def main() -> None:
    env = Environment(
        loader=FileSystemLoader(CODEGEN_DIR),
        autoescape=False,
        undefined=StrictUndefined,
        trim_blocks=True,
        lstrip_blocks=True,
        keep_trailing_newline=True,
    )
    env.filters["cpp_string"] = cpp_string
    template = env.get_template("generated.cpp.j2")

    data = {
        "sketches": SKETCHES,
        "scalar_functions": scalar_functions,
        "aggregate_examples": aggregate_examples,
        "function_block": function_block,
        "parameter_names": parameter_names,
        "parameter_types": parameter_types,
        "cpp_type": CPP_TYPE,
        "to_type_id": to_type_id,
        "type_name": type_name,
        "k_logical_type": K_LOGICAL_TYPE,
    }

    (ROOT / "src" / "generated.cpp").write_text(template.render(data))
    (ROOT / "test" / "sql" / "datasketch_generated_examples.test").write_text(render_examples_test())
    print("Generated src/generated.cpp and test/sql/datasketch_generated_examples.test")


if __name__ == "__main__":
    main()
