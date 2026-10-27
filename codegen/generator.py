"""Generates src/generated.cpp and test/sql/datasketch_generated_examples.test.

The generated sketches are described in SKETCHES and their scalar functions in scalar_functions();
codegen/generated.cpp.j2 renders them. Run it from anywhere: `python codegen/generator.py`.
"""

from __future__ import annotations

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


# ---------------------------------------------------------------------------
# Sketches
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Sketch:
    name: str
    """The C++ identifier stem (DSKLLState, LoadKLLSketch, ...); lowercased, the SQL name."""
    cpp_class: str
    """The datasketches class; templated on the item type unless the sketch is a counting sketch."""
    item_types: tuple
    """The logical types of the values the sketch aggregates, in registration order."""
    example_k: int
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


SKETCHES = [
    Sketch("Quantiles", "datasketches::quantiles_sketch", NUMERIC_TYPES + ("LogicalType::VARCHAR",), example_k=16),
    Sketch("KLL", "datasketches::kll_sketch", NUMERIC_TYPES + ("LogicalType::VARCHAR",), example_k=200),
    Sketch("REQ", "datasketches::req_sketch", NUMERIC_TYPES, example_k=12),
    Sketch("TDigest", "datasketches::tdigest", ("LogicalType::FLOAT", "LogicalType::DOUBLE"), example_k=100),
    Sketch(
        "HLL",
        "datasketches::hll_sketch",
        NUMERIC_TYPES
        + ("LogicalType::HUGEINT", "LogicalType::UHUGEINT", "LogicalType::UUID", "LogicalType::VARCHAR", "LogicalType::BLOB"),
        example_k=12,
        counting=True,
        union_accepts_values=True,
    ),
    Sketch(
        "CPC",
        "datasketches::cpc_sketch",
        NUMERIC_TYPES
        + ("LogicalType::HUGEINT", "LogicalType::UHUGEINT", "LogicalType::UUID", "LogicalType::VARCHAR", "LogicalType::BLOB"),
        example_k=12,
        counting=True,
    ),
]

SKETCH_BY_NAME = {sketch.name: sketch for sketch in SKETCHES}

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
    """A serialized sketch; the function body sees it deserialized as `<name>`."""
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
        process=f"""
            auto {name} = [&]() {{
                try {{
                    return {sketch_class}::deserialize({name}_data.GetDataUnsafe(), {name}_data.GetSize());
                }} catch (const std::exception &e) {{
                    throw InvalidInputException("Failed to deserialize {sketch.name} sketch: %s", e.what());
                }}
            }}();""",
        is_sketch=True,
    )


def list_arg(name: str, child_logical: Callable[[Optional[str]], str], child_cpp: str) -> Arg:
    """A LIST argument whose child values are read through a UnifiedVectorFormat.

    Inside the function body the list entry is `<name>_data`, the child values are
    `<name>_children_data` and their format is `<name>_children_unified`.
    """
    return Arg(
        name,
        "list_entry_t",
        lambda item_type: f"LogicalType::LIST({child_logical(item_type)})",
        pre_executor=f"""
                    auto &{name}_list_children = ListVector::GetChild({name}_vector);
                    UnifiedVectorFormat {name}_children_unified;
                    {name}_list_children.ToUnifiedFormat({name}_children_unified);
                    const {child_cpp} *{name}_children_data = UnifiedVectorFormat::GetData<{child_cpp}>({name}_children_unified);
                    """,
    )


# The split points of a CDF or PMF, converted to the sketch's item type as `passing_points`.
SPLIT_POINTS_ARG = Arg(
    "split_points",
    "list_entry_t",
    lambda item_type: f"LogicalType::LIST({item_type})",
    pre_executor="""
                    auto &split_points_list_children = ListVector::GetChild(split_points_vector);

                    UnifiedVectorFormat split_points_children_unified;
                    split_points_list_children.ToUnifiedFormat(split_points_children_unified);

                    const T *split_points_list_children_data = UnifiedVectorFormat::GetData<T>(split_points_children_unified);
                    """,
    process="""
                    std::vector<DSItem<T>> passing_points(split_points_data.length);
                    for (idx_t i = 0; i < split_points_data.length; i++)
                    {
                        passing_points[i] = DSItemIn(split_points_list_children_data[split_points_children_unified.sel->get_index(i + split_points_data.offset)]);
                    }
                    """,
)


@dataclass(frozen=True)
class Returns:
    cpp: str
    logical: Callable[[Optional[str]], str]
    """The result's DuckDB type, given the overload's item type."""


def returns(logical_type: str) -> Returns:
    return Returns(CPP_TYPE[logical_type], lambda item_type: logical_type)


RETURNS_ITEM = Returns("T", lambda item_type: item_type)
RETURNS_SKETCH = Returns("string_t", lambda item_type: "sketch_type")
# CDF/PMF results are probabilities, so they are always DOUBLE regardless of the sketch item type.
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


def _distribution_body(kind: str, inclusive: bool) -> str:
    """The body of a CDF or PMF: the probabilities at the split points, as a LIST(DOUBLE)."""
    call = f"auto {kind}_result = sketch.get_{kind.upper()}(passing_points.data(), split_points_data.length{', inclusive_data' if inclusive else ''});"
    return (
        call
        + f"""
                auto current_size = ListVector::GetListSize(result);
                auto new_size = current_size + {kind}_result.size();
                if (ListVector::GetListCapacity(result) < new_size)
                {{
                    ListVector::Reserve(result, new_size);
                }}

                auto &child_entry = ListVector::GetChildMutable(result);
                auto child_vals = FlatVector::GetDataMutable<double>(child_entry);
                //auto &child_validity = FlatVector::Validity(child_entry);
                for (idx_t i = 0; i < {kind}_result.size(); i++)
                {{
                    child_vals[current_size + i] = {kind}_result[i];
                }}
                ListVector::SetListSize(result, new_size);
                return list_entry_t(current_size, {kind}_result.size());
                """
    )


def _all_functions(sketch: Sketch) -> list:
    """Every scalar function, as `sketch` would have it, in registration order."""
    s = sketch.sql
    sk = sketch_arg(sketch)
    inclusive = fixed_arg("inclusive", "LogicalType::BOOLEAN")
    std_dev = fixed_arg("std_dev", "LogicalType::UTINYINT")
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
                 (sk, SPLIT_POINTS_ARG, inclusive), RETURNS_DOUBLE_LIST, _distribution_body("cdf", True),
                 "[25.0, 50.0, 75.0], true"),
        Function("cdf", tdigest,
                 "Return the Cumulative Distribution Function (CDF) of the sketch for a series of points",
                 (sk, SPLIT_POINTS_ARG), RETURNS_DOUBLE_LIST, _distribution_body("cdf", False),
                 "[25.0, 50.0, 75.0]"),
        Function("pmf", CLASSIC_QUANTILE_SKETCHES,
                 "Return the Probability Mass Function (PMF) of the sketch for a series of points",
                 (sk, SPLIT_POINTS_ARG, inclusive), RETURNS_DOUBLE_LIST, _distribution_body("pmf", True),
                 "[25.0, 50.0, 75.0], true"),
        Function("pmf", tdigest,
                 "Return the Probability Mass Function (PMF) of the sketch for a series of points",
                 (sk, SPLIT_POINTS_ARG), RETURNS_DOUBLE_LIST, _distribution_body("pmf", False),
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
                 """
                datasketches::hll_union u(std::max(sketch_a.get_lg_config_k(), sketch_b.get_lg_config_k()));
                u.update(sketch_a);
                u.update(sketch_b);
                auto serialized_data = u.get_result(sketch_a.get_target_type()).serialize_updatable();
                return StringVector::AddStringOrBlob(result, std::string(serialized_data.begin(), serialized_data.end()));
                """),

        # --- CPC ---
        Function("merge", cpc,
                 "Return the union of two CPC sketches as a new sketch. The result uses the smaller lg_k of the two inputs",
                 (sketch_arg(sketch, "sketch_a"), sketch_arg(sketch, "sketch_b")), RETURNS_SKETCH,
                 """
                datasketches::cpc_union u(std::max(sketch_a.get_lg_k(), sketch_b.get_lg_k()));
                u.update(sketch_a);
                u.update(sketch_b);
                auto serialized_data = u.get_result().serialize();
                return StringVector::AddStringOrBlob(result, std::string(serialized_data.begin(), serialized_data.end()));
                """),
        Function("describe", cpc, "Return a string representation of the sketch",
                 (sk,), returns("LogicalType::VARCHAR"), "return StringVector::AddString(result, sketch.to_string());"),

        # --- Counting sketches ---
        Function("estimate", COUNTING_SKETCHES, "Return the estimate of the number of distinct items seen by the sketch",
                 (sk,), returns("LogicalType::DOUBLE"), "return sketch.get_estimate();"),
        Function("lower_bound", COUNTING_SKETCHES,
                 "Return the lower bound of the number of distinct items seen by the sketch",
                 (sk, std_dev), returns("LogicalType::DOUBLE"), "return sketch.get_lower_bound(std_dev_data);", "2"),
        Function("upper_bound", COUNTING_SKETCHES,
                 "Return the upper bound of the number of distinct items seen by the sketch",
                 (sk, std_dev), returns("LogicalType::DOUBLE"), "return sketch.get_upper_bound(std_dev_data);", "2"),

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
                 "return DSItemOut<T>(result, sketch.get_quantile(rank_data));", "0.5"),

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
                 "return DSItemOut<T>(result, sketch.get_quantile(rank_data, inclusive_data));", "0.5, true"),
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
                 """
                if (sketch.is_empty())
                {
                    throw InvalidInputException("datasketch_%s_quantiles: operation is undefined for an empty sketch");
                }
                return DSAppendList(result, ranks_data.length, [&](Vector &child, idx_t offset) {
                    auto child_values = FlatVector::GetDataMutable<T>(child);
                    for (idx_t i = 0; i < ranks_data.length; i++)
                    {
                        auto rank_index = ranks_children_unified.sel->get_index(ranks_data.offset + i);
                        if (!ranks_children_unified.validity.RowIsValid(rank_index))
                        {
                            FlatVector::SetNull(child, offset + i, true);
                            continue;
                        }
                        auto rank = ranks_children_data[rank_index];
                        DSCheckRank(rank);
                        child_values[offset + i] = DSItemOut<T>(child, sketch.get_quantile(rank, inclusive_data));
                    }
                });
                """ % s,
                 "[0.25, 0.5, 0.75], true"),
        Function("ranks", CLASSIC_QUANTILE_SKETCHES,
                 "Return the normalized ranks of a list of items. A NULL item yields a NULL rank",
                 (sk, list_arg("items", lambda item_type: item_type, "T"), inclusive),
                 Returns("list_entry_t", lambda item_type: "LogicalType::LIST(LogicalType::DOUBLE)"),
                 """
                return DSAppendList(result, items_data.length, [&](Vector &child, idx_t offset) {
                    auto child_values = FlatVector::GetDataMutable<double>(child);
                    for (idx_t i = 0; i < items_data.length; i++)
                    {
                        auto item_index = items_children_unified.sel->get_index(items_data.offset + i);
                        if (!items_children_unified.validity.RowIsValid(item_index))
                        {
                            FlatVector::SetNull(child, offset + i, true);
                            continue;
                        }
                        child_values[offset + i] = sketch.get_rank(DSItemIn(items_children_data[item_index]), inclusive_data);
                    }
                });
                """,
                 "[10.0, 20.0, 30.0], true"),
        Function("sorted_view", CLASSIC_QUANTILE_SKETCHES,
                 "Return the sketch's sorted view: one entry per retained item in ascending order (equal items can appear more than once), with the item's weight and the inclusive cumulative weight (the number of input items less than or equal to it; estimated once the sketch is in estimation mode). Divide cumulative_weight by datasketch_"
                 + s
                 + "_n(sketch) for the normalized rank. An empty sketch yields an empty list",
                 (sk,), RETURNS_SORTED_VIEW,
                 """
                if (sketch.is_empty())
                {
                    return list_entry_t(ListVector::GetListSize(result), 0);
                }
                auto view = sketch.get_sorted_view();
                return DSAppendList(result, view.size(), [&](Vector &child, idx_t offset) {
                    auto &entries = StructVector::GetEntries(child);
                    auto &item_vector = entries[0];
                    auto item_values = FlatVector::GetDataMutable<T>(item_vector);
                    auto weight_values = FlatVector::GetDataMutable<uint64_t>(entries[1]);
                    auto cumulative_values = FlatVector::GetDataMutable<uint64_t>(entries[2]);
                    idx_t i = offset;
                    for (auto it = view.begin(); it != view.end(); ++it, ++i)
                    {
                        item_values[i] = DSItemOut<T>(item_vector, (*it).first);
                        weight_values[i] = it.get_weight();
                        cumulative_values[i] = it.get_cumulative_weight(true);
                    }
                });
                """),
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
        call = f"datasketch_{sketch.sql}_{function.name}({', '.join(call_args)})"
        result.append(replace(function, example=example_query(sketch, call)))
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
    hll_target_type_arg = ", 'HLL_8'"
    return {
        "create": f"SELECT {example_create(sketch)} FROM range(100) t(i)",
        "create_hll_target_type": f"SELECT {example_create(sketch, hll_target_type_arg)} FROM range(100) t(i)",
        "create_req_hra": f"SELECT {example_create(sketch, ', false')} FROM range(100) t(i)",
        "merge": f"SELECT {name}({k}, sketch) FROM {partial_sketches} s",
        "union": f"SELECT {name}_union({k}, sketch) FROM {partial_sketches} s",
        "union_hll_target_type": f"SELECT {name}_union({k}, sketch, 'HLL_8') FROM {partial_sketches} s",
    }


def all_examples(sketch: Sketch) -> list:
    """Every example the template registers for a sketch, in registration order."""
    examples = [function.example for function in scalar_functions(sketch)]
    aggregates = aggregate_examples(sketch)
    examples.append(aggregates["create"])
    if sketch.name == "HLL":
        examples.append(aggregates["create_hll_target_type"])
    if sketch.name == "REQ":
        examples.append(aggregates["create_req_hra"])
    if sketch.counting:
        examples.append(aggregates["union"])
        if sketch.name == "HLL":
            examples.append(aggregates["union_hll_target_type"])
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


def get_executor_name(arguments: tuple) -> str:
    executors = {1: "UnaryExecutor", 2: "BinaryExecutor", 3: "TernaryExecutor"}
    if len(arguments) not in executors:
        raise NotImplementedError(f"Unhandled number of arguments {len(arguments)}")
    return executors[len(arguments)]


def get_parameter_names(function: Function) -> str:
    return ",".join(f'"{arg.name}"' for arg in function.args)


def get_parameter_types_str(function: Function, item_type: Optional[str]) -> str:
    return ",".join(arg.logical(item_type) for arg in function.args)


def get_scalar_function_args(function: Function, item_type: Optional[str]) -> str:
    return f"{{{get_parameter_types_str(function, item_type)}}},{function.returns.logical(item_type)}"


def get_function_block(function: Function) -> str:
    joined_cpp_types = ",".join([arg.cpp for arg in function.args] + [function.returns.cpp])
    joined_executor_args = ",".join([f"{arg.name}_vector" for arg in function.args] + ["result"])
    joined_lambda_args = ",".join(f"{arg.cpp} {arg.name}_data" for arg in function.args)
    pre_executor_body = "\n".join(arg.pre_executor for arg in function.args if arg.pre_executor)
    lambda_body = "\n".join([arg.process for arg in function.args if arg.process] + [function.body])

    return f"""
        {pre_executor_body}
        {get_executor_name(function.args)}::Execute
        <{joined_cpp_types}>
        (
        {joined_executor_args},
        [&]({joined_lambda_args}) {{

            {lambda_body}
        }});"""


def main() -> None:
    env = Environment(loader=FileSystemLoader(CODEGEN_DIR), autoescape=False, undefined=StrictUndefined)
    template = env.get_template("generated.cpp.j2")

    data = {
        "sketch_class_name": lambda name: SKETCH_BY_NAME[name].cpp_class,
        "counting_sketch_names": [sketch.name for sketch in SKETCHES if sketch.counting],
        "union_class_name": lambda name: SKETCH_BY_NAME[name].union_class,
        "union_accepts_values": [sketch.name for sketch in SKETCHES if sketch.union_accepts_values],
        "sketch_types": [sketch.name for sketch in SKETCHES],
        "logical_type_to_cplusplus_type": lambda name: {t: CPP_TYPE[t] for t in SKETCH_BY_NAME[name].item_types},
        "functions_per_sketch_type": lambda name: scalar_functions(SKETCH_BY_NAME[name]),
        "aggregate_examples": lambda name: aggregate_examples(SKETCH_BY_NAME[name]),
        "get_function_block": get_function_block,
        "get_scalar_function_args": get_scalar_function_args,
        "get_parameter_names": get_parameter_names,
        "get_parameter_types_str": get_parameter_types_str,
        "to_type_id": to_type_id,
        "k_cpp_type": CPP_TYPE[K_LOGICAL_TYPE],
        "k_logical_type": K_LOGICAL_TYPE,
    }

    (ROOT / "src" / "generated.cpp").write_text(template.render(data))
    (ROOT / "test" / "sql" / "datasketch_generated_examples.test").write_text(render_examples_test())
    print("C++ file generated successfully!")


if __name__ == "__main__":
    main()
