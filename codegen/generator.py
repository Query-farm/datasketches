from jinja2 import Environment, FileSystemLoader
from typing import Any

# Set up the Jinja2 environment
env = Environment(loader=FileSystemLoader(searchpath="./codegen/"), autoescape=False)
template = env.get_template("generated.cpp.j2")


counting_sketch_names = ["CPC", "HLL"]

logical_type_mapping = {
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
    "LogicalType::VARCHAR": "string_t",
}

cpp_type_mapping = {value: key for key, value in logical_type_mapping.items()}


def sketch_type_to_allowed_logical_types(sketch_type):
    if sketch_type in counting_sketch_names:
        return {
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
            "LogicalType::VARCHAR": "string_t",
            "LogicalType::BLOB": "string_t",
        }

    if sketch_type == "TDigest":
        return {"LogicalType::FLOAT": "float", "LogicalType::DOUBLE": "double"}

    return {
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
    }


def get_sketch_class_name(sketch_type: str):
    if sketch_type == "TDigest":
        return "datasketches::tdigest"
    return f"datasketches::{sketch_type.lower()}_sketch"



def list_argument(name: str, child_logical_type, child_cpp_type: str) -> dict:
    """A LIST argument whose child values are read through a UnifiedVectorFormat.

    Inside the function body the list entry is `<name>_data`, the child values are
    `<name>_children_data` and their format is `<name>_children_unified`.
    """
    return {
        "cpp_type": "list_entry_t",
        "duckdb_type": lambda contained_type: f"LogicalType::LIST({child_logical_type(contained_type)})",
        "name": name,
        "pre_executor": f"""
                    auto &{name}_list_children = ListVector::GetChild({name}_vector);
                    UnifiedVectorFormat {name}_children_unified;
                    {name}_list_children.ToUnifiedFormat({name}_children_unified);
                    const {child_cpp_type} *{name}_children_data = UnifiedVectorFormat::GetData<{child_cpp_type}>({name}_children_unified);
                    """,
    }


def sorted_view_struct_type(item_logical_type: str) -> str:
    return (
        "LogicalType::LIST(LogicalType::STRUCT(child_list_t<LogicalType>{"
        f'{{"item", {item_logical_type}}}, '
        '{"weight", LogicalType::UBIGINT}, '
        '{"cumulative_weight", LogicalType::UBIGINT}}))'
    )


def unary_functions_per_sketch_type(sketch_type: str):
    def make_sketch_argument(name: str) -> dict:
        """A serialized sketch argument; the body sees the deserialized sketch as `<name>`."""
        if sketch_type in counting_sketch_names:
            sketch_class = get_sketch_class_name(sketch_type)
            duckdb_type = lambda contained_type: "sketch_type"
        else:
            sketch_class = f"{get_sketch_class_name(sketch_type)}<DSItem<T>>"
            duckdb_type = lambda contained_type: f"sketch_map_types[{contained_type.replace('LogicalType', 'LogicalTypeId')}]"
        return {
            "cpp_type": "string_t",
            "duckdb_type": duckdb_type,
            "name": name,
            "process": f"""
            auto {name} = [&]() {{
                try {{
                    return {sketch_class}::deserialize({name}_data.GetDataUnsafe(), {name}_data.GetSize());
                }} catch (const std::exception &e) {{
                    throw InvalidInputException("Failed to deserialize {sketch_type} sketch: %s", e.what());
                }}
            }}();""",
        }

    sketch_argument = make_sketch_argument("sketch")

    cdf_points_argument = {
        "cpp_type": "list_entry_t",
        "duckdb_type": lambda contained_type: f"LogicalType::LIST({contained_type})",
        "name": "split_points",
        "pre_executor": """
                    auto &split_points_list_children = ListVector::GetChild(split_points_vector);

                    UnifiedVectorFormat split_points_children_unified;
                    split_points_list_children.ToUnifiedFormat(split_points_children_unified);

                    const T *split_points_list_children_data = UnifiedVectorFormat::GetData<T>(split_points_children_unified);
                    """,
        "process": """
                    std::vector<DSItem<T>> passing_points(split_points_data.length);
                    for (idx_t i = 0; i < split_points_data.length; i++)
                    {
                        passing_points[i] = DSItemIn(split_points_list_children_data[split_points_children_unified.sel->get_index(i + split_points_data.offset)]);
                    }
                    """,
    }
    pmf_points_argument = cdf_points_argument

    result = [
        {
            "method": "return sketch.is_empty();",
            "name": "is_empty",
            "description": "Return a boolean indicating if the sketch is empty",
            "example": f"datasketch_{sketch_type.lower()}_is_empty(sketch)",
            "arguments": [
                sketch_argument,
            ],
            "return_type": "LogicalType::BOOLEAN",
        },
    ]

    if sketch_type not in counting_sketch_names:
        result.extend(
            [
                {
                    "method": "return sketch.get_k();",
                    "arguments": [sketch_argument],
                    "name": "k",
                    "description": "Return the value of K for this sketch",
                    "example": f"datasketch_{sketch_type.lower()}_k(sketch)",
                    "return_type": "LogicalType::USMALLINT",
                },
                {
                    "name": "cdf",
                    "description": "Return the Cumulative Distribution Function (CDF) of the sketch for a series of points",
                    "example": f"datasketch_{sketch_type.lower()}_cdf(sketch, points, inclusive)"
                    if sketch_type != "TDigest"
                    else f"datasketch_{sketch_type.lower()}_cdf(sketch, points)",
                    "method": (
                        "auto cdf_result = sketch.get_CDF(passing_points.data(), split_points_data.length, inclusive_data);"
                        if sketch_type != "TDigest"
                        else "auto cdf_result = sketch.get_CDF(passing_points.data(), split_points_data.length);"
                    )
                    + """
                auto current_size = ListVector::GetListSize(result);
                auto new_size = current_size + cdf_result.size();
                if (ListVector::GetListCapacity(result) < new_size)
                {
                    ListVector::Reserve(result, new_size);
                }

                auto &child_entry = ListVector::GetChildMutable(result);
                auto child_vals = FlatVector::GetDataMutable<double>(child_entry);
                //auto &child_validity = FlatVector::Validity(child_entry);
                for (idx_t i = 0; i < cdf_result.size(); i++)
                {
                    child_vals[current_size + i] = cdf_result[i];
                }
                ListVector::SetListSize(result, new_size);
                return list_entry_t(current_size, cdf_result.size());
                """,
                    "arguments": [
                        sketch_argument,
                        cdf_points_argument,
                        {
                            "cpp_type": "bool",
                            "name": "inclusive",
                        },
                    ]
                    if sketch_type != "TDigest"
                    else [sketch_argument, cdf_points_argument],
                    "return_type_double_list": True,
                },
                {
                    "name": "pmf",
                    "description": "Return the Probability Mass Function (PMF) of the sketch for a series of points",
                    "example": f"datasketch_{sketch_type.lower()}_pmf(sketch, points, inclusive)"
                    if sketch_type != "TDigest"
                    else f"datasketch_{sketch_type.lower()}_pmf(sketch, points)",
                    "method": (
                        "auto pmf_result = sketch.get_PMF(passing_points.data(), split_points_data.length, inclusive_data);"
                        if sketch_type != "TDigest"
                        else "auto pmf_result = sketch.get_PMF(passing_points.data(), split_points_data.length);"
                    )
                    + """
                auto current_size = ListVector::GetListSize(result);
                auto new_size = current_size + pmf_result.size();
                if (ListVector::GetListCapacity(result) < new_size)
                {
                    ListVector::Reserve(result, new_size);
                }

                auto &child_entry = ListVector::GetChildMutable(result);
                auto child_vals = FlatVector::GetDataMutable<double>(child_entry);
                //auto &child_validity = FlatVector::Validity(child_entry);
                for (idx_t i = 0; i < pmf_result.size(); i++)
                {
                    child_vals[current_size + i] = pmf_result[i];
                }
                ListVector::SetListSize(result, new_size);
                return list_entry_t(current_size, pmf_result.size());
                """,
                    "arguments": [
                        sketch_argument,
                        pmf_points_argument,
                        {
                            "cpp_type": "bool",
                            "name": "inclusive",
                        },
                    ]
                    if sketch_type != "TDigest"
                    else [sketch_argument, pmf_points_argument],
                    "return_type_double_list": True,
                },
            ]
        )

    if sketch_type == "HLL":
        result.extend(
            [
                {
                    "method": "return StringVector::AddString(result, sketch.to_string(summary_data, detail_data, false, false));",
                    "description": "Return a string representation of the sketch",
                    "example": f"datasketch_{sketch_type.lower()}_describe(sketch, include_summary, include_detail)",
                    "arguments": [
                        sketch_argument,
                        {
                            "cpp_type": "bool",
                            "name": "summary",
                        },
                        {
                            "cpp_type": "bool",
                            "name": "detail",
                        },
                    ],
                    "name": "describe",
                    "return_type": "LogicalType::VARCHAR",
                },
                {
                    "method": "return sketch.get_lg_config_k();",
                    "description": "Return the value of log base 2 K for this sketch",
                    "example": f"datasketch_{sketch_type.lower()}_lg_config_k(sketch)",
                    "arguments": [
                        sketch_argument,
                    ],
                    "name": "lg_config_k",
                    "return_type": "LogicalType::UTINYINT",
                },
                {
                    "method": "return sketch.is_compact();",
                    "description": "Return whether the sketch is in compact form",
                    "example": f"datasketch_{sketch_type.lower()}_is_compact(sketch)",
                    "arguments": [
                        sketch_argument,
                    ],
                    "name": "is_compact",
                    "return_type": "LogicalType::BOOLEAN",
                },
            ]
        )

    if sketch_type == "CPC":
        result.append(
            {
                "method": "return StringVector::AddString(result, sketch.to_string());",
                "description": "Return a string representation of the sketch",
                "example": f"datasketch_{sketch_type.lower()}_describe(sketch)",
                "arguments": [
                    sketch_argument,
                ],
                "name": "describe",
                "return_type": "LogicalType::VARCHAR",
            },
        )

    if sketch_type in counting_sketch_names:
        result.extend(
            [
                {
                    "method": "return sketch.get_estimate();",
                    "description": "Return the estimate of the number of distinct items seen by the sketch",
                    "example": f"datasketch_{sketch_type.lower()}_estimate(sketch)",
                    "arguments": [
                        sketch_argument,
                    ],
                    "name": "estimate",
                    "return_type": "LogicalType::DOUBLE",
                },
                {
                    "description": "Return the lower bound of the number of distinct items seen by the sketch",
                    "example": f"datasketch_{sketch_type.lower()}_lower_bound(sketch, std_dev)",
                    "method": "return sketch.get_lower_bound(std_dev_data);",
                    "arguments": [
                        sketch_argument,
                        {
                            "cpp_type": "uint8_t",
                            "name": "std_dev",
                        },
                    ],
                    "name": "lower_bound",
                    "return_type": "LogicalType::DOUBLE",
                },
                {
                    "description": "Return the upper bound of the number of distinct items seen by the sketch",
                    "example": f"datasketch_{sketch_type.lower()}_upper_bound(sketch, std_dev)",
                    "method": "return sketch.get_upper_bound(std_dev_data);",
                    "arguments": [
                        sketch_argument,
                        {
                            "cpp_type": "uint8_t",
                            "name": "std_dev",
                        },
                    ],
                    "name": "upper_bound",
                    "return_type": "LogicalType::DOUBLE",
                },
            ]
        )

    if sketch_type == "TDigest":
        result.extend(
            [
                {
                    "description": "Return a description of this sketch",
                    "example": f"datasketch_{sketch_type.lower()}_describe(sketch, include_centroids)",
                    "method": "return StringVector::AddString(result, sketch.to_string(include_centroids_data));",
                    "arguments": [
                        sketch_argument,
                        {
                            "cpp_type": "bool",
                            "name": "include_centroids",
                        },
                    ],
                    "name": "describe",
                    "return_type": "LogicalType::VARCHAR",
                },
                {
                    "description": "Return the rank of an item in the sketch",
                    "example": f"datasketch_{sketch_type.lower()}_rank(sketch, item)",
                    "method": "return sketch.get_rank(DSItemIn(item_data));",
                    "name": "rank",
                    "arguments": [
                        sketch_argument,
                        {
                            "cpp_type_dynamic": True,
                            "name": "item",
                        },
                    ],
                    "return_type": "LogicalType::DOUBLE",
                },
                {
                    "description": "Return the total weight of this sketch",
                    "example": f"datasketch_{sketch_type.lower()}_total_weight(sketch)",
                    "method": "return sketch.get_total_weight();",
                    "name": "total_weight",
                    "arguments": [
                        sketch_argument,
                    ],
                    "return_type": "LogicalType::UBIGINT",
                },
                {
                    "description": "Return the quantile of a rank in the sketch",
                    "example": f"datasketch_{sketch_type.lower()}_quantile(sketch, rank)",
                    "method": "return DSItemOut<T>(result, sketch.get_quantile(rank_data));",
                    "name": "quantile",
                    "arguments": [
                        sketch_argument,
                        {
                            "cpp_type": "double",
                            "name": "rank",
                        },
                    ],
                    "dynamic_return_type": True,
                },
            ]
        )

    if sketch_type not in ("TDigest", "REQ", "HLL", "CPC"):
        result.extend(
            [
                {
                    "description": "Return the normalized rank error of the sketch",
                    "example": f"datasketch_{sketch_type.lower()}_normalized_rank_error(sketch, is_pmf)",
                    "method": "return sketch.get_normalized_rank_error(is_pmf_data);",
                    "name": "normalized_rank_error",
                    "arguments": [
                        sketch_argument,
                        {
                            "cpp_type": "bool",
                            "name": "is_pmf",
                        },
                    ],
                    "return_type": "LogicalType::DOUBLE",
                },
            ]
        )

    if sketch_type != "TDigest" and sketch_type not in counting_sketch_names:
        result.extend(
            [
                {
                    "description": "Return a description of this sketch",
                    "example": f"datasketch_{sketch_type.lower()}_describe(sketch, include_levels, include_items)",
                    "method": "return StringVector::AddString(result, sketch.to_string(include_levels_data, include_items_data));",
                    "arguments": [
                        sketch_argument,
                        {
                            "cpp_type": "bool",
                            "name": "include_levels",
                        },
                        {
                            "cpp_type": "bool",
                            "name": "include_items",
                        },
                    ],
                    "name": "describe",
                    "return_type": "LogicalType::VARCHAR",
                },
                {
                    "description": "Return the rank of an item in the sketch",
                    "example": f"datasketch_{sketch_type.lower()}_rank(sketch, item, inclusive)",
                    "method": "return sketch.get_rank(DSItemIn(item_data), inclusive_data);",
                    "name": "rank",
                    "arguments": [
                        sketch_argument,
                        {
                            "cpp_type_dynamic": True,
                            "name": "item",
                        },
                        {
                            "cpp_type": "bool",
                            "name": "inclusive",
                        },
                    ],
                    "return_type": "LogicalType::DOUBLE",
                },
                {
                    "description": "Return the quantile of a rank in the sketch",
                    "example": f"datasketch_{sketch_type.lower()}_quantile(sketch, rank, inclusive)",
                    "method": "return DSItemOut<T>(result, sketch.get_quantile(rank_data, inclusive_data));",
                    "name": "quantile",
                    "arguments": [
                        sketch_argument,
                        {
                            "cpp_type": "double",
                            "name": "rank",
                        },
                        {
                            "cpp_type": "bool",
                            "name": "inclusive",
                        },
                    ],
                    "dynamic_return_type": True,
                },
                {
                    "description": "Return the number of items contained in the sketch",
                    "example": f"datasketch_{sketch_type.lower()}_n(sketch)",
                    "method": "return sketch.get_n();",
                    "name": "n",
                    "arguments": [
                        sketch_argument,
                    ],
                    "return_type": "LogicalType::UBIGINT",
                },
                {
                    "description": "Return a boolean indicating if the sketch is in estimation mode",
                    "example": f"datasketch_{sketch_type.lower()}_is_estimation_mode(sketch)",
                    "method": "return sketch.is_estimation_mode();",
                    "name": "is_estimation_mode",
                    "arguments": [
                        sketch_argument,
                    ],
                    "return_type": "LogicalType::BOOLEAN",
                },
                {
                    "description": "Return the number of retained items in the sketch",
                    "example": f"datasketch_{sketch_type.lower()}_num_retained(sketch)",
                    "method": "return sketch.get_num_retained();",
                    "name": "num_retained",
                    "arguments": [
                        sketch_argument,
                    ],
                    "return_type": "LogicalType::UBIGINT",
                },
                {
                    "description": "Return the minimum item in the sketch",
                    "example": f"datasketch_{sketch_type.lower()}_min_item(sketch)",
                    "method": "return DSItemOut<T>(result, sketch.get_min_item());",
                    "name": "min_item",
                    "arguments": [
                        sketch_argument,
                    ],
                    "dynamic_return_type": True,
                },
                {
                    "description": "Return the maximum item in the sketch",
                    "example": f"datasketch_{sketch_type.lower()}_max_item(sketch)",
                    "method": "return DSItemOut<T>(result, sketch.get_max_item());",
                    "name": "max_item",
                    "arguments": [sketch_argument],
                    "dynamic_return_type": True,
                },
            ]
        )

    if sketch_type == "REQ":
        result.extend(
            [
                {
                    "description": "Return an approximate lower bound of the given normalized rank, with a confidence of num_std_dev standard deviations (1, 2 or 3)",
                    "example": "datasketch_req_rank_lower_bound(sketch, 0.5, 2)",
                    "method": "DSCheckRank(rank_data); return sketch.get_rank_lower_bound(rank_data, num_std_dev_data);",
                    "name": "rank_lower_bound",
                    "arguments": [
                        sketch_argument,
                        {
                            "cpp_type": "double",
                            "name": "rank",
                        },
                        {
                            "cpp_type": "uint8_t",
                            "name": "num_std_dev",
                        },
                    ],
                    "return_type": "LogicalType::DOUBLE",
                },
                {
                    "description": "Return an approximate upper bound of the given normalized rank, with a confidence of num_std_dev standard deviations (1, 2 or 3)",
                    "example": "datasketch_req_rank_upper_bound(sketch, 0.5, 2)",
                    "method": "DSCheckRank(rank_data); return sketch.get_rank_upper_bound(rank_data, num_std_dev_data);",
                    "name": "rank_upper_bound",
                    "arguments": [
                        sketch_argument,
                        {
                            "cpp_type": "double",
                            "name": "rank",
                        },
                        {
                            "cpp_type": "uint8_t",
                            "name": "num_std_dev",
                        },
                    ],
                    "return_type": "LogicalType::DOUBLE",
                },
                {
                    "description": "Return true if the sketch favors accuracy at high ranks (HRA), false if it favors low ranks (LRA)",
                    "example": "datasketch_req_is_hra(sketch)",
                    "method": "return sketch.is_HRA();",
                    "name": "is_hra",
                    "arguments": [
                        sketch_argument,
                    ],
                    "return_type": "LogicalType::BOOLEAN",
                },
            ]
        )

    if sketch_type == "TDigest":
        result.extend(
            [
                {
                    "description": "Return the minimum value seen by the sketch",
                    "example": "datasketch_tdigest_min_value(sketch)",
                    "method": "return sketch.get_min_value();",
                    "name": "min_value",
                    "arguments": [
                        sketch_argument,
                    ],
                    "dynamic_return_type": True,
                },
                {
                    "description": "Return the maximum value seen by the sketch",
                    "example": "datasketch_tdigest_max_value(sketch)",
                    "method": "return sketch.get_max_value();",
                    "name": "max_value",
                    "arguments": [
                        sketch_argument,
                    ],
                    "dynamic_return_type": True,
                },
            ]
        )

    if sketch_type in ("Quantiles", "KLL", "REQ"):
        result.extend(
            [
                {
                    "description": "Return the quantiles (items) at a list of normalized ranks in [0, 1]. A NULL rank yields a NULL quantile",
                    "example": f"datasketch_{sketch_type.lower()}_quantiles(sketch, [0.25, 0.5, 0.75], true)",
                    "method": """
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
                """ % sketch_type.lower(),
                    "name": "quantiles",
                    "arguments": [
                        sketch_argument,
                        list_argument("ranks", lambda contained_type: "LogicalType::DOUBLE", "double"),
                        {
                            "cpp_type": "bool",
                            "name": "inclusive",
                        },
                    ],
                    "return_logical_type": lambda contained_type: f"LogicalType::LIST({contained_type})",
                    "return_cpp_type": "list_entry_t",
                },
                {
                    "description": "Return the normalized ranks of a list of items. A NULL item yields a NULL rank",
                    "example": f"datasketch_{sketch_type.lower()}_ranks(sketch, [10, 20, 30], true)",
                    "method": """
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
                    "name": "ranks",
                    "arguments": [
                        sketch_argument,
                        list_argument("items", lambda contained_type: contained_type, "T"),
                        {
                            "cpp_type": "bool",
                            "name": "inclusive",
                        },
                    ],
                    "return_logical_type": lambda contained_type: "LogicalType::LIST(LogicalType::DOUBLE)",
                    "return_cpp_type": "list_entry_t",
                },
                {
                    "description": "Return the sketch's sorted view: one entry per retained item in ascending order (equal items can appear more than once), with the item's weight and the inclusive cumulative weight (the number of input items less than or equal to it; estimated once the sketch is in estimation mode). Divide cumulative_weight by datasketch_"
                    + sketch_type.lower()
                    + "_n(sketch) for the normalized rank. An empty sketch yields an empty list",
                    "example": f"datasketch_{sketch_type.lower()}_sorted_view(sketch)",
                    "method": """
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
                """,
                    "name": "sorted_view",
                    "arguments": [
                        sketch_argument,
                    ],
                    "return_logical_type": sorted_view_struct_type,
                    "return_cpp_type": "list_entry_t",
                },
            ]
        )
    return result


def get_executor_name(arguments: list) -> str:
    if len(arguments) == 1:
        return "UnaryExecutor"
    elif len(arguments) == 2:
        return "BinaryExecutor"
    elif len(arguments) == 3:
        return "TernaryExecutor"
    else:
        raise NotImplementedError(f"Unhandled number of arguments {len(arguments)}")


def get_scalar_parameter_types(
    function_info: Any, logical_type: str, cpp_type: str
) -> list:
    input_parameters = []
    for arg in function_info["arguments"]:
        if "duckdb_type" in arg:
            input_parameters.append(arg["duckdb_type"](logical_type))
        elif "cpp_type_dynamic" in arg:
            input_parameters.append(logical_type)
        else:
            input_parameters.append(cpp_type_mapping[arg["cpp_type"]])
    return input_parameters


def get_parameter_names(function_info: Any) -> str:
    return ",".join(f'"{arg["name"]}"' for arg in function_info["arguments"])


def get_parameter_types_str(
    function_info: Any, logical_type: str, cpp_type: str
) -> str:
    return ",".join(get_scalar_parameter_types(function_info, logical_type, cpp_type))


def get_scalar_function_args(
    function_info: Any, logical_type: str, cpp_type: str
) -> str:
    input_parameters = get_scalar_parameter_types(function_info, logical_type, cpp_type)

    joined_input_parameters = ",".join(input_parameters)

    all_args = [f"{{{joined_input_parameters}}}"]

    if "return_logical_type" in function_info:
        all_args.append(function_info["return_logical_type"](logical_type))
    elif function_info.get("dynamic_return_type"):
        all_args.append(logical_type)
    elif function_info.get("return_type_double_list"):
        # CDF/PMF results are probabilities, so they are always DOUBLE regardless of the sketch item type.
        all_args.append("LogicalType::LIST(LogicalType::DOUBLE)")
    else:
        all_args.append(function_info["return_type"])

    return ",".join(all_args)


def get_function_block(function_info: Any) -> str:
    cpp_types = []
    for value in function_info["arguments"]:
        if value.get("cpp_type_dynamic"):
            cpp_types.append("T")
        else:
            cpp_types.append(value["cpp_type"])

    if "return_cpp_type" in function_info:
        cpp_types.append(function_info["return_cpp_type"])
    elif function_info.get("return_type_double_list"):
        cpp_types.append("list_entry_t")
    elif function_info.get("dynamic_return_type"):
        cpp_types.append("T")
    else:
        cpp_types.append(logical_type_mapping[function_info["return_type"]])

    joined_cpp_types = ",".join(cpp_types)

    executor_args = []
    lambda_args = []
    lambda_lines = []
    pre_executor_lines = []

    for argument in function_info["arguments"]:
        executor_args.append(f"{argument['name']}_vector")

        if argument.get("cpp_type_dynamic"):
            lambda_args.append(f"T {argument['name']}_data")
        else:
            lambda_args.append(f"{argument['cpp_type']} {argument['name']}_data")

        if "process" in argument:
            lambda_lines.append(argument["process"])

        if "pre_executor" in argument:
            pre_executor_lines.append(argument["pre_executor"])

    executor_args.append("result")

    joined_executor_args = ",".join(executor_args)
    joined_lambda_args = ",".join(lambda_args)

    lambda_lines.append(function_info["method"])

    lambda_body = "\n".join(lambda_lines)
    pre_executor_body = "\n".join(pre_executor_lines)

    result = f"""
        {pre_executor_body}
        {get_executor_name(function_info["arguments"])}::Execute
        <{joined_cpp_types}>
        (
        {joined_executor_args},
        [&]({joined_lambda_args}) {{

            {lambda_body}
        }});"""

    return result


# Data to render the template
data = {
    "sketch_class_name": get_sketch_class_name,
    "counting_sketch_names": counting_sketch_names,
    #    "function_names_per_sketch": get_sketch_function_names,
    "sketch_types": ["Quantiles", "KLL", "REQ", "TDigest", "HLL", "CPC"],
    "logical_type_to_cplusplus_type": sketch_type_to_allowed_logical_types,
    "functions_per_sketch_type": unary_functions_per_sketch_type,
    "get_function_block": get_function_block,
    "get_scalar_function_args": get_scalar_function_args,
    "get_parameter_names": get_parameter_names,
    "get_parameter_types_str": get_parameter_types_str,
    "logical_type_mapping": logical_type_mapping,
    "to_type_id": lambda v: v.replace("LogicalType", "LogicalTypeId"),
    "sketch_k_cpp_type": {
        "Quantiles": "int32_t",
        "KLL": "int32_t",
        "REQ": "int32_t",
        "TDigest": "int32_t",
        "HLL": "int32_t",
        "CPC": "int32_t",
    },
    "cpp_type_mapping": cpp_type_mapping,
}


# Render the template
output = template.render(data)

# Write the generated C++ code to a file
with open("src/generated.cpp", "w") as f:
    f.write(output)

print("C++ file generated successfully!")
