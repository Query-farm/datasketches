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


def unary_functions_per_sketch_type(sketch_type: str):
    if sketch_type not in counting_sketch_names:
        deserialize_sketch = f"auto sketch = {get_sketch_class_name(sketch_type)}<T>::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());"
    else:
        deserialize_sketch = f"auto sketch = {get_sketch_class_name(sketch_type)}::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());"

    if sketch_type in counting_sketch_names:
        sketch_argument = {
            "cpp_type": "string_t",
            "duckdb_type": lambda contained_type: "sketch_type",
            "name": "sketch",
            "process": deserialize_sketch,
        }
    else:
        sketch_argument = {
            "cpp_type": "string_t",
            "duckdb_type": lambda contained_type: f"sketch_map_types[{contained_type.replace("LogicalType", "LogicalTypeId")}]",
            "name": "sketch",
            "process": deserialize_sketch,
        }

    cdf_points_argument = {
        "cpp_type": "list_entry_t",
        "duckdb_type": lambda contained_type: f"LogicalType::LIST({contained_type})",
        "name": "split_points",
        "pre_executor": """
                    UnifiedVectorFormat unified_split_points;
                    split_points_vector.ToUnifiedFormat(args.size(), unified_split_points);

                    // auto split_points_list_entries = UnifiedVectorFormat::GetData<list_entry_t>(unified_split_points);
                    //         auto split_points_validitiy = FlatVector::Validity(split_points_vector);

                    auto &split_points_list_children = ListVector::GetEntry(split_points_vector);

                    UnifiedVectorFormat split_points_children_unified;
                    split_points_list_children.ToUnifiedFormat(args.size(), split_points_children_unified);

                    const T *split_points_list_children_data = UnifiedVectorFormat::GetData<T>(split_points_children_unified);
                    """,
        "process": """
                    T *passing_points = (T *)duckdb_malloc(sizeof(T) * split_points_data.length);
                    for (idx_t i = 0; i < split_points_data.length; i++)
                    {
                        passing_points[i] = split_points_list_children_data[i + split_points_data.offset];
                    }
                    """,
    }
    pmf_points_argument = cdf_points_argument

    result = [
        {
            "method": "return sketch.is_empty();",
            "name": "is_empty",
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
                    "return_type": "LogicalType::USMALLINT",
                },
                {
                    "name": "cdf",
                    "method": (
                        "auto cdf_result = sketch.get_CDF(passing_points, split_points_data.length, inclusive_data);"
                        if sketch_type != "TDigest"
                        else "auto cdf_result = sketch.get_CDF(passing_points, split_points_data.length);"
                    )
                    + """
                duckdb_free(passing_points);
                auto current_size = ListVector::GetListSize(result);
                auto new_size = current_size + cdf_result.size();
                if (ListVector::GetListCapacity(result) < new_size)
                {
                    ListVector::Reserve(result, new_size);
                }

                auto &child_entry = ListVector::GetEntry(result);
                auto child_vals = FlatVector::GetData<T>(child_entry);
                //auto &child_validity = FlatVector::Validity(child_entry);
                for (idx_t i = 0; i < cdf_result.size(); i++)
                {
                    child_vals[current_size + i] = cdf_result[i];
                }
                ListVector::SetListSize(result, new_size);
                return list_entry_t{current_size, cdf_result.size()};
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
                    "return_type_dynamic_list": True,
                },
                {
                    "name": "pmf",
                    "method": (
                        "auto pmf_result = sketch.get_PMF(passing_points, split_points_data.length, inclusive_data);"
                        if sketch_type != "TDigest"
                        else "auto pmf_result = sketch.get_PMF(passing_points, split_points_data.length);"
                    )
                    + """
                duckdb_free(passing_points);

                auto current_size = ListVector::GetListSize(result);
                auto new_size = current_size + pmf_result.size();
                if (ListVector::GetListCapacity(result) < new_size)
                {
                    ListVector::Reserve(result, new_size);
                }

                auto &child_entry = ListVector::GetEntry(result);
                auto child_vals = FlatVector::GetData<double>(child_entry);
                //auto &child_validity = FlatVector::Validity(child_entry);
                for (idx_t i = 0; i < pmf_result.size(); i++)
                {
                    child_vals[current_size + i] = pmf_result[i];
                }
                ListVector::SetListSize(result, new_size);
                return list_entry_t{current_size, pmf_result.size()};
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
                    "return_type_dynamic_list": True,
                },
            ]
        )

    if sketch_type == "HLL":
        result.extend(
            [
                {
                    "method": "return StringVector::AddString(result, sketch.to_string(summary_data, detail_data, false, false));",
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
                    "arguments": [
                        sketch_argument,
                    ],
                    "name": "lg_config_k",
                    "return_type": "LogicalType::UTINYINT",
                },
                {
                    "method": "return sketch.is_compact();",
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
                    "arguments": [
                        sketch_argument,
                    ],
                    "name": "estimate",
                    "return_type": "LogicalType::DOUBLE",
                },
                {
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
                    "method": "return sketch.get_rank(item_data);",
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
                    "method": "return sketch.get_total_weight();",
                    "name": "total_weight",
                    "arguments": [
                        sketch_argument,
                    ],
                    "return_type": "LogicalType::UBIGINT",
                },
                {
                    "method": "return sketch.get_quantile(rank_data);",
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
                    "method": "return sketch.get_rank(item_data, inclusive_data);",
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
                    "method": "return sketch.get_quantile(rank_data, inclusive_data);",
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
                    "method": "return sketch.get_n();",
                    "name": "n",
                    "arguments": [
                        sketch_argument,
                    ],
                    "return_type": "LogicalType::UBIGINT",
                },
                {
                    "method": "return sketch.is_estimation_mode();",
                    "name": "is_estimation_mode",
                    "arguments": [
                        sketch_argument,
                    ],
                    "return_type": "LogicalType::BOOLEAN",
                },
                {
                    "method": "return sketch.get_num_retained();",
                    "name": "num_retained",
                    "arguments": [
                        sketch_argument,
                    ],
                    "return_type": "LogicalType::UBIGINT",
                },
                {
                    "method": "return sketch.get_min_item();",
                    "name": "min_item",
                    "arguments": [
                        sketch_argument,
                    ],
                    "dynamic_return_type": True,
                },
                {
                    "method": "return sketch.get_max_item();",
                    "name": "max_item",
                    "arguments": [sketch_argument],
                    "dynamic_return_type": True,
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


def get_scalar_function_args(
    function_info: Any, logical_type: str, cpp_type: str
) -> str:
    input_parameters = []
    for arg in function_info["arguments"]:
        if "duckdb_type" in arg:
            input_parameters.append(arg["duckdb_type"](logical_type))
        elif "cpp_type_dynamic" in arg:
            input_parameters.append(logical_type)
        else:
            input_parameters.append(cpp_type_mapping[arg["cpp_type"]])

    joined_input_parameters = ",".join(input_parameters)

    all_args = [f"{{{joined_input_parameters}}}"]

    if function_info.get("dynamic_return_type"):
        all_args.append(logical_type)
    elif function_info.get("return_type_dynamic_list"):
        all_args.append(f"LogicalType::LIST({logical_type})")
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

    if function_info.get("return_type_dynamic_list"):
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
    executor_args.append("args.size()")

    joined_executor_args = ",".join(executor_args)
    joined_lambda_args = ",".join(lambda_args)

    lambda_lines.append(function_info["method"])

    lambda_body = "\n".join(lambda_lines)
    pre_executor_body = "\n".join(pre_executor_lines)

    result = f"""
        {pre_executor_body}
        {get_executor_name(function_info['arguments'])}::Execute
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
    "logical_type_mapping": logical_type_mapping,
    "to_type_id": lambda v: v.replace("LogicalType", "LogicalTypeId"),
}


# Render the template
output = template.render(data)

# Write the generated C++ code to a file
with open("src/generated.cpp", "w") as f:
    f.write(output)

print("C++ file generated successfully!")
