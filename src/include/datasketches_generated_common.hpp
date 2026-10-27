#pragma once

// Plain C++ shared by the generated sketches (src/generated.cpp, rendered from codegen/generated.cpp.j2): item
// conversions, aggregate operations, the bodies of the larger scalar functions and registration helpers.

#include "datasketches_extension.hpp"

#include "duckdb/common/bswap.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/list_vector.hpp"
#include "duckdb/common/vector/string_vector.hpp"
#include "duckdb/common/vector/struct_vector.hpp"
#include "duckdb/common/vector/unified_vector_format.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/parser/parsed_data/create_aggregate_function_info.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"

#include <DataSketches/cpc_sketch.hpp>
#include <DataSketches/cpc_union.hpp>
#include <DataSketches/hll.hpp>
#include <DataSketches/kll_sketch.hpp>
#include <DataSketches/quantiles_sketch.hpp>
#include <DataSketches/req_sketch.hpp>
#include <DataSketches/tdigest.hpp>

namespace duckdb
{

// ---------------------------------------------------------------------------
// Items
// ---------------------------------------------------------------------------

// The C++ item type a sketch stores for a DuckDB value type: VARCHAR values (string_t) are stored as std::string,
// numeric values as themselves.
template <class T>
struct DSItemTraits {
    using type = T;
};
template <>
struct DSItemTraits<string_t> {
    using type = std::string;
};
template <class T>
using DSItem = typename DSItemTraits<T>::type;

// Converts a DuckDB value to the sketch's item type.
template <class T>
static inline DSItem<T> DSItemIn(const T &value) {
    if constexpr (std::is_same_v<T, string_t>) {
        return value.GetString();
    } else {
        return value;
    }
}

// Converts a sketch item to a DuckDB value stored in `vector` (strings are copied into the vector's heap).
template <class T, class V>
static inline T DSItemOut(Vector &vector, const V &value) {
    if constexpr (std::is_same_v<T, string_t>) {
        return StringVector::AddString(vector, value);
    } else {
        return value;
    }
}

// ---------------------------------------------------------------------------
// Sketches
// ---------------------------------------------------------------------------

// Deserializes a sketch, reporting a corrupt or foreign blob as invalid input.
template <class SKETCH>
static SKETCH DSDeserialize(const string_t &data, const char *sketch_name) {
    try {
        return SKETCH::deserialize(data.GetDataUnsafe(), data.GetSize());
    } catch (const std::exception &e) {
        throw InvalidInputException("Failed to deserialize %s sketch: %s", sketch_name, e.what());
    }
}

// Stores serialized sketch bytes in `vector`.
template <class BYTES>
static inline string_t DSAddBytes(Vector &vector, const BYTES &bytes) {
    return StringVector::AddStringOrBlob(vector, const_char_ptr_cast(bytes.data()), bytes.size());
}

// Merges `source` into `target`, reporting incompatible sketches (e.g. REQ sketches with different rank accuracy
// modes) as invalid input instead of an unexpected library exception.
template <class SKETCH, class SOURCE>
static inline void DSMergeSketch(SKETCH &target, SOURCE &&source) {
    try {
        target.merge(std::forward<SOURCE>(source));
    } catch (const std::invalid_argument &e) {
        throw InvalidInputException("Cannot merge sketches: %s", e.what());
    }
}

static inline void DSCheckRank(double rank) {
    if (!(rank >= 0.0 && rank <= 1.0)) {
        throw InvalidInputException("Normalized rank must be between 0 and 1, got: %f", rank);
    }
}

static inline datasketches::target_hll_type DSParseHLLTargetType(const string &name) {
    auto upper = StringUtil::Upper(name);
    if (upper == "HLL_4") {
        return datasketches::HLL_4;
    }
    if (upper == "HLL_6") {
        return datasketches::HLL_6;
    }
    if (upper == "HLL_8") {
        return datasketches::HLL_8;
    }
    throw BinderException("HLL target_type must be one of 'HLL_4', 'HLL_6' or 'HLL_8', got: '%s'", name);
}

static inline string DSHLLTargetTypeName(datasketches::target_hll_type type) {
    switch (type) {
    case datasketches::HLL_4:
        return "HLL_4";
    case datasketches::HLL_6:
        return "HLL_6";
    case datasketches::HLL_8:
        return "HLL_8";
    default:
        return "UNKNOWN";
    }
}

// ---------------------------------------------------------------------------
// Scalar function bodies
// ---------------------------------------------------------------------------

// One row's LIST argument, read through its child vector's UnifiedVectorFormat.
template <class V>
struct DSListView {
    list_entry_t entry;
    const UnifiedVectorFormat &format;
    const V *values;

    idx_t size() const {
        return entry.length;
    }
    bool IsValid(idx_t i) const {
        return format.validity.RowIsValid(Index(i));
    }
    const V &operator[](idx_t i) const {
        return values[Index(i)];
    }

private:
    idx_t Index(idx_t i) const {
        return format.sel->get_index(entry.offset + i);
    }
};

// The list's elements as sketch items.
template <class T>
static std::vector<DSItem<T>> DSItems(const DSListView<T> &list) {
    std::vector<DSItem<T>> items(list.size());
    for (idx_t i = 0; i < list.size(); i++) {
        items[i] = DSItemIn(list[i]);
    }
    return items;
}

// Appends `length` entries to a LIST result: `fill(child, offset)` writes child rows [offset, offset + length).
template <class F>
static list_entry_t DSAppendList(Vector &result, idx_t length, F &&fill) {
    auto offset = ListVector::GetListSize(result);
    ListVector::Reserve(result, offset + length);
    auto &child = ListVector::GetChildMutable(result);
    fill(child, offset);
    ListVector::SetListSize(result, offset + length);
    return list_entry_t(offset, length);
}

// Appends `values` to a LIST(DOUBLE) result.
template <class VALUES>
static list_entry_t DSAppendDoubles(Vector &result, const VALUES &values) {
    return DSAppendList(result, values.size(), [&](Vector &child, idx_t offset) {
        auto child_values = FlatVector::GetDataMutable<double>(child);
        for (idx_t i = 0; i < values.size(); i++) {
            child_values[offset + i] = values[i];
        }
    });
}

// The quantiles at a list of normalized ranks. A NULL rank yields a NULL quantile.
template <class T, class SKETCH>
static list_entry_t DSQuantiles(Vector &result, const SKETCH &sketch, const DSListView<double> &ranks, bool inclusive,
                                const char *function_name) {
    if (sketch.is_empty()) {
        throw InvalidInputException("%s: operation is undefined for an empty sketch", function_name);
    }
    return DSAppendList(result, ranks.size(), [&](Vector &child, idx_t offset) {
        auto child_values = FlatVector::GetDataMutable<T>(child);
        for (idx_t i = 0; i < ranks.size(); i++) {
            if (!ranks.IsValid(i)) {
                FlatVector::SetNull(child, offset + i, true);
                continue;
            }
            auto rank = ranks[i];
            DSCheckRank(rank);
            child_values[offset + i] = DSItemOut<T>(child, sketch.get_quantile(rank, inclusive));
        }
    });
}

// The normalized ranks of a list of items. A NULL item yields a NULL rank.
template <class T, class SKETCH>
static list_entry_t DSRanks(Vector &result, const SKETCH &sketch, const DSListView<T> &items, bool inclusive) {
    return DSAppendList(result, items.size(), [&](Vector &child, idx_t offset) {
        auto child_values = FlatVector::GetDataMutable<double>(child);
        for (idx_t i = 0; i < items.size(); i++) {
            if (!items.IsValid(i)) {
                FlatVector::SetNull(child, offset + i, true);
                continue;
            }
            child_values[offset + i] = sketch.get_rank(DSItemIn(items[i]), inclusive);
        }
    });
}

// The sketch's sorted view as a LIST(STRUCT(item, weight, cumulative_weight)).
template <class T, class SKETCH>
static list_entry_t DSSortedView(Vector &result, const SKETCH &sketch) {
    if (sketch.is_empty()) {
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
        for (auto it = view.begin(); it != view.end(); ++it, ++i) {
            item_values[i] = DSItemOut<T>(item_vector, (*it).first);
            weight_values[i] = it.get_weight();
            cumulative_values[i] = it.get_cumulative_weight(true);
        }
    });
}

static inline string_t DSHLLMerge(Vector &result, const datasketches::hll_sketch &a, const datasketches::hll_sketch &b) {
    datasketches::hll_union u(std::max(a.get_lg_config_k(), b.get_lg_config_k()));
    u.update(a);
    u.update(b);
    return DSAddBytes(result, u.get_result(a.get_target_type()).serialize_updatable());
}

static inline string_t DSCPCMerge(Vector &result, const datasketches::cpc_sketch &a, const datasketches::cpc_sketch &b) {
    datasketches::cpc_union u(std::max(a.get_lg_k(), b.get_lg_k()));
    u.update(a);
    u.update(b);
    return DSAddBytes(result, u.get_result().serialize());
}

// ---------------------------------------------------------------------------
// Aggregate operations
// ---------------------------------------------------------------------------

struct DSSketchOperationBase {
    template <class STATE>
    static void Initialize(STATE &state) {
        state.sketch = nullptr;
    }

    template <class STATE>
    static void Destroy(STATE &state, AggregateInputData &aggr_input_data) {
        if (state.sketch) {
            delete state.sketch;
            state.sketch = nullptr;
        }
    }

    static bool IgnoreNull() {
        return true;
    }
};

// Marks the UUID overload: its values are read as hugeint_t but hashed as UUIDs.
struct DSUUIDInput {};
template <class T>
struct DSCountingInput {
    using physical_type = T;
    static constexpr bool is_uuid = false;
};
template <>
struct DSCountingInput<DSUUIDInput> {
    using physical_type = hugeint_t;
    static constexpr bool is_uuid = true;
};

// Feeds one input value to a counting sketch or union.
template <bool IS_UUID, class SKETCH, class T>
static inline void DSCountingUpdate(SKETCH &sketch, const T &value) {
    if constexpr (std::is_same_v<T, string_t>) {
        sketch.update(value.GetData(), value.GetSize());
    } else if constexpr (IS_UUID) {
        // The canonical 16 bytes (big-endian, as in UUID::ToBlob), so these sketches equal sketches of `uuid::BLOB`.
        const uint64_t halves[2] = {BSwapIfLE(static_cast<uint64_t>(value.upper) ^ (uint64_t(1) << 63)),
                                    BSwapIfLE(value.lower)};
        sketch.update(halves, sizeof(halves));
    } else if constexpr (std::is_same_v<T, hugeint_t> || std::is_same_v<T, uhugeint_t>) {
        // These inputs used to reach the sketch through an implicit cast to DOUBLE. A value that converts to
        // DOUBLE exactly keeps that hash, so existing sketches still merge; the rest hash all 128 bits instead of
        // collapsing into one DOUBLE.
        double as_double;
        T round_trip;
        if (TryCast::Operation<T, double>(value, as_double, false) &&
            TryCast::Operation<double, T>(as_double, round_trip, false) && round_trip == value) {
            sketch.update(as_double);
            return;
        }
        // Little-endian, lower half first, on every platform.
        const uint64_t halves[2] = {BSwapIfBE(value.lower), BSwapIfBE(static_cast<uint64_t>(value.upper))};
        sketch.update(halves, sizeof(halves));
    } else {
        sketch.update(value);
    }
}

// Shared by the quantile sketches' create and merge aggregates: partial states merge sketch by sketch.
struct DSQuantilesOperationBase : DSSketchOperationBase {
    template <class INPUT_TYPE, class STATE, class OP>
    static void ConstantOperation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input,
                                  idx_t count) {
        for (idx_t i = 0; i < count; i++) {
            OP::template Operation<INPUT_TYPE, STATE, OP>(state, input, unary_input);
        }
    }

    template <class STATE, class OP>
    static void Combine(const STATE &source, STATE &target, AggregateInputData &aggr_input_data) {
        // A source partial that received no tuples has a null sketch
        // (e.g. an empty worker partition under parallel aggregation),
        // so there is nothing to merge.
        if (!source.sketch) {
            return;
        }
        if (!target.sketch) {
            target.CreateSketch(source);
        } else {
            DSMergeSketch(*target.sketch, *source.sketch);
        }
    }

    template <class T, class STATE>
    static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
        if (!state.sketch) {
            finalize_data.ReturnNull();
        } else {
            target = DSAddBytes(finalize_data.result, state.sketch->serialize());
        }
    }
};

template <class BIND_DATA_TYPE>
struct DSQuantilesMergeOperation : DSQuantilesOperationBase {
    template <class A_TYPE, class STATE, class OP>
    static void Operation(STATE &state, const A_TYPE &a_data, AggregateUnaryInput &idata) {
        // this is a sketch in a_data, so we need to deserialize it.
        auto incoming = state.deserialize_sketch(a_data);
        if (!state.sketch) {
            auto &bind_data = idata.input.bind_data->template Cast<BIND_DATA_TYPE>();
            state.CreateSketchForMerge(bind_data, incoming);
        }
        DSMergeSketch(*state.sketch, std::move(incoming));
    }
};

template <class BIND_DATA_TYPE>
struct DSQuantilesCreateOperation : DSQuantilesOperationBase {
    template <class A_TYPE, class STATE, class OP>
    static void Operation(STATE &state, const A_TYPE &a_data, AggregateUnaryInput &idata) {
        if (!state.sketch) {
            auto &bind_data = idata.input.bind_data->template Cast<BIND_DATA_TYPE>();
            state.CreateSketch(bind_data);
        }

        state.sketch->update(DSItemIn(a_data));
    }
};

// A union's contents as a sketch another union can absorb, and its result in each type's wire format.
static inline datasketches::hll_sketch UnionContents(const datasketches::hll_union &u) {
    return u.get_result(datasketches::target_hll_type::HLL_8);
}
static inline datasketches::cpc_sketch UnionContents(const datasketches::cpc_union &u) {
    return u.get_result();
}
template <class BIND_DATA_TYPE>
static inline auto SerializeUnion(const datasketches::hll_union &u, const BIND_DATA_TYPE &bind) {
    return u.get_result(bind.target_type).serialize_updatable();
}
template <class BIND_DATA_TYPE>
static inline auto SerializeUnion(const datasketches::cpc_union &u, const BIND_DATA_TYPE &) {
    return u.get_result().serialize();
}

// For states holding a union. A union absorbs the same input any number of times with the same result,
// so repeats merge once, and partial states merge in place instead of being rebuilt.
template <class BIND_DATA_TYPE>
struct DSUnionOperationBase : DSSketchOperationBase {
    template <class INPUT_TYPE, class STATE, class OP>
    static void ConstantOperation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input,
                                  idx_t count) {
        if (count > 0) {
            OP::template Operation<INPUT_TYPE, STATE, OP>(state, input, unary_input);
        }
    }

    template <class STATE, class OP>
    static void Combine(const STATE &source, STATE &target, AggregateInputData &aggr_input_data) {
        if (!source.sketch) {
            return;
        }
        if (!target.sketch) {
            if (aggr_input_data.combine_type == AggregateCombineType::ALLOW_DESTRUCTIVE) {
                // The source is discarded after this call, so take its union rather than copy it.
                target.sketch = source.sketch;
                const_cast<STATE &>(source).sketch = nullptr;
            } else {
                target.CreateSketch(source);
            }
            return;
        }
        target.sketch->update(UnionContents(*source.sketch));
    }

    template <class STATE, class OP>
    static void RepeatedCombine(const STATE &source, STATE &target, AggregateInputData &aggr_input_data,
                                idx_t multiplicity) {
        Combine<STATE, OP>(source, target, aggr_input_data);
    }

    template <class T, class STATE>
    static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
        if (!state.sketch) {
            finalize_data.ReturnNull();
        } else {
            auto &bind_data = finalize_data.input.bind_data->template Cast<BIND_DATA_TYPE>();
            target = DSAddBytes(finalize_data.result, SerializeUnion(*state.sketch, bind_data));
        }
    }
};

// Merges serialized sketches into a union state; shared by every counting sketch's union aggregate.
template <class BIND_DATA_TYPE>
struct DSUnionMergeOperation : DSUnionOperationBase<BIND_DATA_TYPE> {
    template <class A_TYPE, class STATE, class OP>
    static void Operation(STATE &state, const A_TYPE &a_data, AggregateUnaryInput &idata) {
        if (!state.sketch) {
            auto &bind_data = idata.input.bind_data->template Cast<BIND_DATA_TYPE>();
            state.CreateSketch(bind_data);
        }

        state.sketch->update(state.deserialize_sketch(a_data));
    }
};

template <class BIND_DATA_TYPE, bool IS_UUID = false>
struct DSHLLCreateOperation : DSUnionOperationBase<BIND_DATA_TYPE> {
    template <class A_TYPE, class STATE, class OP>
    static void Operation(STATE &state, const A_TYPE &a_data, AggregateUnaryInput &idata) {
        if (!state.sketch) {
            auto &bind_data = idata.input.bind_data->template Cast<BIND_DATA_TYPE>();
            state.CreateSketch(bind_data);
        }

        DSCountingUpdate<IS_UUID>(*state.sketch, a_data);
    }
};

template <class BIND_DATA_TYPE, bool IS_UUID = false>
struct DSCPCCreateOperation : DSSketchOperationBase {
    template <class A_TYPE, class STATE, class OP>
    static void Operation(STATE &state, const A_TYPE &a_data, AggregateUnaryInput &idata) {
        if (!state.sketch) {
            auto &bind_data = idata.input.bind_data->template Cast<BIND_DATA_TYPE>();
            state.CreateSketch(bind_data);
        }

        DSCountingUpdate<IS_UUID>(*state.sketch, a_data);
    }

    template <class INPUT_TYPE, class STATE, class OP>
    static void ConstantOperation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input,
                                  idx_t count) {
        for (idx_t i = 0; i < count; i++) {
            Operation<INPUT_TYPE, STATE, OP>(state, input, unary_input);
        }
    }

    template <class STATE, class OP>
    static void Combine(const STATE &source, STATE &target, AggregateInputData &aggr_input_data) {
        if (!target.sketch) {
            target.CreateSketch(source);
        } else {
            datasketches::cpc_union u(target.sketch->get_lg_k());
            u.update(*target.sketch);
            if (source.sketch) {
                u.update(*source.sketch);
            }
            *target.sketch = u.get_result();
        }
    }

    template <class T, class STATE>
    static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
        if (!state.sketch) {
            finalize_data.ReturnNull();
        } else {
            target = DSAddBytes(finalize_data.result, state.sketch->serialize());
        }
    }
};

// ---------------------------------------------------------------------------
// Binding and registration
// ---------------------------------------------------------------------------

// The value of a constant argument (K or an option) of an aggregate.
static inline Value DSBindConstant(ClientContext &context, Expression &argument, const string &not_constant_message,
                                   const string &null_message) {
    if (argument.HasParameter()) {
        throw ParameterNotResolvedException();
    }
    if (!argument.IsFoldable()) {
        throw BinderException(not_constant_message);
    }
    auto value = ExpressionExecutor::EvaluateScalar(context, argument);
    if (value.IsNull()) {
        throw BinderException(null_message);
    }
    return value;
}

// Registers a BLOB-backed sketch type with casts to and from BLOB.
static inline LogicalType DSCreateSketchType(ExtensionLoader &loader, const string &name, const string &comment,
                                             bool alias_in_catalog = true) {
    auto new_type = LogicalType(LogicalTypeId::BLOB).WithAlias(name);
    auto type_info = CreateTypeInfo(name, alias_in_catalog ? new_type : LogicalType::BLOB);
    type_info.temporary = false;
    type_info.internal = true;
    type_info.comment = comment;
    auto &system_catalog = Catalog::GetSystemCatalog(loader.GetDatabaseInstance());
    auto data = CatalogTransaction::GetSystemTransaction(loader.GetDatabaseInstance());
    system_catalog.CreateType(data, type_info);
    loader.RegisterCastFunction(LogicalType::BLOB, new_type, DefaultCasts::ReinterpretCast, 1);
    loader.RegisterCastFunction(new_type, LogicalType::BLOB, DefaultCasts::ReinterpretCast, 1);
    return new_type;
}

static inline FunctionDescription DSDescription(vector<LogicalType> parameter_types, vector<string> parameter_names,
                                                string description, string example) {
    FunctionDescription desc;
    desc.description = std::move(description);
    desc.examples.push_back(std::move(example));
    desc.parameter_names = std::move(parameter_names);
    desc.parameter_types = std::move(parameter_types);
    return desc;
}

// Adds an aggregate overload taking `arguments` (its constants included) to `set`, with its description.
static inline void DSAddAggregate(AggregateFunctionSet &set, vector<FunctionDescription> &descriptions,
                                  AggregateFunction fun, bind_aggregate_function_t bind, vector<LogicalType> arguments,
                                  vector<string> parameter_names, string description, string example) {
    fun.SetBindCallback(bind);
    fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
    DSSetArguments(fun, arguments);
    set.AddFunction(fun);
    descriptions.push_back(
        DSDescription(std::move(arguments), std::move(parameter_names), std::move(description), std::move(example)));
}

// Adds a scalar overload to `set`, with its description.
static inline void DSAddScalar(ScalarFunctionSet &set, vector<FunctionDescription> &descriptions, ScalarFunction fun,
                               vector<string> parameter_names, string description, string example) {
    vector<LogicalType> parameter_types;
    for (auto &param : fun.GetSignature().GetParameters()) {
        parameter_types.push_back(param.GetType());
    }
    set.AddFunction(std::move(fun));
    descriptions.push_back(DSDescription(std::move(parameter_types), std::move(parameter_names), std::move(description),
                                         std::move(example)));
}

template <class INFO>
static inline void DSRegisterWithDescriptions(ExtensionLoader &loader, INFO &info,
                                              vector<FunctionDescription> descriptions, const string &sketch) {
    for (auto &desc : descriptions) {
        info.descriptions.push_back(std::move(desc));
    }
    DSSetCategories(info, {"sketch", sketch});
    DSRegister(loader, info);
}

static inline void DSRegisterScalars(ExtensionLoader &loader, ScalarFunctionSet set,
                                     vector<FunctionDescription> descriptions, const string &sketch) {
    // Every scalar function deserializes a sketch, which throws on invalid input.
    set.SetFallible();
    CreateScalarFunctionInfo info(std::move(set));
    DSRegisterWithDescriptions(loader, info, std::move(descriptions), sketch);
}

static inline void DSRegisterAggregates(ExtensionLoader &loader, AggregateFunctionSet set,
                                        vector<FunctionDescription> descriptions, const string &sketch) {
    CreateAggregateFunctionInfo info(std::move(set));
    DSRegisterWithDescriptions(loader, info, std::move(descriptions), sketch);
}

} // namespace duckdb
