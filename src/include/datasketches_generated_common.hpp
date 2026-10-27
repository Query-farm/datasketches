#pragma once

// Plain C++ shared by the generated sketches (src/generated.cpp, rendered from codegen/generated.cpp.j2): item
// conversions, aggregate operations, the bodies of the larger scalar functions and registration helpers.

#include "datasketches_extension.hpp"

#include "duckdb/common/bswap.hpp"
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

// The list's elements as sketch items. A NULL element is an error: it has no place among split points.
template <class T>
static std::vector<DSItem<T>> DSItems(const DSListView<T> &list, const char *function_name) {
    std::vector<DSItem<T>> items(list.size());
    for (idx_t i = 0; i < list.size(); i++) {
        if (!list.IsValid(i)) {
            throw InvalidInputException("%s: split points cannot be NULL", function_name);
        }
        items[i] = DSItemIn(list[i]);
    }
    return items;
}

// Runs fun, reporting errors raised by a DataSketches library (std::exception, e.g. unsorted split points) as
// invalid input of the function.
template <class FUNC>
static inline auto DSGuard(const char *function_name, FUNC &&fun) -> decltype(fun()) {
    try {
        return fun();
    } catch (Exception &) {
        throw;
    } catch (std::exception &e) {
        throw InvalidInputException("%s: %s", function_name, e.what());
    }
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
        // A value that fits in a BIGINT hashes like that BIGINT, as every narrower integer type does, so e.g.
        // sketches of sum() results (HUGEINT) merge with sketches of the same BIGINT values.
        bool fits_int64;
        if constexpr (std::is_same_v<T, hugeint_t>) {
            fits_int64 = value.upper == (static_cast<int64_t>(value.lower) >> 63);
        } else {
            fits_int64 = value.upper == 0 && value.lower <= static_cast<uint64_t>(NumericLimits<int64_t>::Maximum());
        }
        if (fits_int64) {
            sketch.update(static_cast<int64_t>(value.lower));
            return;
        }
        // Wider values hash all 16 bytes, little-endian, lower half first, on every platform.
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
        // A partial that saw no rows has nothing to add. Passing the target through a union anyway would mark it
        // merged, which drops its HIP estimator, the more accurate estimate of a sketch that was never merged.
        if (!source.sketch) {
            return;
        }
        const bool destructive = aggr_input_data.combine_type == AggregateCombineType::ALLOW_DESTRUCTIVE;
        if (!target.sketch) {
            if (destructive) {
                // The source is discarded after this call, so take its sketch rather than copy it.
                target.sketch = source.sketch;
                const_cast<STATE &>(source).sketch = nullptr;
            } else {
                target.CreateSketch(source);
            }
            return;
        }
        // Move the sketches into the union so it adopts their tables instead of copying them.
        datasketches::cpc_union u(target.sketch->get_lg_k());
        u.update(std::move(*target.sketch));
        if (destructive) {
            u.update(std::move(*const_cast<STATE &>(source).sketch));
        } else {
            u.update(*source.sketch);
        }
        *target.sketch = u.get_result();
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

} // namespace duckdb
