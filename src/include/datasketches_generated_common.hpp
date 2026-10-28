#pragma once

// Plain C++ shared by the generated sketches (src/generated.cpp, rendered from codegen/generated.cpp.j2): item
// conversions, aggregate operations, the bodies of the larger scalar functions and registration helpers.

#include "datasketches_extension.hpp"
#include "datasketches_items.hpp"

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
// Sketch classes, allocating through DSAllocator
// ---------------------------------------------------------------------------

template <class T>
using DSQuantilesSketch = datasketches::quantiles_sketch<T, std::less<T>, DSAllocator<T>>;
template <class T>
using DSKLLSketch = datasketches::kll_sketch<T, std::less<T>, DSAllocator<T>>;
template <class T>
using DSREQSketch = datasketches::req_sketch<T, std::less<T>, DSAllocator<T>>;
template <class T>
using DSTDigestSketch = datasketches::tdigest<T, DSAllocator<T>>;
using DSHLLSketch = datasketches::hll_sketch_alloc<DSAllocator<uint8_t>>;
using DSHLLUnion = datasketches::hll_union_alloc<DSAllocator<uint8_t>>;
using DSCPCSketch = datasketches::cpc_sketch_alloc<DSAllocator<uint8_t>>;
using DSCPCUnion = datasketches::cpc_union_alloc<DSAllocator<uint8_t>>;

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

// Stores text in `vector`: the sketches' to_string() results use the sketch's allocator, so they are not std::string.
template <class TEXT>
static inline string_t DSAddText(Vector &vector, const TEXT &text) {
    return StringVector::AddString(vector, text.data(), text.size());
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

static inline string_t DSHLLMerge(Vector &result, const DSHLLSketch &a, const DSHLLSketch &b) {
    DSHLLUnion u(std::max(a.get_lg_config_k(), b.get_lg_config_k()));
    u.update(a);
    u.update(b);
    return DSAddBytes(result, u.get_result(a.get_target_type()).serialize_updatable());
}

static inline string_t DSCPCMerge(Vector &result, const DSCPCSketch &a, const DSCPCSketch &b) {
    DSCPCUnion u(std::max(a.get_lg_k(), b.get_lg_k()));
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
static inline DSHLLSketch UnionContents(const DSHLLUnion &u) {
    return u.get_result(datasketches::target_hll_type::HLL_8);
}
static inline DSCPCSketch UnionContents(const DSCPCUnion &u) {
    return u.get_result();
}
template <class BIND_DATA_TYPE>
static inline auto SerializeUnion(const DSHLLUnion &u, const BIND_DATA_TYPE &bind) {
    return u.get_result(bind.target_type).serialize_updatable();
}
template <class BIND_DATA_TYPE>
static inline auto SerializeUnion(const DSCPCUnion &u, const BIND_DATA_TYPE &) {
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
        DSCPCUnion u(target.sketch->get_lg_k());
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

// ---------------------------------------------------------------------------
// Arena states
// ---------------------------------------------------------------------------
//
// These states keep their sketch, and all of the sketch's memory, in the aggregate arena (DSAllocator's arena
// mode). They have no destructor and export their sketches as BLOBs, so DuckDB counts them toward memory_limit and
// can spill them to disk. Each callback opens a DSArenaScope on the arena DuckDB passes it, so the sketch grows in
// that arena. STATE provides, with `bind` the aggregate's bind data and `arena` the callback's arena:
//   void Initialize();                                 an empty state
//   bool IsEmpty() const;                              no rows yet
//   template <bool IS_UUID, class V> void Update(const V &value, bind, arena);
//   void Merge(const string_t &sketch, bind, arena);   merges a serialized sketch
//   void Combine(const STATE &source, bind, arena);    merges a partial state, which may live in another
//                                                      thread's arena: nothing of it is taken over
//   auto Serialize(bind, arena);                       the aggregate's result
//   auto Export(bind, arena);                          the exported state
//   void Import(const string_t &state, arena);         rebuilds an exported state

template <class BIND_DATA_TYPE>
struct DSArenaOperationBase {
    template <class STATE>
    static void Initialize(STATE &state) {
        state.Initialize();
    }

    static bool IgnoreNull() {
        return true;
    }

    template <class INPUT_TYPE, class STATE, class OP>
    static void ConstantOperation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input,
                                  idx_t count) {
        for (idx_t i = 0; i < count; i++) {
            OP::template Operation<INPUT_TYPE, STATE, OP>(state, input, unary_input);
        }
    }

    template <class STATE, class OP>
    static void Combine(const STATE &source, STATE &target, AggregateInputData &aggr_input_data) {
        if (source.IsEmpty()) {
            return;
        }
        DSArenaScope scope(aggr_input_data.allocator);
        target.Combine(source, aggr_input_data.bind_data->template Cast<BIND_DATA_TYPE>(), aggr_input_data.allocator);
    }

    template <class T, class STATE>
    static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
        if (state.IsEmpty()) {
            finalize_data.ReturnNull();
            return;
        }
        auto &input = finalize_data.input;
        DSArenaScope scope(input.allocator);
        target = DSAddBytes(finalize_data.result,
                            state.Serialize(input.bind_data->template Cast<BIND_DATA_TYPE>(), input.allocator));
    }
};

// Builds the sketch from values.
template <class BIND_DATA_TYPE, bool IS_UUID = false>
struct DSArenaCreateOperation : DSArenaOperationBase<BIND_DATA_TYPE> {
    template <class A_TYPE, class STATE, class OP>
    static void Operation(STATE &state, const A_TYPE &a_data, AggregateUnaryInput &idata) {
        DSArenaScope scope(idata.input.allocator);
        state.template Update<IS_UUID>(a_data, idata.input.bind_data->template Cast<BIND_DATA_TYPE>(),
                                       idata.input.allocator);
    }
};

// Merges serialized sketches.
template <class BIND_DATA_TYPE>
struct DSArenaMergeOperation : DSArenaOperationBase<BIND_DATA_TYPE> {
    template <class A_TYPE, class STATE, class OP>
    static void Operation(STATE &state, const A_TYPE &a_data, AggregateUnaryInput &idata) {
        DSArenaScope scope(idata.input.allocator);
        state.Merge(a_data, idata.input.bind_data->template Cast<BIND_DATA_TYPE>(), idata.input.allocator);
    }
};

// Exports each state (NULL for a state that saw no rows) for spilling.
template <class STATE, class BIND_DATA_TYPE>
static void DSArenaExportState(Vector &state_vector, AggregateFinalizeInputData &aggr_input_data, Vector &result,
                               idx_t count, idx_t offset) {
    auto states = state_vector.Values<STATE *>();
    auto results = FlatVector::GetDataMutable<string_t>(result);
    auto &bind_data = aggr_input_data.bind_data->template Cast<BIND_DATA_TYPE>();
    DSArenaScope scope(aggr_input_data.allocator);
    for (idx_t i = 0; i < count; i++) {
        auto &state = *states[i].GetValue();
        if (state.IsEmpty()) {
            FlatVector::SetNull(result, offset + i, true);
            continue;
        }
        results[offset + i] = DSAddBytes(result, state.Export(bind_data, aggr_input_data.allocator));
    }
}

// Rebuilds exported states in the arena.
template <class STATE>
static void DSArenaImportState(AggregateImportInputData &input) {
    const auto count = input.input_vec.size();
    auto entries = input.input_vec.Values<string_t>();
    DSArenaScope scope(input.allocator);
    for (idx_t i = 0; i < count; i++) {
        auto &state = *reinterpret_cast<STATE *>(input.dest_buffer + i * input.layout.total_state_size);
        state.Initialize();
        const auto entry = entries[i];
        if (entry.IsValid()) {
            state.Import(entry.GetValue(), input.allocator);
        }
    }
}

// The exported state is a BLOB; K is recorded so that re-binding the exported state supplies it.
template <class STATE, class BIND_DATA_TYPE>
static AggregateStateLayout DSArenaGetStateType(AggregateLayoutInput &input) {
    AggregateStateLayout layout(LogicalType::BLOB, AlignValue<idx_t>(sizeof(STATE)));
    if (input.bind_data) {
        auto &bind_data = input.bind_data->template Cast<BIND_DATA_TYPE>();
        layout.constant_parameters.emplace(0, Value::INTEGER(bind_data.k));
    }
    return layout;
}

template <class STATE, class BIND_DATA_TYPE>
static void DSSetArenaStateExport(AggregateFunction &fun) {
    fun.SetStateExportCallbacks(DSArenaGetStateType<STATE, BIND_DATA_TYPE>, DSArenaExportState<STATE, BIND_DATA_TYPE>,
                                DSArenaImportState<STATE>);
}

// Items kept in the arena in insertion order, in an array that doubles as it fills.
template <class ITEM>
struct DSArenaItems {
    static_assert(std::is_trivially_copyable<ITEM>::value, "arena items are copied bytewise");

    ITEM *items;
    uint32_t count;
    uint32_t capacity;

    void Initialize() {
        items = nullptr;
        count = 0;
        capacity = 0;
    }

    void Append(const ITEM &item, ArenaAllocator &arena) {
        if (count == capacity) {
            const uint32_t new_capacity = capacity ? capacity * 2 : 1;
            auto new_items = reinterpret_cast<ITEM *>(arena.AllocateAligned(new_capacity * sizeof(ITEM)));
            if (count) {
                memcpy(new_items, items, count * sizeof(ITEM));
            }
            items = new_items;
            capacity = new_capacity;
        }
        items[count++] = item;
    }
};

} // namespace duckdb
