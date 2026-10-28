#pragma once

// Arena states that buffer their first raw inputs, for the hand-written sketches (frequent items, VarOpt, EBPPS,
// Bloom filter, Count-Min). They build on the arena states of datasketches_generated_common.hpp (the operations, the
// export and import callbacks, DSArenaItems) and follow the same rules: everything a state keeps lives in the
// aggregate arena of the callback that allocated it, a state has no destructor, and Combine copies from the source
// state without taking over or growing its memory.
//
// DuckDB creates one state per input row when grouping reduces little (it skips the hash table lookups), and these
// sketches allocate a lot up front: a Bloom filter its whole bit array, a Count-Min sketch num_hashes * num_buckets
// counters, an EBPPS sketch room for K items. So a state keeps its first inputs (with their weights) as they are, and
// builds its sketch, feeding them in order, only when more arrive.

#include "datasketches_generated_common.hpp"

#include <cstring>
#include <type_traits>
#include <vector>

namespace duckdb
{

// One buffered input: a value and, unless WEIGHT is void, its weight.
template <class T, class WEIGHT>
struct DSBufferedEntry
{
	T value;
	WEIGHT weight;
};

template <class T>
struct DSBufferedEntry<T, void>
{
	T value;
};

// Strings are copied into the arena (unless inlined), so a buffered string_t never points at input or at another
// state's memory.
template <class T>
static inline idx_t DSArenaValueBytes(const T &value)
{
	if constexpr (std::is_same_v<T, string_t>)
	{
		return value.IsInlined() ? 0 : value.GetSize();
	}
	else
	{
		return 0;
	}
}

template <class T>
static inline T DSArenaCopyValue(const T &value, ArenaAllocator &arena)
{
	if constexpr (std::is_same_v<T, string_t>)
	{
		if (value.IsInlined())
		{
			return value;
		}
		auto size = value.GetSize();
		auto data = arena.Allocate(size);
		memcpy(data, value.GetData(), size);
		return string_t(const_char_ptr_cast(data), UnsafeNumericCast<uint32_t>(size));
	}
	else
	{
		return value;
	}
}

// An entry's bytes in an exported state: the weight, then the value as it is (a string as its uint32 length and
// bytes).
template <class T, class WEIGHT>
static inline void DSWriteEntry(std::vector<uint8_t> &out, const DSBufferedEntry<T, WEIGHT> &entry)
{
	auto append = [&](const void *data, idx_t size)
	{
		auto bytes = static_cast<const uint8_t *>(data);
		out.insert(out.end(), bytes, bytes + size);
	};
	if constexpr (!std::is_void_v<WEIGHT>)
	{
		append(&entry.weight, sizeof(WEIGHT));
	}
	if constexpr (std::is_same_v<T, string_t>)
	{
		const uint32_t size = entry.value.GetSize();
		append(&size, sizeof(size));
		append(entry.value.GetData(), size);
	}
	else
	{
		append(&entry.value, sizeof(T));
	}
}

// Reads an entry written by DSWriteEntry, copying a string into the arena. False if the bytes end too soon.
template <class T, class WEIGHT>
static inline bool DSReadEntry(const_data_ptr_t &ptr, const_data_ptr_t end, DSBufferedEntry<T, WEIGHT> &entry,
                               ArenaAllocator &arena)
{
	auto read = [&](void *data, idx_t size)
	{
		if (idx_t(end - ptr) < size)
		{
			return false;
		}
		memcpy(data, ptr, size);
		ptr += size;
		return true;
	};
	if constexpr (!std::is_void_v<WEIGHT>)
	{
		if (!read(&entry.weight, sizeof(WEIGHT)))
		{
			return false;
		}
	}
	if constexpr (std::is_same_v<T, string_t>)
	{
		uint32_t size;
		if (!read(&size, sizeof(size)) || idx_t(end - ptr) < size)
		{
			return false;
		}
		entry.value = DSArenaCopyValue(string_t(const_char_ptr_cast(ptr), size), arena);
		ptr += size;
		return true;
	}
	else
	{
		return read(&entry.value, sizeof(T));
	}
}

// The aggregates' state: up to POLICY::BUFFERED raw inputs, or the sketch they were fed to. POLICY (with BIND its
// bind data, SKETCH what the state keeps in the arena, ENTRY = DSBufferedEntry<VALUE, WEIGHT>) provides:
//   static constexpr uint32_t BUFFERED;                  the most inputs a state buffers
//   static idx_t Budget(const BIND &);                   the most bytes a buffer may take (about the sketch's size)
//   static SKETCH *Create(const BIND &, ArenaAllocator &);  an empty sketch in the arena
//   static auto Temporary(const BIND &);                 an empty sketch on the heap
//   static void Update(S &sketch, const ENTRY &);        feeds an input to a sketch (SKETCH or the temporary)
//   static auto Serialize(S &sketch);                    the sketch's bytes (run in a scratch arena)
//   static SKETCH *Deserialize(const string_t &, ArenaAllocator &);  a sketch read into the arena
//   static SKETCH *Copy(const SKETCH &, ArenaAllocator &);  a copy in the arena
//   static void Combine(SKETCH &target, const SKETCH &source, const BIND &, ArenaAllocator &);
//   static void Merge(SKETCH *&sketch, const string_t &blob, const BIND &, ArenaAllocator &);  merges a serialized
//                                                        sketch, creating the state's sketch if it has none
template <class POLICY>
struct DSBufferedArenaState
{
	using BIND = typename POLICY::BIND;
	using VALUE = typename POLICY::VALUE;
	using WEIGHT = typename POLICY::WEIGHT;
	using SKETCH = typename POLICY::SKETCH;
	using ENTRY = DSBufferedEntry<VALUE, WEIGHT>;

	SKETCH *sketch;
	DSArenaItems<ENTRY> buffer;
	// bytes of the buffered strings that are not inlined
	uint32_t string_bytes;

	void Initialize()
	{
		sketch = nullptr;
		buffer.Initialize();
		string_bytes = 0;
	}
	bool IsEmpty() const
	{
		return !sketch && buffer.count == 0;
	}

	template <class TARGET>
	void Replay(TARGET &target) const
	{
		for (uint32_t i = 0; i < buffer.count; i++)
		{
			POLICY::Update(target, buffer.items[i]);
		}
	}
	void ClearBuffer()
	{
		buffer.Initialize();
		string_bytes = 0;
	}
	// Moves the buffered inputs into a new sketch.
	SKETCH &Promote(const BIND &bind, ArenaAllocator &arena)
	{
		if (!sketch)
		{
			sketch = POLICY::Create(bind, arena);
			Replay(*sketch);
			ClearBuffer();
		}
		return *sketch;
	}
	void Add(const ENTRY &entry, const BIND &bind, ArenaAllocator &arena)
	{
		if (!sketch && buffer.count < POLICY::BUFFERED)
		{
			const idx_t extra = DSArenaValueBytes(entry.value);
			if ((buffer.count + 1) * sizeof(ENTRY) + string_bytes + extra <= POLICY::Budget(bind))
			{
				ENTRY copy = entry;
				copy.value = DSArenaCopyValue(entry.value, arena);
				buffer.Append(copy, arena);
				string_bytes += UnsafeNumericCast<uint32_t>(extra);
				return;
			}
		}
		POLICY::Update(Promote(bind, arena), entry);
	}

	// An input of the default weight (DSArenaCreateOperation).
	template <bool IS_UUID, class V>
	void Update(const V &value, const BIND &bind, ArenaAllocator &arena)
	{
		Add(POLICY::Entry(value), bind, arena);
	}
	// An input with its weight (DSArenaWeightedOperation).
	template <class V, class W>
	void UpdateWeighted(const V &value, const W &weight, const BIND &bind, ArenaAllocator &arena)
	{
		Add(POLICY::Entry(value, weight), bind, arena);
	}
	void Merge(const string_t &blob, const BIND &bind, ArenaAllocator &arena)
	{
		if (!sketch && buffer.count)
		{
			Promote(bind, arena);
		}
		POLICY::Merge(sketch, blob, bind, arena);
	}
	void Combine(const DSBufferedArenaState &source, const BIND &bind, ArenaAllocator &arena)
	{
		if (!source.sketch)
		{
			for (uint32_t i = 0; i < source.buffer.count; i++)
			{
				Add(source.buffer.items[i], bind, arena);
			}
		}
		else if (!sketch)
		{
			// Copy the source's sketch, then feed it what this state buffered.
			sketch = POLICY::Copy(*source.sketch, arena);
			Replay(*sketch);
			ClearBuffer();
		}
		else
		{
			POLICY::Combine(*sketch, *source.sketch, bind, arena);
		}
	}

	// A state that still buffers its inputs is serialized through a temporary sketch on the heap, freed right away.
	std::vector<uint8_t> Serialize(const BIND &bind, ArenaAllocator &) const
	{
		if (sketch)
		{
			return DSScratchBytes([&]() { return POLICY::Serialize(*sketch); });
		}
		auto temporary = POLICY::Temporary(bind);
		Replay(temporary);
		auto bytes = POLICY::Serialize(temporary);
		return std::vector<uint8_t>(bytes.begin(), bytes.end());
	}

	// The exported state is a tag byte, then the buffered inputs (EXPORT_BUFFER, see DSWriteEntry) or the serialized
	// sketch (EXPORT_SKETCH). Buffered states stay buffered when they are imported.
	static constexpr uint8_t EXPORT_BUFFER = 0;
	static constexpr uint8_t EXPORT_SKETCH = 1;
	std::vector<uint8_t> Export(const BIND &, ArenaAllocator &) const
	{
		std::vector<uint8_t> bytes;
		if (sketch)
		{
			auto serialized = DSScratchBytes([&]() { return POLICY::Serialize(*sketch); });
			bytes.reserve(1 + serialized.size());
			bytes.push_back(EXPORT_SKETCH);
			bytes.insert(bytes.end(), serialized.begin(), serialized.end());
			return bytes;
		}
		bytes.push_back(EXPORT_BUFFER);
		for (uint32_t i = 0; i < buffer.count; i++)
		{
			DSWriteEntry(bytes, buffer.items[i]);
		}
		return bytes;
	}
	void Import(const string_t &data, ArenaAllocator &arena)
	{
		const auto size = data.GetSize();
		const auto bytes = const_data_ptr_cast(data.GetData());
		if (size >= 1 && bytes[0] == EXPORT_SKETCH)
		{
			sketch = POLICY::Deserialize(string_t(data.GetData() + 1, UnsafeNumericCast<uint32_t>(size - 1)), arena);
			return;
		}
		if (size < 1 || bytes[0] != EXPORT_BUFFER)
		{
			throw InvalidInputException("Invalid %s aggregate state", POLICY::NAME);
		}
		auto ptr = bytes + 1;
		const auto end = bytes + size;
		while (ptr < end)
		{
			ENTRY entry;
			if (buffer.count == POLICY::BUFFERED || !DSReadEntry(ptr, end, entry, arena))
			{
				throw InvalidInputException("Invalid %s aggregate state", POLICY::NAME);
			}
			buffer.Append(entry, arena);
			string_bytes += UnsafeNumericCast<uint32_t>(DSArenaValueBytes(entry.value));
		}
	}
};

// Builds the sketch from values with weights (a binary aggregate).
template <class BIND_DATA_TYPE>
struct DSArenaWeightedOperation : DSArenaOperationBase<BIND_DATA_TYPE>
{
	template <class A_TYPE, class B_TYPE, class STATE, class OP>
	static void Operation(STATE &state, const A_TYPE &a_data, const B_TYPE &b_data, AggregateBinaryInput &idata)
	{
		DSArenaScope scope(idata.input.allocator);
		state.UpdateWeighted(a_data, b_data, idata.input.bind_data->template Cast<BIND_DATA_TYPE>(), idata.input.allocator);
	}
};

// The exported state is a BLOB. CONSTANTS(bind, constant_parameters) records the aggregate's constant arguments
// (e.g. K), so that re-binding the exported state supplies them.
template <class STATE, class BIND_DATA_TYPE, void (*CONSTANTS)(const BIND_DATA_TYPE &, unordered_map<idx_t, Value> &)>
static AggregateStateLayout DSBufferedGetStateType(AggregateLayoutInput &input)
{
	AggregateStateLayout layout(LogicalType::BLOB, AlignValue<idx_t>(sizeof(STATE)));
	if (input.bind_data)
	{
		CONSTANTS(input.bind_data->template Cast<BIND_DATA_TYPE>(), layout.constant_parameters);
	}
	return layout;
}

template <class BIND_DATA_TYPE>
static void DSNoConstants(const BIND_DATA_TYPE &, unordered_map<idx_t, Value> &)
{
}

template <class STATE, class BIND_DATA_TYPE, void (*CONSTANTS)(const BIND_DATA_TYPE &, unordered_map<idx_t, Value> &)>
static void DSSetBufferedStateExport(AggregateFunction &fun)
{
	fun.SetStateExportCallbacks(DSBufferedGetStateType<STATE, BIND_DATA_TYPE, CONSTANTS>,
	                            DSArenaExportState<STATE, BIND_DATA_TYPE>, DSArenaImportState<STATE>);
}

} // namespace duckdb
