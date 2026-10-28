#pragma once

// How the sketches that hash their input (HLL, CPC, Bloom filter, Count-Min) read DuckDB values, and the input types
// they accept.

#include "datasketches_extension.hpp"

#include "duckdb/common/bswap.hpp"

#include <cmath>
#include <cstring>

namespace duckdb
{

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

// Calls fun(marker, logical_type) for every type a hashing sketch accepts, in registration order. The marker's type
// is the C++ type the values are read as, or DSUUIDInput for UUID (see DSCountingInput).
template <class FUNC>
static inline void DSForEachHashedType(FUNC &&fun) {
    fun(int8_t(), LogicalType::TINYINT);
    fun(int16_t(), LogicalType::SMALLINT);
    fun(int32_t(), LogicalType::INTEGER);
    fun(int64_t(), LogicalType::BIGINT);
    fun(float(), LogicalType::FLOAT);
    fun(double(), LogicalType::DOUBLE);
    fun(uint8_t(), LogicalType::UTINYINT);
    fun(uint16_t(), LogicalType::USMALLINT);
    fun(uint32_t(), LogicalType::UINTEGER);
    fun(uint64_t(), LogicalType::UBIGINT);
    fun(hugeint_t(), LogicalType::HUGEINT);
    fun(uhugeint_t(), LogicalType::UHUGEINT);
    fun(DSUUIDInput(), LogicalType::UUID);
    fun(string_t(), LogicalType::VARCHAR);
    fun(string_t(), LogicalType::BLOB);
}

// The bytes a value is hashed as, passed to key(int64_t), key(double) or key(const void *, size_t), whose result is
// returned. Equal values of different types give the same key, so e.g. a filter built from a BIGINT column answers
// queries with INTEGER literals:
// - every integer that fits in a BIGINT is that BIGINT, wider ones are their 16 bytes (little-endian, lower half
//   first, on every platform);
// - FLOAT and DOUBLE are the DOUBLE (the sketches canonicalize -0.0 and NaN);
// - UUIDs are their canonical 16 bytes (big-endian, as in UUID::ToBlob), so they equal `uuid::BLOB`;
// - VARCHAR and BLOB are their bytes.
template <bool IS_UUID, class T, class KEY>
static inline auto DSHashKey(const T &value, KEY &&key) {
    if constexpr (std::is_same_v<T, string_t>) {
        return key(static_cast<const void *>(value.GetData()), static_cast<size_t>(value.GetSize()));
    } else if constexpr (IS_UUID) {
        const uint64_t halves[2] = {BSwapIfLE(static_cast<uint64_t>(value.upper) ^ (uint64_t(1) << 63)),
                                    BSwapIfLE(value.lower)};
        return key(static_cast<const void *>(halves), sizeof(halves));
    } else if constexpr (std::is_same_v<T, hugeint_t> || std::is_same_v<T, uhugeint_t>) {
        bool fits_int64;
        if constexpr (std::is_same_v<T, hugeint_t>) {
            fits_int64 = value.upper == (static_cast<int64_t>(value.lower) >> 63);
        } else {
            fits_int64 = value.upper == 0 && value.lower <= static_cast<uint64_t>(NumericLimits<int64_t>::Maximum());
        }
        if (fits_int64) {
            return key(static_cast<int64_t>(value.lower));
        }
        const uint64_t halves[2] = {BSwapIfBE(value.lower), BSwapIfBE(static_cast<uint64_t>(value.upper))};
        return key(static_cast<const void *>(halves), sizeof(halves));
    } else if constexpr (std::is_floating_point_v<T>) {
        return key(static_cast<double>(value));
    } else if constexpr (std::is_same_v<T, uint64_t>) {
        if (value <= static_cast<uint64_t>(NumericLimits<int64_t>::Maximum())) {
            return key(static_cast<int64_t>(value));
        }
        // As the UHUGEINT of the same value.
        const uint64_t halves[2] = {BSwapIfBE(value), 0};
        return key(static_cast<const void *>(halves), sizeof(halves));
    } else {
        static_assert(std::is_integral_v<T>, "unsupported hashed input type");
        return key(static_cast<int64_t>(value));
    }
}

// The 8 bytes a DOUBLE key is hashed as, for sketches whose library hashes bytes only: -0.0 as 0.0 and every NaN as
// the canonical NaN (as the HLL, CPC and Bloom filter libraries do), so equal values hash alike.
static inline int64_t DSCanonicalDoubleBits(double value) {
    if (std::isnan(value)) {
        return 0x7ff8000000000000LL;
    }
    value += 0.0;
    int64_t bits;
    memcpy(&bits, &value, sizeof(bits));
    return bits;
}

} // namespace duckdb
