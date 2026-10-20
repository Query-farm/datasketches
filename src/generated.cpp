#include "datasketches_extension.hpp"


#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/parsed_data/create_aggregate_function_info.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/list_vector.hpp"
#include "duckdb/common/vector/string_vector.hpp"
#include "duckdb/common/vector/struct_vector.hpp"
#include "duckdb/common/vector/unified_vector_format.hpp"
#include "duckdb/common/string_util.hpp"

#include <DataSketches/quantiles_sketch.hpp>
#include <DataSketches/kll_sketch.hpp>
#include <DataSketches/req_sketch.hpp>
#include <DataSketches/tdigest.hpp>
#include <DataSketches/hll.hpp>
#include <DataSketches/cpc_sketch.hpp>
#include <DataSketches/cpc_union.hpp>

namespace duckdb
{


static std::string toLowerCase(const std::string& input) {
    std::string result = input;
    std::transform(result.begin(), result.end(), result.begin(), [](unsigned char c) {
        return std::tolower(c);
    });
    return result;
}


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

static datasketches::target_hll_type DSParseHLLTargetType(const string &name) {
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

static string DSHLLTargetTypeName(datasketches::target_hll_type type) {
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
    


struct DSQuantilesBindData : public FunctionData {
	DSQuantilesBindData() {
	}
	explicit DSQuantilesBindData(int32_t k) : k(k) {
	}

	unique_ptr<FunctionData> Copy() const override {
		auto copy = make_uniq<DSQuantilesBindData>(k);
		
		return std::move(copy);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<DSQuantilesBindData>();
		
		return k == other.k;
		
	}

    int32_t k;
    
};


unique_ptr<FunctionData> DSQuantilesBind(BindAggregateFunctionInput &input) {
	auto &context = input.GetClientContext();
	auto &arguments = input.GetArguments();
	if (arguments[0]->HasParameter()) {
		throw ParameterNotResolvedException();
	}
	if (!arguments[0]->IsFoldable()) {
		throw BinderException("Quantiles can only take a constant K value");
	}
	Value k_val = ExpressionExecutor::EvaluateScalar(context, *arguments[0]);
	if (k_val.IsNull()) {
		throw BinderException("Quantiles K value cannot be NULL");
	}

	auto actual_k = k_val.GetValue<int32_t>();

	
	// Validate K parameter: must be in range (0, 32768]
	if (actual_k <= 0 || actual_k > 32768) {
		throw BinderException("Quantiles K value must be between 1 and 32768, got: " + std::to_string(actual_k));
	}
	

	auto bind_data = make_uniq<DSQuantilesBindData>(actual_k);

	

	// The constant arguments stay in the argument list (DuckDB 2.0 plan serialization and the common subplan
	// optimizer only see the arguments, not the bind data); the update callbacks read the data at index 1.
	return std::move(bind_data);
}


    
    template <class T>
    
    struct DSQuantilesState
    {
        
        datasketches::quantiles_sketch<DSItem<T>> *sketch = nullptr;
        

        ~DSQuantilesState()
        {
            if (sketch)
            {
                delete sketch;
            }
        }

        void CreateSketch(const DSQuantilesBindData &bind)
        {
            D_ASSERT(!sketch);
            
            D_ASSERT(bind.k > 0);
            D_ASSERT(bind.k <= 32768);
            sketch = new datasketches::quantiles_sketch<DSItem<T>>(bind.k);
            
        }

        
        // The state of a merge aggregate starts from the first input sketch's configuration where the
        // library requires it: REQ sketches can only be merged with sketches of the same rank accuracy mode.
        void CreateSketchForMerge(const DSQuantilesBindData &bind, const datasketches::quantiles_sketch<DSItem<T>> &first)
        {
            
            CreateSketch(bind);
            
        }
        

        void CreateSketch(const DSQuantilesState &existing)
        {
            if (existing.sketch)
            {
                
                sketch = new datasketches::quantiles_sketch<DSItem<T>>(*existing.sketch);
                
            }
        }

        
        datasketches::quantiles_sketch<DSItem<T>> deserialize_sketch(const string_t &data)
        {
            try {
                return datasketches::quantiles_sketch<DSItem<T>>::deserialize(data.GetDataUnsafe(), data.GetSize());
            } catch (const std::exception &e) {
                throw InvalidInputException("Failed to deserialize Quantiles sketch: %s", e.what());
            }
        }
        
    };



    
    static LogicalType CreateQuantilesSketchType(ExtensionLoader &loader, LogicalType embedded_type)
    {
        auto new_type = LogicalType(LogicalTypeId::BLOB);
        auto type_suffix = toLowerCase(embedded_type.ToString());
        auto new_type_name = "sketch_quantiles_" + type_suffix;


        new_type = new_type.WithAlias(new_type_name);
        auto type_info = CreateTypeInfo(new_type_name, new_type);
        type_info.temporary = false;
        type_info.internal = true;
        type_info.comment = "Sketch type for Quantiles sketch with embedded type " + embedded_type.ToString();
        auto &system_catalog = Catalog::GetSystemCatalog(loader.GetDatabaseInstance());
        auto data = CatalogTransaction::GetSystemTransaction(loader.GetDatabaseInstance());
        system_catalog.CreateType(data, type_info);
	    loader.RegisterCastFunction(LogicalType::BLOB, new_type, DefaultCasts::ReinterpretCast, 1);
        loader.RegisterCastFunction(new_type, LogicalType::BLOB, DefaultCasts::ReinterpretCast, 1);
        return new_type;
    }
    


struct DSKLLBindData : public FunctionData {
	DSKLLBindData() {
	}
	explicit DSKLLBindData(int32_t k) : k(k) {
	}

	unique_ptr<FunctionData> Copy() const override {
		auto copy = make_uniq<DSKLLBindData>(k);
		
		return std::move(copy);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<DSKLLBindData>();
		
		return k == other.k;
		
	}

    int32_t k;
    
};


unique_ptr<FunctionData> DSKLLBind(BindAggregateFunctionInput &input) {
	auto &context = input.GetClientContext();
	auto &arguments = input.GetArguments();
	if (arguments[0]->HasParameter()) {
		throw ParameterNotResolvedException();
	}
	if (!arguments[0]->IsFoldable()) {
		throw BinderException("KLL can only take a constant K value");
	}
	Value k_val = ExpressionExecutor::EvaluateScalar(context, *arguments[0]);
	if (k_val.IsNull()) {
		throw BinderException("KLL K value cannot be NULL");
	}

	auto actual_k = k_val.GetValue<int32_t>();

	
	// Validate K parameter: must be in range (0, 32768]
	if (actual_k <= 0 || actual_k > 32768) {
		throw BinderException("KLL K value must be between 1 and 32768, got: " + std::to_string(actual_k));
	}
	

	auto bind_data = make_uniq<DSKLLBindData>(actual_k);

	

	// The constant arguments stay in the argument list (DuckDB 2.0 plan serialization and the common subplan
	// optimizer only see the arguments, not the bind data); the update callbacks read the data at index 1.
	return std::move(bind_data);
}


    
    template <class T>
    
    struct DSKLLState
    {
        
        datasketches::kll_sketch<DSItem<T>> *sketch = nullptr;
        

        ~DSKLLState()
        {
            if (sketch)
            {
                delete sketch;
            }
        }

        void CreateSketch(const DSKLLBindData &bind)
        {
            D_ASSERT(!sketch);
            
            D_ASSERT(bind.k > 0);
            D_ASSERT(bind.k <= 32768);
            sketch = new datasketches::kll_sketch<DSItem<T>>(bind.k);
            
        }

        
        // The state of a merge aggregate starts from the first input sketch's configuration where the
        // library requires it: REQ sketches can only be merged with sketches of the same rank accuracy mode.
        void CreateSketchForMerge(const DSKLLBindData &bind, const datasketches::kll_sketch<DSItem<T>> &first)
        {
            
            CreateSketch(bind);
            
        }
        

        void CreateSketch(const DSKLLState &existing)
        {
            if (existing.sketch)
            {
                
                sketch = new datasketches::kll_sketch<DSItem<T>>(*existing.sketch);
                
            }
        }

        
        datasketches::kll_sketch<DSItem<T>> deserialize_sketch(const string_t &data)
        {
            try {
                return datasketches::kll_sketch<DSItem<T>>::deserialize(data.GetDataUnsafe(), data.GetSize());
            } catch (const std::exception &e) {
                throw InvalidInputException("Failed to deserialize KLL sketch: %s", e.what());
            }
        }
        
    };



    
    static LogicalType CreateKLLSketchType(ExtensionLoader &loader, LogicalType embedded_type)
    {
        auto new_type = LogicalType(LogicalTypeId::BLOB);
        auto type_suffix = toLowerCase(embedded_type.ToString());
        auto new_type_name = "sketch_kll_" + type_suffix;


        new_type = new_type.WithAlias(new_type_name);
        auto type_info = CreateTypeInfo(new_type_name, new_type);
        type_info.temporary = false;
        type_info.internal = true;
        type_info.comment = "Sketch type for KLL sketch with embedded type " + embedded_type.ToString();
        auto &system_catalog = Catalog::GetSystemCatalog(loader.GetDatabaseInstance());
        auto data = CatalogTransaction::GetSystemTransaction(loader.GetDatabaseInstance());
        system_catalog.CreateType(data, type_info);
	    loader.RegisterCastFunction(LogicalType::BLOB, new_type, DefaultCasts::ReinterpretCast, 1);
        loader.RegisterCastFunction(new_type, LogicalType::BLOB, DefaultCasts::ReinterpretCast, 1);
        return new_type;
    }
    


struct DSREQBindData : public FunctionData {
	DSREQBindData() {
	}
	explicit DSREQBindData(int32_t k) : k(k) {
	}

	unique_ptr<FunctionData> Copy() const override {
		auto copy = make_uniq<DSREQBindData>(k);
		
		copy->hra = hra;
		
		return std::move(copy);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<DSREQBindData>();
		
		return k == other.k && hra == other.hra;
		
	}

    int32_t k;
    
    // true: high-rank accuracy (the library default), false: low-rank accuracy.
    bool hra = true;
    
};


unique_ptr<FunctionData> DSREQBind(BindAggregateFunctionInput &input) {
	auto &context = input.GetClientContext();
	auto &arguments = input.GetArguments();
	if (arguments[0]->HasParameter()) {
		throw ParameterNotResolvedException();
	}
	if (!arguments[0]->IsFoldable()) {
		throw BinderException("REQ can only take a constant K value");
	}
	Value k_val = ExpressionExecutor::EvaluateScalar(context, *arguments[0]);
	if (k_val.IsNull()) {
		throw BinderException("REQ K value cannot be NULL");
	}

	auto actual_k = k_val.GetValue<int32_t>();

	
	// Validate K parameter: must be in range [4, 1024]
	if (actual_k < 4 || actual_k > 1024) {
		throw BinderException("REQ K value must be between 4 and 1024, got: " + std::to_string(actual_k));
	}
	

	auto bind_data = make_uniq<DSREQBindData>(actual_k);

	
	// Optional trailing constant: datasketch_req(k, data, hra).
	if (arguments.size() == 3) {
		if (arguments[2]->HasParameter()) {
			throw ParameterNotResolvedException();
		}
		if (!arguments[2]->IsFoldable()) {
			throw BinderException("REQ hra must be a constant");
		}
		Value option_val = ExpressionExecutor::EvaluateScalar(context, *arguments[2]);
		if (option_val.IsNull()) {
			throw BinderException("REQ hra cannot be NULL");
		}
		
		bind_data->hra = option_val.GetValue<bool>();
		
	}
	

	// The constant arguments stay in the argument list (DuckDB 2.0 plan serialization and the common subplan
	// optimizer only see the arguments, not the bind data); the update callbacks read the data at index 1.
	return std::move(bind_data);
}


    
    template <class T>
    
    struct DSREQState
    {
        
        datasketches::req_sketch<DSItem<T>> *sketch = nullptr;
        

        ~DSREQState()
        {
            if (sketch)
            {
                delete sketch;
            }
        }

        void CreateSketch(const DSREQBindData &bind)
        {
            D_ASSERT(!sketch);
            
            D_ASSERT(bind.k >= 4);
            D_ASSERT(bind.k <= 1024);
            sketch = new datasketches::req_sketch<DSItem<T>>(bind.k, bind.hra);
            
        }

        
        // The state of a merge aggregate starts from the first input sketch's configuration where the
        // library requires it: REQ sketches can only be merged with sketches of the same rank accuracy mode.
        void CreateSketchForMerge(const DSREQBindData &bind, const datasketches::req_sketch<DSItem<T>> &first)
        {
            
            D_ASSERT(!sketch);
            sketch = new datasketches::req_sketch<DSItem<T>>(bind.k, first.is_HRA());
            
        }
        

        void CreateSketch(const DSREQState &existing)
        {
            if (existing.sketch)
            {
                
                sketch = new datasketches::req_sketch<DSItem<T>>(*existing.sketch);
                
            }
        }

        
        datasketches::req_sketch<DSItem<T>> deserialize_sketch(const string_t &data)
        {
            try {
                return datasketches::req_sketch<DSItem<T>>::deserialize(data.GetDataUnsafe(), data.GetSize());
            } catch (const std::exception &e) {
                throw InvalidInputException("Failed to deserialize REQ sketch: %s", e.what());
            }
        }
        
    };



    
    static LogicalType CreateREQSketchType(ExtensionLoader &loader, LogicalType embedded_type)
    {
        auto new_type = LogicalType(LogicalTypeId::BLOB);
        auto type_suffix = toLowerCase(embedded_type.ToString());
        auto new_type_name = "sketch_req_" + type_suffix;


        new_type = new_type.WithAlias(new_type_name);
        auto type_info = CreateTypeInfo(new_type_name, new_type);
        type_info.temporary = false;
        type_info.internal = true;
        type_info.comment = "Sketch type for REQ sketch with embedded type " + embedded_type.ToString();
        auto &system_catalog = Catalog::GetSystemCatalog(loader.GetDatabaseInstance());
        auto data = CatalogTransaction::GetSystemTransaction(loader.GetDatabaseInstance());
        system_catalog.CreateType(data, type_info);
	    loader.RegisterCastFunction(LogicalType::BLOB, new_type, DefaultCasts::ReinterpretCast, 1);
        loader.RegisterCastFunction(new_type, LogicalType::BLOB, DefaultCasts::ReinterpretCast, 1);
        return new_type;
    }
    


struct DSTDigestBindData : public FunctionData {
	DSTDigestBindData() {
	}
	explicit DSTDigestBindData(int32_t k) : k(k) {
	}

	unique_ptr<FunctionData> Copy() const override {
		auto copy = make_uniq<DSTDigestBindData>(k);
		
		return std::move(copy);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<DSTDigestBindData>();
		
		return k == other.k;
		
	}

    int32_t k;
    
};


unique_ptr<FunctionData> DSTDigestBind(BindAggregateFunctionInput &input) {
	auto &context = input.GetClientContext();
	auto &arguments = input.GetArguments();
	if (arguments[0]->HasParameter()) {
		throw ParameterNotResolvedException();
	}
	if (!arguments[0]->IsFoldable()) {
		throw BinderException("TDigest can only take a constant K value");
	}
	Value k_val = ExpressionExecutor::EvaluateScalar(context, *arguments[0]);
	if (k_val.IsNull()) {
		throw BinderException("TDigest K value cannot be NULL");
	}

	auto actual_k = k_val.GetValue<int32_t>();

	
	// Validate K parameter: must be positive (TDigest compression parameter)
	if (actual_k <= 0) {
		throw BinderException("TDigest K (compression) value must be positive, got: " + std::to_string(actual_k));
	}
	

	auto bind_data = make_uniq<DSTDigestBindData>(actual_k);

	

	// The constant arguments stay in the argument list (DuckDB 2.0 plan serialization and the common subplan
	// optimizer only see the arguments, not the bind data); the update callbacks read the data at index 1.
	return std::move(bind_data);
}


    
    template <class T>
    
    struct DSTDigestState
    {
        
        datasketches::tdigest<DSItem<T>> *sketch = nullptr;
        

        ~DSTDigestState()
        {
            if (sketch)
            {
                delete sketch;
            }
        }

        void CreateSketch(const DSTDigestBindData &bind)
        {
            D_ASSERT(!sketch);
            
            sketch = new datasketches::tdigest<DSItem<T>>(bind.k);
            
        }

        
        // The state of a merge aggregate starts from the first input sketch's configuration where the
        // library requires it: REQ sketches can only be merged with sketches of the same rank accuracy mode.
        void CreateSketchForMerge(const DSTDigestBindData &bind, const datasketches::tdigest<DSItem<T>> &first)
        {
            
            CreateSketch(bind);
            
        }
        

        void CreateSketch(const DSTDigestState &existing)
        {
            if (existing.sketch)
            {
                
                sketch = new datasketches::tdigest<DSItem<T>>(*existing.sketch);
                
            }
        }

        
        datasketches::tdigest<DSItem<T>> deserialize_sketch(const string_t &data)
        {
            try {
                return datasketches::tdigest<DSItem<T>>::deserialize(data.GetDataUnsafe(), data.GetSize());
            } catch (const std::exception &e) {
                throw InvalidInputException("Failed to deserialize TDigest sketch: %s", e.what());
            }
        }
        
    };



    
    static LogicalType CreateTDigestSketchType(ExtensionLoader &loader, LogicalType embedded_type)
    {
        auto new_type = LogicalType(LogicalTypeId::BLOB);
        auto type_suffix = toLowerCase(embedded_type.ToString());
        auto new_type_name = "sketch_tdigest_" + type_suffix;


        new_type = new_type.WithAlias(new_type_name);
        auto type_info = CreateTypeInfo(new_type_name, new_type);
        type_info.temporary = false;
        type_info.internal = true;
        type_info.comment = "Sketch type for TDigest sketch with embedded type " + embedded_type.ToString();
        auto &system_catalog = Catalog::GetSystemCatalog(loader.GetDatabaseInstance());
        auto data = CatalogTransaction::GetSystemTransaction(loader.GetDatabaseInstance());
        system_catalog.CreateType(data, type_info);
	    loader.RegisterCastFunction(LogicalType::BLOB, new_type, DefaultCasts::ReinterpretCast, 1);
        loader.RegisterCastFunction(new_type, LogicalType::BLOB, DefaultCasts::ReinterpretCast, 1);
        return new_type;
    }
    


struct DSHLLBindData : public FunctionData {
	DSHLLBindData() {
	}
	explicit DSHLLBindData(int32_t k) : k(k) {
	}

	unique_ptr<FunctionData> Copy() const override {
		auto copy = make_uniq<DSHLLBindData>(k);
		
		copy->target_type = target_type;
		
		return std::move(copy);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<DSHLLBindData>();
		
		return k == other.k && target_type == other.target_type;
		
	}

    int32_t k;
    
    datasketches::target_hll_type target_type = datasketches::HLL_4;
    
};


unique_ptr<FunctionData> DSHLLBind(BindAggregateFunctionInput &input) {
	auto &context = input.GetClientContext();
	auto &arguments = input.GetArguments();
	if (arguments[0]->HasParameter()) {
		throw ParameterNotResolvedException();
	}
	if (!arguments[0]->IsFoldable()) {
		throw BinderException("HLL can only take a constant K value");
	}
	Value k_val = ExpressionExecutor::EvaluateScalar(context, *arguments[0]);
	if (k_val.IsNull()) {
		throw BinderException("HLL K value cannot be NULL");
	}

	auto actual_k = k_val.GetValue<int32_t>();

	
	// Validate K parameter: lg_k must be in range [4, 21] for HLL
	if (actual_k < 4 || actual_k > 21) {
		throw BinderException("HLL K (lg_k) value must be between 4 and 21, got: " + std::to_string(actual_k));
	}
	

	auto bind_data = make_uniq<DSHLLBindData>(actual_k);

	
	// Optional trailing constant: datasketch_hll(k, data, target_type).
	if (arguments.size() == 3) {
		if (arguments[2]->HasParameter()) {
			throw ParameterNotResolvedException();
		}
		if (!arguments[2]->IsFoldable()) {
			throw BinderException("HLL target_type must be a constant");
		}
		Value option_val = ExpressionExecutor::EvaluateScalar(context, *arguments[2]);
		if (option_val.IsNull()) {
			throw BinderException("HLL target_type cannot be NULL");
		}
		
		bind_data->target_type = DSParseHLLTargetType(option_val.GetValue<string>());
		
	}
	

	// The constant arguments stay in the argument list (DuckDB 2.0 plan serialization and the common subplan
	// optimizer only see the arguments, not the bind data); the update callbacks read the data at index 1.
	return std::move(bind_data);
}


    
    struct DSHLLState
    {
        
        datasketches::hll_sketch *sketch = nullptr;
        

        ~DSHLLState()
        {
            if (sketch)
            {
                delete sketch;
            }
        }

        void CreateSketch(const DSHLLBindData &bind)
        {
            D_ASSERT(!sketch);
            
            sketch = new datasketches::hll_sketch(bind.k, bind.target_type);
            
        }

        

        void CreateSketch(const DSHLLState &existing)
        {
            if (existing.sketch)
            {
                
                sketch = new datasketches::hll_sketch(*existing.sketch);
                
            }
        }

        
        datasketches::hll_sketch deserialize_sketch(const string_t &data)
        {
            try {
                return datasketches::hll_sketch::deserialize(data.GetDataUnsafe(), data.GetSize());
            } catch (const std::exception &e) {
                throw InvalidInputException("Failed to deserialize HLL sketch: %s", e.what());
            }
        }
        
    };



    
    static LogicalType CreateHLLCountingSketchType(ExtensionLoader &loader)
    {
        auto new_type = LogicalType(LogicalTypeId::BLOB);
        auto new_type_name = "sketch_hll";
        auto type_info = CreateTypeInfo(new_type_name, LogicalType::BLOB);
        type_info.temporary = false;
        type_info.internal = true;
        type_info.comment = "Sketch type for HLL sketch";
        new_type = new_type.WithAlias(new_type_name);
        auto &system_catalog = Catalog::GetSystemCatalog(loader.GetDatabaseInstance());
        auto data = CatalogTransaction::GetSystemTransaction(loader.GetDatabaseInstance());
        system_catalog.CreateType(data, type_info);
	    loader.RegisterCastFunction(LogicalType::BLOB, new_type, DefaultCasts::ReinterpretCast, 1);
        loader.RegisterCastFunction(new_type, LogicalType::BLOB, DefaultCasts::ReinterpretCast, 1);
        return new_type;
    }
    


struct DSCPCBindData : public FunctionData {
	DSCPCBindData() {
	}
	explicit DSCPCBindData(int32_t k) : k(k) {
	}

	unique_ptr<FunctionData> Copy() const override {
		auto copy = make_uniq<DSCPCBindData>(k);
		
		return std::move(copy);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<DSCPCBindData>();
		
		return k == other.k;
		
	}

    int32_t k;
    
};


unique_ptr<FunctionData> DSCPCBind(BindAggregateFunctionInput &input) {
	auto &context = input.GetClientContext();
	auto &arguments = input.GetArguments();
	if (arguments[0]->HasParameter()) {
		throw ParameterNotResolvedException();
	}
	if (!arguments[0]->IsFoldable()) {
		throw BinderException("CPC can only take a constant K value");
	}
	Value k_val = ExpressionExecutor::EvaluateScalar(context, *arguments[0]);
	if (k_val.IsNull()) {
		throw BinderException("CPC K value cannot be NULL");
	}

	auto actual_k = k_val.GetValue<int32_t>();

	
	// Validate K parameter: lg_k must be in range [4, 26] for CPC
	if (actual_k < 4 || actual_k > 26) {
		throw BinderException("CPC K (lg_k) value must be between 4 and 26, got: " + std::to_string(actual_k));
	}
	

	auto bind_data = make_uniq<DSCPCBindData>(actual_k);

	

	// The constant arguments stay in the argument list (DuckDB 2.0 plan serialization and the common subplan
	// optimizer only see the arguments, not the bind data); the update callbacks read the data at index 1.
	return std::move(bind_data);
}


    
    struct DSCPCState
    {
        
        datasketches::cpc_sketch *sketch = nullptr;
        

        ~DSCPCState()
        {
            if (sketch)
            {
                delete sketch;
            }
        }

        void CreateSketch(const DSCPCBindData &bind)
        {
            D_ASSERT(!sketch);
            
            sketch = new datasketches::cpc_sketch(bind.k);
            
        }

        

        void CreateSketch(const DSCPCState &existing)
        {
            if (existing.sketch)
            {
                
                sketch = new datasketches::cpc_sketch(*existing.sketch);
                
            }
        }

        
        datasketches::cpc_sketch deserialize_sketch(const string_t &data)
        {
            try {
                return datasketches::cpc_sketch::deserialize(data.GetDataUnsafe(), data.GetSize());
            } catch (const std::exception &e) {
                throw InvalidInputException("Failed to deserialize CPC sketch: %s", e.what());
            }
        }
        
    };



    
    static LogicalType CreateCPCCountingSketchType(ExtensionLoader &loader)
    {
        auto new_type = LogicalType(LogicalTypeId::BLOB);
        auto new_type_name = "sketch_cpc";
        auto type_info = CreateTypeInfo(new_type_name, LogicalType::BLOB);
        type_info.temporary = false;
        type_info.internal = true;
        type_info.comment = "Sketch type for CPC sketch";
        new_type = new_type.WithAlias(new_type_name);
        auto &system_catalog = Catalog::GetSystemCatalog(loader.GetDatabaseInstance());
        auto data = CatalogTransaction::GetSystemTransaction(loader.GetDatabaseInstance());
        system_catalog.CreateType(data, type_info);
	    loader.RegisterCastFunction(LogicalType::BLOB, new_type, DefaultCasts::ReinterpretCast, 1);
        loader.RegisterCastFunction(new_type, LogicalType::BLOB, DefaultCasts::ReinterpretCast, 1);
        return new_type;
    }
    

    struct DSSketchOperationBase {
        template <class STATE>
        static void Initialize(STATE &state)
        {
            state.sketch = nullptr;
        }

        template <class STATE>
        static void Destroy(STATE &state, AggregateInputData &aggr_input_data) {
            if (state.sketch) {
                delete state.sketch;
                state.sketch = nullptr;
            }
        }

        static bool IgnoreNull() { return true; }
    };

    template <class BIND_DATA_TYPE>
    struct DSQuantilesMergeOperation : DSSketchOperationBase
    {
        template <class A_TYPE, class STATE, class OP>
        static void Operation(STATE &state,
                              const A_TYPE &a_data,
                              AggregateUnaryInput &idata)
        {
            // this is a sketch in a_data, so we need to deserialize it.
            auto incoming = state.deserialize_sketch(a_data);
            if (!state.sketch)
            {
                auto &bind_data = idata.input.bind_data->template Cast<BIND_DATA_TYPE>();
                state.CreateSketchForMerge(bind_data, incoming);
            }
            DSMergeSketch(*state.sketch, std::move(incoming));
        }

        template <class INPUT_TYPE, class STATE, class OP>
        static void ConstantOperation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input,
                                    idx_t count) {
            for (idx_t i = 0; i < count; i++) {
                Operation<INPUT_TYPE, STATE, OP>(state, input, unary_input);
            }
        }

        template <class STATE, class OP>
        static void Combine(const STATE &source, STATE &target,
                            AggregateInputData &aggr_input_data)
        {
            // A source partial that received no tuples has a null sketch
            // (e.g. an empty worker partition under parallel aggregation),
            // so there is nothing to merge.
            if (!source.sketch)
            {
                return;
            }
            if (!target.sketch)
            {
                target.CreateSketch(source);
            }
            else
            {
                DSMergeSketch(*target.sketch, *source.sketch);
            }
        }

        template <class T, class STATE>
        static void Finalize(STATE &state, T &target,
                             AggregateFinalizeData &finalize_data)
        {
            if (!state.sketch)
            {
                finalize_data.ReturnNull();
            }
            else
            {
                auto serialized_data = state.sketch->serialize();
                auto sketch_string = std::string(serialized_data.begin(), serialized_data.end());
                target = StringVector::AddStringOrBlob(finalize_data.result, sketch_string);
            }
        }
    };

    template <class BIND_DATA_TYPE>
    struct DSQuantilesCreateOperation : DSSketchOperationBase
    {
        template <class A_TYPE, class STATE, class OP>
        static void Operation(STATE &state,
                              const A_TYPE &a_data,
                              AggregateUnaryInput &idata)
        {
            if (!state.sketch)
            {
                auto &bind_data = idata.input.bind_data->template Cast<BIND_DATA_TYPE>();
                state.CreateSketch(bind_data);
            }

            state.sketch->update(DSItemIn(a_data));
        }

        template <class INPUT_TYPE, class STATE, class OP>
        static void ConstantOperation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input,
                                    idx_t count) {
            for (idx_t i = 0; i < count; i++) {
                Operation<INPUT_TYPE, STATE, OP>(state, input, unary_input);
            }
        }

        template <class STATE, class OP>
        static void Combine(const STATE &source, STATE &target,
                            AggregateInputData &aggr_input_data)
        {
            // A source partial that received no tuples has a null sketch
            // (e.g. an empty worker partition under parallel aggregation),
            // so there is nothing to merge.
            if (!source.sketch)
            {
                return;
            }
            if (!target.sketch)
            {
                target.CreateSketch(source);
            }
            else
            {
                DSMergeSketch(*target.sketch, *source.sketch);
            }
        }

        template <class T, class STATE>
        static void Finalize(STATE &state, T &target,
                             AggregateFinalizeData &finalize_data)
        {
            if (!state.sketch)
            {
                finalize_data.ReturnNull();
            }
            else
            {
                auto serialized_data = state.sketch->serialize();
                auto sketch_string = std::string(serialized_data.begin(), serialized_data.end());
                target = StringVector::AddStringOrBlob(finalize_data.result, sketch_string);
            }
        }
    };

    template <class BIND_DATA_TYPE>
    struct DSHLLCreateOperation : DSSketchOperationBase
    {
        template <class A_TYPE, class STATE, class OP>
        static void Operation(STATE &state,
                              const A_TYPE &a_data,
                              AggregateUnaryInput &idata)
        {
            if (!state.sketch)
            {
                auto &bind_data = idata.input.bind_data->template Cast<BIND_DATA_TYPE>();
                state.CreateSketch(bind_data);
            }

            if constexpr (std::is_same_v<A_TYPE, duckdb::string_t>) {
                state.sketch->update(a_data.GetData(), a_data.GetSize());
            } else {
                state.sketch->update(a_data);
            }
        }

        template <class INPUT_TYPE, class STATE, class OP>
        static void ConstantOperation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input,
                                    idx_t count) {
            for (idx_t i = 0; i < count; i++) {
                Operation<INPUT_TYPE, STATE, OP>(state, input, unary_input);
            }
        }

        template <class STATE, class OP>
        static void Combine(const STATE &source, STATE &target,
                            AggregateInputData &aggr_input_data)
        {
            if (!target.sketch)
            {
                target.CreateSketch(source);
            }
            else
            {
                auto &bind_data = aggr_input_data.bind_data->template Cast<BIND_DATA_TYPE>();
                datasketches::hll_union u(target.sketch->get_lg_config_k());
                u.update(*target.sketch);
                if(source.sketch) {
                    u.update(*source.sketch);
                }
                *target.sketch = u.get_result(bind_data.target_type);
            }
        }

        template <class T, class STATE>
        static void Finalize(STATE &state, T &target,
                             AggregateFinalizeData &finalize_data)
        {
            if (!state.sketch)
            {
                finalize_data.ReturnNull();
            }
            else
            {
                auto serialized_data = state.sketch->serialize_updatable();
                auto sketch_string = std::string(serialized_data.begin(), serialized_data.end());
                target = StringVector::AddStringOrBlob(finalize_data.result, sketch_string);
            }
        }
    };

    template <class BIND_DATA_TYPE>
    struct DSHLLMergeOperation : DSSketchOperationBase
    {

        template <class A_TYPE, class STATE, class OP>
        static void Operation(STATE &state,
                              const A_TYPE &a_data,
                              AggregateUnaryInput &idata)
        {
            auto &bind_data = idata.input.bind_data->template Cast<BIND_DATA_TYPE>();

            if (!state.sketch)
            {
                state.CreateSketch(bind_data);
            }

            auto a_sketch = state.deserialize_sketch(a_data);

            datasketches::hll_union u(bind_data.k);
            if(state.sketch) {
                u.update(*state.sketch);
            }
            u.update(a_sketch);

            *state.sketch = u.get_result(bind_data.target_type);
        }

        template <class INPUT_TYPE, class STATE, class OP>
        static void ConstantOperation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input,
                                    idx_t count) {
            for (idx_t i = 0; i < count; i++) {
                Operation<INPUT_TYPE, STATE, OP>(state, input, unary_input);
            }
        }

        template <class STATE, class OP>
        static void Combine(const STATE &source, STATE &target,
                            AggregateInputData &aggr_input_data)
        {
            if (!target.sketch)
            {
                target.CreateSketch(source);
            }
            else
            {
                auto &bind_data = aggr_input_data.bind_data->template Cast<BIND_DATA_TYPE>();
                datasketches::hll_union u(target.sketch->get_lg_config_k());
                if(source.sketch) {
                    u.update(*source.sketch);
                }
                u.update(*target.sketch);

                *target.sketch = u.get_result(bind_data.target_type);
            }
        }

        template <class T, class STATE>
        static void Finalize(STATE &state, T &target,
                             AggregateFinalizeData &finalize_data)
        {
            if (!state.sketch)
            {
                finalize_data.ReturnNull();
            }
            else
            {
                auto serialized_data = state.sketch->serialize_updatable();
                auto sketch_string = std::string(serialized_data.begin(), serialized_data.end());
                target = StringVector::AddStringOrBlob(finalize_data.result, sketch_string);
            }
        }
    };


    template <class BIND_DATA_TYPE>
    struct DSCPCMergeOperation : DSSketchOperationBase
    {
        template <class A_TYPE, class STATE, class OP>
        static void Operation(STATE &state,
                              const A_TYPE &a_data,
                              AggregateUnaryInput &idata)
        {
            auto &bind_data = idata.input.bind_data->template Cast<BIND_DATA_TYPE>();

            if (!state.sketch)
            {
                state.CreateSketch(bind_data);
            }

            auto a_sketch = state.deserialize_sketch(a_data);
            datasketches::cpc_union u(bind_data.k);
            if(state.sketch) {
                u.update(*state.sketch);
            }
            u.update(a_sketch);

            *state.sketch = u.get_result();
        }

        template <class INPUT_TYPE, class STATE, class OP>
        static void ConstantOperation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input,
                                    idx_t count) {
            for (idx_t i = 0; i < count; i++) {
                Operation<INPUT_TYPE, STATE, OP>(state, input, unary_input);
            }
        }

        template <class STATE, class OP>
        static void Combine(const STATE &source, STATE &target,
                            AggregateInputData &aggr_input_data)
        {
            if (!target.sketch)
            {
                target.CreateSketch(source);
            }
            else
            {
                datasketches::cpc_union u(target.sketch->get_lg_k());
                if(source.sketch) {
                    u.update(*source.sketch);
                }
                u.update(*target.sketch);
                *target.sketch = u.get_result();
            }
        }

        template <class T, class STATE>
        static void Finalize(STATE &state, T &target,
                             AggregateFinalizeData &finalize_data)
        {
            if (!state.sketch)
            {
                finalize_data.ReturnNull();
            }
            else
            {
                auto serialized_data = state.sketch->serialize();
                auto sketch_string = std::string(serialized_data.begin(), serialized_data.end());
                target = StringVector::AddStringOrBlob(finalize_data.result, sketch_string);
            }
        }
    };

    template <class BIND_DATA_TYPE>
    struct DSCPCCreateOperation : DSSketchOperationBase
    {
        template <class A_TYPE, class STATE, class OP>
        static void Operation(STATE &state,
                              const A_TYPE &a_data,
                              AggregateUnaryInput &idata)
        {
            if (!state.sketch)
            {
                auto &bind_data = idata.input.bind_data->template Cast<BIND_DATA_TYPE>();
                state.CreateSketch(bind_data);
            }

            if constexpr (std::is_same_v<A_TYPE, duckdb::string_t>) {
                state.sketch->update(a_data.GetData(), a_data.GetSize());
            } else {
                state.sketch->update(a_data);
            }
        }

        template <class INPUT_TYPE, class STATE, class OP>
        static void ConstantOperation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input,
                                    idx_t count) {
            for (idx_t i = 0; i < count; i++) {
                Operation<INPUT_TYPE, STATE, OP>(state, input, unary_input);
            }
        }


        template <class STATE, class OP>
        static void Combine(const STATE &source, STATE &target,
                            AggregateInputData &aggr_input_data)
        {
            if (!target.sketch)
            {
                target.CreateSketch(source);
            }
            else
            {
                datasketches::cpc_union u(target.sketch->get_lg_k());
                u.update(*target.sketch);
                if(source.sketch) {
                    u.update(*source.sketch);
                }
                *target.sketch = u.get_result();
            }
        }


        template <class T, class STATE>
        static void Finalize(STATE &state, T &target,
                             AggregateFinalizeData &finalize_data)
        {
            if (!state.sketch)
            {
                finalize_data.ReturnNull();
            }
            else
            {
                auto serialized_data = state.sketch->serialize();
                auto sketch_string = std::string(serialized_data.begin(), serialized_data.end());
                target = StringVector::AddStringOrBlob(finalize_data.result, sketch_string);
            }
        }
    };




    


template <class T>

static inline void DSQuantilesis_empty(DataChunk &args, ExpressionState &state, Vector &result)
{
    // Get the references to the incoming vectors.
    D_ASSERT(args.ColumnCount() == 1);

    
        auto &sketch_vector = args.data[0];

    
        
        UnaryExecutor::Execute
        <string_t,bool>
        (
        sketch_vector,result,
        [&](string_t sketch_data) {

            
            auto sketch = [&]() {
                try {
                    return datasketches::quantiles_sketch<DSItem<T>>::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
                } catch (const std::exception &e) {
                    throw InvalidInputException("Failed to deserialize Quantiles sketch: %s", e.what());
                }
            }();
return sketch.is_empty();
        });

}

    


template <class T>

static inline void DSQuantilesk(DataChunk &args, ExpressionState &state, Vector &result)
{
    // Get the references to the incoming vectors.
    D_ASSERT(args.ColumnCount() == 1);

    
        auto &sketch_vector = args.data[0];

    
        
        UnaryExecutor::Execute
        <string_t,uint16_t>
        (
        sketch_vector,result,
        [&](string_t sketch_data) {

            
            auto sketch = [&]() {
                try {
                    return datasketches::quantiles_sketch<DSItem<T>>::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
                } catch (const std::exception &e) {
                    throw InvalidInputException("Failed to deserialize Quantiles sketch: %s", e.what());
                }
            }();
return sketch.get_k();
        });

}

    


template <class T>

static inline void DSQuantilescdf(DataChunk &args, ExpressionState &state, Vector &result)
{
    // Get the references to the incoming vectors.
    D_ASSERT(args.ColumnCount() == 3);

    
        auto &sketch_vector = args.data[0];
        auto &split_points_vector = args.data[1];
        auto &inclusive_vector = args.data[2];

    
        
                    auto &split_points_list_children = ListVector::GetChild(split_points_vector);

                    UnifiedVectorFormat split_points_children_unified;
                    split_points_list_children.ToUnifiedFormat(split_points_children_unified);

                    const T *split_points_list_children_data = UnifiedVectorFormat::GetData<T>(split_points_children_unified);
                    
        TernaryExecutor::Execute
        <string_t,list_entry_t,bool,list_entry_t>
        (
        sketch_vector,split_points_vector,inclusive_vector,result,
        [&](string_t sketch_data,list_entry_t split_points_data,bool inclusive_data) {

            
            auto sketch = [&]() {
                try {
                    return datasketches::quantiles_sketch<DSItem<T>>::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
                } catch (const std::exception &e) {
                    throw InvalidInputException("Failed to deserialize Quantiles sketch: %s", e.what());
                }
            }();

                    std::vector<DSItem<T>> passing_points(split_points_data.length);
                    for (idx_t i = 0; i < split_points_data.length; i++)
                    {
                        passing_points[i] = DSItemIn(split_points_list_children_data[split_points_children_unified.sel->get_index(i + split_points_data.offset)]);
                    }
                    
auto cdf_result = sketch.get_CDF(passing_points.data(), split_points_data.length, inclusive_data);
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
                
        });

}

    


template <class T>

static inline void DSQuantilespmf(DataChunk &args, ExpressionState &state, Vector &result)
{
    // Get the references to the incoming vectors.
    D_ASSERT(args.ColumnCount() == 3);

    
        auto &sketch_vector = args.data[0];
        auto &split_points_vector = args.data[1];
        auto &inclusive_vector = args.data[2];

    
        
                    auto &split_points_list_children = ListVector::GetChild(split_points_vector);

                    UnifiedVectorFormat split_points_children_unified;
                    split_points_list_children.ToUnifiedFormat(split_points_children_unified);

                    const T *split_points_list_children_data = UnifiedVectorFormat::GetData<T>(split_points_children_unified);
                    
        TernaryExecutor::Execute
        <string_t,list_entry_t,bool,list_entry_t>
        (
        sketch_vector,split_points_vector,inclusive_vector,result,
        [&](string_t sketch_data,list_entry_t split_points_data,bool inclusive_data) {

            
            auto sketch = [&]() {
                try {
                    return datasketches::quantiles_sketch<DSItem<T>>::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
                } catch (const std::exception &e) {
                    throw InvalidInputException("Failed to deserialize Quantiles sketch: %s", e.what());
                }
            }();

                    std::vector<DSItem<T>> passing_points(split_points_data.length);
                    for (idx_t i = 0; i < split_points_data.length; i++)
                    {
                        passing_points[i] = DSItemIn(split_points_list_children_data[split_points_children_unified.sel->get_index(i + split_points_data.offset)]);
                    }
                    
auto pmf_result = sketch.get_PMF(passing_points.data(), split_points_data.length, inclusive_data);
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
                
        });

}

    


template <class T>

static inline void DSQuantilesnormalized_rank_error(DataChunk &args, ExpressionState &state, Vector &result)
{
    // Get the references to the incoming vectors.
    D_ASSERT(args.ColumnCount() == 2);

    
        auto &sketch_vector = args.data[0];
        auto &is_pmf_vector = args.data[1];

    
        
        BinaryExecutor::Execute
        <string_t,bool,double>
        (
        sketch_vector,is_pmf_vector,result,
        [&](string_t sketch_data,bool is_pmf_data) {

            
            auto sketch = [&]() {
                try {
                    return datasketches::quantiles_sketch<DSItem<T>>::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
                } catch (const std::exception &e) {
                    throw InvalidInputException("Failed to deserialize Quantiles sketch: %s", e.what());
                }
            }();
return sketch.get_normalized_rank_error(is_pmf_data);
        });

}

    


template <class T>

static inline void DSQuantilesdescribe(DataChunk &args, ExpressionState &state, Vector &result)
{
    // Get the references to the incoming vectors.
    D_ASSERT(args.ColumnCount() == 3);

    
        auto &sketch_vector = args.data[0];
        auto &include_levels_vector = args.data[1];
        auto &include_items_vector = args.data[2];

    
        
        TernaryExecutor::Execute
        <string_t,bool,bool,string_t>
        (
        sketch_vector,include_levels_vector,include_items_vector,result,
        [&](string_t sketch_data,bool include_levels_data,bool include_items_data) {

            
            auto sketch = [&]() {
                try {
                    return datasketches::quantiles_sketch<DSItem<T>>::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
                } catch (const std::exception &e) {
                    throw InvalidInputException("Failed to deserialize Quantiles sketch: %s", e.what());
                }
            }();
return StringVector::AddString(result, sketch.to_string(include_levels_data, include_items_data));
        });

}

    


template <class T>

static inline void DSQuantilesrank(DataChunk &args, ExpressionState &state, Vector &result)
{
    // Get the references to the incoming vectors.
    D_ASSERT(args.ColumnCount() == 3);

    
        auto &sketch_vector = args.data[0];
        auto &item_vector = args.data[1];
        auto &inclusive_vector = args.data[2];

    
        
        TernaryExecutor::Execute
        <string_t,T,bool,double>
        (
        sketch_vector,item_vector,inclusive_vector,result,
        [&](string_t sketch_data,T item_data,bool inclusive_data) {

            
            auto sketch = [&]() {
                try {
                    return datasketches::quantiles_sketch<DSItem<T>>::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
                } catch (const std::exception &e) {
                    throw InvalidInputException("Failed to deserialize Quantiles sketch: %s", e.what());
                }
            }();
return sketch.get_rank(DSItemIn(item_data), inclusive_data);
        });

}

    


template <class T>

static inline void DSQuantilesquantile(DataChunk &args, ExpressionState &state, Vector &result)
{
    // Get the references to the incoming vectors.
    D_ASSERT(args.ColumnCount() == 3);

    
        auto &sketch_vector = args.data[0];
        auto &rank_vector = args.data[1];
        auto &inclusive_vector = args.data[2];

    
        
        TernaryExecutor::Execute
        <string_t,double,bool,T>
        (
        sketch_vector,rank_vector,inclusive_vector,result,
        [&](string_t sketch_data,double rank_data,bool inclusive_data) {

            
            auto sketch = [&]() {
                try {
                    return datasketches::quantiles_sketch<DSItem<T>>::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
                } catch (const std::exception &e) {
                    throw InvalidInputException("Failed to deserialize Quantiles sketch: %s", e.what());
                }
            }();
return DSItemOut<T>(result, sketch.get_quantile(rank_data, inclusive_data));
        });

}

    


template <class T>

static inline void DSQuantilesn(DataChunk &args, ExpressionState &state, Vector &result)
{
    // Get the references to the incoming vectors.
    D_ASSERT(args.ColumnCount() == 1);

    
        auto &sketch_vector = args.data[0];

    
        
        UnaryExecutor::Execute
        <string_t,uint64_t>
        (
        sketch_vector,result,
        [&](string_t sketch_data) {

            
            auto sketch = [&]() {
                try {
                    return datasketches::quantiles_sketch<DSItem<T>>::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
                } catch (const std::exception &e) {
                    throw InvalidInputException("Failed to deserialize Quantiles sketch: %s", e.what());
                }
            }();
return sketch.get_n();
        });

}

    


template <class T>

static inline void DSQuantilesis_estimation_mode(DataChunk &args, ExpressionState &state, Vector &result)
{
    // Get the references to the incoming vectors.
    D_ASSERT(args.ColumnCount() == 1);

    
        auto &sketch_vector = args.data[0];

    
        
        UnaryExecutor::Execute
        <string_t,bool>
        (
        sketch_vector,result,
        [&](string_t sketch_data) {

            
            auto sketch = [&]() {
                try {
                    return datasketches::quantiles_sketch<DSItem<T>>::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
                } catch (const std::exception &e) {
                    throw InvalidInputException("Failed to deserialize Quantiles sketch: %s", e.what());
                }
            }();
return sketch.is_estimation_mode();
        });

}

    


template <class T>

static inline void DSQuantilesnum_retained(DataChunk &args, ExpressionState &state, Vector &result)
{
    // Get the references to the incoming vectors.
    D_ASSERT(args.ColumnCount() == 1);

    
        auto &sketch_vector = args.data[0];

    
        
        UnaryExecutor::Execute
        <string_t,uint64_t>
        (
        sketch_vector,result,
        [&](string_t sketch_data) {

            
            auto sketch = [&]() {
                try {
                    return datasketches::quantiles_sketch<DSItem<T>>::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
                } catch (const std::exception &e) {
                    throw InvalidInputException("Failed to deserialize Quantiles sketch: %s", e.what());
                }
            }();
return sketch.get_num_retained();
        });

}

    


template <class T>

static inline void DSQuantilesmin_item(DataChunk &args, ExpressionState &state, Vector &result)
{
    // Get the references to the incoming vectors.
    D_ASSERT(args.ColumnCount() == 1);

    
        auto &sketch_vector = args.data[0];

    
        
        UnaryExecutor::Execute
        <string_t,T>
        (
        sketch_vector,result,
        [&](string_t sketch_data) {

            
            auto sketch = [&]() {
                try {
                    return datasketches::quantiles_sketch<DSItem<T>>::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
                } catch (const std::exception &e) {
                    throw InvalidInputException("Failed to deserialize Quantiles sketch: %s", e.what());
                }
            }();
return DSItemOut<T>(result, sketch.get_min_item());
        });

}

    


template <class T>

static inline void DSQuantilesmax_item(DataChunk &args, ExpressionState &state, Vector &result)
{
    // Get the references to the incoming vectors.
    D_ASSERT(args.ColumnCount() == 1);

    
        auto &sketch_vector = args.data[0];

    
        
        UnaryExecutor::Execute
        <string_t,T>
        (
        sketch_vector,result,
        [&](string_t sketch_data) {

            
            auto sketch = [&]() {
                try {
                    return datasketches::quantiles_sketch<DSItem<T>>::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
                } catch (const std::exception &e) {
                    throw InvalidInputException("Failed to deserialize Quantiles sketch: %s", e.what());
                }
            }();
return DSItemOut<T>(result, sketch.get_max_item());
        });

}

    


template <class T>

static inline void DSQuantilesquantiles(DataChunk &args, ExpressionState &state, Vector &result)
{
    // Get the references to the incoming vectors.
    D_ASSERT(args.ColumnCount() == 3);

    
        auto &sketch_vector = args.data[0];
        auto &ranks_vector = args.data[1];
        auto &inclusive_vector = args.data[2];

    
        
                    auto &ranks_list_children = ListVector::GetChild(ranks_vector);
                    UnifiedVectorFormat ranks_children_unified;
                    ranks_list_children.ToUnifiedFormat(ranks_children_unified);
                    const double *ranks_children_data = UnifiedVectorFormat::GetData<double>(ranks_children_unified);
                    
        TernaryExecutor::Execute
        <string_t,list_entry_t,bool,list_entry_t>
        (
        sketch_vector,ranks_vector,inclusive_vector,result,
        [&](string_t sketch_data,list_entry_t ranks_data,bool inclusive_data) {

            
            auto sketch = [&]() {
                try {
                    return datasketches::quantiles_sketch<DSItem<T>>::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
                } catch (const std::exception &e) {
                    throw InvalidInputException("Failed to deserialize Quantiles sketch: %s", e.what());
                }
            }();

                if (sketch.is_empty())
                {
                    throw InvalidInputException("datasketch_quantiles_quantiles: operation is undefined for an empty sketch");
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
                
        });

}

    


template <class T>

static inline void DSQuantilesranks(DataChunk &args, ExpressionState &state, Vector &result)
{
    // Get the references to the incoming vectors.
    D_ASSERT(args.ColumnCount() == 3);

    
        auto &sketch_vector = args.data[0];
        auto &items_vector = args.data[1];
        auto &inclusive_vector = args.data[2];

    
        
                    auto &items_list_children = ListVector::GetChild(items_vector);
                    UnifiedVectorFormat items_children_unified;
                    items_list_children.ToUnifiedFormat(items_children_unified);
                    const T *items_children_data = UnifiedVectorFormat::GetData<T>(items_children_unified);
                    
        TernaryExecutor::Execute
        <string_t,list_entry_t,bool,list_entry_t>
        (
        sketch_vector,items_vector,inclusive_vector,result,
        [&](string_t sketch_data,list_entry_t items_data,bool inclusive_data) {

            
            auto sketch = [&]() {
                try {
                    return datasketches::quantiles_sketch<DSItem<T>>::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
                } catch (const std::exception &e) {
                    throw InvalidInputException("Failed to deserialize Quantiles sketch: %s", e.what());
                }
            }();

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
                
        });

}

    


template <class T>

static inline void DSQuantilessorted_view(DataChunk &args, ExpressionState &state, Vector &result)
{
    // Get the references to the incoming vectors.
    D_ASSERT(args.ColumnCount() == 1);

    
        auto &sketch_vector = args.data[0];

    
        
        UnaryExecutor::Execute
        <string_t,list_entry_t>
        (
        sketch_vector,result,
        [&](string_t sketch_data) {

            
            auto sketch = [&]() {
                try {
                    return datasketches::quantiles_sketch<DSItem<T>>::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
                } catch (const std::exception &e) {
                    throw InvalidInputException("Failed to deserialize Quantiles sketch: %s", e.what());
                }
            }();

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
                
        });

}

    


    
    template <typename T>
    auto static DSQuantilesMergeAggregate(const LogicalType &type, const LogicalType &result_type) -> AggregateFunction
    
    {
        
        auto fun = AggregateFunction::UnaryAggregate<DSQuantilesState<T>, string_t, string_t, DSQuantilesMergeOperation<DSQuantilesBindData>, AggregateDestructorType::LEGACY>(result_type, result_type);
        DSReadDataArgumentAt<1, DSQuantilesState<T>, string_t, DSQuantilesMergeOperation<DSQuantilesBindData>>(fun);
        return fun;
        
    }



    template <typename T>
    auto static DSQuantilesCreateAggregate(const LogicalType &type, const LogicalType &result_type) -> AggregateFunction
    {
        
        auto fun = AggregateFunction::UnaryAggregate<DSQuantilesState<T>, T, string_t, DSQuantilesCreateOperation<DSQuantilesBindData>, AggregateDestructorType::LEGACY>(type, result_type);
        DSReadDataArgumentAt<1, DSQuantilesState<T>, T, DSQuantilesCreateOperation<DSQuantilesBindData>>(fun);
        return fun;
        
    }


  void LoadQuantilesSketch(ExtensionLoader &loader) {


      
        std::unordered_map<LogicalTypeId, LogicalType> sketch_map_types;
        
            sketch_map_types.insert({ LogicalTypeId::TINYINT, CreateQuantilesSketchType(loader, LogicalType(LogicalTypeId::TINYINT))});
            sketch_map_types.insert({ LogicalTypeId::SMALLINT, CreateQuantilesSketchType(loader, LogicalType(LogicalTypeId::SMALLINT))});
            sketch_map_types.insert({ LogicalTypeId::INTEGER, CreateQuantilesSketchType(loader, LogicalType(LogicalTypeId::INTEGER))});
            sketch_map_types.insert({ LogicalTypeId::BIGINT, CreateQuantilesSketchType(loader, LogicalType(LogicalTypeId::BIGINT))});
            sketch_map_types.insert({ LogicalTypeId::FLOAT, CreateQuantilesSketchType(loader, LogicalType(LogicalTypeId::FLOAT))});
            sketch_map_types.insert({ LogicalTypeId::DOUBLE, CreateQuantilesSketchType(loader, LogicalType(LogicalTypeId::DOUBLE))});
            sketch_map_types.insert({ LogicalTypeId::UTINYINT, CreateQuantilesSketchType(loader, LogicalType(LogicalTypeId::UTINYINT))});
            sketch_map_types.insert({ LogicalTypeId::USMALLINT, CreateQuantilesSketchType(loader, LogicalType(LogicalTypeId::USMALLINT))});
            sketch_map_types.insert({ LogicalTypeId::UINTEGER, CreateQuantilesSketchType(loader, LogicalType(LogicalTypeId::UINTEGER))});
            sketch_map_types.insert({ LogicalTypeId::UBIGINT, CreateQuantilesSketchType(loader, LogicalType(LogicalTypeId::UBIGINT))});
            sketch_map_types.insert({ LogicalTypeId::VARCHAR, CreateQuantilesSketchType(loader, LogicalType(LogicalTypeId::VARCHAR))});
      

      
      {
        ScalarFunctionSet fs("datasketch_quantiles_is_empty");
        std::vector<FunctionDescription> fn_descs;
        
            
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::TINYINT]},LogicalType::BOOLEAN
                ,    DSQuantilesis_empty<int8_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return a boolean indicating if the sketch is empty";
                    desc.examples.push_back("datasketch_quantiles_is_empty(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::TINYINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::SMALLINT]},LogicalType::BOOLEAN
                ,    DSQuantilesis_empty<int16_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return a boolean indicating if the sketch is empty";
                    desc.examples.push_back("datasketch_quantiles_is_empty(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::SMALLINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::INTEGER]},LogicalType::BOOLEAN
                ,    DSQuantilesis_empty<int32_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return a boolean indicating if the sketch is empty";
                    desc.examples.push_back("datasketch_quantiles_is_empty(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::INTEGER] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::BIGINT]},LogicalType::BOOLEAN
                ,    DSQuantilesis_empty<int64_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return a boolean indicating if the sketch is empty";
                    desc.examples.push_back("datasketch_quantiles_is_empty(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::BIGINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::FLOAT]},LogicalType::BOOLEAN
                ,    DSQuantilesis_empty<float>));
                {
                    FunctionDescription desc;
                    desc.description = "Return a boolean indicating if the sketch is empty";
                    desc.examples.push_back("datasketch_quantiles_is_empty(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::FLOAT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::DOUBLE]},LogicalType::BOOLEAN
                ,    DSQuantilesis_empty<double>));
                {
                    FunctionDescription desc;
                    desc.description = "Return a boolean indicating if the sketch is empty";
                    desc.examples.push_back("datasketch_quantiles_is_empty(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::DOUBLE] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UTINYINT]},LogicalType::BOOLEAN
                ,    DSQuantilesis_empty<uint8_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return a boolean indicating if the sketch is empty";
                    desc.examples.push_back("datasketch_quantiles_is_empty(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UTINYINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::USMALLINT]},LogicalType::BOOLEAN
                ,    DSQuantilesis_empty<uint16_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return a boolean indicating if the sketch is empty";
                    desc.examples.push_back("datasketch_quantiles_is_empty(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::USMALLINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UINTEGER]},LogicalType::BOOLEAN
                ,    DSQuantilesis_empty<uint32_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return a boolean indicating if the sketch is empty";
                    desc.examples.push_back("datasketch_quantiles_is_empty(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UINTEGER] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UBIGINT]},LogicalType::BOOLEAN
                ,    DSQuantilesis_empty<uint64_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return a boolean indicating if the sketch is empty";
                    desc.examples.push_back("datasketch_quantiles_is_empty(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UBIGINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::VARCHAR]},LogicalType::BOOLEAN
                ,    DSQuantilesis_empty<string_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return a boolean indicating if the sketch is empty";
                    desc.examples.push_back("datasketch_quantiles_is_empty(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::VARCHAR] };
                    fn_descs.push_back(std::move(desc));
                }
        

        // Every scalar function deserializes a sketch, which throws on invalid input.
        fs.SetFallible();
        CreateScalarFunctionInfo info(std::move(fs));
        for (auto &d : fn_descs) {
            info.descriptions.push_back(std::move(d));
        }

        DSSetCategories(info, {"sketch", "quantiles"});

        DSRegister(loader, info);


      }
      {
        ScalarFunctionSet fs("datasketch_quantiles_k");
        std::vector<FunctionDescription> fn_descs;
        
            
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::TINYINT]},LogicalType::USMALLINT
                ,    DSQuantilesk<int8_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the value of K for this sketch";
                    desc.examples.push_back("datasketch_quantiles_k(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::TINYINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::SMALLINT]},LogicalType::USMALLINT
                ,    DSQuantilesk<int16_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the value of K for this sketch";
                    desc.examples.push_back("datasketch_quantiles_k(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::SMALLINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::INTEGER]},LogicalType::USMALLINT
                ,    DSQuantilesk<int32_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the value of K for this sketch";
                    desc.examples.push_back("datasketch_quantiles_k(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::INTEGER] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::BIGINT]},LogicalType::USMALLINT
                ,    DSQuantilesk<int64_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the value of K for this sketch";
                    desc.examples.push_back("datasketch_quantiles_k(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::BIGINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::FLOAT]},LogicalType::USMALLINT
                ,    DSQuantilesk<float>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the value of K for this sketch";
                    desc.examples.push_back("datasketch_quantiles_k(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::FLOAT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::DOUBLE]},LogicalType::USMALLINT
                ,    DSQuantilesk<double>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the value of K for this sketch";
                    desc.examples.push_back("datasketch_quantiles_k(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::DOUBLE] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UTINYINT]},LogicalType::USMALLINT
                ,    DSQuantilesk<uint8_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the value of K for this sketch";
                    desc.examples.push_back("datasketch_quantiles_k(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UTINYINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::USMALLINT]},LogicalType::USMALLINT
                ,    DSQuantilesk<uint16_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the value of K for this sketch";
                    desc.examples.push_back("datasketch_quantiles_k(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::USMALLINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UINTEGER]},LogicalType::USMALLINT
                ,    DSQuantilesk<uint32_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the value of K for this sketch";
                    desc.examples.push_back("datasketch_quantiles_k(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UINTEGER] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UBIGINT]},LogicalType::USMALLINT
                ,    DSQuantilesk<uint64_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the value of K for this sketch";
                    desc.examples.push_back("datasketch_quantiles_k(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UBIGINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::VARCHAR]},LogicalType::USMALLINT
                ,    DSQuantilesk<string_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the value of K for this sketch";
                    desc.examples.push_back("datasketch_quantiles_k(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::VARCHAR] };
                    fn_descs.push_back(std::move(desc));
                }
        

        // Every scalar function deserializes a sketch, which throws on invalid input.
        fs.SetFallible();
        CreateScalarFunctionInfo info(std::move(fs));
        for (auto &d : fn_descs) {
            info.descriptions.push_back(std::move(d));
        }

        DSSetCategories(info, {"sketch", "quantiles"});

        DSRegister(loader, info);


      }
      {
        ScalarFunctionSet fs("datasketch_quantiles_cdf");
        std::vector<FunctionDescription> fn_descs;
        
            
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::TINYINT],LogicalType::LIST(LogicalType::TINYINT),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::DOUBLE)
                ,    DSQuantilescdf<int8_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the Cumulative Distribution Function (CDF) of the sketch for a series of points";
                    desc.examples.push_back("datasketch_quantiles_cdf(sketch, points, inclusive)");
                    desc.parameter_names = { "sketch","split_points","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::TINYINT],LogicalType::LIST(LogicalType::TINYINT),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::SMALLINT],LogicalType::LIST(LogicalType::SMALLINT),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::DOUBLE)
                ,    DSQuantilescdf<int16_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the Cumulative Distribution Function (CDF) of the sketch for a series of points";
                    desc.examples.push_back("datasketch_quantiles_cdf(sketch, points, inclusive)");
                    desc.parameter_names = { "sketch","split_points","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::SMALLINT],LogicalType::LIST(LogicalType::SMALLINT),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::INTEGER],LogicalType::LIST(LogicalType::INTEGER),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::DOUBLE)
                ,    DSQuantilescdf<int32_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the Cumulative Distribution Function (CDF) of the sketch for a series of points";
                    desc.examples.push_back("datasketch_quantiles_cdf(sketch, points, inclusive)");
                    desc.parameter_names = { "sketch","split_points","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::INTEGER],LogicalType::LIST(LogicalType::INTEGER),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::BIGINT],LogicalType::LIST(LogicalType::BIGINT),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::DOUBLE)
                ,    DSQuantilescdf<int64_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the Cumulative Distribution Function (CDF) of the sketch for a series of points";
                    desc.examples.push_back("datasketch_quantiles_cdf(sketch, points, inclusive)");
                    desc.parameter_names = { "sketch","split_points","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::BIGINT],LogicalType::LIST(LogicalType::BIGINT),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::FLOAT],LogicalType::LIST(LogicalType::FLOAT),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::DOUBLE)
                ,    DSQuantilescdf<float>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the Cumulative Distribution Function (CDF) of the sketch for a series of points";
                    desc.examples.push_back("datasketch_quantiles_cdf(sketch, points, inclusive)");
                    desc.parameter_names = { "sketch","split_points","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::FLOAT],LogicalType::LIST(LogicalType::FLOAT),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::DOUBLE],LogicalType::LIST(LogicalType::DOUBLE),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::DOUBLE)
                ,    DSQuantilescdf<double>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the Cumulative Distribution Function (CDF) of the sketch for a series of points";
                    desc.examples.push_back("datasketch_quantiles_cdf(sketch, points, inclusive)");
                    desc.parameter_names = { "sketch","split_points","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::DOUBLE],LogicalType::LIST(LogicalType::DOUBLE),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UTINYINT],LogicalType::LIST(LogicalType::UTINYINT),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::DOUBLE)
                ,    DSQuantilescdf<uint8_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the Cumulative Distribution Function (CDF) of the sketch for a series of points";
                    desc.examples.push_back("datasketch_quantiles_cdf(sketch, points, inclusive)");
                    desc.parameter_names = { "sketch","split_points","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UTINYINT],LogicalType::LIST(LogicalType::UTINYINT),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::USMALLINT],LogicalType::LIST(LogicalType::USMALLINT),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::DOUBLE)
                ,    DSQuantilescdf<uint16_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the Cumulative Distribution Function (CDF) of the sketch for a series of points";
                    desc.examples.push_back("datasketch_quantiles_cdf(sketch, points, inclusive)");
                    desc.parameter_names = { "sketch","split_points","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::USMALLINT],LogicalType::LIST(LogicalType::USMALLINT),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UINTEGER],LogicalType::LIST(LogicalType::UINTEGER),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::DOUBLE)
                ,    DSQuantilescdf<uint32_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the Cumulative Distribution Function (CDF) of the sketch for a series of points";
                    desc.examples.push_back("datasketch_quantiles_cdf(sketch, points, inclusive)");
                    desc.parameter_names = { "sketch","split_points","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UINTEGER],LogicalType::LIST(LogicalType::UINTEGER),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UBIGINT],LogicalType::LIST(LogicalType::UBIGINT),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::DOUBLE)
                ,    DSQuantilescdf<uint64_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the Cumulative Distribution Function (CDF) of the sketch for a series of points";
                    desc.examples.push_back("datasketch_quantiles_cdf(sketch, points, inclusive)");
                    desc.parameter_names = { "sketch","split_points","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UBIGINT],LogicalType::LIST(LogicalType::UBIGINT),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::VARCHAR],LogicalType::LIST(LogicalType::VARCHAR),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::DOUBLE)
                ,    DSQuantilescdf<string_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the Cumulative Distribution Function (CDF) of the sketch for a series of points";
                    desc.examples.push_back("datasketch_quantiles_cdf(sketch, points, inclusive)");
                    desc.parameter_names = { "sketch","split_points","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::VARCHAR],LogicalType::LIST(LogicalType::VARCHAR),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
        

        // Every scalar function deserializes a sketch, which throws on invalid input.
        fs.SetFallible();
        CreateScalarFunctionInfo info(std::move(fs));
        for (auto &d : fn_descs) {
            info.descriptions.push_back(std::move(d));
        }

        DSSetCategories(info, {"sketch", "quantiles"});

        DSRegister(loader, info);


      }
      {
        ScalarFunctionSet fs("datasketch_quantiles_pmf");
        std::vector<FunctionDescription> fn_descs;
        
            
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::TINYINT],LogicalType::LIST(LogicalType::TINYINT),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::DOUBLE)
                ,    DSQuantilespmf<int8_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the Probability Mass Function (PMF) of the sketch for a series of points";
                    desc.examples.push_back("datasketch_quantiles_pmf(sketch, points, inclusive)");
                    desc.parameter_names = { "sketch","split_points","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::TINYINT],LogicalType::LIST(LogicalType::TINYINT),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::SMALLINT],LogicalType::LIST(LogicalType::SMALLINT),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::DOUBLE)
                ,    DSQuantilespmf<int16_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the Probability Mass Function (PMF) of the sketch for a series of points";
                    desc.examples.push_back("datasketch_quantiles_pmf(sketch, points, inclusive)");
                    desc.parameter_names = { "sketch","split_points","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::SMALLINT],LogicalType::LIST(LogicalType::SMALLINT),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::INTEGER],LogicalType::LIST(LogicalType::INTEGER),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::DOUBLE)
                ,    DSQuantilespmf<int32_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the Probability Mass Function (PMF) of the sketch for a series of points";
                    desc.examples.push_back("datasketch_quantiles_pmf(sketch, points, inclusive)");
                    desc.parameter_names = { "sketch","split_points","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::INTEGER],LogicalType::LIST(LogicalType::INTEGER),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::BIGINT],LogicalType::LIST(LogicalType::BIGINT),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::DOUBLE)
                ,    DSQuantilespmf<int64_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the Probability Mass Function (PMF) of the sketch for a series of points";
                    desc.examples.push_back("datasketch_quantiles_pmf(sketch, points, inclusive)");
                    desc.parameter_names = { "sketch","split_points","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::BIGINT],LogicalType::LIST(LogicalType::BIGINT),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::FLOAT],LogicalType::LIST(LogicalType::FLOAT),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::DOUBLE)
                ,    DSQuantilespmf<float>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the Probability Mass Function (PMF) of the sketch for a series of points";
                    desc.examples.push_back("datasketch_quantiles_pmf(sketch, points, inclusive)");
                    desc.parameter_names = { "sketch","split_points","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::FLOAT],LogicalType::LIST(LogicalType::FLOAT),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::DOUBLE],LogicalType::LIST(LogicalType::DOUBLE),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::DOUBLE)
                ,    DSQuantilespmf<double>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the Probability Mass Function (PMF) of the sketch for a series of points";
                    desc.examples.push_back("datasketch_quantiles_pmf(sketch, points, inclusive)");
                    desc.parameter_names = { "sketch","split_points","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::DOUBLE],LogicalType::LIST(LogicalType::DOUBLE),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UTINYINT],LogicalType::LIST(LogicalType::UTINYINT),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::DOUBLE)
                ,    DSQuantilespmf<uint8_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the Probability Mass Function (PMF) of the sketch for a series of points";
                    desc.examples.push_back("datasketch_quantiles_pmf(sketch, points, inclusive)");
                    desc.parameter_names = { "sketch","split_points","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UTINYINT],LogicalType::LIST(LogicalType::UTINYINT),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::USMALLINT],LogicalType::LIST(LogicalType::USMALLINT),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::DOUBLE)
                ,    DSQuantilespmf<uint16_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the Probability Mass Function (PMF) of the sketch for a series of points";
                    desc.examples.push_back("datasketch_quantiles_pmf(sketch, points, inclusive)");
                    desc.parameter_names = { "sketch","split_points","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::USMALLINT],LogicalType::LIST(LogicalType::USMALLINT),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UINTEGER],LogicalType::LIST(LogicalType::UINTEGER),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::DOUBLE)
                ,    DSQuantilespmf<uint32_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the Probability Mass Function (PMF) of the sketch for a series of points";
                    desc.examples.push_back("datasketch_quantiles_pmf(sketch, points, inclusive)");
                    desc.parameter_names = { "sketch","split_points","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UINTEGER],LogicalType::LIST(LogicalType::UINTEGER),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UBIGINT],LogicalType::LIST(LogicalType::UBIGINT),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::DOUBLE)
                ,    DSQuantilespmf<uint64_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the Probability Mass Function (PMF) of the sketch for a series of points";
                    desc.examples.push_back("datasketch_quantiles_pmf(sketch, points, inclusive)");
                    desc.parameter_names = { "sketch","split_points","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UBIGINT],LogicalType::LIST(LogicalType::UBIGINT),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::VARCHAR],LogicalType::LIST(LogicalType::VARCHAR),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::DOUBLE)
                ,    DSQuantilespmf<string_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the Probability Mass Function (PMF) of the sketch for a series of points";
                    desc.examples.push_back("datasketch_quantiles_pmf(sketch, points, inclusive)");
                    desc.parameter_names = { "sketch","split_points","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::VARCHAR],LogicalType::LIST(LogicalType::VARCHAR),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
        

        // Every scalar function deserializes a sketch, which throws on invalid input.
        fs.SetFallible();
        CreateScalarFunctionInfo info(std::move(fs));
        for (auto &d : fn_descs) {
            info.descriptions.push_back(std::move(d));
        }

        DSSetCategories(info, {"sketch", "quantiles"});

        DSRegister(loader, info);


      }
      {
        ScalarFunctionSet fs("datasketch_quantiles_normalized_rank_error");
        std::vector<FunctionDescription> fn_descs;
        
            
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::TINYINT],LogicalType::BOOLEAN},LogicalType::DOUBLE
                ,    DSQuantilesnormalized_rank_error<int8_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the normalized rank error of the sketch";
                    desc.examples.push_back("datasketch_quantiles_normalized_rank_error(sketch, is_pmf)");
                    desc.parameter_names = { "sketch","is_pmf" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::TINYINT],LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::SMALLINT],LogicalType::BOOLEAN},LogicalType::DOUBLE
                ,    DSQuantilesnormalized_rank_error<int16_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the normalized rank error of the sketch";
                    desc.examples.push_back("datasketch_quantiles_normalized_rank_error(sketch, is_pmf)");
                    desc.parameter_names = { "sketch","is_pmf" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::SMALLINT],LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::INTEGER],LogicalType::BOOLEAN},LogicalType::DOUBLE
                ,    DSQuantilesnormalized_rank_error<int32_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the normalized rank error of the sketch";
                    desc.examples.push_back("datasketch_quantiles_normalized_rank_error(sketch, is_pmf)");
                    desc.parameter_names = { "sketch","is_pmf" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::INTEGER],LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::BIGINT],LogicalType::BOOLEAN},LogicalType::DOUBLE
                ,    DSQuantilesnormalized_rank_error<int64_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the normalized rank error of the sketch";
                    desc.examples.push_back("datasketch_quantiles_normalized_rank_error(sketch, is_pmf)");
                    desc.parameter_names = { "sketch","is_pmf" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::BIGINT],LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::FLOAT],LogicalType::BOOLEAN},LogicalType::DOUBLE
                ,    DSQuantilesnormalized_rank_error<float>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the normalized rank error of the sketch";
                    desc.examples.push_back("datasketch_quantiles_normalized_rank_error(sketch, is_pmf)");
                    desc.parameter_names = { "sketch","is_pmf" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::FLOAT],LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::DOUBLE],LogicalType::BOOLEAN},LogicalType::DOUBLE
                ,    DSQuantilesnormalized_rank_error<double>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the normalized rank error of the sketch";
                    desc.examples.push_back("datasketch_quantiles_normalized_rank_error(sketch, is_pmf)");
                    desc.parameter_names = { "sketch","is_pmf" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::DOUBLE],LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UTINYINT],LogicalType::BOOLEAN},LogicalType::DOUBLE
                ,    DSQuantilesnormalized_rank_error<uint8_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the normalized rank error of the sketch";
                    desc.examples.push_back("datasketch_quantiles_normalized_rank_error(sketch, is_pmf)");
                    desc.parameter_names = { "sketch","is_pmf" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UTINYINT],LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::USMALLINT],LogicalType::BOOLEAN},LogicalType::DOUBLE
                ,    DSQuantilesnormalized_rank_error<uint16_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the normalized rank error of the sketch";
                    desc.examples.push_back("datasketch_quantiles_normalized_rank_error(sketch, is_pmf)");
                    desc.parameter_names = { "sketch","is_pmf" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::USMALLINT],LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UINTEGER],LogicalType::BOOLEAN},LogicalType::DOUBLE
                ,    DSQuantilesnormalized_rank_error<uint32_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the normalized rank error of the sketch";
                    desc.examples.push_back("datasketch_quantiles_normalized_rank_error(sketch, is_pmf)");
                    desc.parameter_names = { "sketch","is_pmf" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UINTEGER],LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UBIGINT],LogicalType::BOOLEAN},LogicalType::DOUBLE
                ,    DSQuantilesnormalized_rank_error<uint64_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the normalized rank error of the sketch";
                    desc.examples.push_back("datasketch_quantiles_normalized_rank_error(sketch, is_pmf)");
                    desc.parameter_names = { "sketch","is_pmf" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UBIGINT],LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::VARCHAR],LogicalType::BOOLEAN},LogicalType::DOUBLE
                ,    DSQuantilesnormalized_rank_error<string_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the normalized rank error of the sketch";
                    desc.examples.push_back("datasketch_quantiles_normalized_rank_error(sketch, is_pmf)");
                    desc.parameter_names = { "sketch","is_pmf" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::VARCHAR],LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
        

        // Every scalar function deserializes a sketch, which throws on invalid input.
        fs.SetFallible();
        CreateScalarFunctionInfo info(std::move(fs));
        for (auto &d : fn_descs) {
            info.descriptions.push_back(std::move(d));
        }

        DSSetCategories(info, {"sketch", "quantiles"});

        DSRegister(loader, info);


      }
      {
        ScalarFunctionSet fs("datasketch_quantiles_describe");
        std::vector<FunctionDescription> fn_descs;
        
            
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::TINYINT],LogicalType::BOOLEAN,LogicalType::BOOLEAN},LogicalType::VARCHAR
                ,    DSQuantilesdescribe<int8_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return a description of this sketch";
                    desc.examples.push_back("datasketch_quantiles_describe(sketch, include_levels, include_items)");
                    desc.parameter_names = { "sketch","include_levels","include_items" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::TINYINT],LogicalType::BOOLEAN,LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::SMALLINT],LogicalType::BOOLEAN,LogicalType::BOOLEAN},LogicalType::VARCHAR
                ,    DSQuantilesdescribe<int16_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return a description of this sketch";
                    desc.examples.push_back("datasketch_quantiles_describe(sketch, include_levels, include_items)");
                    desc.parameter_names = { "sketch","include_levels","include_items" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::SMALLINT],LogicalType::BOOLEAN,LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::INTEGER],LogicalType::BOOLEAN,LogicalType::BOOLEAN},LogicalType::VARCHAR
                ,    DSQuantilesdescribe<int32_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return a description of this sketch";
                    desc.examples.push_back("datasketch_quantiles_describe(sketch, include_levels, include_items)");
                    desc.parameter_names = { "sketch","include_levels","include_items" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::INTEGER],LogicalType::BOOLEAN,LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::BIGINT],LogicalType::BOOLEAN,LogicalType::BOOLEAN},LogicalType::VARCHAR
                ,    DSQuantilesdescribe<int64_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return a description of this sketch";
                    desc.examples.push_back("datasketch_quantiles_describe(sketch, include_levels, include_items)");
                    desc.parameter_names = { "sketch","include_levels","include_items" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::BIGINT],LogicalType::BOOLEAN,LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::FLOAT],LogicalType::BOOLEAN,LogicalType::BOOLEAN},LogicalType::VARCHAR
                ,    DSQuantilesdescribe<float>));
                {
                    FunctionDescription desc;
                    desc.description = "Return a description of this sketch";
                    desc.examples.push_back("datasketch_quantiles_describe(sketch, include_levels, include_items)");
                    desc.parameter_names = { "sketch","include_levels","include_items" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::FLOAT],LogicalType::BOOLEAN,LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::DOUBLE],LogicalType::BOOLEAN,LogicalType::BOOLEAN},LogicalType::VARCHAR
                ,    DSQuantilesdescribe<double>));
                {
                    FunctionDescription desc;
                    desc.description = "Return a description of this sketch";
                    desc.examples.push_back("datasketch_quantiles_describe(sketch, include_levels, include_items)");
                    desc.parameter_names = { "sketch","include_levels","include_items" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::DOUBLE],LogicalType::BOOLEAN,LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UTINYINT],LogicalType::BOOLEAN,LogicalType::BOOLEAN},LogicalType::VARCHAR
                ,    DSQuantilesdescribe<uint8_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return a description of this sketch";
                    desc.examples.push_back("datasketch_quantiles_describe(sketch, include_levels, include_items)");
                    desc.parameter_names = { "sketch","include_levels","include_items" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UTINYINT],LogicalType::BOOLEAN,LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::USMALLINT],LogicalType::BOOLEAN,LogicalType::BOOLEAN},LogicalType::VARCHAR
                ,    DSQuantilesdescribe<uint16_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return a description of this sketch";
                    desc.examples.push_back("datasketch_quantiles_describe(sketch, include_levels, include_items)");
                    desc.parameter_names = { "sketch","include_levels","include_items" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::USMALLINT],LogicalType::BOOLEAN,LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UINTEGER],LogicalType::BOOLEAN,LogicalType::BOOLEAN},LogicalType::VARCHAR
                ,    DSQuantilesdescribe<uint32_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return a description of this sketch";
                    desc.examples.push_back("datasketch_quantiles_describe(sketch, include_levels, include_items)");
                    desc.parameter_names = { "sketch","include_levels","include_items" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UINTEGER],LogicalType::BOOLEAN,LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UBIGINT],LogicalType::BOOLEAN,LogicalType::BOOLEAN},LogicalType::VARCHAR
                ,    DSQuantilesdescribe<uint64_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return a description of this sketch";
                    desc.examples.push_back("datasketch_quantiles_describe(sketch, include_levels, include_items)");
                    desc.parameter_names = { "sketch","include_levels","include_items" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UBIGINT],LogicalType::BOOLEAN,LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::VARCHAR],LogicalType::BOOLEAN,LogicalType::BOOLEAN},LogicalType::VARCHAR
                ,    DSQuantilesdescribe<string_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return a description of this sketch";
                    desc.examples.push_back("datasketch_quantiles_describe(sketch, include_levels, include_items)");
                    desc.parameter_names = { "sketch","include_levels","include_items" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::VARCHAR],LogicalType::BOOLEAN,LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
        

        // Every scalar function deserializes a sketch, which throws on invalid input.
        fs.SetFallible();
        CreateScalarFunctionInfo info(std::move(fs));
        for (auto &d : fn_descs) {
            info.descriptions.push_back(std::move(d));
        }

        DSSetCategories(info, {"sketch", "quantiles"});

        DSRegister(loader, info);


      }
      {
        ScalarFunctionSet fs("datasketch_quantiles_rank");
        std::vector<FunctionDescription> fn_descs;
        
            
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::TINYINT],LogicalType::TINYINT,LogicalType::BOOLEAN},LogicalType::DOUBLE
                ,    DSQuantilesrank<int8_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the rank of an item in the sketch";
                    desc.examples.push_back("datasketch_quantiles_rank(sketch, item, inclusive)");
                    desc.parameter_names = { "sketch","item","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::TINYINT],LogicalType::TINYINT,LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::SMALLINT],LogicalType::SMALLINT,LogicalType::BOOLEAN},LogicalType::DOUBLE
                ,    DSQuantilesrank<int16_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the rank of an item in the sketch";
                    desc.examples.push_back("datasketch_quantiles_rank(sketch, item, inclusive)");
                    desc.parameter_names = { "sketch","item","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::SMALLINT],LogicalType::SMALLINT,LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::INTEGER],LogicalType::INTEGER,LogicalType::BOOLEAN},LogicalType::DOUBLE
                ,    DSQuantilesrank<int32_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the rank of an item in the sketch";
                    desc.examples.push_back("datasketch_quantiles_rank(sketch, item, inclusive)");
                    desc.parameter_names = { "sketch","item","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::INTEGER],LogicalType::INTEGER,LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::BIGINT],LogicalType::BIGINT,LogicalType::BOOLEAN},LogicalType::DOUBLE
                ,    DSQuantilesrank<int64_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the rank of an item in the sketch";
                    desc.examples.push_back("datasketch_quantiles_rank(sketch, item, inclusive)");
                    desc.parameter_names = { "sketch","item","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::BIGINT],LogicalType::BIGINT,LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::FLOAT],LogicalType::FLOAT,LogicalType::BOOLEAN},LogicalType::DOUBLE
                ,    DSQuantilesrank<float>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the rank of an item in the sketch";
                    desc.examples.push_back("datasketch_quantiles_rank(sketch, item, inclusive)");
                    desc.parameter_names = { "sketch","item","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::FLOAT],LogicalType::FLOAT,LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::DOUBLE],LogicalType::DOUBLE,LogicalType::BOOLEAN},LogicalType::DOUBLE
                ,    DSQuantilesrank<double>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the rank of an item in the sketch";
                    desc.examples.push_back("datasketch_quantiles_rank(sketch, item, inclusive)");
                    desc.parameter_names = { "sketch","item","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::DOUBLE],LogicalType::DOUBLE,LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UTINYINT],LogicalType::UTINYINT,LogicalType::BOOLEAN},LogicalType::DOUBLE
                ,    DSQuantilesrank<uint8_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the rank of an item in the sketch";
                    desc.examples.push_back("datasketch_quantiles_rank(sketch, item, inclusive)");
                    desc.parameter_names = { "sketch","item","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UTINYINT],LogicalType::UTINYINT,LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::USMALLINT],LogicalType::USMALLINT,LogicalType::BOOLEAN},LogicalType::DOUBLE
                ,    DSQuantilesrank<uint16_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the rank of an item in the sketch";
                    desc.examples.push_back("datasketch_quantiles_rank(sketch, item, inclusive)");
                    desc.parameter_names = { "sketch","item","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::USMALLINT],LogicalType::USMALLINT,LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UINTEGER],LogicalType::UINTEGER,LogicalType::BOOLEAN},LogicalType::DOUBLE
                ,    DSQuantilesrank<uint32_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the rank of an item in the sketch";
                    desc.examples.push_back("datasketch_quantiles_rank(sketch, item, inclusive)");
                    desc.parameter_names = { "sketch","item","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UINTEGER],LogicalType::UINTEGER,LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UBIGINT],LogicalType::UBIGINT,LogicalType::BOOLEAN},LogicalType::DOUBLE
                ,    DSQuantilesrank<uint64_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the rank of an item in the sketch";
                    desc.examples.push_back("datasketch_quantiles_rank(sketch, item, inclusive)");
                    desc.parameter_names = { "sketch","item","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UBIGINT],LogicalType::UBIGINT,LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::VARCHAR],LogicalType::VARCHAR,LogicalType::BOOLEAN},LogicalType::DOUBLE
                ,    DSQuantilesrank<string_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the rank of an item in the sketch";
                    desc.examples.push_back("datasketch_quantiles_rank(sketch, item, inclusive)");
                    desc.parameter_names = { "sketch","item","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::VARCHAR],LogicalType::VARCHAR,LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
        

        // Every scalar function deserializes a sketch, which throws on invalid input.
        fs.SetFallible();
        CreateScalarFunctionInfo info(std::move(fs));
        for (auto &d : fn_descs) {
            info.descriptions.push_back(std::move(d));
        }

        DSSetCategories(info, {"sketch", "quantiles"});

        DSRegister(loader, info);


      }
      {
        ScalarFunctionSet fs("datasketch_quantiles_quantile");
        std::vector<FunctionDescription> fn_descs;
        
            
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::TINYINT],LogicalType::DOUBLE,LogicalType::BOOLEAN},LogicalType::TINYINT
                ,    DSQuantilesquantile<int8_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the quantile of a rank in the sketch";
                    desc.examples.push_back("datasketch_quantiles_quantile(sketch, rank, inclusive)");
                    desc.parameter_names = { "sketch","rank","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::TINYINT],LogicalType::DOUBLE,LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::SMALLINT],LogicalType::DOUBLE,LogicalType::BOOLEAN},LogicalType::SMALLINT
                ,    DSQuantilesquantile<int16_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the quantile of a rank in the sketch";
                    desc.examples.push_back("datasketch_quantiles_quantile(sketch, rank, inclusive)");
                    desc.parameter_names = { "sketch","rank","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::SMALLINT],LogicalType::DOUBLE,LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::INTEGER],LogicalType::DOUBLE,LogicalType::BOOLEAN},LogicalType::INTEGER
                ,    DSQuantilesquantile<int32_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the quantile of a rank in the sketch";
                    desc.examples.push_back("datasketch_quantiles_quantile(sketch, rank, inclusive)");
                    desc.parameter_names = { "sketch","rank","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::INTEGER],LogicalType::DOUBLE,LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::BIGINT],LogicalType::DOUBLE,LogicalType::BOOLEAN},LogicalType::BIGINT
                ,    DSQuantilesquantile<int64_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the quantile of a rank in the sketch";
                    desc.examples.push_back("datasketch_quantiles_quantile(sketch, rank, inclusive)");
                    desc.parameter_names = { "sketch","rank","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::BIGINT],LogicalType::DOUBLE,LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::FLOAT],LogicalType::DOUBLE,LogicalType::BOOLEAN},LogicalType::FLOAT
                ,    DSQuantilesquantile<float>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the quantile of a rank in the sketch";
                    desc.examples.push_back("datasketch_quantiles_quantile(sketch, rank, inclusive)");
                    desc.parameter_names = { "sketch","rank","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::FLOAT],LogicalType::DOUBLE,LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::DOUBLE],LogicalType::DOUBLE,LogicalType::BOOLEAN},LogicalType::DOUBLE
                ,    DSQuantilesquantile<double>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the quantile of a rank in the sketch";
                    desc.examples.push_back("datasketch_quantiles_quantile(sketch, rank, inclusive)");
                    desc.parameter_names = { "sketch","rank","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::DOUBLE],LogicalType::DOUBLE,LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UTINYINT],LogicalType::DOUBLE,LogicalType::BOOLEAN},LogicalType::UTINYINT
                ,    DSQuantilesquantile<uint8_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the quantile of a rank in the sketch";
                    desc.examples.push_back("datasketch_quantiles_quantile(sketch, rank, inclusive)");
                    desc.parameter_names = { "sketch","rank","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UTINYINT],LogicalType::DOUBLE,LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::USMALLINT],LogicalType::DOUBLE,LogicalType::BOOLEAN},LogicalType::USMALLINT
                ,    DSQuantilesquantile<uint16_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the quantile of a rank in the sketch";
                    desc.examples.push_back("datasketch_quantiles_quantile(sketch, rank, inclusive)");
                    desc.parameter_names = { "sketch","rank","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::USMALLINT],LogicalType::DOUBLE,LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UINTEGER],LogicalType::DOUBLE,LogicalType::BOOLEAN},LogicalType::UINTEGER
                ,    DSQuantilesquantile<uint32_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the quantile of a rank in the sketch";
                    desc.examples.push_back("datasketch_quantiles_quantile(sketch, rank, inclusive)");
                    desc.parameter_names = { "sketch","rank","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UINTEGER],LogicalType::DOUBLE,LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UBIGINT],LogicalType::DOUBLE,LogicalType::BOOLEAN},LogicalType::UBIGINT
                ,    DSQuantilesquantile<uint64_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the quantile of a rank in the sketch";
                    desc.examples.push_back("datasketch_quantiles_quantile(sketch, rank, inclusive)");
                    desc.parameter_names = { "sketch","rank","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UBIGINT],LogicalType::DOUBLE,LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::VARCHAR],LogicalType::DOUBLE,LogicalType::BOOLEAN},LogicalType::VARCHAR
                ,    DSQuantilesquantile<string_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the quantile of a rank in the sketch";
                    desc.examples.push_back("datasketch_quantiles_quantile(sketch, rank, inclusive)");
                    desc.parameter_names = { "sketch","rank","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::VARCHAR],LogicalType::DOUBLE,LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
        

        // Every scalar function deserializes a sketch, which throws on invalid input.
        fs.SetFallible();
        CreateScalarFunctionInfo info(std::move(fs));
        for (auto &d : fn_descs) {
            info.descriptions.push_back(std::move(d));
        }

        DSSetCategories(info, {"sketch", "quantiles"});

        DSRegister(loader, info);


      }
      {
        ScalarFunctionSet fs("datasketch_quantiles_n");
        std::vector<FunctionDescription> fn_descs;
        
            
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::TINYINT]},LogicalType::UBIGINT
                ,    DSQuantilesn<int8_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the number of items contained in the sketch";
                    desc.examples.push_back("datasketch_quantiles_n(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::TINYINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::SMALLINT]},LogicalType::UBIGINT
                ,    DSQuantilesn<int16_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the number of items contained in the sketch";
                    desc.examples.push_back("datasketch_quantiles_n(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::SMALLINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::INTEGER]},LogicalType::UBIGINT
                ,    DSQuantilesn<int32_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the number of items contained in the sketch";
                    desc.examples.push_back("datasketch_quantiles_n(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::INTEGER] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::BIGINT]},LogicalType::UBIGINT
                ,    DSQuantilesn<int64_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the number of items contained in the sketch";
                    desc.examples.push_back("datasketch_quantiles_n(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::BIGINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::FLOAT]},LogicalType::UBIGINT
                ,    DSQuantilesn<float>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the number of items contained in the sketch";
                    desc.examples.push_back("datasketch_quantiles_n(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::FLOAT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::DOUBLE]},LogicalType::UBIGINT
                ,    DSQuantilesn<double>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the number of items contained in the sketch";
                    desc.examples.push_back("datasketch_quantiles_n(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::DOUBLE] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UTINYINT]},LogicalType::UBIGINT
                ,    DSQuantilesn<uint8_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the number of items contained in the sketch";
                    desc.examples.push_back("datasketch_quantiles_n(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UTINYINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::USMALLINT]},LogicalType::UBIGINT
                ,    DSQuantilesn<uint16_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the number of items contained in the sketch";
                    desc.examples.push_back("datasketch_quantiles_n(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::USMALLINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UINTEGER]},LogicalType::UBIGINT
                ,    DSQuantilesn<uint32_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the number of items contained in the sketch";
                    desc.examples.push_back("datasketch_quantiles_n(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UINTEGER] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UBIGINT]},LogicalType::UBIGINT
                ,    DSQuantilesn<uint64_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the number of items contained in the sketch";
                    desc.examples.push_back("datasketch_quantiles_n(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UBIGINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::VARCHAR]},LogicalType::UBIGINT
                ,    DSQuantilesn<string_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the number of items contained in the sketch";
                    desc.examples.push_back("datasketch_quantiles_n(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::VARCHAR] };
                    fn_descs.push_back(std::move(desc));
                }
        

        // Every scalar function deserializes a sketch, which throws on invalid input.
        fs.SetFallible();
        CreateScalarFunctionInfo info(std::move(fs));
        for (auto &d : fn_descs) {
            info.descriptions.push_back(std::move(d));
        }

        DSSetCategories(info, {"sketch", "quantiles"});

        DSRegister(loader, info);


      }
      {
        ScalarFunctionSet fs("datasketch_quantiles_is_estimation_mode");
        std::vector<FunctionDescription> fn_descs;
        
            
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::TINYINT]},LogicalType::BOOLEAN
                ,    DSQuantilesis_estimation_mode<int8_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return a boolean indicating if the sketch is in estimation mode";
                    desc.examples.push_back("datasketch_quantiles_is_estimation_mode(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::TINYINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::SMALLINT]},LogicalType::BOOLEAN
                ,    DSQuantilesis_estimation_mode<int16_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return a boolean indicating if the sketch is in estimation mode";
                    desc.examples.push_back("datasketch_quantiles_is_estimation_mode(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::SMALLINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::INTEGER]},LogicalType::BOOLEAN
                ,    DSQuantilesis_estimation_mode<int32_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return a boolean indicating if the sketch is in estimation mode";
                    desc.examples.push_back("datasketch_quantiles_is_estimation_mode(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::INTEGER] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::BIGINT]},LogicalType::BOOLEAN
                ,    DSQuantilesis_estimation_mode<int64_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return a boolean indicating if the sketch is in estimation mode";
                    desc.examples.push_back("datasketch_quantiles_is_estimation_mode(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::BIGINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::FLOAT]},LogicalType::BOOLEAN
                ,    DSQuantilesis_estimation_mode<float>));
                {
                    FunctionDescription desc;
                    desc.description = "Return a boolean indicating if the sketch is in estimation mode";
                    desc.examples.push_back("datasketch_quantiles_is_estimation_mode(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::FLOAT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::DOUBLE]},LogicalType::BOOLEAN
                ,    DSQuantilesis_estimation_mode<double>));
                {
                    FunctionDescription desc;
                    desc.description = "Return a boolean indicating if the sketch is in estimation mode";
                    desc.examples.push_back("datasketch_quantiles_is_estimation_mode(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::DOUBLE] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UTINYINT]},LogicalType::BOOLEAN
                ,    DSQuantilesis_estimation_mode<uint8_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return a boolean indicating if the sketch is in estimation mode";
                    desc.examples.push_back("datasketch_quantiles_is_estimation_mode(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UTINYINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::USMALLINT]},LogicalType::BOOLEAN
                ,    DSQuantilesis_estimation_mode<uint16_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return a boolean indicating if the sketch is in estimation mode";
                    desc.examples.push_back("datasketch_quantiles_is_estimation_mode(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::USMALLINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UINTEGER]},LogicalType::BOOLEAN
                ,    DSQuantilesis_estimation_mode<uint32_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return a boolean indicating if the sketch is in estimation mode";
                    desc.examples.push_back("datasketch_quantiles_is_estimation_mode(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UINTEGER] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UBIGINT]},LogicalType::BOOLEAN
                ,    DSQuantilesis_estimation_mode<uint64_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return a boolean indicating if the sketch is in estimation mode";
                    desc.examples.push_back("datasketch_quantiles_is_estimation_mode(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UBIGINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::VARCHAR]},LogicalType::BOOLEAN
                ,    DSQuantilesis_estimation_mode<string_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return a boolean indicating if the sketch is in estimation mode";
                    desc.examples.push_back("datasketch_quantiles_is_estimation_mode(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::VARCHAR] };
                    fn_descs.push_back(std::move(desc));
                }
        

        // Every scalar function deserializes a sketch, which throws on invalid input.
        fs.SetFallible();
        CreateScalarFunctionInfo info(std::move(fs));
        for (auto &d : fn_descs) {
            info.descriptions.push_back(std::move(d));
        }

        DSSetCategories(info, {"sketch", "quantiles"});

        DSRegister(loader, info);


      }
      {
        ScalarFunctionSet fs("datasketch_quantiles_num_retained");
        std::vector<FunctionDescription> fn_descs;
        
            
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::TINYINT]},LogicalType::UBIGINT
                ,    DSQuantilesnum_retained<int8_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the number of retained items in the sketch";
                    desc.examples.push_back("datasketch_quantiles_num_retained(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::TINYINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::SMALLINT]},LogicalType::UBIGINT
                ,    DSQuantilesnum_retained<int16_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the number of retained items in the sketch";
                    desc.examples.push_back("datasketch_quantiles_num_retained(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::SMALLINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::INTEGER]},LogicalType::UBIGINT
                ,    DSQuantilesnum_retained<int32_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the number of retained items in the sketch";
                    desc.examples.push_back("datasketch_quantiles_num_retained(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::INTEGER] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::BIGINT]},LogicalType::UBIGINT
                ,    DSQuantilesnum_retained<int64_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the number of retained items in the sketch";
                    desc.examples.push_back("datasketch_quantiles_num_retained(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::BIGINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::FLOAT]},LogicalType::UBIGINT
                ,    DSQuantilesnum_retained<float>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the number of retained items in the sketch";
                    desc.examples.push_back("datasketch_quantiles_num_retained(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::FLOAT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::DOUBLE]},LogicalType::UBIGINT
                ,    DSQuantilesnum_retained<double>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the number of retained items in the sketch";
                    desc.examples.push_back("datasketch_quantiles_num_retained(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::DOUBLE] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UTINYINT]},LogicalType::UBIGINT
                ,    DSQuantilesnum_retained<uint8_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the number of retained items in the sketch";
                    desc.examples.push_back("datasketch_quantiles_num_retained(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UTINYINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::USMALLINT]},LogicalType::UBIGINT
                ,    DSQuantilesnum_retained<uint16_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the number of retained items in the sketch";
                    desc.examples.push_back("datasketch_quantiles_num_retained(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::USMALLINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UINTEGER]},LogicalType::UBIGINT
                ,    DSQuantilesnum_retained<uint32_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the number of retained items in the sketch";
                    desc.examples.push_back("datasketch_quantiles_num_retained(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UINTEGER] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UBIGINT]},LogicalType::UBIGINT
                ,    DSQuantilesnum_retained<uint64_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the number of retained items in the sketch";
                    desc.examples.push_back("datasketch_quantiles_num_retained(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UBIGINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::VARCHAR]},LogicalType::UBIGINT
                ,    DSQuantilesnum_retained<string_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the number of retained items in the sketch";
                    desc.examples.push_back("datasketch_quantiles_num_retained(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::VARCHAR] };
                    fn_descs.push_back(std::move(desc));
                }
        

        // Every scalar function deserializes a sketch, which throws on invalid input.
        fs.SetFallible();
        CreateScalarFunctionInfo info(std::move(fs));
        for (auto &d : fn_descs) {
            info.descriptions.push_back(std::move(d));
        }

        DSSetCategories(info, {"sketch", "quantiles"});

        DSRegister(loader, info);


      }
      {
        ScalarFunctionSet fs("datasketch_quantiles_min_item");
        std::vector<FunctionDescription> fn_descs;
        
            
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::TINYINT]},LogicalType::TINYINT
                ,    DSQuantilesmin_item<int8_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the minimum item in the sketch";
                    desc.examples.push_back("datasketch_quantiles_min_item(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::TINYINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::SMALLINT]},LogicalType::SMALLINT
                ,    DSQuantilesmin_item<int16_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the minimum item in the sketch";
                    desc.examples.push_back("datasketch_quantiles_min_item(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::SMALLINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::INTEGER]},LogicalType::INTEGER
                ,    DSQuantilesmin_item<int32_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the minimum item in the sketch";
                    desc.examples.push_back("datasketch_quantiles_min_item(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::INTEGER] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::BIGINT]},LogicalType::BIGINT
                ,    DSQuantilesmin_item<int64_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the minimum item in the sketch";
                    desc.examples.push_back("datasketch_quantiles_min_item(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::BIGINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::FLOAT]},LogicalType::FLOAT
                ,    DSQuantilesmin_item<float>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the minimum item in the sketch";
                    desc.examples.push_back("datasketch_quantiles_min_item(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::FLOAT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::DOUBLE]},LogicalType::DOUBLE
                ,    DSQuantilesmin_item<double>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the minimum item in the sketch";
                    desc.examples.push_back("datasketch_quantiles_min_item(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::DOUBLE] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UTINYINT]},LogicalType::UTINYINT
                ,    DSQuantilesmin_item<uint8_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the minimum item in the sketch";
                    desc.examples.push_back("datasketch_quantiles_min_item(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UTINYINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::USMALLINT]},LogicalType::USMALLINT
                ,    DSQuantilesmin_item<uint16_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the minimum item in the sketch";
                    desc.examples.push_back("datasketch_quantiles_min_item(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::USMALLINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UINTEGER]},LogicalType::UINTEGER
                ,    DSQuantilesmin_item<uint32_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the minimum item in the sketch";
                    desc.examples.push_back("datasketch_quantiles_min_item(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UINTEGER] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UBIGINT]},LogicalType::UBIGINT
                ,    DSQuantilesmin_item<uint64_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the minimum item in the sketch";
                    desc.examples.push_back("datasketch_quantiles_min_item(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UBIGINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::VARCHAR]},LogicalType::VARCHAR
                ,    DSQuantilesmin_item<string_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the minimum item in the sketch";
                    desc.examples.push_back("datasketch_quantiles_min_item(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::VARCHAR] };
                    fn_descs.push_back(std::move(desc));
                }
        

        // Every scalar function deserializes a sketch, which throws on invalid input.
        fs.SetFallible();
        CreateScalarFunctionInfo info(std::move(fs));
        for (auto &d : fn_descs) {
            info.descriptions.push_back(std::move(d));
        }

        DSSetCategories(info, {"sketch", "quantiles"});

        DSRegister(loader, info);


      }
      {
        ScalarFunctionSet fs("datasketch_quantiles_max_item");
        std::vector<FunctionDescription> fn_descs;
        
            
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::TINYINT]},LogicalType::TINYINT
                ,    DSQuantilesmax_item<int8_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the maximum item in the sketch";
                    desc.examples.push_back("datasketch_quantiles_max_item(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::TINYINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::SMALLINT]},LogicalType::SMALLINT
                ,    DSQuantilesmax_item<int16_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the maximum item in the sketch";
                    desc.examples.push_back("datasketch_quantiles_max_item(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::SMALLINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::INTEGER]},LogicalType::INTEGER
                ,    DSQuantilesmax_item<int32_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the maximum item in the sketch";
                    desc.examples.push_back("datasketch_quantiles_max_item(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::INTEGER] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::BIGINT]},LogicalType::BIGINT
                ,    DSQuantilesmax_item<int64_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the maximum item in the sketch";
                    desc.examples.push_back("datasketch_quantiles_max_item(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::BIGINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::FLOAT]},LogicalType::FLOAT
                ,    DSQuantilesmax_item<float>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the maximum item in the sketch";
                    desc.examples.push_back("datasketch_quantiles_max_item(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::FLOAT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::DOUBLE]},LogicalType::DOUBLE
                ,    DSQuantilesmax_item<double>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the maximum item in the sketch";
                    desc.examples.push_back("datasketch_quantiles_max_item(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::DOUBLE] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UTINYINT]},LogicalType::UTINYINT
                ,    DSQuantilesmax_item<uint8_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the maximum item in the sketch";
                    desc.examples.push_back("datasketch_quantiles_max_item(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UTINYINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::USMALLINT]},LogicalType::USMALLINT
                ,    DSQuantilesmax_item<uint16_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the maximum item in the sketch";
                    desc.examples.push_back("datasketch_quantiles_max_item(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::USMALLINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UINTEGER]},LogicalType::UINTEGER
                ,    DSQuantilesmax_item<uint32_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the maximum item in the sketch";
                    desc.examples.push_back("datasketch_quantiles_max_item(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UINTEGER] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UBIGINT]},LogicalType::UBIGINT
                ,    DSQuantilesmax_item<uint64_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the maximum item in the sketch";
                    desc.examples.push_back("datasketch_quantiles_max_item(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UBIGINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::VARCHAR]},LogicalType::VARCHAR
                ,    DSQuantilesmax_item<string_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the maximum item in the sketch";
                    desc.examples.push_back("datasketch_quantiles_max_item(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::VARCHAR] };
                    fn_descs.push_back(std::move(desc));
                }
        

        // Every scalar function deserializes a sketch, which throws on invalid input.
        fs.SetFallible();
        CreateScalarFunctionInfo info(std::move(fs));
        for (auto &d : fn_descs) {
            info.descriptions.push_back(std::move(d));
        }

        DSSetCategories(info, {"sketch", "quantiles"});

        DSRegister(loader, info);


      }
      {
        ScalarFunctionSet fs("datasketch_quantiles_quantiles");
        std::vector<FunctionDescription> fn_descs;
        
            
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::TINYINT],LogicalType::LIST(LogicalType::DOUBLE),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::TINYINT)
                ,    DSQuantilesquantiles<int8_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the quantiles (items) at a list of normalized ranks in [0, 1]. A NULL rank yields a NULL quantile";
                    desc.examples.push_back("datasketch_quantiles_quantiles(sketch, [0.25, 0.5, 0.75], true)");
                    desc.parameter_names = { "sketch","ranks","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::TINYINT],LogicalType::LIST(LogicalType::DOUBLE),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::SMALLINT],LogicalType::LIST(LogicalType::DOUBLE),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::SMALLINT)
                ,    DSQuantilesquantiles<int16_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the quantiles (items) at a list of normalized ranks in [0, 1]. A NULL rank yields a NULL quantile";
                    desc.examples.push_back("datasketch_quantiles_quantiles(sketch, [0.25, 0.5, 0.75], true)");
                    desc.parameter_names = { "sketch","ranks","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::SMALLINT],LogicalType::LIST(LogicalType::DOUBLE),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::INTEGER],LogicalType::LIST(LogicalType::DOUBLE),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::INTEGER)
                ,    DSQuantilesquantiles<int32_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the quantiles (items) at a list of normalized ranks in [0, 1]. A NULL rank yields a NULL quantile";
                    desc.examples.push_back("datasketch_quantiles_quantiles(sketch, [0.25, 0.5, 0.75], true)");
                    desc.parameter_names = { "sketch","ranks","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::INTEGER],LogicalType::LIST(LogicalType::DOUBLE),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::BIGINT],LogicalType::LIST(LogicalType::DOUBLE),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::BIGINT)
                ,    DSQuantilesquantiles<int64_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the quantiles (items) at a list of normalized ranks in [0, 1]. A NULL rank yields a NULL quantile";
                    desc.examples.push_back("datasketch_quantiles_quantiles(sketch, [0.25, 0.5, 0.75], true)");
                    desc.parameter_names = { "sketch","ranks","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::BIGINT],LogicalType::LIST(LogicalType::DOUBLE),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::FLOAT],LogicalType::LIST(LogicalType::DOUBLE),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::FLOAT)
                ,    DSQuantilesquantiles<float>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the quantiles (items) at a list of normalized ranks in [0, 1]. A NULL rank yields a NULL quantile";
                    desc.examples.push_back("datasketch_quantiles_quantiles(sketch, [0.25, 0.5, 0.75], true)");
                    desc.parameter_names = { "sketch","ranks","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::FLOAT],LogicalType::LIST(LogicalType::DOUBLE),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::DOUBLE],LogicalType::LIST(LogicalType::DOUBLE),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::DOUBLE)
                ,    DSQuantilesquantiles<double>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the quantiles (items) at a list of normalized ranks in [0, 1]. A NULL rank yields a NULL quantile";
                    desc.examples.push_back("datasketch_quantiles_quantiles(sketch, [0.25, 0.5, 0.75], true)");
                    desc.parameter_names = { "sketch","ranks","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::DOUBLE],LogicalType::LIST(LogicalType::DOUBLE),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UTINYINT],LogicalType::LIST(LogicalType::DOUBLE),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::UTINYINT)
                ,    DSQuantilesquantiles<uint8_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the quantiles (items) at a list of normalized ranks in [0, 1]. A NULL rank yields a NULL quantile";
                    desc.examples.push_back("datasketch_quantiles_quantiles(sketch, [0.25, 0.5, 0.75], true)");
                    desc.parameter_names = { "sketch","ranks","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UTINYINT],LogicalType::LIST(LogicalType::DOUBLE),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::USMALLINT],LogicalType::LIST(LogicalType::DOUBLE),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::USMALLINT)
                ,    DSQuantilesquantiles<uint16_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the quantiles (items) at a list of normalized ranks in [0, 1]. A NULL rank yields a NULL quantile";
                    desc.examples.push_back("datasketch_quantiles_quantiles(sketch, [0.25, 0.5, 0.75], true)");
                    desc.parameter_names = { "sketch","ranks","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::USMALLINT],LogicalType::LIST(LogicalType::DOUBLE),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UINTEGER],LogicalType::LIST(LogicalType::DOUBLE),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::UINTEGER)
                ,    DSQuantilesquantiles<uint32_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the quantiles (items) at a list of normalized ranks in [0, 1]. A NULL rank yields a NULL quantile";
                    desc.examples.push_back("datasketch_quantiles_quantiles(sketch, [0.25, 0.5, 0.75], true)");
                    desc.parameter_names = { "sketch","ranks","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UINTEGER],LogicalType::LIST(LogicalType::DOUBLE),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UBIGINT],LogicalType::LIST(LogicalType::DOUBLE),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::UBIGINT)
                ,    DSQuantilesquantiles<uint64_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the quantiles (items) at a list of normalized ranks in [0, 1]. A NULL rank yields a NULL quantile";
                    desc.examples.push_back("datasketch_quantiles_quantiles(sketch, [0.25, 0.5, 0.75], true)");
                    desc.parameter_names = { "sketch","ranks","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UBIGINT],LogicalType::LIST(LogicalType::DOUBLE),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::VARCHAR],LogicalType::LIST(LogicalType::DOUBLE),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::VARCHAR)
                ,    DSQuantilesquantiles<string_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the quantiles (items) at a list of normalized ranks in [0, 1]. A NULL rank yields a NULL quantile";
                    desc.examples.push_back("datasketch_quantiles_quantiles(sketch, [0.25, 0.5, 0.75], true)");
                    desc.parameter_names = { "sketch","ranks","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::VARCHAR],LogicalType::LIST(LogicalType::DOUBLE),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
        

        // Every scalar function deserializes a sketch, which throws on invalid input.
        fs.SetFallible();
        CreateScalarFunctionInfo info(std::move(fs));
        for (auto &d : fn_descs) {
            info.descriptions.push_back(std::move(d));
        }

        DSSetCategories(info, {"sketch", "quantiles"});

        DSRegister(loader, info);


      }
      {
        ScalarFunctionSet fs("datasketch_quantiles_ranks");
        std::vector<FunctionDescription> fn_descs;
        
            
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::TINYINT],LogicalType::LIST(LogicalType::TINYINT),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::DOUBLE)
                ,    DSQuantilesranks<int8_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the normalized ranks of a list of items. A NULL item yields a NULL rank";
                    desc.examples.push_back("datasketch_quantiles_ranks(sketch, [10, 20, 30], true)");
                    desc.parameter_names = { "sketch","items","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::TINYINT],LogicalType::LIST(LogicalType::TINYINT),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::SMALLINT],LogicalType::LIST(LogicalType::SMALLINT),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::DOUBLE)
                ,    DSQuantilesranks<int16_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the normalized ranks of a list of items. A NULL item yields a NULL rank";
                    desc.examples.push_back("datasketch_quantiles_ranks(sketch, [10, 20, 30], true)");
                    desc.parameter_names = { "sketch","items","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::SMALLINT],LogicalType::LIST(LogicalType::SMALLINT),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::INTEGER],LogicalType::LIST(LogicalType::INTEGER),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::DOUBLE)
                ,    DSQuantilesranks<int32_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the normalized ranks of a list of items. A NULL item yields a NULL rank";
                    desc.examples.push_back("datasketch_quantiles_ranks(sketch, [10, 20, 30], true)");
                    desc.parameter_names = { "sketch","items","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::INTEGER],LogicalType::LIST(LogicalType::INTEGER),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::BIGINT],LogicalType::LIST(LogicalType::BIGINT),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::DOUBLE)
                ,    DSQuantilesranks<int64_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the normalized ranks of a list of items. A NULL item yields a NULL rank";
                    desc.examples.push_back("datasketch_quantiles_ranks(sketch, [10, 20, 30], true)");
                    desc.parameter_names = { "sketch","items","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::BIGINT],LogicalType::LIST(LogicalType::BIGINT),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::FLOAT],LogicalType::LIST(LogicalType::FLOAT),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::DOUBLE)
                ,    DSQuantilesranks<float>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the normalized ranks of a list of items. A NULL item yields a NULL rank";
                    desc.examples.push_back("datasketch_quantiles_ranks(sketch, [10, 20, 30], true)");
                    desc.parameter_names = { "sketch","items","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::FLOAT],LogicalType::LIST(LogicalType::FLOAT),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::DOUBLE],LogicalType::LIST(LogicalType::DOUBLE),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::DOUBLE)
                ,    DSQuantilesranks<double>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the normalized ranks of a list of items. A NULL item yields a NULL rank";
                    desc.examples.push_back("datasketch_quantiles_ranks(sketch, [10, 20, 30], true)");
                    desc.parameter_names = { "sketch","items","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::DOUBLE],LogicalType::LIST(LogicalType::DOUBLE),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UTINYINT],LogicalType::LIST(LogicalType::UTINYINT),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::DOUBLE)
                ,    DSQuantilesranks<uint8_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the normalized ranks of a list of items. A NULL item yields a NULL rank";
                    desc.examples.push_back("datasketch_quantiles_ranks(sketch, [10, 20, 30], true)");
                    desc.parameter_names = { "sketch","items","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UTINYINT],LogicalType::LIST(LogicalType::UTINYINT),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::USMALLINT],LogicalType::LIST(LogicalType::USMALLINT),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::DOUBLE)
                ,    DSQuantilesranks<uint16_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the normalized ranks of a list of items. A NULL item yields a NULL rank";
                    desc.examples.push_back("datasketch_quantiles_ranks(sketch, [10, 20, 30], true)");
                    desc.parameter_names = { "sketch","items","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::USMALLINT],LogicalType::LIST(LogicalType::USMALLINT),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UINTEGER],LogicalType::LIST(LogicalType::UINTEGER),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::DOUBLE)
                ,    DSQuantilesranks<uint32_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the normalized ranks of a list of items. A NULL item yields a NULL rank";
                    desc.examples.push_back("datasketch_quantiles_ranks(sketch, [10, 20, 30], true)");
                    desc.parameter_names = { "sketch","items","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UINTEGER],LogicalType::LIST(LogicalType::UINTEGER),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UBIGINT],LogicalType::LIST(LogicalType::UBIGINT),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::DOUBLE)
                ,    DSQuantilesranks<uint64_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the normalized ranks of a list of items. A NULL item yields a NULL rank";
                    desc.examples.push_back("datasketch_quantiles_ranks(sketch, [10, 20, 30], true)");
                    desc.parameter_names = { "sketch","items","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UBIGINT],LogicalType::LIST(LogicalType::UBIGINT),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::VARCHAR],LogicalType::LIST(LogicalType::VARCHAR),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::DOUBLE)
                ,    DSQuantilesranks<string_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the normalized ranks of a list of items. A NULL item yields a NULL rank";
                    desc.examples.push_back("datasketch_quantiles_ranks(sketch, [10, 20, 30], true)");
                    desc.parameter_names = { "sketch","items","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::VARCHAR],LogicalType::LIST(LogicalType::VARCHAR),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
        

        // Every scalar function deserializes a sketch, which throws on invalid input.
        fs.SetFallible();
        CreateScalarFunctionInfo info(std::move(fs));
        for (auto &d : fn_descs) {
            info.descriptions.push_back(std::move(d));
        }

        DSSetCategories(info, {"sketch", "quantiles"});

        DSRegister(loader, info);


      }
      {
        ScalarFunctionSet fs("datasketch_quantiles_sorted_view");
        std::vector<FunctionDescription> fn_descs;
        
            
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::TINYINT]},LogicalType::LIST(LogicalType::STRUCT(child_list_t<LogicalType>{{"item", LogicalType::TINYINT}, {"weight", LogicalType::UBIGINT}, {"cumulative_weight", LogicalType::UBIGINT}}))
                ,    DSQuantilessorted_view<int8_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the sketch's sorted view: one entry per retained item in ascending order (equal items can appear more than once), with the item's weight and the inclusive cumulative weight (the number of input items less than or equal to it; estimated once the sketch is in estimation mode). Divide cumulative_weight by datasketch_quantiles_n(sketch) for the normalized rank. An empty sketch yields an empty list";
                    desc.examples.push_back("datasketch_quantiles_sorted_view(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::TINYINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::SMALLINT]},LogicalType::LIST(LogicalType::STRUCT(child_list_t<LogicalType>{{"item", LogicalType::SMALLINT}, {"weight", LogicalType::UBIGINT}, {"cumulative_weight", LogicalType::UBIGINT}}))
                ,    DSQuantilessorted_view<int16_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the sketch's sorted view: one entry per retained item in ascending order (equal items can appear more than once), with the item's weight and the inclusive cumulative weight (the number of input items less than or equal to it; estimated once the sketch is in estimation mode). Divide cumulative_weight by datasketch_quantiles_n(sketch) for the normalized rank. An empty sketch yields an empty list";
                    desc.examples.push_back("datasketch_quantiles_sorted_view(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::SMALLINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::INTEGER]},LogicalType::LIST(LogicalType::STRUCT(child_list_t<LogicalType>{{"item", LogicalType::INTEGER}, {"weight", LogicalType::UBIGINT}, {"cumulative_weight", LogicalType::UBIGINT}}))
                ,    DSQuantilessorted_view<int32_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the sketch's sorted view: one entry per retained item in ascending order (equal items can appear more than once), with the item's weight and the inclusive cumulative weight (the number of input items less than or equal to it; estimated once the sketch is in estimation mode). Divide cumulative_weight by datasketch_quantiles_n(sketch) for the normalized rank. An empty sketch yields an empty list";
                    desc.examples.push_back("datasketch_quantiles_sorted_view(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::INTEGER] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::BIGINT]},LogicalType::LIST(LogicalType::STRUCT(child_list_t<LogicalType>{{"item", LogicalType::BIGINT}, {"weight", LogicalType::UBIGINT}, {"cumulative_weight", LogicalType::UBIGINT}}))
                ,    DSQuantilessorted_view<int64_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the sketch's sorted view: one entry per retained item in ascending order (equal items can appear more than once), with the item's weight and the inclusive cumulative weight (the number of input items less than or equal to it; estimated once the sketch is in estimation mode). Divide cumulative_weight by datasketch_quantiles_n(sketch) for the normalized rank. An empty sketch yields an empty list";
                    desc.examples.push_back("datasketch_quantiles_sorted_view(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::BIGINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::FLOAT]},LogicalType::LIST(LogicalType::STRUCT(child_list_t<LogicalType>{{"item", LogicalType::FLOAT}, {"weight", LogicalType::UBIGINT}, {"cumulative_weight", LogicalType::UBIGINT}}))
                ,    DSQuantilessorted_view<float>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the sketch's sorted view: one entry per retained item in ascending order (equal items can appear more than once), with the item's weight and the inclusive cumulative weight (the number of input items less than or equal to it; estimated once the sketch is in estimation mode). Divide cumulative_weight by datasketch_quantiles_n(sketch) for the normalized rank. An empty sketch yields an empty list";
                    desc.examples.push_back("datasketch_quantiles_sorted_view(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::FLOAT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::DOUBLE]},LogicalType::LIST(LogicalType::STRUCT(child_list_t<LogicalType>{{"item", LogicalType::DOUBLE}, {"weight", LogicalType::UBIGINT}, {"cumulative_weight", LogicalType::UBIGINT}}))
                ,    DSQuantilessorted_view<double>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the sketch's sorted view: one entry per retained item in ascending order (equal items can appear more than once), with the item's weight and the inclusive cumulative weight (the number of input items less than or equal to it; estimated once the sketch is in estimation mode). Divide cumulative_weight by datasketch_quantiles_n(sketch) for the normalized rank. An empty sketch yields an empty list";
                    desc.examples.push_back("datasketch_quantiles_sorted_view(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::DOUBLE] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UTINYINT]},LogicalType::LIST(LogicalType::STRUCT(child_list_t<LogicalType>{{"item", LogicalType::UTINYINT}, {"weight", LogicalType::UBIGINT}, {"cumulative_weight", LogicalType::UBIGINT}}))
                ,    DSQuantilessorted_view<uint8_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the sketch's sorted view: one entry per retained item in ascending order (equal items can appear more than once), with the item's weight and the inclusive cumulative weight (the number of input items less than or equal to it; estimated once the sketch is in estimation mode). Divide cumulative_weight by datasketch_quantiles_n(sketch) for the normalized rank. An empty sketch yields an empty list";
                    desc.examples.push_back("datasketch_quantiles_sorted_view(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UTINYINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::USMALLINT]},LogicalType::LIST(LogicalType::STRUCT(child_list_t<LogicalType>{{"item", LogicalType::USMALLINT}, {"weight", LogicalType::UBIGINT}, {"cumulative_weight", LogicalType::UBIGINT}}))
                ,    DSQuantilessorted_view<uint16_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the sketch's sorted view: one entry per retained item in ascending order (equal items can appear more than once), with the item's weight and the inclusive cumulative weight (the number of input items less than or equal to it; estimated once the sketch is in estimation mode). Divide cumulative_weight by datasketch_quantiles_n(sketch) for the normalized rank. An empty sketch yields an empty list";
                    desc.examples.push_back("datasketch_quantiles_sorted_view(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::USMALLINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UINTEGER]},LogicalType::LIST(LogicalType::STRUCT(child_list_t<LogicalType>{{"item", LogicalType::UINTEGER}, {"weight", LogicalType::UBIGINT}, {"cumulative_weight", LogicalType::UBIGINT}}))
                ,    DSQuantilessorted_view<uint32_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the sketch's sorted view: one entry per retained item in ascending order (equal items can appear more than once), with the item's weight and the inclusive cumulative weight (the number of input items less than or equal to it; estimated once the sketch is in estimation mode). Divide cumulative_weight by datasketch_quantiles_n(sketch) for the normalized rank. An empty sketch yields an empty list";
                    desc.examples.push_back("datasketch_quantiles_sorted_view(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UINTEGER] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UBIGINT]},LogicalType::LIST(LogicalType::STRUCT(child_list_t<LogicalType>{{"item", LogicalType::UBIGINT}, {"weight", LogicalType::UBIGINT}, {"cumulative_weight", LogicalType::UBIGINT}}))
                ,    DSQuantilessorted_view<uint64_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the sketch's sorted view: one entry per retained item in ascending order (equal items can appear more than once), with the item's weight and the inclusive cumulative weight (the number of input items less than or equal to it; estimated once the sketch is in estimation mode). Divide cumulative_weight by datasketch_quantiles_n(sketch) for the normalized rank. An empty sketch yields an empty list";
                    desc.examples.push_back("datasketch_quantiles_sorted_view(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UBIGINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::VARCHAR]},LogicalType::LIST(LogicalType::STRUCT(child_list_t<LogicalType>{{"item", LogicalType::VARCHAR}, {"weight", LogicalType::UBIGINT}, {"cumulative_weight", LogicalType::UBIGINT}}))
                ,    DSQuantilessorted_view<string_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the sketch's sorted view: one entry per retained item in ascending order (equal items can appear more than once), with the item's weight and the inclusive cumulative weight (the number of input items less than or equal to it; estimated once the sketch is in estimation mode). Divide cumulative_weight by datasketch_quantiles_n(sketch) for the normalized rank. An empty sketch yields an empty list";
                    desc.examples.push_back("datasketch_quantiles_sorted_view(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::VARCHAR] };
                    fn_descs.push_back(std::move(desc));
                }
        

        // Every scalar function deserializes a sketch, which throws on invalid input.
        fs.SetFallible();
        CreateScalarFunctionInfo info(std::move(fs));
        for (auto &d : fn_descs) {
            info.descriptions.push_back(std::move(d));
        }

        DSSetCategories(info, {"sketch", "quantiles"});

        DSRegister(loader, info);


      }

      // This funciton creates the sketches.
      {
      AggregateFunctionSet sketch("datasketch_quantiles");
      std::vector<FunctionDescription> agg_descs;
      
        
            {
                auto fun = DSQuantilesCreateAggregate<int8_t>(LogicalType::TINYINT, sketch_map_types[LogicalTypeId::TINYINT]);
                fun.SetBindCallback(DSQuantilesBind);
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                DSPrependArgument(fun, LogicalType::INTEGER);
                sketch.AddFunction(fun);

                FunctionDescription desc;
                desc.description = "Creates a sketch_quantiles data sketch by aggregating values";
                desc.examples.push_back("datasketch_quantiles(k, data)");
                desc.parameter_names = {"k", "data"};
                desc.parameter_types = { LogicalType::INTEGER, LogicalType::TINYINT };
                agg_descs.push_back(std::move(desc));
            }
            
            {
                auto fun = DSQuantilesMergeAggregate<int8_t>(LogicalType::TINYINT, sketch_map_types[LogicalTypeId::TINYINT]);
                fun.SetBindCallback(DSQuantilesBind);
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                DSPrependArgument(fun, LogicalType::INTEGER);
                sketch.AddFunction(fun);

                FunctionDescription desc;
                desc.description = "Creates a sketch_quantiles data sketch by merging other Quantiles data sketches";
                desc.examples.push_back("datasketch_quantiles(k, sketch)");
                desc.parameter_names = {"k", "sketch"};
                desc.parameter_types = { LogicalType::INTEGER, sketch_map_types[LogicalTypeId::TINYINT] };
                agg_descs.push_back(std::move(desc));
            }
        
        
            {
                auto fun = DSQuantilesCreateAggregate<int16_t>(LogicalType::SMALLINT, sketch_map_types[LogicalTypeId::SMALLINT]);
                fun.SetBindCallback(DSQuantilesBind);
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                DSPrependArgument(fun, LogicalType::INTEGER);
                sketch.AddFunction(fun);

                FunctionDescription desc;
                desc.description = "Creates a sketch_quantiles data sketch by aggregating values";
                desc.examples.push_back("datasketch_quantiles(k, data)");
                desc.parameter_names = {"k", "data"};
                desc.parameter_types = { LogicalType::INTEGER, LogicalType::SMALLINT };
                agg_descs.push_back(std::move(desc));
            }
            
            {
                auto fun = DSQuantilesMergeAggregate<int16_t>(LogicalType::SMALLINT, sketch_map_types[LogicalTypeId::SMALLINT]);
                fun.SetBindCallback(DSQuantilesBind);
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                DSPrependArgument(fun, LogicalType::INTEGER);
                sketch.AddFunction(fun);

                FunctionDescription desc;
                desc.description = "Creates a sketch_quantiles data sketch by merging other Quantiles data sketches";
                desc.examples.push_back("datasketch_quantiles(k, sketch)");
                desc.parameter_names = {"k", "sketch"};
                desc.parameter_types = { LogicalType::INTEGER, sketch_map_types[LogicalTypeId::SMALLINT] };
                agg_descs.push_back(std::move(desc));
            }
        
        
            {
                auto fun = DSQuantilesCreateAggregate<int32_t>(LogicalType::INTEGER, sketch_map_types[LogicalTypeId::INTEGER]);
                fun.SetBindCallback(DSQuantilesBind);
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                DSPrependArgument(fun, LogicalType::INTEGER);
                sketch.AddFunction(fun);

                FunctionDescription desc;
                desc.description = "Creates a sketch_quantiles data sketch by aggregating values";
                desc.examples.push_back("datasketch_quantiles(k, data)");
                desc.parameter_names = {"k", "data"};
                desc.parameter_types = { LogicalType::INTEGER, LogicalType::INTEGER };
                agg_descs.push_back(std::move(desc));
            }
            
            {
                auto fun = DSQuantilesMergeAggregate<int32_t>(LogicalType::INTEGER, sketch_map_types[LogicalTypeId::INTEGER]);
                fun.SetBindCallback(DSQuantilesBind);
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                DSPrependArgument(fun, LogicalType::INTEGER);
                sketch.AddFunction(fun);

                FunctionDescription desc;
                desc.description = "Creates a sketch_quantiles data sketch by merging other Quantiles data sketches";
                desc.examples.push_back("datasketch_quantiles(k, sketch)");
                desc.parameter_names = {"k", "sketch"};
                desc.parameter_types = { LogicalType::INTEGER, sketch_map_types[LogicalTypeId::INTEGER] };
                agg_descs.push_back(std::move(desc));
            }
        
        
            {
                auto fun = DSQuantilesCreateAggregate<int64_t>(LogicalType::BIGINT, sketch_map_types[LogicalTypeId::BIGINT]);
                fun.SetBindCallback(DSQuantilesBind);
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                DSPrependArgument(fun, LogicalType::INTEGER);
                sketch.AddFunction(fun);

                FunctionDescription desc;
                desc.description = "Creates a sketch_quantiles data sketch by aggregating values";
                desc.examples.push_back("datasketch_quantiles(k, data)");
                desc.parameter_names = {"k", "data"};
                desc.parameter_types = { LogicalType::INTEGER, LogicalType::BIGINT };
                agg_descs.push_back(std::move(desc));
            }
            
            {
                auto fun = DSQuantilesMergeAggregate<int64_t>(LogicalType::BIGINT, sketch_map_types[LogicalTypeId::BIGINT]);
                fun.SetBindCallback(DSQuantilesBind);
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                DSPrependArgument(fun, LogicalType::INTEGER);
                sketch.AddFunction(fun);

                FunctionDescription desc;
                desc.description = "Creates a sketch_quantiles data sketch by merging other Quantiles data sketches";
                desc.examples.push_back("datasketch_quantiles(k, sketch)");
                desc.parameter_names = {"k", "sketch"};
                desc.parameter_types = { LogicalType::INTEGER, sketch_map_types[LogicalTypeId::BIGINT] };
                agg_descs.push_back(std::move(desc));
            }
        
        
            {
                auto fun = DSQuantilesCreateAggregate<float>(LogicalType::FLOAT, sketch_map_types[LogicalTypeId::FLOAT]);
                fun.SetBindCallback(DSQuantilesBind);
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                DSPrependArgument(fun, LogicalType::INTEGER);
                sketch.AddFunction(fun);

                FunctionDescription desc;
                desc.description = "Creates a sketch_quantiles data sketch by aggregating values";
                desc.examples.push_back("datasketch_quantiles(k, data)");
                desc.parameter_names = {"k", "data"};
                desc.parameter_types = { LogicalType::INTEGER, LogicalType::FLOAT };
                agg_descs.push_back(std::move(desc));
            }
            
            {
                auto fun = DSQuantilesMergeAggregate<float>(LogicalType::FLOAT, sketch_map_types[LogicalTypeId::FLOAT]);
                fun.SetBindCallback(DSQuantilesBind);
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                DSPrependArgument(fun, LogicalType::INTEGER);
                sketch.AddFunction(fun);

                FunctionDescription desc;
                desc.description = "Creates a sketch_quantiles data sketch by merging other Quantiles data sketches";
                desc.examples.push_back("datasketch_quantiles(k, sketch)");
                desc.parameter_names = {"k", "sketch"};
                desc.parameter_types = { LogicalType::INTEGER, sketch_map_types[LogicalTypeId::FLOAT] };
                agg_descs.push_back(std::move(desc));
            }
        
        
            {
                auto fun = DSQuantilesCreateAggregate<double>(LogicalType::DOUBLE, sketch_map_types[LogicalTypeId::DOUBLE]);
                fun.SetBindCallback(DSQuantilesBind);
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                DSPrependArgument(fun, LogicalType::INTEGER);
                sketch.AddFunction(fun);

                FunctionDescription desc;
                desc.description = "Creates a sketch_quantiles data sketch by aggregating values";
                desc.examples.push_back("datasketch_quantiles(k, data)");
                desc.parameter_names = {"k", "data"};
                desc.parameter_types = { LogicalType::INTEGER, LogicalType::DOUBLE };
                agg_descs.push_back(std::move(desc));
            }
            
            {
                auto fun = DSQuantilesMergeAggregate<double>(LogicalType::DOUBLE, sketch_map_types[LogicalTypeId::DOUBLE]);
                fun.SetBindCallback(DSQuantilesBind);
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                DSPrependArgument(fun, LogicalType::INTEGER);
                sketch.AddFunction(fun);

                FunctionDescription desc;
                desc.description = "Creates a sketch_quantiles data sketch by merging other Quantiles data sketches";
                desc.examples.push_back("datasketch_quantiles(k, sketch)");
                desc.parameter_names = {"k", "sketch"};
                desc.parameter_types = { LogicalType::INTEGER, sketch_map_types[LogicalTypeId::DOUBLE] };
                agg_descs.push_back(std::move(desc));
            }
        
        
            {
                auto fun = DSQuantilesCreateAggregate<uint8_t>(LogicalType::UTINYINT, sketch_map_types[LogicalTypeId::UTINYINT]);
                fun.SetBindCallback(DSQuantilesBind);
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                DSPrependArgument(fun, LogicalType::INTEGER);
                sketch.AddFunction(fun);

                FunctionDescription desc;
                desc.description = "Creates a sketch_quantiles data sketch by aggregating values";
                desc.examples.push_back("datasketch_quantiles(k, data)");
                desc.parameter_names = {"k", "data"};
                desc.parameter_types = { LogicalType::INTEGER, LogicalType::UTINYINT };
                agg_descs.push_back(std::move(desc));
            }
            
            {
                auto fun = DSQuantilesMergeAggregate<uint8_t>(LogicalType::UTINYINT, sketch_map_types[LogicalTypeId::UTINYINT]);
                fun.SetBindCallback(DSQuantilesBind);
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                DSPrependArgument(fun, LogicalType::INTEGER);
                sketch.AddFunction(fun);

                FunctionDescription desc;
                desc.description = "Creates a sketch_quantiles data sketch by merging other Quantiles data sketches";
                desc.examples.push_back("datasketch_quantiles(k, sketch)");
                desc.parameter_names = {"k", "sketch"};
                desc.parameter_types = { LogicalType::INTEGER, sketch_map_types[LogicalTypeId::UTINYINT] };
                agg_descs.push_back(std::move(desc));
            }
        
        
            {
                auto fun = DSQuantilesCreateAggregate<uint16_t>(LogicalType::USMALLINT, sketch_map_types[LogicalTypeId::USMALLINT]);
                fun.SetBindCallback(DSQuantilesBind);
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                DSPrependArgument(fun, LogicalType::INTEGER);
                sketch.AddFunction(fun);

                FunctionDescription desc;
                desc.description = "Creates a sketch_quantiles data sketch by aggregating values";
                desc.examples.push_back("datasketch_quantiles(k, data)");
                desc.parameter_names = {"k", "data"};
                desc.parameter_types = { LogicalType::INTEGER, LogicalType::USMALLINT };
                agg_descs.push_back(std::move(desc));
            }
            
            {
                auto fun = DSQuantilesMergeAggregate<uint16_t>(LogicalType::USMALLINT, sketch_map_types[LogicalTypeId::USMALLINT]);
                fun.SetBindCallback(DSQuantilesBind);
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                DSPrependArgument(fun, LogicalType::INTEGER);
                sketch.AddFunction(fun);

                FunctionDescription desc;
                desc.description = "Creates a sketch_quantiles data sketch by merging other Quantiles data sketches";
                desc.examples.push_back("datasketch_quantiles(k, sketch)");
                desc.parameter_names = {"k", "sketch"};
                desc.parameter_types = { LogicalType::INTEGER, sketch_map_types[LogicalTypeId::USMALLINT] };
                agg_descs.push_back(std::move(desc));
            }
        
        
            {
                auto fun = DSQuantilesCreateAggregate<uint32_t>(LogicalType::UINTEGER, sketch_map_types[LogicalTypeId::UINTEGER]);
                fun.SetBindCallback(DSQuantilesBind);
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                DSPrependArgument(fun, LogicalType::INTEGER);
                sketch.AddFunction(fun);

                FunctionDescription desc;
                desc.description = "Creates a sketch_quantiles data sketch by aggregating values";
                desc.examples.push_back("datasketch_quantiles(k, data)");
                desc.parameter_names = {"k", "data"};
                desc.parameter_types = { LogicalType::INTEGER, LogicalType::UINTEGER };
                agg_descs.push_back(std::move(desc));
            }
            
            {
                auto fun = DSQuantilesMergeAggregate<uint32_t>(LogicalType::UINTEGER, sketch_map_types[LogicalTypeId::UINTEGER]);
                fun.SetBindCallback(DSQuantilesBind);
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                DSPrependArgument(fun, LogicalType::INTEGER);
                sketch.AddFunction(fun);

                FunctionDescription desc;
                desc.description = "Creates a sketch_quantiles data sketch by merging other Quantiles data sketches";
                desc.examples.push_back("datasketch_quantiles(k, sketch)");
                desc.parameter_names = {"k", "sketch"};
                desc.parameter_types = { LogicalType::INTEGER, sketch_map_types[LogicalTypeId::UINTEGER] };
                agg_descs.push_back(std::move(desc));
            }
        
        
            {
                auto fun = DSQuantilesCreateAggregate<uint64_t>(LogicalType::UBIGINT, sketch_map_types[LogicalTypeId::UBIGINT]);
                fun.SetBindCallback(DSQuantilesBind);
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                DSPrependArgument(fun, LogicalType::INTEGER);
                sketch.AddFunction(fun);

                FunctionDescription desc;
                desc.description = "Creates a sketch_quantiles data sketch by aggregating values";
                desc.examples.push_back("datasketch_quantiles(k, data)");
                desc.parameter_names = {"k", "data"};
                desc.parameter_types = { LogicalType::INTEGER, LogicalType::UBIGINT };
                agg_descs.push_back(std::move(desc));
            }
            
            {
                auto fun = DSQuantilesMergeAggregate<uint64_t>(LogicalType::UBIGINT, sketch_map_types[LogicalTypeId::UBIGINT]);
                fun.SetBindCallback(DSQuantilesBind);
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                DSPrependArgument(fun, LogicalType::INTEGER);
                sketch.AddFunction(fun);

                FunctionDescription desc;
                desc.description = "Creates a sketch_quantiles data sketch by merging other Quantiles data sketches";
                desc.examples.push_back("datasketch_quantiles(k, sketch)");
                desc.parameter_names = {"k", "sketch"};
                desc.parameter_types = { LogicalType::INTEGER, sketch_map_types[LogicalTypeId::UBIGINT] };
                agg_descs.push_back(std::move(desc));
            }
        
        
            {
                auto fun = DSQuantilesCreateAggregate<string_t>(LogicalType::VARCHAR, sketch_map_types[LogicalTypeId::VARCHAR]);
                fun.SetBindCallback(DSQuantilesBind);
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                DSPrependArgument(fun, LogicalType::INTEGER);
                sketch.AddFunction(fun);

                FunctionDescription desc;
                desc.description = "Creates a sketch_quantiles data sketch by aggregating values";
                desc.examples.push_back("datasketch_quantiles(k, data)");
                desc.parameter_names = {"k", "data"};
                desc.parameter_types = { LogicalType::INTEGER, LogicalType::VARCHAR };
                agg_descs.push_back(std::move(desc));
            }
            
            {
                auto fun = DSQuantilesMergeAggregate<string_t>(LogicalType::VARCHAR, sketch_map_types[LogicalTypeId::VARCHAR]);
                fun.SetBindCallback(DSQuantilesBind);
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                DSPrependArgument(fun, LogicalType::INTEGER);
                sketch.AddFunction(fun);

                FunctionDescription desc;
                desc.description = "Creates a sketch_quantiles data sketch by merging other Quantiles data sketches";
                desc.examples.push_back("datasketch_quantiles(k, sketch)");
                desc.parameter_names = {"k", "sketch"};
                desc.parameter_types = { LogicalType::INTEGER, sketch_map_types[LogicalTypeId::VARCHAR] };
                agg_descs.push_back(std::move(desc));
            }
        
      CreateAggregateFunctionInfo sketch_info(sketch);
      for (auto &d : agg_descs) {
          sketch_info.descriptions.push_back(std::move(d));
      }

      DSSetCategories(sketch_info, {"sketch", "quantiles"});

      DSRegister(loader, sketch_info);
      }



      


  }
    


template <class T>

static inline void DSKLLis_empty(DataChunk &args, ExpressionState &state, Vector &result)
{
    // Get the references to the incoming vectors.
    D_ASSERT(args.ColumnCount() == 1);

    
        auto &sketch_vector = args.data[0];

    
        
        UnaryExecutor::Execute
        <string_t,bool>
        (
        sketch_vector,result,
        [&](string_t sketch_data) {

            
            auto sketch = [&]() {
                try {
                    return datasketches::kll_sketch<DSItem<T>>::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
                } catch (const std::exception &e) {
                    throw InvalidInputException("Failed to deserialize KLL sketch: %s", e.what());
                }
            }();
return sketch.is_empty();
        });

}

    


template <class T>

static inline void DSKLLk(DataChunk &args, ExpressionState &state, Vector &result)
{
    // Get the references to the incoming vectors.
    D_ASSERT(args.ColumnCount() == 1);

    
        auto &sketch_vector = args.data[0];

    
        
        UnaryExecutor::Execute
        <string_t,uint16_t>
        (
        sketch_vector,result,
        [&](string_t sketch_data) {

            
            auto sketch = [&]() {
                try {
                    return datasketches::kll_sketch<DSItem<T>>::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
                } catch (const std::exception &e) {
                    throw InvalidInputException("Failed to deserialize KLL sketch: %s", e.what());
                }
            }();
return sketch.get_k();
        });

}

    


template <class T>

static inline void DSKLLcdf(DataChunk &args, ExpressionState &state, Vector &result)
{
    // Get the references to the incoming vectors.
    D_ASSERT(args.ColumnCount() == 3);

    
        auto &sketch_vector = args.data[0];
        auto &split_points_vector = args.data[1];
        auto &inclusive_vector = args.data[2];

    
        
                    auto &split_points_list_children = ListVector::GetChild(split_points_vector);

                    UnifiedVectorFormat split_points_children_unified;
                    split_points_list_children.ToUnifiedFormat(split_points_children_unified);

                    const T *split_points_list_children_data = UnifiedVectorFormat::GetData<T>(split_points_children_unified);
                    
        TernaryExecutor::Execute
        <string_t,list_entry_t,bool,list_entry_t>
        (
        sketch_vector,split_points_vector,inclusive_vector,result,
        [&](string_t sketch_data,list_entry_t split_points_data,bool inclusive_data) {

            
            auto sketch = [&]() {
                try {
                    return datasketches::kll_sketch<DSItem<T>>::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
                } catch (const std::exception &e) {
                    throw InvalidInputException("Failed to deserialize KLL sketch: %s", e.what());
                }
            }();

                    std::vector<DSItem<T>> passing_points(split_points_data.length);
                    for (idx_t i = 0; i < split_points_data.length; i++)
                    {
                        passing_points[i] = DSItemIn(split_points_list_children_data[split_points_children_unified.sel->get_index(i + split_points_data.offset)]);
                    }
                    
auto cdf_result = sketch.get_CDF(passing_points.data(), split_points_data.length, inclusive_data);
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
                
        });

}

    


template <class T>

static inline void DSKLLpmf(DataChunk &args, ExpressionState &state, Vector &result)
{
    // Get the references to the incoming vectors.
    D_ASSERT(args.ColumnCount() == 3);

    
        auto &sketch_vector = args.data[0];
        auto &split_points_vector = args.data[1];
        auto &inclusive_vector = args.data[2];

    
        
                    auto &split_points_list_children = ListVector::GetChild(split_points_vector);

                    UnifiedVectorFormat split_points_children_unified;
                    split_points_list_children.ToUnifiedFormat(split_points_children_unified);

                    const T *split_points_list_children_data = UnifiedVectorFormat::GetData<T>(split_points_children_unified);
                    
        TernaryExecutor::Execute
        <string_t,list_entry_t,bool,list_entry_t>
        (
        sketch_vector,split_points_vector,inclusive_vector,result,
        [&](string_t sketch_data,list_entry_t split_points_data,bool inclusive_data) {

            
            auto sketch = [&]() {
                try {
                    return datasketches::kll_sketch<DSItem<T>>::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
                } catch (const std::exception &e) {
                    throw InvalidInputException("Failed to deserialize KLL sketch: %s", e.what());
                }
            }();

                    std::vector<DSItem<T>> passing_points(split_points_data.length);
                    for (idx_t i = 0; i < split_points_data.length; i++)
                    {
                        passing_points[i] = DSItemIn(split_points_list_children_data[split_points_children_unified.sel->get_index(i + split_points_data.offset)]);
                    }
                    
auto pmf_result = sketch.get_PMF(passing_points.data(), split_points_data.length, inclusive_data);
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
                
        });

}

    


template <class T>

static inline void DSKLLnormalized_rank_error(DataChunk &args, ExpressionState &state, Vector &result)
{
    // Get the references to the incoming vectors.
    D_ASSERT(args.ColumnCount() == 2);

    
        auto &sketch_vector = args.data[0];
        auto &is_pmf_vector = args.data[1];

    
        
        BinaryExecutor::Execute
        <string_t,bool,double>
        (
        sketch_vector,is_pmf_vector,result,
        [&](string_t sketch_data,bool is_pmf_data) {

            
            auto sketch = [&]() {
                try {
                    return datasketches::kll_sketch<DSItem<T>>::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
                } catch (const std::exception &e) {
                    throw InvalidInputException("Failed to deserialize KLL sketch: %s", e.what());
                }
            }();
return sketch.get_normalized_rank_error(is_pmf_data);
        });

}

    


template <class T>

static inline void DSKLLdescribe(DataChunk &args, ExpressionState &state, Vector &result)
{
    // Get the references to the incoming vectors.
    D_ASSERT(args.ColumnCount() == 3);

    
        auto &sketch_vector = args.data[0];
        auto &include_levels_vector = args.data[1];
        auto &include_items_vector = args.data[2];

    
        
        TernaryExecutor::Execute
        <string_t,bool,bool,string_t>
        (
        sketch_vector,include_levels_vector,include_items_vector,result,
        [&](string_t sketch_data,bool include_levels_data,bool include_items_data) {

            
            auto sketch = [&]() {
                try {
                    return datasketches::kll_sketch<DSItem<T>>::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
                } catch (const std::exception &e) {
                    throw InvalidInputException("Failed to deserialize KLL sketch: %s", e.what());
                }
            }();
return StringVector::AddString(result, sketch.to_string(include_levels_data, include_items_data));
        });

}

    


template <class T>

static inline void DSKLLrank(DataChunk &args, ExpressionState &state, Vector &result)
{
    // Get the references to the incoming vectors.
    D_ASSERT(args.ColumnCount() == 3);

    
        auto &sketch_vector = args.data[0];
        auto &item_vector = args.data[1];
        auto &inclusive_vector = args.data[2];

    
        
        TernaryExecutor::Execute
        <string_t,T,bool,double>
        (
        sketch_vector,item_vector,inclusive_vector,result,
        [&](string_t sketch_data,T item_data,bool inclusive_data) {

            
            auto sketch = [&]() {
                try {
                    return datasketches::kll_sketch<DSItem<T>>::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
                } catch (const std::exception &e) {
                    throw InvalidInputException("Failed to deserialize KLL sketch: %s", e.what());
                }
            }();
return sketch.get_rank(DSItemIn(item_data), inclusive_data);
        });

}

    


template <class T>

static inline void DSKLLquantile(DataChunk &args, ExpressionState &state, Vector &result)
{
    // Get the references to the incoming vectors.
    D_ASSERT(args.ColumnCount() == 3);

    
        auto &sketch_vector = args.data[0];
        auto &rank_vector = args.data[1];
        auto &inclusive_vector = args.data[2];

    
        
        TernaryExecutor::Execute
        <string_t,double,bool,T>
        (
        sketch_vector,rank_vector,inclusive_vector,result,
        [&](string_t sketch_data,double rank_data,bool inclusive_data) {

            
            auto sketch = [&]() {
                try {
                    return datasketches::kll_sketch<DSItem<T>>::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
                } catch (const std::exception &e) {
                    throw InvalidInputException("Failed to deserialize KLL sketch: %s", e.what());
                }
            }();
return DSItemOut<T>(result, sketch.get_quantile(rank_data, inclusive_data));
        });

}

    


template <class T>

static inline void DSKLLn(DataChunk &args, ExpressionState &state, Vector &result)
{
    // Get the references to the incoming vectors.
    D_ASSERT(args.ColumnCount() == 1);

    
        auto &sketch_vector = args.data[0];

    
        
        UnaryExecutor::Execute
        <string_t,uint64_t>
        (
        sketch_vector,result,
        [&](string_t sketch_data) {

            
            auto sketch = [&]() {
                try {
                    return datasketches::kll_sketch<DSItem<T>>::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
                } catch (const std::exception &e) {
                    throw InvalidInputException("Failed to deserialize KLL sketch: %s", e.what());
                }
            }();
return sketch.get_n();
        });

}

    


template <class T>

static inline void DSKLLis_estimation_mode(DataChunk &args, ExpressionState &state, Vector &result)
{
    // Get the references to the incoming vectors.
    D_ASSERT(args.ColumnCount() == 1);

    
        auto &sketch_vector = args.data[0];

    
        
        UnaryExecutor::Execute
        <string_t,bool>
        (
        sketch_vector,result,
        [&](string_t sketch_data) {

            
            auto sketch = [&]() {
                try {
                    return datasketches::kll_sketch<DSItem<T>>::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
                } catch (const std::exception &e) {
                    throw InvalidInputException("Failed to deserialize KLL sketch: %s", e.what());
                }
            }();
return sketch.is_estimation_mode();
        });

}

    


template <class T>

static inline void DSKLLnum_retained(DataChunk &args, ExpressionState &state, Vector &result)
{
    // Get the references to the incoming vectors.
    D_ASSERT(args.ColumnCount() == 1);

    
        auto &sketch_vector = args.data[0];

    
        
        UnaryExecutor::Execute
        <string_t,uint64_t>
        (
        sketch_vector,result,
        [&](string_t sketch_data) {

            
            auto sketch = [&]() {
                try {
                    return datasketches::kll_sketch<DSItem<T>>::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
                } catch (const std::exception &e) {
                    throw InvalidInputException("Failed to deserialize KLL sketch: %s", e.what());
                }
            }();
return sketch.get_num_retained();
        });

}

    


template <class T>

static inline void DSKLLmin_item(DataChunk &args, ExpressionState &state, Vector &result)
{
    // Get the references to the incoming vectors.
    D_ASSERT(args.ColumnCount() == 1);

    
        auto &sketch_vector = args.data[0];

    
        
        UnaryExecutor::Execute
        <string_t,T>
        (
        sketch_vector,result,
        [&](string_t sketch_data) {

            
            auto sketch = [&]() {
                try {
                    return datasketches::kll_sketch<DSItem<T>>::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
                } catch (const std::exception &e) {
                    throw InvalidInputException("Failed to deserialize KLL sketch: %s", e.what());
                }
            }();
return DSItemOut<T>(result, sketch.get_min_item());
        });

}

    


template <class T>

static inline void DSKLLmax_item(DataChunk &args, ExpressionState &state, Vector &result)
{
    // Get the references to the incoming vectors.
    D_ASSERT(args.ColumnCount() == 1);

    
        auto &sketch_vector = args.data[0];

    
        
        UnaryExecutor::Execute
        <string_t,T>
        (
        sketch_vector,result,
        [&](string_t sketch_data) {

            
            auto sketch = [&]() {
                try {
                    return datasketches::kll_sketch<DSItem<T>>::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
                } catch (const std::exception &e) {
                    throw InvalidInputException("Failed to deserialize KLL sketch: %s", e.what());
                }
            }();
return DSItemOut<T>(result, sketch.get_max_item());
        });

}

    


template <class T>

static inline void DSKLLquantiles(DataChunk &args, ExpressionState &state, Vector &result)
{
    // Get the references to the incoming vectors.
    D_ASSERT(args.ColumnCount() == 3);

    
        auto &sketch_vector = args.data[0];
        auto &ranks_vector = args.data[1];
        auto &inclusive_vector = args.data[2];

    
        
                    auto &ranks_list_children = ListVector::GetChild(ranks_vector);
                    UnifiedVectorFormat ranks_children_unified;
                    ranks_list_children.ToUnifiedFormat(ranks_children_unified);
                    const double *ranks_children_data = UnifiedVectorFormat::GetData<double>(ranks_children_unified);
                    
        TernaryExecutor::Execute
        <string_t,list_entry_t,bool,list_entry_t>
        (
        sketch_vector,ranks_vector,inclusive_vector,result,
        [&](string_t sketch_data,list_entry_t ranks_data,bool inclusive_data) {

            
            auto sketch = [&]() {
                try {
                    return datasketches::kll_sketch<DSItem<T>>::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
                } catch (const std::exception &e) {
                    throw InvalidInputException("Failed to deserialize KLL sketch: %s", e.what());
                }
            }();

                if (sketch.is_empty())
                {
                    throw InvalidInputException("datasketch_kll_quantiles: operation is undefined for an empty sketch");
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
                
        });

}

    


template <class T>

static inline void DSKLLranks(DataChunk &args, ExpressionState &state, Vector &result)
{
    // Get the references to the incoming vectors.
    D_ASSERT(args.ColumnCount() == 3);

    
        auto &sketch_vector = args.data[0];
        auto &items_vector = args.data[1];
        auto &inclusive_vector = args.data[2];

    
        
                    auto &items_list_children = ListVector::GetChild(items_vector);
                    UnifiedVectorFormat items_children_unified;
                    items_list_children.ToUnifiedFormat(items_children_unified);
                    const T *items_children_data = UnifiedVectorFormat::GetData<T>(items_children_unified);
                    
        TernaryExecutor::Execute
        <string_t,list_entry_t,bool,list_entry_t>
        (
        sketch_vector,items_vector,inclusive_vector,result,
        [&](string_t sketch_data,list_entry_t items_data,bool inclusive_data) {

            
            auto sketch = [&]() {
                try {
                    return datasketches::kll_sketch<DSItem<T>>::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
                } catch (const std::exception &e) {
                    throw InvalidInputException("Failed to deserialize KLL sketch: %s", e.what());
                }
            }();

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
                
        });

}

    


template <class T>

static inline void DSKLLsorted_view(DataChunk &args, ExpressionState &state, Vector &result)
{
    // Get the references to the incoming vectors.
    D_ASSERT(args.ColumnCount() == 1);

    
        auto &sketch_vector = args.data[0];

    
        
        UnaryExecutor::Execute
        <string_t,list_entry_t>
        (
        sketch_vector,result,
        [&](string_t sketch_data) {

            
            auto sketch = [&]() {
                try {
                    return datasketches::kll_sketch<DSItem<T>>::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
                } catch (const std::exception &e) {
                    throw InvalidInputException("Failed to deserialize KLL sketch: %s", e.what());
                }
            }();

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
                
        });

}

    


    
    template <typename T>
    auto static DSKLLMergeAggregate(const LogicalType &type, const LogicalType &result_type) -> AggregateFunction
    
    {
        
        auto fun = AggregateFunction::UnaryAggregate<DSKLLState<T>, string_t, string_t, DSQuantilesMergeOperation<DSKLLBindData>, AggregateDestructorType::LEGACY>(result_type, result_type);
        DSReadDataArgumentAt<1, DSKLLState<T>, string_t, DSQuantilesMergeOperation<DSKLLBindData>>(fun);
        return fun;
        
    }



    template <typename T>
    auto static DSKLLCreateAggregate(const LogicalType &type, const LogicalType &result_type) -> AggregateFunction
    {
        
        auto fun = AggregateFunction::UnaryAggregate<DSKLLState<T>, T, string_t, DSQuantilesCreateOperation<DSKLLBindData>, AggregateDestructorType::LEGACY>(type, result_type);
        DSReadDataArgumentAt<1, DSKLLState<T>, T, DSQuantilesCreateOperation<DSKLLBindData>>(fun);
        return fun;
        
    }


  void LoadKLLSketch(ExtensionLoader &loader) {


      
        std::unordered_map<LogicalTypeId, LogicalType> sketch_map_types;
        
            sketch_map_types.insert({ LogicalTypeId::TINYINT, CreateKLLSketchType(loader, LogicalType(LogicalTypeId::TINYINT))});
            sketch_map_types.insert({ LogicalTypeId::SMALLINT, CreateKLLSketchType(loader, LogicalType(LogicalTypeId::SMALLINT))});
            sketch_map_types.insert({ LogicalTypeId::INTEGER, CreateKLLSketchType(loader, LogicalType(LogicalTypeId::INTEGER))});
            sketch_map_types.insert({ LogicalTypeId::BIGINT, CreateKLLSketchType(loader, LogicalType(LogicalTypeId::BIGINT))});
            sketch_map_types.insert({ LogicalTypeId::FLOAT, CreateKLLSketchType(loader, LogicalType(LogicalTypeId::FLOAT))});
            sketch_map_types.insert({ LogicalTypeId::DOUBLE, CreateKLLSketchType(loader, LogicalType(LogicalTypeId::DOUBLE))});
            sketch_map_types.insert({ LogicalTypeId::UTINYINT, CreateKLLSketchType(loader, LogicalType(LogicalTypeId::UTINYINT))});
            sketch_map_types.insert({ LogicalTypeId::USMALLINT, CreateKLLSketchType(loader, LogicalType(LogicalTypeId::USMALLINT))});
            sketch_map_types.insert({ LogicalTypeId::UINTEGER, CreateKLLSketchType(loader, LogicalType(LogicalTypeId::UINTEGER))});
            sketch_map_types.insert({ LogicalTypeId::UBIGINT, CreateKLLSketchType(loader, LogicalType(LogicalTypeId::UBIGINT))});
            sketch_map_types.insert({ LogicalTypeId::VARCHAR, CreateKLLSketchType(loader, LogicalType(LogicalTypeId::VARCHAR))});
      

      
      {
        ScalarFunctionSet fs("datasketch_kll_is_empty");
        std::vector<FunctionDescription> fn_descs;
        
            
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::TINYINT]},LogicalType::BOOLEAN
                ,    DSKLLis_empty<int8_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return a boolean indicating if the sketch is empty";
                    desc.examples.push_back("datasketch_kll_is_empty(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::TINYINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::SMALLINT]},LogicalType::BOOLEAN
                ,    DSKLLis_empty<int16_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return a boolean indicating if the sketch is empty";
                    desc.examples.push_back("datasketch_kll_is_empty(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::SMALLINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::INTEGER]},LogicalType::BOOLEAN
                ,    DSKLLis_empty<int32_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return a boolean indicating if the sketch is empty";
                    desc.examples.push_back("datasketch_kll_is_empty(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::INTEGER] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::BIGINT]},LogicalType::BOOLEAN
                ,    DSKLLis_empty<int64_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return a boolean indicating if the sketch is empty";
                    desc.examples.push_back("datasketch_kll_is_empty(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::BIGINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::FLOAT]},LogicalType::BOOLEAN
                ,    DSKLLis_empty<float>));
                {
                    FunctionDescription desc;
                    desc.description = "Return a boolean indicating if the sketch is empty";
                    desc.examples.push_back("datasketch_kll_is_empty(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::FLOAT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::DOUBLE]},LogicalType::BOOLEAN
                ,    DSKLLis_empty<double>));
                {
                    FunctionDescription desc;
                    desc.description = "Return a boolean indicating if the sketch is empty";
                    desc.examples.push_back("datasketch_kll_is_empty(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::DOUBLE] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UTINYINT]},LogicalType::BOOLEAN
                ,    DSKLLis_empty<uint8_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return a boolean indicating if the sketch is empty";
                    desc.examples.push_back("datasketch_kll_is_empty(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UTINYINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::USMALLINT]},LogicalType::BOOLEAN
                ,    DSKLLis_empty<uint16_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return a boolean indicating if the sketch is empty";
                    desc.examples.push_back("datasketch_kll_is_empty(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::USMALLINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UINTEGER]},LogicalType::BOOLEAN
                ,    DSKLLis_empty<uint32_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return a boolean indicating if the sketch is empty";
                    desc.examples.push_back("datasketch_kll_is_empty(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UINTEGER] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UBIGINT]},LogicalType::BOOLEAN
                ,    DSKLLis_empty<uint64_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return a boolean indicating if the sketch is empty";
                    desc.examples.push_back("datasketch_kll_is_empty(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UBIGINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::VARCHAR]},LogicalType::BOOLEAN
                ,    DSKLLis_empty<string_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return a boolean indicating if the sketch is empty";
                    desc.examples.push_back("datasketch_kll_is_empty(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::VARCHAR] };
                    fn_descs.push_back(std::move(desc));
                }
        

        // Every scalar function deserializes a sketch, which throws on invalid input.
        fs.SetFallible();
        CreateScalarFunctionInfo info(std::move(fs));
        for (auto &d : fn_descs) {
            info.descriptions.push_back(std::move(d));
        }

        DSSetCategories(info, {"sketch", "kll"});

        DSRegister(loader, info);


      }
      {
        ScalarFunctionSet fs("datasketch_kll_k");
        std::vector<FunctionDescription> fn_descs;
        
            
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::TINYINT]},LogicalType::USMALLINT
                ,    DSKLLk<int8_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the value of K for this sketch";
                    desc.examples.push_back("datasketch_kll_k(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::TINYINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::SMALLINT]},LogicalType::USMALLINT
                ,    DSKLLk<int16_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the value of K for this sketch";
                    desc.examples.push_back("datasketch_kll_k(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::SMALLINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::INTEGER]},LogicalType::USMALLINT
                ,    DSKLLk<int32_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the value of K for this sketch";
                    desc.examples.push_back("datasketch_kll_k(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::INTEGER] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::BIGINT]},LogicalType::USMALLINT
                ,    DSKLLk<int64_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the value of K for this sketch";
                    desc.examples.push_back("datasketch_kll_k(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::BIGINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::FLOAT]},LogicalType::USMALLINT
                ,    DSKLLk<float>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the value of K for this sketch";
                    desc.examples.push_back("datasketch_kll_k(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::FLOAT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::DOUBLE]},LogicalType::USMALLINT
                ,    DSKLLk<double>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the value of K for this sketch";
                    desc.examples.push_back("datasketch_kll_k(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::DOUBLE] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UTINYINT]},LogicalType::USMALLINT
                ,    DSKLLk<uint8_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the value of K for this sketch";
                    desc.examples.push_back("datasketch_kll_k(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UTINYINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::USMALLINT]},LogicalType::USMALLINT
                ,    DSKLLk<uint16_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the value of K for this sketch";
                    desc.examples.push_back("datasketch_kll_k(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::USMALLINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UINTEGER]},LogicalType::USMALLINT
                ,    DSKLLk<uint32_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the value of K for this sketch";
                    desc.examples.push_back("datasketch_kll_k(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UINTEGER] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UBIGINT]},LogicalType::USMALLINT
                ,    DSKLLk<uint64_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the value of K for this sketch";
                    desc.examples.push_back("datasketch_kll_k(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UBIGINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::VARCHAR]},LogicalType::USMALLINT
                ,    DSKLLk<string_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the value of K for this sketch";
                    desc.examples.push_back("datasketch_kll_k(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::VARCHAR] };
                    fn_descs.push_back(std::move(desc));
                }
        

        // Every scalar function deserializes a sketch, which throws on invalid input.
        fs.SetFallible();
        CreateScalarFunctionInfo info(std::move(fs));
        for (auto &d : fn_descs) {
            info.descriptions.push_back(std::move(d));
        }

        DSSetCategories(info, {"sketch", "kll"});

        DSRegister(loader, info);


      }
      {
        ScalarFunctionSet fs("datasketch_kll_cdf");
        std::vector<FunctionDescription> fn_descs;
        
            
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::TINYINT],LogicalType::LIST(LogicalType::TINYINT),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::DOUBLE)
                ,    DSKLLcdf<int8_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the Cumulative Distribution Function (CDF) of the sketch for a series of points";
                    desc.examples.push_back("datasketch_kll_cdf(sketch, points, inclusive)");
                    desc.parameter_names = { "sketch","split_points","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::TINYINT],LogicalType::LIST(LogicalType::TINYINT),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::SMALLINT],LogicalType::LIST(LogicalType::SMALLINT),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::DOUBLE)
                ,    DSKLLcdf<int16_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the Cumulative Distribution Function (CDF) of the sketch for a series of points";
                    desc.examples.push_back("datasketch_kll_cdf(sketch, points, inclusive)");
                    desc.parameter_names = { "sketch","split_points","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::SMALLINT],LogicalType::LIST(LogicalType::SMALLINT),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::INTEGER],LogicalType::LIST(LogicalType::INTEGER),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::DOUBLE)
                ,    DSKLLcdf<int32_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the Cumulative Distribution Function (CDF) of the sketch for a series of points";
                    desc.examples.push_back("datasketch_kll_cdf(sketch, points, inclusive)");
                    desc.parameter_names = { "sketch","split_points","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::INTEGER],LogicalType::LIST(LogicalType::INTEGER),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::BIGINT],LogicalType::LIST(LogicalType::BIGINT),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::DOUBLE)
                ,    DSKLLcdf<int64_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the Cumulative Distribution Function (CDF) of the sketch for a series of points";
                    desc.examples.push_back("datasketch_kll_cdf(sketch, points, inclusive)");
                    desc.parameter_names = { "sketch","split_points","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::BIGINT],LogicalType::LIST(LogicalType::BIGINT),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::FLOAT],LogicalType::LIST(LogicalType::FLOAT),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::DOUBLE)
                ,    DSKLLcdf<float>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the Cumulative Distribution Function (CDF) of the sketch for a series of points";
                    desc.examples.push_back("datasketch_kll_cdf(sketch, points, inclusive)");
                    desc.parameter_names = { "sketch","split_points","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::FLOAT],LogicalType::LIST(LogicalType::FLOAT),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::DOUBLE],LogicalType::LIST(LogicalType::DOUBLE),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::DOUBLE)
                ,    DSKLLcdf<double>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the Cumulative Distribution Function (CDF) of the sketch for a series of points";
                    desc.examples.push_back("datasketch_kll_cdf(sketch, points, inclusive)");
                    desc.parameter_names = { "sketch","split_points","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::DOUBLE],LogicalType::LIST(LogicalType::DOUBLE),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UTINYINT],LogicalType::LIST(LogicalType::UTINYINT),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::DOUBLE)
                ,    DSKLLcdf<uint8_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the Cumulative Distribution Function (CDF) of the sketch for a series of points";
                    desc.examples.push_back("datasketch_kll_cdf(sketch, points, inclusive)");
                    desc.parameter_names = { "sketch","split_points","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UTINYINT],LogicalType::LIST(LogicalType::UTINYINT),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::USMALLINT],LogicalType::LIST(LogicalType::USMALLINT),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::DOUBLE)
                ,    DSKLLcdf<uint16_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the Cumulative Distribution Function (CDF) of the sketch for a series of points";
                    desc.examples.push_back("datasketch_kll_cdf(sketch, points, inclusive)");
                    desc.parameter_names = { "sketch","split_points","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::USMALLINT],LogicalType::LIST(LogicalType::USMALLINT),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UINTEGER],LogicalType::LIST(LogicalType::UINTEGER),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::DOUBLE)
                ,    DSKLLcdf<uint32_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the Cumulative Distribution Function (CDF) of the sketch for a series of points";
                    desc.examples.push_back("datasketch_kll_cdf(sketch, points, inclusive)");
                    desc.parameter_names = { "sketch","split_points","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UINTEGER],LogicalType::LIST(LogicalType::UINTEGER),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UBIGINT],LogicalType::LIST(LogicalType::UBIGINT),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::DOUBLE)
                ,    DSKLLcdf<uint64_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the Cumulative Distribution Function (CDF) of the sketch for a series of points";
                    desc.examples.push_back("datasketch_kll_cdf(sketch, points, inclusive)");
                    desc.parameter_names = { "sketch","split_points","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UBIGINT],LogicalType::LIST(LogicalType::UBIGINT),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::VARCHAR],LogicalType::LIST(LogicalType::VARCHAR),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::DOUBLE)
                ,    DSKLLcdf<string_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the Cumulative Distribution Function (CDF) of the sketch for a series of points";
                    desc.examples.push_back("datasketch_kll_cdf(sketch, points, inclusive)");
                    desc.parameter_names = { "sketch","split_points","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::VARCHAR],LogicalType::LIST(LogicalType::VARCHAR),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
        

        // Every scalar function deserializes a sketch, which throws on invalid input.
        fs.SetFallible();
        CreateScalarFunctionInfo info(std::move(fs));
        for (auto &d : fn_descs) {
            info.descriptions.push_back(std::move(d));
        }

        DSSetCategories(info, {"sketch", "kll"});

        DSRegister(loader, info);


      }
      {
        ScalarFunctionSet fs("datasketch_kll_pmf");
        std::vector<FunctionDescription> fn_descs;
        
            
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::TINYINT],LogicalType::LIST(LogicalType::TINYINT),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::DOUBLE)
                ,    DSKLLpmf<int8_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the Probability Mass Function (PMF) of the sketch for a series of points";
                    desc.examples.push_back("datasketch_kll_pmf(sketch, points, inclusive)");
                    desc.parameter_names = { "sketch","split_points","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::TINYINT],LogicalType::LIST(LogicalType::TINYINT),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::SMALLINT],LogicalType::LIST(LogicalType::SMALLINT),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::DOUBLE)
                ,    DSKLLpmf<int16_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the Probability Mass Function (PMF) of the sketch for a series of points";
                    desc.examples.push_back("datasketch_kll_pmf(sketch, points, inclusive)");
                    desc.parameter_names = { "sketch","split_points","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::SMALLINT],LogicalType::LIST(LogicalType::SMALLINT),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::INTEGER],LogicalType::LIST(LogicalType::INTEGER),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::DOUBLE)
                ,    DSKLLpmf<int32_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the Probability Mass Function (PMF) of the sketch for a series of points";
                    desc.examples.push_back("datasketch_kll_pmf(sketch, points, inclusive)");
                    desc.parameter_names = { "sketch","split_points","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::INTEGER],LogicalType::LIST(LogicalType::INTEGER),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::BIGINT],LogicalType::LIST(LogicalType::BIGINT),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::DOUBLE)
                ,    DSKLLpmf<int64_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the Probability Mass Function (PMF) of the sketch for a series of points";
                    desc.examples.push_back("datasketch_kll_pmf(sketch, points, inclusive)");
                    desc.parameter_names = { "sketch","split_points","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::BIGINT],LogicalType::LIST(LogicalType::BIGINT),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::FLOAT],LogicalType::LIST(LogicalType::FLOAT),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::DOUBLE)
                ,    DSKLLpmf<float>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the Probability Mass Function (PMF) of the sketch for a series of points";
                    desc.examples.push_back("datasketch_kll_pmf(sketch, points, inclusive)");
                    desc.parameter_names = { "sketch","split_points","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::FLOAT],LogicalType::LIST(LogicalType::FLOAT),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::DOUBLE],LogicalType::LIST(LogicalType::DOUBLE),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::DOUBLE)
                ,    DSKLLpmf<double>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the Probability Mass Function (PMF) of the sketch for a series of points";
                    desc.examples.push_back("datasketch_kll_pmf(sketch, points, inclusive)");
                    desc.parameter_names = { "sketch","split_points","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::DOUBLE],LogicalType::LIST(LogicalType::DOUBLE),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UTINYINT],LogicalType::LIST(LogicalType::UTINYINT),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::DOUBLE)
                ,    DSKLLpmf<uint8_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the Probability Mass Function (PMF) of the sketch for a series of points";
                    desc.examples.push_back("datasketch_kll_pmf(sketch, points, inclusive)");
                    desc.parameter_names = { "sketch","split_points","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UTINYINT],LogicalType::LIST(LogicalType::UTINYINT),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::USMALLINT],LogicalType::LIST(LogicalType::USMALLINT),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::DOUBLE)
                ,    DSKLLpmf<uint16_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the Probability Mass Function (PMF) of the sketch for a series of points";
                    desc.examples.push_back("datasketch_kll_pmf(sketch, points, inclusive)");
                    desc.parameter_names = { "sketch","split_points","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::USMALLINT],LogicalType::LIST(LogicalType::USMALLINT),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UINTEGER],LogicalType::LIST(LogicalType::UINTEGER),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::DOUBLE)
                ,    DSKLLpmf<uint32_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the Probability Mass Function (PMF) of the sketch for a series of points";
                    desc.examples.push_back("datasketch_kll_pmf(sketch, points, inclusive)");
                    desc.parameter_names = { "sketch","split_points","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UINTEGER],LogicalType::LIST(LogicalType::UINTEGER),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UBIGINT],LogicalType::LIST(LogicalType::UBIGINT),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::DOUBLE)
                ,    DSKLLpmf<uint64_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the Probability Mass Function (PMF) of the sketch for a series of points";
                    desc.examples.push_back("datasketch_kll_pmf(sketch, points, inclusive)");
                    desc.parameter_names = { "sketch","split_points","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UBIGINT],LogicalType::LIST(LogicalType::UBIGINT),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::VARCHAR],LogicalType::LIST(LogicalType::VARCHAR),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::DOUBLE)
                ,    DSKLLpmf<string_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the Probability Mass Function (PMF) of the sketch for a series of points";
                    desc.examples.push_back("datasketch_kll_pmf(sketch, points, inclusive)");
                    desc.parameter_names = { "sketch","split_points","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::VARCHAR],LogicalType::LIST(LogicalType::VARCHAR),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
        

        // Every scalar function deserializes a sketch, which throws on invalid input.
        fs.SetFallible();
        CreateScalarFunctionInfo info(std::move(fs));
        for (auto &d : fn_descs) {
            info.descriptions.push_back(std::move(d));
        }

        DSSetCategories(info, {"sketch", "kll"});

        DSRegister(loader, info);


      }
      {
        ScalarFunctionSet fs("datasketch_kll_normalized_rank_error");
        std::vector<FunctionDescription> fn_descs;
        
            
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::TINYINT],LogicalType::BOOLEAN},LogicalType::DOUBLE
                ,    DSKLLnormalized_rank_error<int8_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the normalized rank error of the sketch";
                    desc.examples.push_back("datasketch_kll_normalized_rank_error(sketch, is_pmf)");
                    desc.parameter_names = { "sketch","is_pmf" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::TINYINT],LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::SMALLINT],LogicalType::BOOLEAN},LogicalType::DOUBLE
                ,    DSKLLnormalized_rank_error<int16_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the normalized rank error of the sketch";
                    desc.examples.push_back("datasketch_kll_normalized_rank_error(sketch, is_pmf)");
                    desc.parameter_names = { "sketch","is_pmf" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::SMALLINT],LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::INTEGER],LogicalType::BOOLEAN},LogicalType::DOUBLE
                ,    DSKLLnormalized_rank_error<int32_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the normalized rank error of the sketch";
                    desc.examples.push_back("datasketch_kll_normalized_rank_error(sketch, is_pmf)");
                    desc.parameter_names = { "sketch","is_pmf" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::INTEGER],LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::BIGINT],LogicalType::BOOLEAN},LogicalType::DOUBLE
                ,    DSKLLnormalized_rank_error<int64_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the normalized rank error of the sketch";
                    desc.examples.push_back("datasketch_kll_normalized_rank_error(sketch, is_pmf)");
                    desc.parameter_names = { "sketch","is_pmf" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::BIGINT],LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::FLOAT],LogicalType::BOOLEAN},LogicalType::DOUBLE
                ,    DSKLLnormalized_rank_error<float>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the normalized rank error of the sketch";
                    desc.examples.push_back("datasketch_kll_normalized_rank_error(sketch, is_pmf)");
                    desc.parameter_names = { "sketch","is_pmf" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::FLOAT],LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::DOUBLE],LogicalType::BOOLEAN},LogicalType::DOUBLE
                ,    DSKLLnormalized_rank_error<double>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the normalized rank error of the sketch";
                    desc.examples.push_back("datasketch_kll_normalized_rank_error(sketch, is_pmf)");
                    desc.parameter_names = { "sketch","is_pmf" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::DOUBLE],LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UTINYINT],LogicalType::BOOLEAN},LogicalType::DOUBLE
                ,    DSKLLnormalized_rank_error<uint8_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the normalized rank error of the sketch";
                    desc.examples.push_back("datasketch_kll_normalized_rank_error(sketch, is_pmf)");
                    desc.parameter_names = { "sketch","is_pmf" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UTINYINT],LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::USMALLINT],LogicalType::BOOLEAN},LogicalType::DOUBLE
                ,    DSKLLnormalized_rank_error<uint16_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the normalized rank error of the sketch";
                    desc.examples.push_back("datasketch_kll_normalized_rank_error(sketch, is_pmf)");
                    desc.parameter_names = { "sketch","is_pmf" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::USMALLINT],LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UINTEGER],LogicalType::BOOLEAN},LogicalType::DOUBLE
                ,    DSKLLnormalized_rank_error<uint32_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the normalized rank error of the sketch";
                    desc.examples.push_back("datasketch_kll_normalized_rank_error(sketch, is_pmf)");
                    desc.parameter_names = { "sketch","is_pmf" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UINTEGER],LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UBIGINT],LogicalType::BOOLEAN},LogicalType::DOUBLE
                ,    DSKLLnormalized_rank_error<uint64_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the normalized rank error of the sketch";
                    desc.examples.push_back("datasketch_kll_normalized_rank_error(sketch, is_pmf)");
                    desc.parameter_names = { "sketch","is_pmf" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UBIGINT],LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::VARCHAR],LogicalType::BOOLEAN},LogicalType::DOUBLE
                ,    DSKLLnormalized_rank_error<string_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the normalized rank error of the sketch";
                    desc.examples.push_back("datasketch_kll_normalized_rank_error(sketch, is_pmf)");
                    desc.parameter_names = { "sketch","is_pmf" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::VARCHAR],LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
        

        // Every scalar function deserializes a sketch, which throws on invalid input.
        fs.SetFallible();
        CreateScalarFunctionInfo info(std::move(fs));
        for (auto &d : fn_descs) {
            info.descriptions.push_back(std::move(d));
        }

        DSSetCategories(info, {"sketch", "kll"});

        DSRegister(loader, info);


      }
      {
        ScalarFunctionSet fs("datasketch_kll_describe");
        std::vector<FunctionDescription> fn_descs;
        
            
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::TINYINT],LogicalType::BOOLEAN,LogicalType::BOOLEAN},LogicalType::VARCHAR
                ,    DSKLLdescribe<int8_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return a description of this sketch";
                    desc.examples.push_back("datasketch_kll_describe(sketch, include_levels, include_items)");
                    desc.parameter_names = { "sketch","include_levels","include_items" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::TINYINT],LogicalType::BOOLEAN,LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::SMALLINT],LogicalType::BOOLEAN,LogicalType::BOOLEAN},LogicalType::VARCHAR
                ,    DSKLLdescribe<int16_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return a description of this sketch";
                    desc.examples.push_back("datasketch_kll_describe(sketch, include_levels, include_items)");
                    desc.parameter_names = { "sketch","include_levels","include_items" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::SMALLINT],LogicalType::BOOLEAN,LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::INTEGER],LogicalType::BOOLEAN,LogicalType::BOOLEAN},LogicalType::VARCHAR
                ,    DSKLLdescribe<int32_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return a description of this sketch";
                    desc.examples.push_back("datasketch_kll_describe(sketch, include_levels, include_items)");
                    desc.parameter_names = { "sketch","include_levels","include_items" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::INTEGER],LogicalType::BOOLEAN,LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::BIGINT],LogicalType::BOOLEAN,LogicalType::BOOLEAN},LogicalType::VARCHAR
                ,    DSKLLdescribe<int64_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return a description of this sketch";
                    desc.examples.push_back("datasketch_kll_describe(sketch, include_levels, include_items)");
                    desc.parameter_names = { "sketch","include_levels","include_items" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::BIGINT],LogicalType::BOOLEAN,LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::FLOAT],LogicalType::BOOLEAN,LogicalType::BOOLEAN},LogicalType::VARCHAR
                ,    DSKLLdescribe<float>));
                {
                    FunctionDescription desc;
                    desc.description = "Return a description of this sketch";
                    desc.examples.push_back("datasketch_kll_describe(sketch, include_levels, include_items)");
                    desc.parameter_names = { "sketch","include_levels","include_items" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::FLOAT],LogicalType::BOOLEAN,LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::DOUBLE],LogicalType::BOOLEAN,LogicalType::BOOLEAN},LogicalType::VARCHAR
                ,    DSKLLdescribe<double>));
                {
                    FunctionDescription desc;
                    desc.description = "Return a description of this sketch";
                    desc.examples.push_back("datasketch_kll_describe(sketch, include_levels, include_items)");
                    desc.parameter_names = { "sketch","include_levels","include_items" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::DOUBLE],LogicalType::BOOLEAN,LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UTINYINT],LogicalType::BOOLEAN,LogicalType::BOOLEAN},LogicalType::VARCHAR
                ,    DSKLLdescribe<uint8_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return a description of this sketch";
                    desc.examples.push_back("datasketch_kll_describe(sketch, include_levels, include_items)");
                    desc.parameter_names = { "sketch","include_levels","include_items" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UTINYINT],LogicalType::BOOLEAN,LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::USMALLINT],LogicalType::BOOLEAN,LogicalType::BOOLEAN},LogicalType::VARCHAR
                ,    DSKLLdescribe<uint16_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return a description of this sketch";
                    desc.examples.push_back("datasketch_kll_describe(sketch, include_levels, include_items)");
                    desc.parameter_names = { "sketch","include_levels","include_items" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::USMALLINT],LogicalType::BOOLEAN,LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UINTEGER],LogicalType::BOOLEAN,LogicalType::BOOLEAN},LogicalType::VARCHAR
                ,    DSKLLdescribe<uint32_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return a description of this sketch";
                    desc.examples.push_back("datasketch_kll_describe(sketch, include_levels, include_items)");
                    desc.parameter_names = { "sketch","include_levels","include_items" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UINTEGER],LogicalType::BOOLEAN,LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UBIGINT],LogicalType::BOOLEAN,LogicalType::BOOLEAN},LogicalType::VARCHAR
                ,    DSKLLdescribe<uint64_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return a description of this sketch";
                    desc.examples.push_back("datasketch_kll_describe(sketch, include_levels, include_items)");
                    desc.parameter_names = { "sketch","include_levels","include_items" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UBIGINT],LogicalType::BOOLEAN,LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::VARCHAR],LogicalType::BOOLEAN,LogicalType::BOOLEAN},LogicalType::VARCHAR
                ,    DSKLLdescribe<string_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return a description of this sketch";
                    desc.examples.push_back("datasketch_kll_describe(sketch, include_levels, include_items)");
                    desc.parameter_names = { "sketch","include_levels","include_items" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::VARCHAR],LogicalType::BOOLEAN,LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
        

        // Every scalar function deserializes a sketch, which throws on invalid input.
        fs.SetFallible();
        CreateScalarFunctionInfo info(std::move(fs));
        for (auto &d : fn_descs) {
            info.descriptions.push_back(std::move(d));
        }

        DSSetCategories(info, {"sketch", "kll"});

        DSRegister(loader, info);


      }
      {
        ScalarFunctionSet fs("datasketch_kll_rank");
        std::vector<FunctionDescription> fn_descs;
        
            
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::TINYINT],LogicalType::TINYINT,LogicalType::BOOLEAN},LogicalType::DOUBLE
                ,    DSKLLrank<int8_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the rank of an item in the sketch";
                    desc.examples.push_back("datasketch_kll_rank(sketch, item, inclusive)");
                    desc.parameter_names = { "sketch","item","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::TINYINT],LogicalType::TINYINT,LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::SMALLINT],LogicalType::SMALLINT,LogicalType::BOOLEAN},LogicalType::DOUBLE
                ,    DSKLLrank<int16_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the rank of an item in the sketch";
                    desc.examples.push_back("datasketch_kll_rank(sketch, item, inclusive)");
                    desc.parameter_names = { "sketch","item","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::SMALLINT],LogicalType::SMALLINT,LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::INTEGER],LogicalType::INTEGER,LogicalType::BOOLEAN},LogicalType::DOUBLE
                ,    DSKLLrank<int32_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the rank of an item in the sketch";
                    desc.examples.push_back("datasketch_kll_rank(sketch, item, inclusive)");
                    desc.parameter_names = { "sketch","item","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::INTEGER],LogicalType::INTEGER,LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::BIGINT],LogicalType::BIGINT,LogicalType::BOOLEAN},LogicalType::DOUBLE
                ,    DSKLLrank<int64_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the rank of an item in the sketch";
                    desc.examples.push_back("datasketch_kll_rank(sketch, item, inclusive)");
                    desc.parameter_names = { "sketch","item","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::BIGINT],LogicalType::BIGINT,LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::FLOAT],LogicalType::FLOAT,LogicalType::BOOLEAN},LogicalType::DOUBLE
                ,    DSKLLrank<float>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the rank of an item in the sketch";
                    desc.examples.push_back("datasketch_kll_rank(sketch, item, inclusive)");
                    desc.parameter_names = { "sketch","item","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::FLOAT],LogicalType::FLOAT,LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::DOUBLE],LogicalType::DOUBLE,LogicalType::BOOLEAN},LogicalType::DOUBLE
                ,    DSKLLrank<double>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the rank of an item in the sketch";
                    desc.examples.push_back("datasketch_kll_rank(sketch, item, inclusive)");
                    desc.parameter_names = { "sketch","item","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::DOUBLE],LogicalType::DOUBLE,LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UTINYINT],LogicalType::UTINYINT,LogicalType::BOOLEAN},LogicalType::DOUBLE
                ,    DSKLLrank<uint8_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the rank of an item in the sketch";
                    desc.examples.push_back("datasketch_kll_rank(sketch, item, inclusive)");
                    desc.parameter_names = { "sketch","item","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UTINYINT],LogicalType::UTINYINT,LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::USMALLINT],LogicalType::USMALLINT,LogicalType::BOOLEAN},LogicalType::DOUBLE
                ,    DSKLLrank<uint16_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the rank of an item in the sketch";
                    desc.examples.push_back("datasketch_kll_rank(sketch, item, inclusive)");
                    desc.parameter_names = { "sketch","item","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::USMALLINT],LogicalType::USMALLINT,LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UINTEGER],LogicalType::UINTEGER,LogicalType::BOOLEAN},LogicalType::DOUBLE
                ,    DSKLLrank<uint32_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the rank of an item in the sketch";
                    desc.examples.push_back("datasketch_kll_rank(sketch, item, inclusive)");
                    desc.parameter_names = { "sketch","item","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UINTEGER],LogicalType::UINTEGER,LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UBIGINT],LogicalType::UBIGINT,LogicalType::BOOLEAN},LogicalType::DOUBLE
                ,    DSKLLrank<uint64_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the rank of an item in the sketch";
                    desc.examples.push_back("datasketch_kll_rank(sketch, item, inclusive)");
                    desc.parameter_names = { "sketch","item","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UBIGINT],LogicalType::UBIGINT,LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::VARCHAR],LogicalType::VARCHAR,LogicalType::BOOLEAN},LogicalType::DOUBLE
                ,    DSKLLrank<string_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the rank of an item in the sketch";
                    desc.examples.push_back("datasketch_kll_rank(sketch, item, inclusive)");
                    desc.parameter_names = { "sketch","item","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::VARCHAR],LogicalType::VARCHAR,LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
        

        // Every scalar function deserializes a sketch, which throws on invalid input.
        fs.SetFallible();
        CreateScalarFunctionInfo info(std::move(fs));
        for (auto &d : fn_descs) {
            info.descriptions.push_back(std::move(d));
        }

        DSSetCategories(info, {"sketch", "kll"});

        DSRegister(loader, info);


      }
      {
        ScalarFunctionSet fs("datasketch_kll_quantile");
        std::vector<FunctionDescription> fn_descs;
        
            
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::TINYINT],LogicalType::DOUBLE,LogicalType::BOOLEAN},LogicalType::TINYINT
                ,    DSKLLquantile<int8_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the quantile of a rank in the sketch";
                    desc.examples.push_back("datasketch_kll_quantile(sketch, rank, inclusive)");
                    desc.parameter_names = { "sketch","rank","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::TINYINT],LogicalType::DOUBLE,LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::SMALLINT],LogicalType::DOUBLE,LogicalType::BOOLEAN},LogicalType::SMALLINT
                ,    DSKLLquantile<int16_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the quantile of a rank in the sketch";
                    desc.examples.push_back("datasketch_kll_quantile(sketch, rank, inclusive)");
                    desc.parameter_names = { "sketch","rank","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::SMALLINT],LogicalType::DOUBLE,LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::INTEGER],LogicalType::DOUBLE,LogicalType::BOOLEAN},LogicalType::INTEGER
                ,    DSKLLquantile<int32_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the quantile of a rank in the sketch";
                    desc.examples.push_back("datasketch_kll_quantile(sketch, rank, inclusive)");
                    desc.parameter_names = { "sketch","rank","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::INTEGER],LogicalType::DOUBLE,LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::BIGINT],LogicalType::DOUBLE,LogicalType::BOOLEAN},LogicalType::BIGINT
                ,    DSKLLquantile<int64_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the quantile of a rank in the sketch";
                    desc.examples.push_back("datasketch_kll_quantile(sketch, rank, inclusive)");
                    desc.parameter_names = { "sketch","rank","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::BIGINT],LogicalType::DOUBLE,LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::FLOAT],LogicalType::DOUBLE,LogicalType::BOOLEAN},LogicalType::FLOAT
                ,    DSKLLquantile<float>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the quantile of a rank in the sketch";
                    desc.examples.push_back("datasketch_kll_quantile(sketch, rank, inclusive)");
                    desc.parameter_names = { "sketch","rank","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::FLOAT],LogicalType::DOUBLE,LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::DOUBLE],LogicalType::DOUBLE,LogicalType::BOOLEAN},LogicalType::DOUBLE
                ,    DSKLLquantile<double>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the quantile of a rank in the sketch";
                    desc.examples.push_back("datasketch_kll_quantile(sketch, rank, inclusive)");
                    desc.parameter_names = { "sketch","rank","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::DOUBLE],LogicalType::DOUBLE,LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UTINYINT],LogicalType::DOUBLE,LogicalType::BOOLEAN},LogicalType::UTINYINT
                ,    DSKLLquantile<uint8_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the quantile of a rank in the sketch";
                    desc.examples.push_back("datasketch_kll_quantile(sketch, rank, inclusive)");
                    desc.parameter_names = { "sketch","rank","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UTINYINT],LogicalType::DOUBLE,LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::USMALLINT],LogicalType::DOUBLE,LogicalType::BOOLEAN},LogicalType::USMALLINT
                ,    DSKLLquantile<uint16_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the quantile of a rank in the sketch";
                    desc.examples.push_back("datasketch_kll_quantile(sketch, rank, inclusive)");
                    desc.parameter_names = { "sketch","rank","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::USMALLINT],LogicalType::DOUBLE,LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UINTEGER],LogicalType::DOUBLE,LogicalType::BOOLEAN},LogicalType::UINTEGER
                ,    DSKLLquantile<uint32_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the quantile of a rank in the sketch";
                    desc.examples.push_back("datasketch_kll_quantile(sketch, rank, inclusive)");
                    desc.parameter_names = { "sketch","rank","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UINTEGER],LogicalType::DOUBLE,LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UBIGINT],LogicalType::DOUBLE,LogicalType::BOOLEAN},LogicalType::UBIGINT
                ,    DSKLLquantile<uint64_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the quantile of a rank in the sketch";
                    desc.examples.push_back("datasketch_kll_quantile(sketch, rank, inclusive)");
                    desc.parameter_names = { "sketch","rank","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UBIGINT],LogicalType::DOUBLE,LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::VARCHAR],LogicalType::DOUBLE,LogicalType::BOOLEAN},LogicalType::VARCHAR
                ,    DSKLLquantile<string_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the quantile of a rank in the sketch";
                    desc.examples.push_back("datasketch_kll_quantile(sketch, rank, inclusive)");
                    desc.parameter_names = { "sketch","rank","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::VARCHAR],LogicalType::DOUBLE,LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
        

        // Every scalar function deserializes a sketch, which throws on invalid input.
        fs.SetFallible();
        CreateScalarFunctionInfo info(std::move(fs));
        for (auto &d : fn_descs) {
            info.descriptions.push_back(std::move(d));
        }

        DSSetCategories(info, {"sketch", "kll"});

        DSRegister(loader, info);


      }
      {
        ScalarFunctionSet fs("datasketch_kll_n");
        std::vector<FunctionDescription> fn_descs;
        
            
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::TINYINT]},LogicalType::UBIGINT
                ,    DSKLLn<int8_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the number of items contained in the sketch";
                    desc.examples.push_back("datasketch_kll_n(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::TINYINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::SMALLINT]},LogicalType::UBIGINT
                ,    DSKLLn<int16_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the number of items contained in the sketch";
                    desc.examples.push_back("datasketch_kll_n(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::SMALLINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::INTEGER]},LogicalType::UBIGINT
                ,    DSKLLn<int32_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the number of items contained in the sketch";
                    desc.examples.push_back("datasketch_kll_n(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::INTEGER] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::BIGINT]},LogicalType::UBIGINT
                ,    DSKLLn<int64_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the number of items contained in the sketch";
                    desc.examples.push_back("datasketch_kll_n(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::BIGINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::FLOAT]},LogicalType::UBIGINT
                ,    DSKLLn<float>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the number of items contained in the sketch";
                    desc.examples.push_back("datasketch_kll_n(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::FLOAT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::DOUBLE]},LogicalType::UBIGINT
                ,    DSKLLn<double>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the number of items contained in the sketch";
                    desc.examples.push_back("datasketch_kll_n(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::DOUBLE] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UTINYINT]},LogicalType::UBIGINT
                ,    DSKLLn<uint8_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the number of items contained in the sketch";
                    desc.examples.push_back("datasketch_kll_n(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UTINYINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::USMALLINT]},LogicalType::UBIGINT
                ,    DSKLLn<uint16_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the number of items contained in the sketch";
                    desc.examples.push_back("datasketch_kll_n(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::USMALLINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UINTEGER]},LogicalType::UBIGINT
                ,    DSKLLn<uint32_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the number of items contained in the sketch";
                    desc.examples.push_back("datasketch_kll_n(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UINTEGER] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UBIGINT]},LogicalType::UBIGINT
                ,    DSKLLn<uint64_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the number of items contained in the sketch";
                    desc.examples.push_back("datasketch_kll_n(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UBIGINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::VARCHAR]},LogicalType::UBIGINT
                ,    DSKLLn<string_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the number of items contained in the sketch";
                    desc.examples.push_back("datasketch_kll_n(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::VARCHAR] };
                    fn_descs.push_back(std::move(desc));
                }
        

        // Every scalar function deserializes a sketch, which throws on invalid input.
        fs.SetFallible();
        CreateScalarFunctionInfo info(std::move(fs));
        for (auto &d : fn_descs) {
            info.descriptions.push_back(std::move(d));
        }

        DSSetCategories(info, {"sketch", "kll"});

        DSRegister(loader, info);


      }
      {
        ScalarFunctionSet fs("datasketch_kll_is_estimation_mode");
        std::vector<FunctionDescription> fn_descs;
        
            
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::TINYINT]},LogicalType::BOOLEAN
                ,    DSKLLis_estimation_mode<int8_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return a boolean indicating if the sketch is in estimation mode";
                    desc.examples.push_back("datasketch_kll_is_estimation_mode(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::TINYINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::SMALLINT]},LogicalType::BOOLEAN
                ,    DSKLLis_estimation_mode<int16_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return a boolean indicating if the sketch is in estimation mode";
                    desc.examples.push_back("datasketch_kll_is_estimation_mode(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::SMALLINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::INTEGER]},LogicalType::BOOLEAN
                ,    DSKLLis_estimation_mode<int32_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return a boolean indicating if the sketch is in estimation mode";
                    desc.examples.push_back("datasketch_kll_is_estimation_mode(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::INTEGER] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::BIGINT]},LogicalType::BOOLEAN
                ,    DSKLLis_estimation_mode<int64_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return a boolean indicating if the sketch is in estimation mode";
                    desc.examples.push_back("datasketch_kll_is_estimation_mode(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::BIGINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::FLOAT]},LogicalType::BOOLEAN
                ,    DSKLLis_estimation_mode<float>));
                {
                    FunctionDescription desc;
                    desc.description = "Return a boolean indicating if the sketch is in estimation mode";
                    desc.examples.push_back("datasketch_kll_is_estimation_mode(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::FLOAT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::DOUBLE]},LogicalType::BOOLEAN
                ,    DSKLLis_estimation_mode<double>));
                {
                    FunctionDescription desc;
                    desc.description = "Return a boolean indicating if the sketch is in estimation mode";
                    desc.examples.push_back("datasketch_kll_is_estimation_mode(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::DOUBLE] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UTINYINT]},LogicalType::BOOLEAN
                ,    DSKLLis_estimation_mode<uint8_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return a boolean indicating if the sketch is in estimation mode";
                    desc.examples.push_back("datasketch_kll_is_estimation_mode(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UTINYINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::USMALLINT]},LogicalType::BOOLEAN
                ,    DSKLLis_estimation_mode<uint16_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return a boolean indicating if the sketch is in estimation mode";
                    desc.examples.push_back("datasketch_kll_is_estimation_mode(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::USMALLINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UINTEGER]},LogicalType::BOOLEAN
                ,    DSKLLis_estimation_mode<uint32_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return a boolean indicating if the sketch is in estimation mode";
                    desc.examples.push_back("datasketch_kll_is_estimation_mode(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UINTEGER] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UBIGINT]},LogicalType::BOOLEAN
                ,    DSKLLis_estimation_mode<uint64_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return a boolean indicating if the sketch is in estimation mode";
                    desc.examples.push_back("datasketch_kll_is_estimation_mode(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UBIGINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::VARCHAR]},LogicalType::BOOLEAN
                ,    DSKLLis_estimation_mode<string_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return a boolean indicating if the sketch is in estimation mode";
                    desc.examples.push_back("datasketch_kll_is_estimation_mode(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::VARCHAR] };
                    fn_descs.push_back(std::move(desc));
                }
        

        // Every scalar function deserializes a sketch, which throws on invalid input.
        fs.SetFallible();
        CreateScalarFunctionInfo info(std::move(fs));
        for (auto &d : fn_descs) {
            info.descriptions.push_back(std::move(d));
        }

        DSSetCategories(info, {"sketch", "kll"});

        DSRegister(loader, info);


      }
      {
        ScalarFunctionSet fs("datasketch_kll_num_retained");
        std::vector<FunctionDescription> fn_descs;
        
            
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::TINYINT]},LogicalType::UBIGINT
                ,    DSKLLnum_retained<int8_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the number of retained items in the sketch";
                    desc.examples.push_back("datasketch_kll_num_retained(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::TINYINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::SMALLINT]},LogicalType::UBIGINT
                ,    DSKLLnum_retained<int16_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the number of retained items in the sketch";
                    desc.examples.push_back("datasketch_kll_num_retained(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::SMALLINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::INTEGER]},LogicalType::UBIGINT
                ,    DSKLLnum_retained<int32_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the number of retained items in the sketch";
                    desc.examples.push_back("datasketch_kll_num_retained(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::INTEGER] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::BIGINT]},LogicalType::UBIGINT
                ,    DSKLLnum_retained<int64_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the number of retained items in the sketch";
                    desc.examples.push_back("datasketch_kll_num_retained(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::BIGINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::FLOAT]},LogicalType::UBIGINT
                ,    DSKLLnum_retained<float>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the number of retained items in the sketch";
                    desc.examples.push_back("datasketch_kll_num_retained(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::FLOAT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::DOUBLE]},LogicalType::UBIGINT
                ,    DSKLLnum_retained<double>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the number of retained items in the sketch";
                    desc.examples.push_back("datasketch_kll_num_retained(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::DOUBLE] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UTINYINT]},LogicalType::UBIGINT
                ,    DSKLLnum_retained<uint8_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the number of retained items in the sketch";
                    desc.examples.push_back("datasketch_kll_num_retained(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UTINYINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::USMALLINT]},LogicalType::UBIGINT
                ,    DSKLLnum_retained<uint16_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the number of retained items in the sketch";
                    desc.examples.push_back("datasketch_kll_num_retained(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::USMALLINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UINTEGER]},LogicalType::UBIGINT
                ,    DSKLLnum_retained<uint32_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the number of retained items in the sketch";
                    desc.examples.push_back("datasketch_kll_num_retained(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UINTEGER] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UBIGINT]},LogicalType::UBIGINT
                ,    DSKLLnum_retained<uint64_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the number of retained items in the sketch";
                    desc.examples.push_back("datasketch_kll_num_retained(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UBIGINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::VARCHAR]},LogicalType::UBIGINT
                ,    DSKLLnum_retained<string_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the number of retained items in the sketch";
                    desc.examples.push_back("datasketch_kll_num_retained(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::VARCHAR] };
                    fn_descs.push_back(std::move(desc));
                }
        

        // Every scalar function deserializes a sketch, which throws on invalid input.
        fs.SetFallible();
        CreateScalarFunctionInfo info(std::move(fs));
        for (auto &d : fn_descs) {
            info.descriptions.push_back(std::move(d));
        }

        DSSetCategories(info, {"sketch", "kll"});

        DSRegister(loader, info);


      }
      {
        ScalarFunctionSet fs("datasketch_kll_min_item");
        std::vector<FunctionDescription> fn_descs;
        
            
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::TINYINT]},LogicalType::TINYINT
                ,    DSKLLmin_item<int8_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the minimum item in the sketch";
                    desc.examples.push_back("datasketch_kll_min_item(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::TINYINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::SMALLINT]},LogicalType::SMALLINT
                ,    DSKLLmin_item<int16_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the minimum item in the sketch";
                    desc.examples.push_back("datasketch_kll_min_item(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::SMALLINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::INTEGER]},LogicalType::INTEGER
                ,    DSKLLmin_item<int32_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the minimum item in the sketch";
                    desc.examples.push_back("datasketch_kll_min_item(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::INTEGER] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::BIGINT]},LogicalType::BIGINT
                ,    DSKLLmin_item<int64_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the minimum item in the sketch";
                    desc.examples.push_back("datasketch_kll_min_item(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::BIGINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::FLOAT]},LogicalType::FLOAT
                ,    DSKLLmin_item<float>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the minimum item in the sketch";
                    desc.examples.push_back("datasketch_kll_min_item(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::FLOAT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::DOUBLE]},LogicalType::DOUBLE
                ,    DSKLLmin_item<double>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the minimum item in the sketch";
                    desc.examples.push_back("datasketch_kll_min_item(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::DOUBLE] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UTINYINT]},LogicalType::UTINYINT
                ,    DSKLLmin_item<uint8_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the minimum item in the sketch";
                    desc.examples.push_back("datasketch_kll_min_item(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UTINYINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::USMALLINT]},LogicalType::USMALLINT
                ,    DSKLLmin_item<uint16_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the minimum item in the sketch";
                    desc.examples.push_back("datasketch_kll_min_item(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::USMALLINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UINTEGER]},LogicalType::UINTEGER
                ,    DSKLLmin_item<uint32_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the minimum item in the sketch";
                    desc.examples.push_back("datasketch_kll_min_item(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UINTEGER] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UBIGINT]},LogicalType::UBIGINT
                ,    DSKLLmin_item<uint64_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the minimum item in the sketch";
                    desc.examples.push_back("datasketch_kll_min_item(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UBIGINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::VARCHAR]},LogicalType::VARCHAR
                ,    DSKLLmin_item<string_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the minimum item in the sketch";
                    desc.examples.push_back("datasketch_kll_min_item(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::VARCHAR] };
                    fn_descs.push_back(std::move(desc));
                }
        

        // Every scalar function deserializes a sketch, which throws on invalid input.
        fs.SetFallible();
        CreateScalarFunctionInfo info(std::move(fs));
        for (auto &d : fn_descs) {
            info.descriptions.push_back(std::move(d));
        }

        DSSetCategories(info, {"sketch", "kll"});

        DSRegister(loader, info);


      }
      {
        ScalarFunctionSet fs("datasketch_kll_max_item");
        std::vector<FunctionDescription> fn_descs;
        
            
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::TINYINT]},LogicalType::TINYINT
                ,    DSKLLmax_item<int8_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the maximum item in the sketch";
                    desc.examples.push_back("datasketch_kll_max_item(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::TINYINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::SMALLINT]},LogicalType::SMALLINT
                ,    DSKLLmax_item<int16_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the maximum item in the sketch";
                    desc.examples.push_back("datasketch_kll_max_item(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::SMALLINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::INTEGER]},LogicalType::INTEGER
                ,    DSKLLmax_item<int32_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the maximum item in the sketch";
                    desc.examples.push_back("datasketch_kll_max_item(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::INTEGER] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::BIGINT]},LogicalType::BIGINT
                ,    DSKLLmax_item<int64_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the maximum item in the sketch";
                    desc.examples.push_back("datasketch_kll_max_item(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::BIGINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::FLOAT]},LogicalType::FLOAT
                ,    DSKLLmax_item<float>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the maximum item in the sketch";
                    desc.examples.push_back("datasketch_kll_max_item(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::FLOAT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::DOUBLE]},LogicalType::DOUBLE
                ,    DSKLLmax_item<double>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the maximum item in the sketch";
                    desc.examples.push_back("datasketch_kll_max_item(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::DOUBLE] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UTINYINT]},LogicalType::UTINYINT
                ,    DSKLLmax_item<uint8_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the maximum item in the sketch";
                    desc.examples.push_back("datasketch_kll_max_item(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UTINYINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::USMALLINT]},LogicalType::USMALLINT
                ,    DSKLLmax_item<uint16_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the maximum item in the sketch";
                    desc.examples.push_back("datasketch_kll_max_item(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::USMALLINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UINTEGER]},LogicalType::UINTEGER
                ,    DSKLLmax_item<uint32_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the maximum item in the sketch";
                    desc.examples.push_back("datasketch_kll_max_item(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UINTEGER] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UBIGINT]},LogicalType::UBIGINT
                ,    DSKLLmax_item<uint64_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the maximum item in the sketch";
                    desc.examples.push_back("datasketch_kll_max_item(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UBIGINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::VARCHAR]},LogicalType::VARCHAR
                ,    DSKLLmax_item<string_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the maximum item in the sketch";
                    desc.examples.push_back("datasketch_kll_max_item(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::VARCHAR] };
                    fn_descs.push_back(std::move(desc));
                }
        

        // Every scalar function deserializes a sketch, which throws on invalid input.
        fs.SetFallible();
        CreateScalarFunctionInfo info(std::move(fs));
        for (auto &d : fn_descs) {
            info.descriptions.push_back(std::move(d));
        }

        DSSetCategories(info, {"sketch", "kll"});

        DSRegister(loader, info);


      }
      {
        ScalarFunctionSet fs("datasketch_kll_quantiles");
        std::vector<FunctionDescription> fn_descs;
        
            
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::TINYINT],LogicalType::LIST(LogicalType::DOUBLE),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::TINYINT)
                ,    DSKLLquantiles<int8_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the quantiles (items) at a list of normalized ranks in [0, 1]. A NULL rank yields a NULL quantile";
                    desc.examples.push_back("datasketch_kll_quantiles(sketch, [0.25, 0.5, 0.75], true)");
                    desc.parameter_names = { "sketch","ranks","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::TINYINT],LogicalType::LIST(LogicalType::DOUBLE),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::SMALLINT],LogicalType::LIST(LogicalType::DOUBLE),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::SMALLINT)
                ,    DSKLLquantiles<int16_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the quantiles (items) at a list of normalized ranks in [0, 1]. A NULL rank yields a NULL quantile";
                    desc.examples.push_back("datasketch_kll_quantiles(sketch, [0.25, 0.5, 0.75], true)");
                    desc.parameter_names = { "sketch","ranks","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::SMALLINT],LogicalType::LIST(LogicalType::DOUBLE),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::INTEGER],LogicalType::LIST(LogicalType::DOUBLE),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::INTEGER)
                ,    DSKLLquantiles<int32_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the quantiles (items) at a list of normalized ranks in [0, 1]. A NULL rank yields a NULL quantile";
                    desc.examples.push_back("datasketch_kll_quantiles(sketch, [0.25, 0.5, 0.75], true)");
                    desc.parameter_names = { "sketch","ranks","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::INTEGER],LogicalType::LIST(LogicalType::DOUBLE),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::BIGINT],LogicalType::LIST(LogicalType::DOUBLE),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::BIGINT)
                ,    DSKLLquantiles<int64_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the quantiles (items) at a list of normalized ranks in [0, 1]. A NULL rank yields a NULL quantile";
                    desc.examples.push_back("datasketch_kll_quantiles(sketch, [0.25, 0.5, 0.75], true)");
                    desc.parameter_names = { "sketch","ranks","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::BIGINT],LogicalType::LIST(LogicalType::DOUBLE),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::FLOAT],LogicalType::LIST(LogicalType::DOUBLE),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::FLOAT)
                ,    DSKLLquantiles<float>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the quantiles (items) at a list of normalized ranks in [0, 1]. A NULL rank yields a NULL quantile";
                    desc.examples.push_back("datasketch_kll_quantiles(sketch, [0.25, 0.5, 0.75], true)");
                    desc.parameter_names = { "sketch","ranks","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::FLOAT],LogicalType::LIST(LogicalType::DOUBLE),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::DOUBLE],LogicalType::LIST(LogicalType::DOUBLE),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::DOUBLE)
                ,    DSKLLquantiles<double>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the quantiles (items) at a list of normalized ranks in [0, 1]. A NULL rank yields a NULL quantile";
                    desc.examples.push_back("datasketch_kll_quantiles(sketch, [0.25, 0.5, 0.75], true)");
                    desc.parameter_names = { "sketch","ranks","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::DOUBLE],LogicalType::LIST(LogicalType::DOUBLE),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UTINYINT],LogicalType::LIST(LogicalType::DOUBLE),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::UTINYINT)
                ,    DSKLLquantiles<uint8_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the quantiles (items) at a list of normalized ranks in [0, 1]. A NULL rank yields a NULL quantile";
                    desc.examples.push_back("datasketch_kll_quantiles(sketch, [0.25, 0.5, 0.75], true)");
                    desc.parameter_names = { "sketch","ranks","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UTINYINT],LogicalType::LIST(LogicalType::DOUBLE),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::USMALLINT],LogicalType::LIST(LogicalType::DOUBLE),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::USMALLINT)
                ,    DSKLLquantiles<uint16_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the quantiles (items) at a list of normalized ranks in [0, 1]. A NULL rank yields a NULL quantile";
                    desc.examples.push_back("datasketch_kll_quantiles(sketch, [0.25, 0.5, 0.75], true)");
                    desc.parameter_names = { "sketch","ranks","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::USMALLINT],LogicalType::LIST(LogicalType::DOUBLE),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UINTEGER],LogicalType::LIST(LogicalType::DOUBLE),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::UINTEGER)
                ,    DSKLLquantiles<uint32_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the quantiles (items) at a list of normalized ranks in [0, 1]. A NULL rank yields a NULL quantile";
                    desc.examples.push_back("datasketch_kll_quantiles(sketch, [0.25, 0.5, 0.75], true)");
                    desc.parameter_names = { "sketch","ranks","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UINTEGER],LogicalType::LIST(LogicalType::DOUBLE),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UBIGINT],LogicalType::LIST(LogicalType::DOUBLE),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::UBIGINT)
                ,    DSKLLquantiles<uint64_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the quantiles (items) at a list of normalized ranks in [0, 1]. A NULL rank yields a NULL quantile";
                    desc.examples.push_back("datasketch_kll_quantiles(sketch, [0.25, 0.5, 0.75], true)");
                    desc.parameter_names = { "sketch","ranks","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UBIGINT],LogicalType::LIST(LogicalType::DOUBLE),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::VARCHAR],LogicalType::LIST(LogicalType::DOUBLE),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::VARCHAR)
                ,    DSKLLquantiles<string_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the quantiles (items) at a list of normalized ranks in [0, 1]. A NULL rank yields a NULL quantile";
                    desc.examples.push_back("datasketch_kll_quantiles(sketch, [0.25, 0.5, 0.75], true)");
                    desc.parameter_names = { "sketch","ranks","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::VARCHAR],LogicalType::LIST(LogicalType::DOUBLE),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
        

        // Every scalar function deserializes a sketch, which throws on invalid input.
        fs.SetFallible();
        CreateScalarFunctionInfo info(std::move(fs));
        for (auto &d : fn_descs) {
            info.descriptions.push_back(std::move(d));
        }

        DSSetCategories(info, {"sketch", "kll"});

        DSRegister(loader, info);


      }
      {
        ScalarFunctionSet fs("datasketch_kll_ranks");
        std::vector<FunctionDescription> fn_descs;
        
            
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::TINYINT],LogicalType::LIST(LogicalType::TINYINT),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::DOUBLE)
                ,    DSKLLranks<int8_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the normalized ranks of a list of items. A NULL item yields a NULL rank";
                    desc.examples.push_back("datasketch_kll_ranks(sketch, [10, 20, 30], true)");
                    desc.parameter_names = { "sketch","items","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::TINYINT],LogicalType::LIST(LogicalType::TINYINT),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::SMALLINT],LogicalType::LIST(LogicalType::SMALLINT),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::DOUBLE)
                ,    DSKLLranks<int16_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the normalized ranks of a list of items. A NULL item yields a NULL rank";
                    desc.examples.push_back("datasketch_kll_ranks(sketch, [10, 20, 30], true)");
                    desc.parameter_names = { "sketch","items","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::SMALLINT],LogicalType::LIST(LogicalType::SMALLINT),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::INTEGER],LogicalType::LIST(LogicalType::INTEGER),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::DOUBLE)
                ,    DSKLLranks<int32_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the normalized ranks of a list of items. A NULL item yields a NULL rank";
                    desc.examples.push_back("datasketch_kll_ranks(sketch, [10, 20, 30], true)");
                    desc.parameter_names = { "sketch","items","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::INTEGER],LogicalType::LIST(LogicalType::INTEGER),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::BIGINT],LogicalType::LIST(LogicalType::BIGINT),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::DOUBLE)
                ,    DSKLLranks<int64_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the normalized ranks of a list of items. A NULL item yields a NULL rank";
                    desc.examples.push_back("datasketch_kll_ranks(sketch, [10, 20, 30], true)");
                    desc.parameter_names = { "sketch","items","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::BIGINT],LogicalType::LIST(LogicalType::BIGINT),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::FLOAT],LogicalType::LIST(LogicalType::FLOAT),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::DOUBLE)
                ,    DSKLLranks<float>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the normalized ranks of a list of items. A NULL item yields a NULL rank";
                    desc.examples.push_back("datasketch_kll_ranks(sketch, [10, 20, 30], true)");
                    desc.parameter_names = { "sketch","items","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::FLOAT],LogicalType::LIST(LogicalType::FLOAT),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::DOUBLE],LogicalType::LIST(LogicalType::DOUBLE),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::DOUBLE)
                ,    DSKLLranks<double>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the normalized ranks of a list of items. A NULL item yields a NULL rank";
                    desc.examples.push_back("datasketch_kll_ranks(sketch, [10, 20, 30], true)");
                    desc.parameter_names = { "sketch","items","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::DOUBLE],LogicalType::LIST(LogicalType::DOUBLE),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UTINYINT],LogicalType::LIST(LogicalType::UTINYINT),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::DOUBLE)
                ,    DSKLLranks<uint8_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the normalized ranks of a list of items. A NULL item yields a NULL rank";
                    desc.examples.push_back("datasketch_kll_ranks(sketch, [10, 20, 30], true)");
                    desc.parameter_names = { "sketch","items","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UTINYINT],LogicalType::LIST(LogicalType::UTINYINT),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::USMALLINT],LogicalType::LIST(LogicalType::USMALLINT),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::DOUBLE)
                ,    DSKLLranks<uint16_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the normalized ranks of a list of items. A NULL item yields a NULL rank";
                    desc.examples.push_back("datasketch_kll_ranks(sketch, [10, 20, 30], true)");
                    desc.parameter_names = { "sketch","items","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::USMALLINT],LogicalType::LIST(LogicalType::USMALLINT),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UINTEGER],LogicalType::LIST(LogicalType::UINTEGER),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::DOUBLE)
                ,    DSKLLranks<uint32_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the normalized ranks of a list of items. A NULL item yields a NULL rank";
                    desc.examples.push_back("datasketch_kll_ranks(sketch, [10, 20, 30], true)");
                    desc.parameter_names = { "sketch","items","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UINTEGER],LogicalType::LIST(LogicalType::UINTEGER),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UBIGINT],LogicalType::LIST(LogicalType::UBIGINT),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::DOUBLE)
                ,    DSKLLranks<uint64_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the normalized ranks of a list of items. A NULL item yields a NULL rank";
                    desc.examples.push_back("datasketch_kll_ranks(sketch, [10, 20, 30], true)");
                    desc.parameter_names = { "sketch","items","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UBIGINT],LogicalType::LIST(LogicalType::UBIGINT),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::VARCHAR],LogicalType::LIST(LogicalType::VARCHAR),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::DOUBLE)
                ,    DSKLLranks<string_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the normalized ranks of a list of items. A NULL item yields a NULL rank";
                    desc.examples.push_back("datasketch_kll_ranks(sketch, [10, 20, 30], true)");
                    desc.parameter_names = { "sketch","items","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::VARCHAR],LogicalType::LIST(LogicalType::VARCHAR),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
        

        // Every scalar function deserializes a sketch, which throws on invalid input.
        fs.SetFallible();
        CreateScalarFunctionInfo info(std::move(fs));
        for (auto &d : fn_descs) {
            info.descriptions.push_back(std::move(d));
        }

        DSSetCategories(info, {"sketch", "kll"});

        DSRegister(loader, info);


      }
      {
        ScalarFunctionSet fs("datasketch_kll_sorted_view");
        std::vector<FunctionDescription> fn_descs;
        
            
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::TINYINT]},LogicalType::LIST(LogicalType::STRUCT(child_list_t<LogicalType>{{"item", LogicalType::TINYINT}, {"weight", LogicalType::UBIGINT}, {"cumulative_weight", LogicalType::UBIGINT}}))
                ,    DSKLLsorted_view<int8_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the sketch's sorted view: one entry per retained item in ascending order (equal items can appear more than once), with the item's weight and the inclusive cumulative weight (the number of input items less than or equal to it; estimated once the sketch is in estimation mode). Divide cumulative_weight by datasketch_kll_n(sketch) for the normalized rank. An empty sketch yields an empty list";
                    desc.examples.push_back("datasketch_kll_sorted_view(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::TINYINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::SMALLINT]},LogicalType::LIST(LogicalType::STRUCT(child_list_t<LogicalType>{{"item", LogicalType::SMALLINT}, {"weight", LogicalType::UBIGINT}, {"cumulative_weight", LogicalType::UBIGINT}}))
                ,    DSKLLsorted_view<int16_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the sketch's sorted view: one entry per retained item in ascending order (equal items can appear more than once), with the item's weight and the inclusive cumulative weight (the number of input items less than or equal to it; estimated once the sketch is in estimation mode). Divide cumulative_weight by datasketch_kll_n(sketch) for the normalized rank. An empty sketch yields an empty list";
                    desc.examples.push_back("datasketch_kll_sorted_view(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::SMALLINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::INTEGER]},LogicalType::LIST(LogicalType::STRUCT(child_list_t<LogicalType>{{"item", LogicalType::INTEGER}, {"weight", LogicalType::UBIGINT}, {"cumulative_weight", LogicalType::UBIGINT}}))
                ,    DSKLLsorted_view<int32_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the sketch's sorted view: one entry per retained item in ascending order (equal items can appear more than once), with the item's weight and the inclusive cumulative weight (the number of input items less than or equal to it; estimated once the sketch is in estimation mode). Divide cumulative_weight by datasketch_kll_n(sketch) for the normalized rank. An empty sketch yields an empty list";
                    desc.examples.push_back("datasketch_kll_sorted_view(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::INTEGER] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::BIGINT]},LogicalType::LIST(LogicalType::STRUCT(child_list_t<LogicalType>{{"item", LogicalType::BIGINT}, {"weight", LogicalType::UBIGINT}, {"cumulative_weight", LogicalType::UBIGINT}}))
                ,    DSKLLsorted_view<int64_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the sketch's sorted view: one entry per retained item in ascending order (equal items can appear more than once), with the item's weight and the inclusive cumulative weight (the number of input items less than or equal to it; estimated once the sketch is in estimation mode). Divide cumulative_weight by datasketch_kll_n(sketch) for the normalized rank. An empty sketch yields an empty list";
                    desc.examples.push_back("datasketch_kll_sorted_view(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::BIGINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::FLOAT]},LogicalType::LIST(LogicalType::STRUCT(child_list_t<LogicalType>{{"item", LogicalType::FLOAT}, {"weight", LogicalType::UBIGINT}, {"cumulative_weight", LogicalType::UBIGINT}}))
                ,    DSKLLsorted_view<float>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the sketch's sorted view: one entry per retained item in ascending order (equal items can appear more than once), with the item's weight and the inclusive cumulative weight (the number of input items less than or equal to it; estimated once the sketch is in estimation mode). Divide cumulative_weight by datasketch_kll_n(sketch) for the normalized rank. An empty sketch yields an empty list";
                    desc.examples.push_back("datasketch_kll_sorted_view(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::FLOAT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::DOUBLE]},LogicalType::LIST(LogicalType::STRUCT(child_list_t<LogicalType>{{"item", LogicalType::DOUBLE}, {"weight", LogicalType::UBIGINT}, {"cumulative_weight", LogicalType::UBIGINT}}))
                ,    DSKLLsorted_view<double>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the sketch's sorted view: one entry per retained item in ascending order (equal items can appear more than once), with the item's weight and the inclusive cumulative weight (the number of input items less than or equal to it; estimated once the sketch is in estimation mode). Divide cumulative_weight by datasketch_kll_n(sketch) for the normalized rank. An empty sketch yields an empty list";
                    desc.examples.push_back("datasketch_kll_sorted_view(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::DOUBLE] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UTINYINT]},LogicalType::LIST(LogicalType::STRUCT(child_list_t<LogicalType>{{"item", LogicalType::UTINYINT}, {"weight", LogicalType::UBIGINT}, {"cumulative_weight", LogicalType::UBIGINT}}))
                ,    DSKLLsorted_view<uint8_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the sketch's sorted view: one entry per retained item in ascending order (equal items can appear more than once), with the item's weight and the inclusive cumulative weight (the number of input items less than or equal to it; estimated once the sketch is in estimation mode). Divide cumulative_weight by datasketch_kll_n(sketch) for the normalized rank. An empty sketch yields an empty list";
                    desc.examples.push_back("datasketch_kll_sorted_view(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UTINYINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::USMALLINT]},LogicalType::LIST(LogicalType::STRUCT(child_list_t<LogicalType>{{"item", LogicalType::USMALLINT}, {"weight", LogicalType::UBIGINT}, {"cumulative_weight", LogicalType::UBIGINT}}))
                ,    DSKLLsorted_view<uint16_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the sketch's sorted view: one entry per retained item in ascending order (equal items can appear more than once), with the item's weight and the inclusive cumulative weight (the number of input items less than or equal to it; estimated once the sketch is in estimation mode). Divide cumulative_weight by datasketch_kll_n(sketch) for the normalized rank. An empty sketch yields an empty list";
                    desc.examples.push_back("datasketch_kll_sorted_view(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::USMALLINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UINTEGER]},LogicalType::LIST(LogicalType::STRUCT(child_list_t<LogicalType>{{"item", LogicalType::UINTEGER}, {"weight", LogicalType::UBIGINT}, {"cumulative_weight", LogicalType::UBIGINT}}))
                ,    DSKLLsorted_view<uint32_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the sketch's sorted view: one entry per retained item in ascending order (equal items can appear more than once), with the item's weight and the inclusive cumulative weight (the number of input items less than or equal to it; estimated once the sketch is in estimation mode). Divide cumulative_weight by datasketch_kll_n(sketch) for the normalized rank. An empty sketch yields an empty list";
                    desc.examples.push_back("datasketch_kll_sorted_view(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UINTEGER] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UBIGINT]},LogicalType::LIST(LogicalType::STRUCT(child_list_t<LogicalType>{{"item", LogicalType::UBIGINT}, {"weight", LogicalType::UBIGINT}, {"cumulative_weight", LogicalType::UBIGINT}}))
                ,    DSKLLsorted_view<uint64_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the sketch's sorted view: one entry per retained item in ascending order (equal items can appear more than once), with the item's weight and the inclusive cumulative weight (the number of input items less than or equal to it; estimated once the sketch is in estimation mode). Divide cumulative_weight by datasketch_kll_n(sketch) for the normalized rank. An empty sketch yields an empty list";
                    desc.examples.push_back("datasketch_kll_sorted_view(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UBIGINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::VARCHAR]},LogicalType::LIST(LogicalType::STRUCT(child_list_t<LogicalType>{{"item", LogicalType::VARCHAR}, {"weight", LogicalType::UBIGINT}, {"cumulative_weight", LogicalType::UBIGINT}}))
                ,    DSKLLsorted_view<string_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the sketch's sorted view: one entry per retained item in ascending order (equal items can appear more than once), with the item's weight and the inclusive cumulative weight (the number of input items less than or equal to it; estimated once the sketch is in estimation mode). Divide cumulative_weight by datasketch_kll_n(sketch) for the normalized rank. An empty sketch yields an empty list";
                    desc.examples.push_back("datasketch_kll_sorted_view(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::VARCHAR] };
                    fn_descs.push_back(std::move(desc));
                }
        

        // Every scalar function deserializes a sketch, which throws on invalid input.
        fs.SetFallible();
        CreateScalarFunctionInfo info(std::move(fs));
        for (auto &d : fn_descs) {
            info.descriptions.push_back(std::move(d));
        }

        DSSetCategories(info, {"sketch", "kll"});

        DSRegister(loader, info);


      }

      // This funciton creates the sketches.
      {
      AggregateFunctionSet sketch("datasketch_kll");
      std::vector<FunctionDescription> agg_descs;
      
        
            {
                auto fun = DSKLLCreateAggregate<int8_t>(LogicalType::TINYINT, sketch_map_types[LogicalTypeId::TINYINT]);
                fun.SetBindCallback(DSKLLBind);
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                DSPrependArgument(fun, LogicalType::INTEGER);
                sketch.AddFunction(fun);

                FunctionDescription desc;
                desc.description = "Creates a sketch_kll data sketch by aggregating values";
                desc.examples.push_back("datasketch_kll(k, data)");
                desc.parameter_names = {"k", "data"};
                desc.parameter_types = { LogicalType::INTEGER, LogicalType::TINYINT };
                agg_descs.push_back(std::move(desc));
            }
            
            {
                auto fun = DSKLLMergeAggregate<int8_t>(LogicalType::TINYINT, sketch_map_types[LogicalTypeId::TINYINT]);
                fun.SetBindCallback(DSKLLBind);
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                DSPrependArgument(fun, LogicalType::INTEGER);
                sketch.AddFunction(fun);

                FunctionDescription desc;
                desc.description = "Creates a sketch_kll data sketch by merging other KLL data sketches";
                desc.examples.push_back("datasketch_kll(k, sketch)");
                desc.parameter_names = {"k", "sketch"};
                desc.parameter_types = { LogicalType::INTEGER, sketch_map_types[LogicalTypeId::TINYINT] };
                agg_descs.push_back(std::move(desc));
            }
        
        
            {
                auto fun = DSKLLCreateAggregate<int16_t>(LogicalType::SMALLINT, sketch_map_types[LogicalTypeId::SMALLINT]);
                fun.SetBindCallback(DSKLLBind);
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                DSPrependArgument(fun, LogicalType::INTEGER);
                sketch.AddFunction(fun);

                FunctionDescription desc;
                desc.description = "Creates a sketch_kll data sketch by aggregating values";
                desc.examples.push_back("datasketch_kll(k, data)");
                desc.parameter_names = {"k", "data"};
                desc.parameter_types = { LogicalType::INTEGER, LogicalType::SMALLINT };
                agg_descs.push_back(std::move(desc));
            }
            
            {
                auto fun = DSKLLMergeAggregate<int16_t>(LogicalType::SMALLINT, sketch_map_types[LogicalTypeId::SMALLINT]);
                fun.SetBindCallback(DSKLLBind);
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                DSPrependArgument(fun, LogicalType::INTEGER);
                sketch.AddFunction(fun);

                FunctionDescription desc;
                desc.description = "Creates a sketch_kll data sketch by merging other KLL data sketches";
                desc.examples.push_back("datasketch_kll(k, sketch)");
                desc.parameter_names = {"k", "sketch"};
                desc.parameter_types = { LogicalType::INTEGER, sketch_map_types[LogicalTypeId::SMALLINT] };
                agg_descs.push_back(std::move(desc));
            }
        
        
            {
                auto fun = DSKLLCreateAggregate<int32_t>(LogicalType::INTEGER, sketch_map_types[LogicalTypeId::INTEGER]);
                fun.SetBindCallback(DSKLLBind);
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                DSPrependArgument(fun, LogicalType::INTEGER);
                sketch.AddFunction(fun);

                FunctionDescription desc;
                desc.description = "Creates a sketch_kll data sketch by aggregating values";
                desc.examples.push_back("datasketch_kll(k, data)");
                desc.parameter_names = {"k", "data"};
                desc.parameter_types = { LogicalType::INTEGER, LogicalType::INTEGER };
                agg_descs.push_back(std::move(desc));
            }
            
            {
                auto fun = DSKLLMergeAggregate<int32_t>(LogicalType::INTEGER, sketch_map_types[LogicalTypeId::INTEGER]);
                fun.SetBindCallback(DSKLLBind);
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                DSPrependArgument(fun, LogicalType::INTEGER);
                sketch.AddFunction(fun);

                FunctionDescription desc;
                desc.description = "Creates a sketch_kll data sketch by merging other KLL data sketches";
                desc.examples.push_back("datasketch_kll(k, sketch)");
                desc.parameter_names = {"k", "sketch"};
                desc.parameter_types = { LogicalType::INTEGER, sketch_map_types[LogicalTypeId::INTEGER] };
                agg_descs.push_back(std::move(desc));
            }
        
        
            {
                auto fun = DSKLLCreateAggregate<int64_t>(LogicalType::BIGINT, sketch_map_types[LogicalTypeId::BIGINT]);
                fun.SetBindCallback(DSKLLBind);
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                DSPrependArgument(fun, LogicalType::INTEGER);
                sketch.AddFunction(fun);

                FunctionDescription desc;
                desc.description = "Creates a sketch_kll data sketch by aggregating values";
                desc.examples.push_back("datasketch_kll(k, data)");
                desc.parameter_names = {"k", "data"};
                desc.parameter_types = { LogicalType::INTEGER, LogicalType::BIGINT };
                agg_descs.push_back(std::move(desc));
            }
            
            {
                auto fun = DSKLLMergeAggregate<int64_t>(LogicalType::BIGINT, sketch_map_types[LogicalTypeId::BIGINT]);
                fun.SetBindCallback(DSKLLBind);
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                DSPrependArgument(fun, LogicalType::INTEGER);
                sketch.AddFunction(fun);

                FunctionDescription desc;
                desc.description = "Creates a sketch_kll data sketch by merging other KLL data sketches";
                desc.examples.push_back("datasketch_kll(k, sketch)");
                desc.parameter_names = {"k", "sketch"};
                desc.parameter_types = { LogicalType::INTEGER, sketch_map_types[LogicalTypeId::BIGINT] };
                agg_descs.push_back(std::move(desc));
            }
        
        
            {
                auto fun = DSKLLCreateAggregate<float>(LogicalType::FLOAT, sketch_map_types[LogicalTypeId::FLOAT]);
                fun.SetBindCallback(DSKLLBind);
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                DSPrependArgument(fun, LogicalType::INTEGER);
                sketch.AddFunction(fun);

                FunctionDescription desc;
                desc.description = "Creates a sketch_kll data sketch by aggregating values";
                desc.examples.push_back("datasketch_kll(k, data)");
                desc.parameter_names = {"k", "data"};
                desc.parameter_types = { LogicalType::INTEGER, LogicalType::FLOAT };
                agg_descs.push_back(std::move(desc));
            }
            
            {
                auto fun = DSKLLMergeAggregate<float>(LogicalType::FLOAT, sketch_map_types[LogicalTypeId::FLOAT]);
                fun.SetBindCallback(DSKLLBind);
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                DSPrependArgument(fun, LogicalType::INTEGER);
                sketch.AddFunction(fun);

                FunctionDescription desc;
                desc.description = "Creates a sketch_kll data sketch by merging other KLL data sketches";
                desc.examples.push_back("datasketch_kll(k, sketch)");
                desc.parameter_names = {"k", "sketch"};
                desc.parameter_types = { LogicalType::INTEGER, sketch_map_types[LogicalTypeId::FLOAT] };
                agg_descs.push_back(std::move(desc));
            }
        
        
            {
                auto fun = DSKLLCreateAggregate<double>(LogicalType::DOUBLE, sketch_map_types[LogicalTypeId::DOUBLE]);
                fun.SetBindCallback(DSKLLBind);
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                DSPrependArgument(fun, LogicalType::INTEGER);
                sketch.AddFunction(fun);

                FunctionDescription desc;
                desc.description = "Creates a sketch_kll data sketch by aggregating values";
                desc.examples.push_back("datasketch_kll(k, data)");
                desc.parameter_names = {"k", "data"};
                desc.parameter_types = { LogicalType::INTEGER, LogicalType::DOUBLE };
                agg_descs.push_back(std::move(desc));
            }
            
            {
                auto fun = DSKLLMergeAggregate<double>(LogicalType::DOUBLE, sketch_map_types[LogicalTypeId::DOUBLE]);
                fun.SetBindCallback(DSKLLBind);
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                DSPrependArgument(fun, LogicalType::INTEGER);
                sketch.AddFunction(fun);

                FunctionDescription desc;
                desc.description = "Creates a sketch_kll data sketch by merging other KLL data sketches";
                desc.examples.push_back("datasketch_kll(k, sketch)");
                desc.parameter_names = {"k", "sketch"};
                desc.parameter_types = { LogicalType::INTEGER, sketch_map_types[LogicalTypeId::DOUBLE] };
                agg_descs.push_back(std::move(desc));
            }
        
        
            {
                auto fun = DSKLLCreateAggregate<uint8_t>(LogicalType::UTINYINT, sketch_map_types[LogicalTypeId::UTINYINT]);
                fun.SetBindCallback(DSKLLBind);
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                DSPrependArgument(fun, LogicalType::INTEGER);
                sketch.AddFunction(fun);

                FunctionDescription desc;
                desc.description = "Creates a sketch_kll data sketch by aggregating values";
                desc.examples.push_back("datasketch_kll(k, data)");
                desc.parameter_names = {"k", "data"};
                desc.parameter_types = { LogicalType::INTEGER, LogicalType::UTINYINT };
                agg_descs.push_back(std::move(desc));
            }
            
            {
                auto fun = DSKLLMergeAggregate<uint8_t>(LogicalType::UTINYINT, sketch_map_types[LogicalTypeId::UTINYINT]);
                fun.SetBindCallback(DSKLLBind);
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                DSPrependArgument(fun, LogicalType::INTEGER);
                sketch.AddFunction(fun);

                FunctionDescription desc;
                desc.description = "Creates a sketch_kll data sketch by merging other KLL data sketches";
                desc.examples.push_back("datasketch_kll(k, sketch)");
                desc.parameter_names = {"k", "sketch"};
                desc.parameter_types = { LogicalType::INTEGER, sketch_map_types[LogicalTypeId::UTINYINT] };
                agg_descs.push_back(std::move(desc));
            }
        
        
            {
                auto fun = DSKLLCreateAggregate<uint16_t>(LogicalType::USMALLINT, sketch_map_types[LogicalTypeId::USMALLINT]);
                fun.SetBindCallback(DSKLLBind);
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                DSPrependArgument(fun, LogicalType::INTEGER);
                sketch.AddFunction(fun);

                FunctionDescription desc;
                desc.description = "Creates a sketch_kll data sketch by aggregating values";
                desc.examples.push_back("datasketch_kll(k, data)");
                desc.parameter_names = {"k", "data"};
                desc.parameter_types = { LogicalType::INTEGER, LogicalType::USMALLINT };
                agg_descs.push_back(std::move(desc));
            }
            
            {
                auto fun = DSKLLMergeAggregate<uint16_t>(LogicalType::USMALLINT, sketch_map_types[LogicalTypeId::USMALLINT]);
                fun.SetBindCallback(DSKLLBind);
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                DSPrependArgument(fun, LogicalType::INTEGER);
                sketch.AddFunction(fun);

                FunctionDescription desc;
                desc.description = "Creates a sketch_kll data sketch by merging other KLL data sketches";
                desc.examples.push_back("datasketch_kll(k, sketch)");
                desc.parameter_names = {"k", "sketch"};
                desc.parameter_types = { LogicalType::INTEGER, sketch_map_types[LogicalTypeId::USMALLINT] };
                agg_descs.push_back(std::move(desc));
            }
        
        
            {
                auto fun = DSKLLCreateAggregate<uint32_t>(LogicalType::UINTEGER, sketch_map_types[LogicalTypeId::UINTEGER]);
                fun.SetBindCallback(DSKLLBind);
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                DSPrependArgument(fun, LogicalType::INTEGER);
                sketch.AddFunction(fun);

                FunctionDescription desc;
                desc.description = "Creates a sketch_kll data sketch by aggregating values";
                desc.examples.push_back("datasketch_kll(k, data)");
                desc.parameter_names = {"k", "data"};
                desc.parameter_types = { LogicalType::INTEGER, LogicalType::UINTEGER };
                agg_descs.push_back(std::move(desc));
            }
            
            {
                auto fun = DSKLLMergeAggregate<uint32_t>(LogicalType::UINTEGER, sketch_map_types[LogicalTypeId::UINTEGER]);
                fun.SetBindCallback(DSKLLBind);
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                DSPrependArgument(fun, LogicalType::INTEGER);
                sketch.AddFunction(fun);

                FunctionDescription desc;
                desc.description = "Creates a sketch_kll data sketch by merging other KLL data sketches";
                desc.examples.push_back("datasketch_kll(k, sketch)");
                desc.parameter_names = {"k", "sketch"};
                desc.parameter_types = { LogicalType::INTEGER, sketch_map_types[LogicalTypeId::UINTEGER] };
                agg_descs.push_back(std::move(desc));
            }
        
        
            {
                auto fun = DSKLLCreateAggregate<uint64_t>(LogicalType::UBIGINT, sketch_map_types[LogicalTypeId::UBIGINT]);
                fun.SetBindCallback(DSKLLBind);
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                DSPrependArgument(fun, LogicalType::INTEGER);
                sketch.AddFunction(fun);

                FunctionDescription desc;
                desc.description = "Creates a sketch_kll data sketch by aggregating values";
                desc.examples.push_back("datasketch_kll(k, data)");
                desc.parameter_names = {"k", "data"};
                desc.parameter_types = { LogicalType::INTEGER, LogicalType::UBIGINT };
                agg_descs.push_back(std::move(desc));
            }
            
            {
                auto fun = DSKLLMergeAggregate<uint64_t>(LogicalType::UBIGINT, sketch_map_types[LogicalTypeId::UBIGINT]);
                fun.SetBindCallback(DSKLLBind);
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                DSPrependArgument(fun, LogicalType::INTEGER);
                sketch.AddFunction(fun);

                FunctionDescription desc;
                desc.description = "Creates a sketch_kll data sketch by merging other KLL data sketches";
                desc.examples.push_back("datasketch_kll(k, sketch)");
                desc.parameter_names = {"k", "sketch"};
                desc.parameter_types = { LogicalType::INTEGER, sketch_map_types[LogicalTypeId::UBIGINT] };
                agg_descs.push_back(std::move(desc));
            }
        
        
            {
                auto fun = DSKLLCreateAggregate<string_t>(LogicalType::VARCHAR, sketch_map_types[LogicalTypeId::VARCHAR]);
                fun.SetBindCallback(DSKLLBind);
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                DSPrependArgument(fun, LogicalType::INTEGER);
                sketch.AddFunction(fun);

                FunctionDescription desc;
                desc.description = "Creates a sketch_kll data sketch by aggregating values";
                desc.examples.push_back("datasketch_kll(k, data)");
                desc.parameter_names = {"k", "data"};
                desc.parameter_types = { LogicalType::INTEGER, LogicalType::VARCHAR };
                agg_descs.push_back(std::move(desc));
            }
            
            {
                auto fun = DSKLLMergeAggregate<string_t>(LogicalType::VARCHAR, sketch_map_types[LogicalTypeId::VARCHAR]);
                fun.SetBindCallback(DSKLLBind);
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                DSPrependArgument(fun, LogicalType::INTEGER);
                sketch.AddFunction(fun);

                FunctionDescription desc;
                desc.description = "Creates a sketch_kll data sketch by merging other KLL data sketches";
                desc.examples.push_back("datasketch_kll(k, sketch)");
                desc.parameter_names = {"k", "sketch"};
                desc.parameter_types = { LogicalType::INTEGER, sketch_map_types[LogicalTypeId::VARCHAR] };
                agg_descs.push_back(std::move(desc));
            }
        
      CreateAggregateFunctionInfo sketch_info(sketch);
      for (auto &d : agg_descs) {
          sketch_info.descriptions.push_back(std::move(d));
      }

      DSSetCategories(sketch_info, {"sketch", "kll"});

      DSRegister(loader, sketch_info);
      }



      


  }
    


template <class T>

static inline void DSREQis_empty(DataChunk &args, ExpressionState &state, Vector &result)
{
    // Get the references to the incoming vectors.
    D_ASSERT(args.ColumnCount() == 1);

    
        auto &sketch_vector = args.data[0];

    
        
        UnaryExecutor::Execute
        <string_t,bool>
        (
        sketch_vector,result,
        [&](string_t sketch_data) {

            
            auto sketch = [&]() {
                try {
                    return datasketches::req_sketch<DSItem<T>>::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
                } catch (const std::exception &e) {
                    throw InvalidInputException("Failed to deserialize REQ sketch: %s", e.what());
                }
            }();
return sketch.is_empty();
        });

}

    


template <class T>

static inline void DSREQk(DataChunk &args, ExpressionState &state, Vector &result)
{
    // Get the references to the incoming vectors.
    D_ASSERT(args.ColumnCount() == 1);

    
        auto &sketch_vector = args.data[0];

    
        
        UnaryExecutor::Execute
        <string_t,uint16_t>
        (
        sketch_vector,result,
        [&](string_t sketch_data) {

            
            auto sketch = [&]() {
                try {
                    return datasketches::req_sketch<DSItem<T>>::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
                } catch (const std::exception &e) {
                    throw InvalidInputException("Failed to deserialize REQ sketch: %s", e.what());
                }
            }();
return sketch.get_k();
        });

}

    


template <class T>

static inline void DSREQcdf(DataChunk &args, ExpressionState &state, Vector &result)
{
    // Get the references to the incoming vectors.
    D_ASSERT(args.ColumnCount() == 3);

    
        auto &sketch_vector = args.data[0];
        auto &split_points_vector = args.data[1];
        auto &inclusive_vector = args.data[2];

    
        
                    auto &split_points_list_children = ListVector::GetChild(split_points_vector);

                    UnifiedVectorFormat split_points_children_unified;
                    split_points_list_children.ToUnifiedFormat(split_points_children_unified);

                    const T *split_points_list_children_data = UnifiedVectorFormat::GetData<T>(split_points_children_unified);
                    
        TernaryExecutor::Execute
        <string_t,list_entry_t,bool,list_entry_t>
        (
        sketch_vector,split_points_vector,inclusive_vector,result,
        [&](string_t sketch_data,list_entry_t split_points_data,bool inclusive_data) {

            
            auto sketch = [&]() {
                try {
                    return datasketches::req_sketch<DSItem<T>>::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
                } catch (const std::exception &e) {
                    throw InvalidInputException("Failed to deserialize REQ sketch: %s", e.what());
                }
            }();

                    std::vector<DSItem<T>> passing_points(split_points_data.length);
                    for (idx_t i = 0; i < split_points_data.length; i++)
                    {
                        passing_points[i] = DSItemIn(split_points_list_children_data[split_points_children_unified.sel->get_index(i + split_points_data.offset)]);
                    }
                    
auto cdf_result = sketch.get_CDF(passing_points.data(), split_points_data.length, inclusive_data);
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
                
        });

}

    


template <class T>

static inline void DSREQpmf(DataChunk &args, ExpressionState &state, Vector &result)
{
    // Get the references to the incoming vectors.
    D_ASSERT(args.ColumnCount() == 3);

    
        auto &sketch_vector = args.data[0];
        auto &split_points_vector = args.data[1];
        auto &inclusive_vector = args.data[2];

    
        
                    auto &split_points_list_children = ListVector::GetChild(split_points_vector);

                    UnifiedVectorFormat split_points_children_unified;
                    split_points_list_children.ToUnifiedFormat(split_points_children_unified);

                    const T *split_points_list_children_data = UnifiedVectorFormat::GetData<T>(split_points_children_unified);
                    
        TernaryExecutor::Execute
        <string_t,list_entry_t,bool,list_entry_t>
        (
        sketch_vector,split_points_vector,inclusive_vector,result,
        [&](string_t sketch_data,list_entry_t split_points_data,bool inclusive_data) {

            
            auto sketch = [&]() {
                try {
                    return datasketches::req_sketch<DSItem<T>>::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
                } catch (const std::exception &e) {
                    throw InvalidInputException("Failed to deserialize REQ sketch: %s", e.what());
                }
            }();

                    std::vector<DSItem<T>> passing_points(split_points_data.length);
                    for (idx_t i = 0; i < split_points_data.length; i++)
                    {
                        passing_points[i] = DSItemIn(split_points_list_children_data[split_points_children_unified.sel->get_index(i + split_points_data.offset)]);
                    }
                    
auto pmf_result = sketch.get_PMF(passing_points.data(), split_points_data.length, inclusive_data);
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
                
        });

}

    


template <class T>

static inline void DSREQdescribe(DataChunk &args, ExpressionState &state, Vector &result)
{
    // Get the references to the incoming vectors.
    D_ASSERT(args.ColumnCount() == 3);

    
        auto &sketch_vector = args.data[0];
        auto &include_levels_vector = args.data[1];
        auto &include_items_vector = args.data[2];

    
        
        TernaryExecutor::Execute
        <string_t,bool,bool,string_t>
        (
        sketch_vector,include_levels_vector,include_items_vector,result,
        [&](string_t sketch_data,bool include_levels_data,bool include_items_data) {

            
            auto sketch = [&]() {
                try {
                    return datasketches::req_sketch<DSItem<T>>::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
                } catch (const std::exception &e) {
                    throw InvalidInputException("Failed to deserialize REQ sketch: %s", e.what());
                }
            }();
return StringVector::AddString(result, sketch.to_string(include_levels_data, include_items_data));
        });

}

    


template <class T>

static inline void DSREQrank(DataChunk &args, ExpressionState &state, Vector &result)
{
    // Get the references to the incoming vectors.
    D_ASSERT(args.ColumnCount() == 3);

    
        auto &sketch_vector = args.data[0];
        auto &item_vector = args.data[1];
        auto &inclusive_vector = args.data[2];

    
        
        TernaryExecutor::Execute
        <string_t,T,bool,double>
        (
        sketch_vector,item_vector,inclusive_vector,result,
        [&](string_t sketch_data,T item_data,bool inclusive_data) {

            
            auto sketch = [&]() {
                try {
                    return datasketches::req_sketch<DSItem<T>>::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
                } catch (const std::exception &e) {
                    throw InvalidInputException("Failed to deserialize REQ sketch: %s", e.what());
                }
            }();
return sketch.get_rank(DSItemIn(item_data), inclusive_data);
        });

}

    


template <class T>

static inline void DSREQquantile(DataChunk &args, ExpressionState &state, Vector &result)
{
    // Get the references to the incoming vectors.
    D_ASSERT(args.ColumnCount() == 3);

    
        auto &sketch_vector = args.data[0];
        auto &rank_vector = args.data[1];
        auto &inclusive_vector = args.data[2];

    
        
        TernaryExecutor::Execute
        <string_t,double,bool,T>
        (
        sketch_vector,rank_vector,inclusive_vector,result,
        [&](string_t sketch_data,double rank_data,bool inclusive_data) {

            
            auto sketch = [&]() {
                try {
                    return datasketches::req_sketch<DSItem<T>>::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
                } catch (const std::exception &e) {
                    throw InvalidInputException("Failed to deserialize REQ sketch: %s", e.what());
                }
            }();
return DSItemOut<T>(result, sketch.get_quantile(rank_data, inclusive_data));
        });

}

    


template <class T>

static inline void DSREQn(DataChunk &args, ExpressionState &state, Vector &result)
{
    // Get the references to the incoming vectors.
    D_ASSERT(args.ColumnCount() == 1);

    
        auto &sketch_vector = args.data[0];

    
        
        UnaryExecutor::Execute
        <string_t,uint64_t>
        (
        sketch_vector,result,
        [&](string_t sketch_data) {

            
            auto sketch = [&]() {
                try {
                    return datasketches::req_sketch<DSItem<T>>::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
                } catch (const std::exception &e) {
                    throw InvalidInputException("Failed to deserialize REQ sketch: %s", e.what());
                }
            }();
return sketch.get_n();
        });

}

    


template <class T>

static inline void DSREQis_estimation_mode(DataChunk &args, ExpressionState &state, Vector &result)
{
    // Get the references to the incoming vectors.
    D_ASSERT(args.ColumnCount() == 1);

    
        auto &sketch_vector = args.data[0];

    
        
        UnaryExecutor::Execute
        <string_t,bool>
        (
        sketch_vector,result,
        [&](string_t sketch_data) {

            
            auto sketch = [&]() {
                try {
                    return datasketches::req_sketch<DSItem<T>>::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
                } catch (const std::exception &e) {
                    throw InvalidInputException("Failed to deserialize REQ sketch: %s", e.what());
                }
            }();
return sketch.is_estimation_mode();
        });

}

    


template <class T>

static inline void DSREQnum_retained(DataChunk &args, ExpressionState &state, Vector &result)
{
    // Get the references to the incoming vectors.
    D_ASSERT(args.ColumnCount() == 1);

    
        auto &sketch_vector = args.data[0];

    
        
        UnaryExecutor::Execute
        <string_t,uint64_t>
        (
        sketch_vector,result,
        [&](string_t sketch_data) {

            
            auto sketch = [&]() {
                try {
                    return datasketches::req_sketch<DSItem<T>>::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
                } catch (const std::exception &e) {
                    throw InvalidInputException("Failed to deserialize REQ sketch: %s", e.what());
                }
            }();
return sketch.get_num_retained();
        });

}

    


template <class T>

static inline void DSREQmin_item(DataChunk &args, ExpressionState &state, Vector &result)
{
    // Get the references to the incoming vectors.
    D_ASSERT(args.ColumnCount() == 1);

    
        auto &sketch_vector = args.data[0];

    
        
        UnaryExecutor::Execute
        <string_t,T>
        (
        sketch_vector,result,
        [&](string_t sketch_data) {

            
            auto sketch = [&]() {
                try {
                    return datasketches::req_sketch<DSItem<T>>::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
                } catch (const std::exception &e) {
                    throw InvalidInputException("Failed to deserialize REQ sketch: %s", e.what());
                }
            }();
return DSItemOut<T>(result, sketch.get_min_item());
        });

}

    


template <class T>

static inline void DSREQmax_item(DataChunk &args, ExpressionState &state, Vector &result)
{
    // Get the references to the incoming vectors.
    D_ASSERT(args.ColumnCount() == 1);

    
        auto &sketch_vector = args.data[0];

    
        
        UnaryExecutor::Execute
        <string_t,T>
        (
        sketch_vector,result,
        [&](string_t sketch_data) {

            
            auto sketch = [&]() {
                try {
                    return datasketches::req_sketch<DSItem<T>>::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
                } catch (const std::exception &e) {
                    throw InvalidInputException("Failed to deserialize REQ sketch: %s", e.what());
                }
            }();
return DSItemOut<T>(result, sketch.get_max_item());
        });

}

    


template <class T>

static inline void DSREQrank_lower_bound(DataChunk &args, ExpressionState &state, Vector &result)
{
    // Get the references to the incoming vectors.
    D_ASSERT(args.ColumnCount() == 3);

    
        auto &sketch_vector = args.data[0];
        auto &rank_vector = args.data[1];
        auto &num_std_dev_vector = args.data[2];

    
        
        TernaryExecutor::Execute
        <string_t,double,uint8_t,double>
        (
        sketch_vector,rank_vector,num_std_dev_vector,result,
        [&](string_t sketch_data,double rank_data,uint8_t num_std_dev_data) {

            
            auto sketch = [&]() {
                try {
                    return datasketches::req_sketch<DSItem<T>>::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
                } catch (const std::exception &e) {
                    throw InvalidInputException("Failed to deserialize REQ sketch: %s", e.what());
                }
            }();
DSCheckRank(rank_data); return sketch.get_rank_lower_bound(rank_data, num_std_dev_data);
        });

}

    


template <class T>

static inline void DSREQrank_upper_bound(DataChunk &args, ExpressionState &state, Vector &result)
{
    // Get the references to the incoming vectors.
    D_ASSERT(args.ColumnCount() == 3);

    
        auto &sketch_vector = args.data[0];
        auto &rank_vector = args.data[1];
        auto &num_std_dev_vector = args.data[2];

    
        
        TernaryExecutor::Execute
        <string_t,double,uint8_t,double>
        (
        sketch_vector,rank_vector,num_std_dev_vector,result,
        [&](string_t sketch_data,double rank_data,uint8_t num_std_dev_data) {

            
            auto sketch = [&]() {
                try {
                    return datasketches::req_sketch<DSItem<T>>::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
                } catch (const std::exception &e) {
                    throw InvalidInputException("Failed to deserialize REQ sketch: %s", e.what());
                }
            }();
DSCheckRank(rank_data); return sketch.get_rank_upper_bound(rank_data, num_std_dev_data);
        });

}

    


template <class T>

static inline void DSREQis_hra(DataChunk &args, ExpressionState &state, Vector &result)
{
    // Get the references to the incoming vectors.
    D_ASSERT(args.ColumnCount() == 1);

    
        auto &sketch_vector = args.data[0];

    
        
        UnaryExecutor::Execute
        <string_t,bool>
        (
        sketch_vector,result,
        [&](string_t sketch_data) {

            
            auto sketch = [&]() {
                try {
                    return datasketches::req_sketch<DSItem<T>>::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
                } catch (const std::exception &e) {
                    throw InvalidInputException("Failed to deserialize REQ sketch: %s", e.what());
                }
            }();
return sketch.is_HRA();
        });

}

    


template <class T>

static inline void DSREQquantiles(DataChunk &args, ExpressionState &state, Vector &result)
{
    // Get the references to the incoming vectors.
    D_ASSERT(args.ColumnCount() == 3);

    
        auto &sketch_vector = args.data[0];
        auto &ranks_vector = args.data[1];
        auto &inclusive_vector = args.data[2];

    
        
                    auto &ranks_list_children = ListVector::GetChild(ranks_vector);
                    UnifiedVectorFormat ranks_children_unified;
                    ranks_list_children.ToUnifiedFormat(ranks_children_unified);
                    const double *ranks_children_data = UnifiedVectorFormat::GetData<double>(ranks_children_unified);
                    
        TernaryExecutor::Execute
        <string_t,list_entry_t,bool,list_entry_t>
        (
        sketch_vector,ranks_vector,inclusive_vector,result,
        [&](string_t sketch_data,list_entry_t ranks_data,bool inclusive_data) {

            
            auto sketch = [&]() {
                try {
                    return datasketches::req_sketch<DSItem<T>>::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
                } catch (const std::exception &e) {
                    throw InvalidInputException("Failed to deserialize REQ sketch: %s", e.what());
                }
            }();

                if (sketch.is_empty())
                {
                    throw InvalidInputException("datasketch_req_quantiles: operation is undefined for an empty sketch");
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
                
        });

}

    


template <class T>

static inline void DSREQranks(DataChunk &args, ExpressionState &state, Vector &result)
{
    // Get the references to the incoming vectors.
    D_ASSERT(args.ColumnCount() == 3);

    
        auto &sketch_vector = args.data[0];
        auto &items_vector = args.data[1];
        auto &inclusive_vector = args.data[2];

    
        
                    auto &items_list_children = ListVector::GetChild(items_vector);
                    UnifiedVectorFormat items_children_unified;
                    items_list_children.ToUnifiedFormat(items_children_unified);
                    const T *items_children_data = UnifiedVectorFormat::GetData<T>(items_children_unified);
                    
        TernaryExecutor::Execute
        <string_t,list_entry_t,bool,list_entry_t>
        (
        sketch_vector,items_vector,inclusive_vector,result,
        [&](string_t sketch_data,list_entry_t items_data,bool inclusive_data) {

            
            auto sketch = [&]() {
                try {
                    return datasketches::req_sketch<DSItem<T>>::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
                } catch (const std::exception &e) {
                    throw InvalidInputException("Failed to deserialize REQ sketch: %s", e.what());
                }
            }();

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
                
        });

}

    


template <class T>

static inline void DSREQsorted_view(DataChunk &args, ExpressionState &state, Vector &result)
{
    // Get the references to the incoming vectors.
    D_ASSERT(args.ColumnCount() == 1);

    
        auto &sketch_vector = args.data[0];

    
        
        UnaryExecutor::Execute
        <string_t,list_entry_t>
        (
        sketch_vector,result,
        [&](string_t sketch_data) {

            
            auto sketch = [&]() {
                try {
                    return datasketches::req_sketch<DSItem<T>>::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
                } catch (const std::exception &e) {
                    throw InvalidInputException("Failed to deserialize REQ sketch: %s", e.what());
                }
            }();

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
                
        });

}

    


    
    template <typename T>
    auto static DSREQMergeAggregate(const LogicalType &type, const LogicalType &result_type) -> AggregateFunction
    
    {
        
        auto fun = AggregateFunction::UnaryAggregate<DSREQState<T>, string_t, string_t, DSQuantilesMergeOperation<DSREQBindData>, AggregateDestructorType::LEGACY>(result_type, result_type);
        DSReadDataArgumentAt<1, DSREQState<T>, string_t, DSQuantilesMergeOperation<DSREQBindData>>(fun);
        return fun;
        
    }



    template <typename T>
    auto static DSREQCreateAggregate(const LogicalType &type, const LogicalType &result_type) -> AggregateFunction
    {
        
        auto fun = AggregateFunction::UnaryAggregate<DSREQState<T>, T, string_t, DSQuantilesCreateOperation<DSREQBindData>, AggregateDestructorType::LEGACY>(type, result_type);
        DSReadDataArgumentAt<1, DSREQState<T>, T, DSQuantilesCreateOperation<DSREQBindData>>(fun);
        return fun;
        
    }


  void LoadREQSketch(ExtensionLoader &loader) {


      
        std::unordered_map<LogicalTypeId, LogicalType> sketch_map_types;
        
            sketch_map_types.insert({ LogicalTypeId::TINYINT, CreateREQSketchType(loader, LogicalType(LogicalTypeId::TINYINT))});
            sketch_map_types.insert({ LogicalTypeId::SMALLINT, CreateREQSketchType(loader, LogicalType(LogicalTypeId::SMALLINT))});
            sketch_map_types.insert({ LogicalTypeId::INTEGER, CreateREQSketchType(loader, LogicalType(LogicalTypeId::INTEGER))});
            sketch_map_types.insert({ LogicalTypeId::BIGINT, CreateREQSketchType(loader, LogicalType(LogicalTypeId::BIGINT))});
            sketch_map_types.insert({ LogicalTypeId::FLOAT, CreateREQSketchType(loader, LogicalType(LogicalTypeId::FLOAT))});
            sketch_map_types.insert({ LogicalTypeId::DOUBLE, CreateREQSketchType(loader, LogicalType(LogicalTypeId::DOUBLE))});
            sketch_map_types.insert({ LogicalTypeId::UTINYINT, CreateREQSketchType(loader, LogicalType(LogicalTypeId::UTINYINT))});
            sketch_map_types.insert({ LogicalTypeId::USMALLINT, CreateREQSketchType(loader, LogicalType(LogicalTypeId::USMALLINT))});
            sketch_map_types.insert({ LogicalTypeId::UINTEGER, CreateREQSketchType(loader, LogicalType(LogicalTypeId::UINTEGER))});
            sketch_map_types.insert({ LogicalTypeId::UBIGINT, CreateREQSketchType(loader, LogicalType(LogicalTypeId::UBIGINT))});
      

      
      {
        ScalarFunctionSet fs("datasketch_req_is_empty");
        std::vector<FunctionDescription> fn_descs;
        
            
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::TINYINT]},LogicalType::BOOLEAN
                ,    DSREQis_empty<int8_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return a boolean indicating if the sketch is empty";
                    desc.examples.push_back("datasketch_req_is_empty(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::TINYINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::SMALLINT]},LogicalType::BOOLEAN
                ,    DSREQis_empty<int16_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return a boolean indicating if the sketch is empty";
                    desc.examples.push_back("datasketch_req_is_empty(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::SMALLINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::INTEGER]},LogicalType::BOOLEAN
                ,    DSREQis_empty<int32_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return a boolean indicating if the sketch is empty";
                    desc.examples.push_back("datasketch_req_is_empty(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::INTEGER] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::BIGINT]},LogicalType::BOOLEAN
                ,    DSREQis_empty<int64_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return a boolean indicating if the sketch is empty";
                    desc.examples.push_back("datasketch_req_is_empty(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::BIGINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::FLOAT]},LogicalType::BOOLEAN
                ,    DSREQis_empty<float>));
                {
                    FunctionDescription desc;
                    desc.description = "Return a boolean indicating if the sketch is empty";
                    desc.examples.push_back("datasketch_req_is_empty(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::FLOAT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::DOUBLE]},LogicalType::BOOLEAN
                ,    DSREQis_empty<double>));
                {
                    FunctionDescription desc;
                    desc.description = "Return a boolean indicating if the sketch is empty";
                    desc.examples.push_back("datasketch_req_is_empty(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::DOUBLE] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UTINYINT]},LogicalType::BOOLEAN
                ,    DSREQis_empty<uint8_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return a boolean indicating if the sketch is empty";
                    desc.examples.push_back("datasketch_req_is_empty(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UTINYINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::USMALLINT]},LogicalType::BOOLEAN
                ,    DSREQis_empty<uint16_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return a boolean indicating if the sketch is empty";
                    desc.examples.push_back("datasketch_req_is_empty(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::USMALLINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UINTEGER]},LogicalType::BOOLEAN
                ,    DSREQis_empty<uint32_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return a boolean indicating if the sketch is empty";
                    desc.examples.push_back("datasketch_req_is_empty(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UINTEGER] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UBIGINT]},LogicalType::BOOLEAN
                ,    DSREQis_empty<uint64_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return a boolean indicating if the sketch is empty";
                    desc.examples.push_back("datasketch_req_is_empty(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UBIGINT] };
                    fn_descs.push_back(std::move(desc));
                }
        

        // Every scalar function deserializes a sketch, which throws on invalid input.
        fs.SetFallible();
        CreateScalarFunctionInfo info(std::move(fs));
        for (auto &d : fn_descs) {
            info.descriptions.push_back(std::move(d));
        }

        DSSetCategories(info, {"sketch", "req"});

        DSRegister(loader, info);


      }
      {
        ScalarFunctionSet fs("datasketch_req_k");
        std::vector<FunctionDescription> fn_descs;
        
            
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::TINYINT]},LogicalType::USMALLINT
                ,    DSREQk<int8_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the value of K for this sketch";
                    desc.examples.push_back("datasketch_req_k(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::TINYINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::SMALLINT]},LogicalType::USMALLINT
                ,    DSREQk<int16_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the value of K for this sketch";
                    desc.examples.push_back("datasketch_req_k(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::SMALLINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::INTEGER]},LogicalType::USMALLINT
                ,    DSREQk<int32_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the value of K for this sketch";
                    desc.examples.push_back("datasketch_req_k(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::INTEGER] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::BIGINT]},LogicalType::USMALLINT
                ,    DSREQk<int64_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the value of K for this sketch";
                    desc.examples.push_back("datasketch_req_k(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::BIGINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::FLOAT]},LogicalType::USMALLINT
                ,    DSREQk<float>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the value of K for this sketch";
                    desc.examples.push_back("datasketch_req_k(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::FLOAT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::DOUBLE]},LogicalType::USMALLINT
                ,    DSREQk<double>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the value of K for this sketch";
                    desc.examples.push_back("datasketch_req_k(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::DOUBLE] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UTINYINT]},LogicalType::USMALLINT
                ,    DSREQk<uint8_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the value of K for this sketch";
                    desc.examples.push_back("datasketch_req_k(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UTINYINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::USMALLINT]},LogicalType::USMALLINT
                ,    DSREQk<uint16_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the value of K for this sketch";
                    desc.examples.push_back("datasketch_req_k(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::USMALLINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UINTEGER]},LogicalType::USMALLINT
                ,    DSREQk<uint32_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the value of K for this sketch";
                    desc.examples.push_back("datasketch_req_k(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UINTEGER] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UBIGINT]},LogicalType::USMALLINT
                ,    DSREQk<uint64_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the value of K for this sketch";
                    desc.examples.push_back("datasketch_req_k(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UBIGINT] };
                    fn_descs.push_back(std::move(desc));
                }
        

        // Every scalar function deserializes a sketch, which throws on invalid input.
        fs.SetFallible();
        CreateScalarFunctionInfo info(std::move(fs));
        for (auto &d : fn_descs) {
            info.descriptions.push_back(std::move(d));
        }

        DSSetCategories(info, {"sketch", "req"});

        DSRegister(loader, info);


      }
      {
        ScalarFunctionSet fs("datasketch_req_cdf");
        std::vector<FunctionDescription> fn_descs;
        
            
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::TINYINT],LogicalType::LIST(LogicalType::TINYINT),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::DOUBLE)
                ,    DSREQcdf<int8_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the Cumulative Distribution Function (CDF) of the sketch for a series of points";
                    desc.examples.push_back("datasketch_req_cdf(sketch, points, inclusive)");
                    desc.parameter_names = { "sketch","split_points","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::TINYINT],LogicalType::LIST(LogicalType::TINYINT),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::SMALLINT],LogicalType::LIST(LogicalType::SMALLINT),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::DOUBLE)
                ,    DSREQcdf<int16_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the Cumulative Distribution Function (CDF) of the sketch for a series of points";
                    desc.examples.push_back("datasketch_req_cdf(sketch, points, inclusive)");
                    desc.parameter_names = { "sketch","split_points","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::SMALLINT],LogicalType::LIST(LogicalType::SMALLINT),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::INTEGER],LogicalType::LIST(LogicalType::INTEGER),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::DOUBLE)
                ,    DSREQcdf<int32_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the Cumulative Distribution Function (CDF) of the sketch for a series of points";
                    desc.examples.push_back("datasketch_req_cdf(sketch, points, inclusive)");
                    desc.parameter_names = { "sketch","split_points","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::INTEGER],LogicalType::LIST(LogicalType::INTEGER),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::BIGINT],LogicalType::LIST(LogicalType::BIGINT),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::DOUBLE)
                ,    DSREQcdf<int64_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the Cumulative Distribution Function (CDF) of the sketch for a series of points";
                    desc.examples.push_back("datasketch_req_cdf(sketch, points, inclusive)");
                    desc.parameter_names = { "sketch","split_points","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::BIGINT],LogicalType::LIST(LogicalType::BIGINT),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::FLOAT],LogicalType::LIST(LogicalType::FLOAT),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::DOUBLE)
                ,    DSREQcdf<float>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the Cumulative Distribution Function (CDF) of the sketch for a series of points";
                    desc.examples.push_back("datasketch_req_cdf(sketch, points, inclusive)");
                    desc.parameter_names = { "sketch","split_points","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::FLOAT],LogicalType::LIST(LogicalType::FLOAT),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::DOUBLE],LogicalType::LIST(LogicalType::DOUBLE),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::DOUBLE)
                ,    DSREQcdf<double>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the Cumulative Distribution Function (CDF) of the sketch for a series of points";
                    desc.examples.push_back("datasketch_req_cdf(sketch, points, inclusive)");
                    desc.parameter_names = { "sketch","split_points","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::DOUBLE],LogicalType::LIST(LogicalType::DOUBLE),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UTINYINT],LogicalType::LIST(LogicalType::UTINYINT),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::DOUBLE)
                ,    DSREQcdf<uint8_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the Cumulative Distribution Function (CDF) of the sketch for a series of points";
                    desc.examples.push_back("datasketch_req_cdf(sketch, points, inclusive)");
                    desc.parameter_names = { "sketch","split_points","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UTINYINT],LogicalType::LIST(LogicalType::UTINYINT),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::USMALLINT],LogicalType::LIST(LogicalType::USMALLINT),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::DOUBLE)
                ,    DSREQcdf<uint16_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the Cumulative Distribution Function (CDF) of the sketch for a series of points";
                    desc.examples.push_back("datasketch_req_cdf(sketch, points, inclusive)");
                    desc.parameter_names = { "sketch","split_points","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::USMALLINT],LogicalType::LIST(LogicalType::USMALLINT),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UINTEGER],LogicalType::LIST(LogicalType::UINTEGER),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::DOUBLE)
                ,    DSREQcdf<uint32_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the Cumulative Distribution Function (CDF) of the sketch for a series of points";
                    desc.examples.push_back("datasketch_req_cdf(sketch, points, inclusive)");
                    desc.parameter_names = { "sketch","split_points","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UINTEGER],LogicalType::LIST(LogicalType::UINTEGER),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UBIGINT],LogicalType::LIST(LogicalType::UBIGINT),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::DOUBLE)
                ,    DSREQcdf<uint64_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the Cumulative Distribution Function (CDF) of the sketch for a series of points";
                    desc.examples.push_back("datasketch_req_cdf(sketch, points, inclusive)");
                    desc.parameter_names = { "sketch","split_points","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UBIGINT],LogicalType::LIST(LogicalType::UBIGINT),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
        

        // Every scalar function deserializes a sketch, which throws on invalid input.
        fs.SetFallible();
        CreateScalarFunctionInfo info(std::move(fs));
        for (auto &d : fn_descs) {
            info.descriptions.push_back(std::move(d));
        }

        DSSetCategories(info, {"sketch", "req"});

        DSRegister(loader, info);


      }
      {
        ScalarFunctionSet fs("datasketch_req_pmf");
        std::vector<FunctionDescription> fn_descs;
        
            
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::TINYINT],LogicalType::LIST(LogicalType::TINYINT),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::DOUBLE)
                ,    DSREQpmf<int8_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the Probability Mass Function (PMF) of the sketch for a series of points";
                    desc.examples.push_back("datasketch_req_pmf(sketch, points, inclusive)");
                    desc.parameter_names = { "sketch","split_points","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::TINYINT],LogicalType::LIST(LogicalType::TINYINT),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::SMALLINT],LogicalType::LIST(LogicalType::SMALLINT),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::DOUBLE)
                ,    DSREQpmf<int16_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the Probability Mass Function (PMF) of the sketch for a series of points";
                    desc.examples.push_back("datasketch_req_pmf(sketch, points, inclusive)");
                    desc.parameter_names = { "sketch","split_points","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::SMALLINT],LogicalType::LIST(LogicalType::SMALLINT),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::INTEGER],LogicalType::LIST(LogicalType::INTEGER),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::DOUBLE)
                ,    DSREQpmf<int32_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the Probability Mass Function (PMF) of the sketch for a series of points";
                    desc.examples.push_back("datasketch_req_pmf(sketch, points, inclusive)");
                    desc.parameter_names = { "sketch","split_points","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::INTEGER],LogicalType::LIST(LogicalType::INTEGER),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::BIGINT],LogicalType::LIST(LogicalType::BIGINT),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::DOUBLE)
                ,    DSREQpmf<int64_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the Probability Mass Function (PMF) of the sketch for a series of points";
                    desc.examples.push_back("datasketch_req_pmf(sketch, points, inclusive)");
                    desc.parameter_names = { "sketch","split_points","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::BIGINT],LogicalType::LIST(LogicalType::BIGINT),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::FLOAT],LogicalType::LIST(LogicalType::FLOAT),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::DOUBLE)
                ,    DSREQpmf<float>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the Probability Mass Function (PMF) of the sketch for a series of points";
                    desc.examples.push_back("datasketch_req_pmf(sketch, points, inclusive)");
                    desc.parameter_names = { "sketch","split_points","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::FLOAT],LogicalType::LIST(LogicalType::FLOAT),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::DOUBLE],LogicalType::LIST(LogicalType::DOUBLE),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::DOUBLE)
                ,    DSREQpmf<double>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the Probability Mass Function (PMF) of the sketch for a series of points";
                    desc.examples.push_back("datasketch_req_pmf(sketch, points, inclusive)");
                    desc.parameter_names = { "sketch","split_points","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::DOUBLE],LogicalType::LIST(LogicalType::DOUBLE),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UTINYINT],LogicalType::LIST(LogicalType::UTINYINT),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::DOUBLE)
                ,    DSREQpmf<uint8_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the Probability Mass Function (PMF) of the sketch for a series of points";
                    desc.examples.push_back("datasketch_req_pmf(sketch, points, inclusive)");
                    desc.parameter_names = { "sketch","split_points","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UTINYINT],LogicalType::LIST(LogicalType::UTINYINT),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::USMALLINT],LogicalType::LIST(LogicalType::USMALLINT),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::DOUBLE)
                ,    DSREQpmf<uint16_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the Probability Mass Function (PMF) of the sketch for a series of points";
                    desc.examples.push_back("datasketch_req_pmf(sketch, points, inclusive)");
                    desc.parameter_names = { "sketch","split_points","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::USMALLINT],LogicalType::LIST(LogicalType::USMALLINT),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UINTEGER],LogicalType::LIST(LogicalType::UINTEGER),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::DOUBLE)
                ,    DSREQpmf<uint32_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the Probability Mass Function (PMF) of the sketch for a series of points";
                    desc.examples.push_back("datasketch_req_pmf(sketch, points, inclusive)");
                    desc.parameter_names = { "sketch","split_points","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UINTEGER],LogicalType::LIST(LogicalType::UINTEGER),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UBIGINT],LogicalType::LIST(LogicalType::UBIGINT),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::DOUBLE)
                ,    DSREQpmf<uint64_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the Probability Mass Function (PMF) of the sketch for a series of points";
                    desc.examples.push_back("datasketch_req_pmf(sketch, points, inclusive)");
                    desc.parameter_names = { "sketch","split_points","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UBIGINT],LogicalType::LIST(LogicalType::UBIGINT),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
        

        // Every scalar function deserializes a sketch, which throws on invalid input.
        fs.SetFallible();
        CreateScalarFunctionInfo info(std::move(fs));
        for (auto &d : fn_descs) {
            info.descriptions.push_back(std::move(d));
        }

        DSSetCategories(info, {"sketch", "req"});

        DSRegister(loader, info);


      }
      {
        ScalarFunctionSet fs("datasketch_req_describe");
        std::vector<FunctionDescription> fn_descs;
        
            
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::TINYINT],LogicalType::BOOLEAN,LogicalType::BOOLEAN},LogicalType::VARCHAR
                ,    DSREQdescribe<int8_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return a description of this sketch";
                    desc.examples.push_back("datasketch_req_describe(sketch, include_levels, include_items)");
                    desc.parameter_names = { "sketch","include_levels","include_items" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::TINYINT],LogicalType::BOOLEAN,LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::SMALLINT],LogicalType::BOOLEAN,LogicalType::BOOLEAN},LogicalType::VARCHAR
                ,    DSREQdescribe<int16_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return a description of this sketch";
                    desc.examples.push_back("datasketch_req_describe(sketch, include_levels, include_items)");
                    desc.parameter_names = { "sketch","include_levels","include_items" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::SMALLINT],LogicalType::BOOLEAN,LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::INTEGER],LogicalType::BOOLEAN,LogicalType::BOOLEAN},LogicalType::VARCHAR
                ,    DSREQdescribe<int32_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return a description of this sketch";
                    desc.examples.push_back("datasketch_req_describe(sketch, include_levels, include_items)");
                    desc.parameter_names = { "sketch","include_levels","include_items" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::INTEGER],LogicalType::BOOLEAN,LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::BIGINT],LogicalType::BOOLEAN,LogicalType::BOOLEAN},LogicalType::VARCHAR
                ,    DSREQdescribe<int64_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return a description of this sketch";
                    desc.examples.push_back("datasketch_req_describe(sketch, include_levels, include_items)");
                    desc.parameter_names = { "sketch","include_levels","include_items" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::BIGINT],LogicalType::BOOLEAN,LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::FLOAT],LogicalType::BOOLEAN,LogicalType::BOOLEAN},LogicalType::VARCHAR
                ,    DSREQdescribe<float>));
                {
                    FunctionDescription desc;
                    desc.description = "Return a description of this sketch";
                    desc.examples.push_back("datasketch_req_describe(sketch, include_levels, include_items)");
                    desc.parameter_names = { "sketch","include_levels","include_items" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::FLOAT],LogicalType::BOOLEAN,LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::DOUBLE],LogicalType::BOOLEAN,LogicalType::BOOLEAN},LogicalType::VARCHAR
                ,    DSREQdescribe<double>));
                {
                    FunctionDescription desc;
                    desc.description = "Return a description of this sketch";
                    desc.examples.push_back("datasketch_req_describe(sketch, include_levels, include_items)");
                    desc.parameter_names = { "sketch","include_levels","include_items" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::DOUBLE],LogicalType::BOOLEAN,LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UTINYINT],LogicalType::BOOLEAN,LogicalType::BOOLEAN},LogicalType::VARCHAR
                ,    DSREQdescribe<uint8_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return a description of this sketch";
                    desc.examples.push_back("datasketch_req_describe(sketch, include_levels, include_items)");
                    desc.parameter_names = { "sketch","include_levels","include_items" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UTINYINT],LogicalType::BOOLEAN,LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::USMALLINT],LogicalType::BOOLEAN,LogicalType::BOOLEAN},LogicalType::VARCHAR
                ,    DSREQdescribe<uint16_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return a description of this sketch";
                    desc.examples.push_back("datasketch_req_describe(sketch, include_levels, include_items)");
                    desc.parameter_names = { "sketch","include_levels","include_items" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::USMALLINT],LogicalType::BOOLEAN,LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UINTEGER],LogicalType::BOOLEAN,LogicalType::BOOLEAN},LogicalType::VARCHAR
                ,    DSREQdescribe<uint32_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return a description of this sketch";
                    desc.examples.push_back("datasketch_req_describe(sketch, include_levels, include_items)");
                    desc.parameter_names = { "sketch","include_levels","include_items" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UINTEGER],LogicalType::BOOLEAN,LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UBIGINT],LogicalType::BOOLEAN,LogicalType::BOOLEAN},LogicalType::VARCHAR
                ,    DSREQdescribe<uint64_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return a description of this sketch";
                    desc.examples.push_back("datasketch_req_describe(sketch, include_levels, include_items)");
                    desc.parameter_names = { "sketch","include_levels","include_items" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UBIGINT],LogicalType::BOOLEAN,LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
        

        // Every scalar function deserializes a sketch, which throws on invalid input.
        fs.SetFallible();
        CreateScalarFunctionInfo info(std::move(fs));
        for (auto &d : fn_descs) {
            info.descriptions.push_back(std::move(d));
        }

        DSSetCategories(info, {"sketch", "req"});

        DSRegister(loader, info);


      }
      {
        ScalarFunctionSet fs("datasketch_req_rank");
        std::vector<FunctionDescription> fn_descs;
        
            
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::TINYINT],LogicalType::TINYINT,LogicalType::BOOLEAN},LogicalType::DOUBLE
                ,    DSREQrank<int8_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the rank of an item in the sketch";
                    desc.examples.push_back("datasketch_req_rank(sketch, item, inclusive)");
                    desc.parameter_names = { "sketch","item","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::TINYINT],LogicalType::TINYINT,LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::SMALLINT],LogicalType::SMALLINT,LogicalType::BOOLEAN},LogicalType::DOUBLE
                ,    DSREQrank<int16_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the rank of an item in the sketch";
                    desc.examples.push_back("datasketch_req_rank(sketch, item, inclusive)");
                    desc.parameter_names = { "sketch","item","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::SMALLINT],LogicalType::SMALLINT,LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::INTEGER],LogicalType::INTEGER,LogicalType::BOOLEAN},LogicalType::DOUBLE
                ,    DSREQrank<int32_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the rank of an item in the sketch";
                    desc.examples.push_back("datasketch_req_rank(sketch, item, inclusive)");
                    desc.parameter_names = { "sketch","item","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::INTEGER],LogicalType::INTEGER,LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::BIGINT],LogicalType::BIGINT,LogicalType::BOOLEAN},LogicalType::DOUBLE
                ,    DSREQrank<int64_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the rank of an item in the sketch";
                    desc.examples.push_back("datasketch_req_rank(sketch, item, inclusive)");
                    desc.parameter_names = { "sketch","item","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::BIGINT],LogicalType::BIGINT,LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::FLOAT],LogicalType::FLOAT,LogicalType::BOOLEAN},LogicalType::DOUBLE
                ,    DSREQrank<float>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the rank of an item in the sketch";
                    desc.examples.push_back("datasketch_req_rank(sketch, item, inclusive)");
                    desc.parameter_names = { "sketch","item","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::FLOAT],LogicalType::FLOAT,LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::DOUBLE],LogicalType::DOUBLE,LogicalType::BOOLEAN},LogicalType::DOUBLE
                ,    DSREQrank<double>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the rank of an item in the sketch";
                    desc.examples.push_back("datasketch_req_rank(sketch, item, inclusive)");
                    desc.parameter_names = { "sketch","item","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::DOUBLE],LogicalType::DOUBLE,LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UTINYINT],LogicalType::UTINYINT,LogicalType::BOOLEAN},LogicalType::DOUBLE
                ,    DSREQrank<uint8_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the rank of an item in the sketch";
                    desc.examples.push_back("datasketch_req_rank(sketch, item, inclusive)");
                    desc.parameter_names = { "sketch","item","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UTINYINT],LogicalType::UTINYINT,LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::USMALLINT],LogicalType::USMALLINT,LogicalType::BOOLEAN},LogicalType::DOUBLE
                ,    DSREQrank<uint16_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the rank of an item in the sketch";
                    desc.examples.push_back("datasketch_req_rank(sketch, item, inclusive)");
                    desc.parameter_names = { "sketch","item","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::USMALLINT],LogicalType::USMALLINT,LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UINTEGER],LogicalType::UINTEGER,LogicalType::BOOLEAN},LogicalType::DOUBLE
                ,    DSREQrank<uint32_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the rank of an item in the sketch";
                    desc.examples.push_back("datasketch_req_rank(sketch, item, inclusive)");
                    desc.parameter_names = { "sketch","item","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UINTEGER],LogicalType::UINTEGER,LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UBIGINT],LogicalType::UBIGINT,LogicalType::BOOLEAN},LogicalType::DOUBLE
                ,    DSREQrank<uint64_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the rank of an item in the sketch";
                    desc.examples.push_back("datasketch_req_rank(sketch, item, inclusive)");
                    desc.parameter_names = { "sketch","item","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UBIGINT],LogicalType::UBIGINT,LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
        

        // Every scalar function deserializes a sketch, which throws on invalid input.
        fs.SetFallible();
        CreateScalarFunctionInfo info(std::move(fs));
        for (auto &d : fn_descs) {
            info.descriptions.push_back(std::move(d));
        }

        DSSetCategories(info, {"sketch", "req"});

        DSRegister(loader, info);


      }
      {
        ScalarFunctionSet fs("datasketch_req_quantile");
        std::vector<FunctionDescription> fn_descs;
        
            
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::TINYINT],LogicalType::DOUBLE,LogicalType::BOOLEAN},LogicalType::TINYINT
                ,    DSREQquantile<int8_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the quantile of a rank in the sketch";
                    desc.examples.push_back("datasketch_req_quantile(sketch, rank, inclusive)");
                    desc.parameter_names = { "sketch","rank","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::TINYINT],LogicalType::DOUBLE,LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::SMALLINT],LogicalType::DOUBLE,LogicalType::BOOLEAN},LogicalType::SMALLINT
                ,    DSREQquantile<int16_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the quantile of a rank in the sketch";
                    desc.examples.push_back("datasketch_req_quantile(sketch, rank, inclusive)");
                    desc.parameter_names = { "sketch","rank","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::SMALLINT],LogicalType::DOUBLE,LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::INTEGER],LogicalType::DOUBLE,LogicalType::BOOLEAN},LogicalType::INTEGER
                ,    DSREQquantile<int32_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the quantile of a rank in the sketch";
                    desc.examples.push_back("datasketch_req_quantile(sketch, rank, inclusive)");
                    desc.parameter_names = { "sketch","rank","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::INTEGER],LogicalType::DOUBLE,LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::BIGINT],LogicalType::DOUBLE,LogicalType::BOOLEAN},LogicalType::BIGINT
                ,    DSREQquantile<int64_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the quantile of a rank in the sketch";
                    desc.examples.push_back("datasketch_req_quantile(sketch, rank, inclusive)");
                    desc.parameter_names = { "sketch","rank","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::BIGINT],LogicalType::DOUBLE,LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::FLOAT],LogicalType::DOUBLE,LogicalType::BOOLEAN},LogicalType::FLOAT
                ,    DSREQquantile<float>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the quantile of a rank in the sketch";
                    desc.examples.push_back("datasketch_req_quantile(sketch, rank, inclusive)");
                    desc.parameter_names = { "sketch","rank","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::FLOAT],LogicalType::DOUBLE,LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::DOUBLE],LogicalType::DOUBLE,LogicalType::BOOLEAN},LogicalType::DOUBLE
                ,    DSREQquantile<double>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the quantile of a rank in the sketch";
                    desc.examples.push_back("datasketch_req_quantile(sketch, rank, inclusive)");
                    desc.parameter_names = { "sketch","rank","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::DOUBLE],LogicalType::DOUBLE,LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UTINYINT],LogicalType::DOUBLE,LogicalType::BOOLEAN},LogicalType::UTINYINT
                ,    DSREQquantile<uint8_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the quantile of a rank in the sketch";
                    desc.examples.push_back("datasketch_req_quantile(sketch, rank, inclusive)");
                    desc.parameter_names = { "sketch","rank","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UTINYINT],LogicalType::DOUBLE,LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::USMALLINT],LogicalType::DOUBLE,LogicalType::BOOLEAN},LogicalType::USMALLINT
                ,    DSREQquantile<uint16_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the quantile of a rank in the sketch";
                    desc.examples.push_back("datasketch_req_quantile(sketch, rank, inclusive)");
                    desc.parameter_names = { "sketch","rank","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::USMALLINT],LogicalType::DOUBLE,LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UINTEGER],LogicalType::DOUBLE,LogicalType::BOOLEAN},LogicalType::UINTEGER
                ,    DSREQquantile<uint32_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the quantile of a rank in the sketch";
                    desc.examples.push_back("datasketch_req_quantile(sketch, rank, inclusive)");
                    desc.parameter_names = { "sketch","rank","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UINTEGER],LogicalType::DOUBLE,LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UBIGINT],LogicalType::DOUBLE,LogicalType::BOOLEAN},LogicalType::UBIGINT
                ,    DSREQquantile<uint64_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the quantile of a rank in the sketch";
                    desc.examples.push_back("datasketch_req_quantile(sketch, rank, inclusive)");
                    desc.parameter_names = { "sketch","rank","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UBIGINT],LogicalType::DOUBLE,LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
        

        // Every scalar function deserializes a sketch, which throws on invalid input.
        fs.SetFallible();
        CreateScalarFunctionInfo info(std::move(fs));
        for (auto &d : fn_descs) {
            info.descriptions.push_back(std::move(d));
        }

        DSSetCategories(info, {"sketch", "req"});

        DSRegister(loader, info);


      }
      {
        ScalarFunctionSet fs("datasketch_req_n");
        std::vector<FunctionDescription> fn_descs;
        
            
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::TINYINT]},LogicalType::UBIGINT
                ,    DSREQn<int8_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the number of items contained in the sketch";
                    desc.examples.push_back("datasketch_req_n(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::TINYINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::SMALLINT]},LogicalType::UBIGINT
                ,    DSREQn<int16_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the number of items contained in the sketch";
                    desc.examples.push_back("datasketch_req_n(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::SMALLINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::INTEGER]},LogicalType::UBIGINT
                ,    DSREQn<int32_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the number of items contained in the sketch";
                    desc.examples.push_back("datasketch_req_n(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::INTEGER] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::BIGINT]},LogicalType::UBIGINT
                ,    DSREQn<int64_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the number of items contained in the sketch";
                    desc.examples.push_back("datasketch_req_n(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::BIGINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::FLOAT]},LogicalType::UBIGINT
                ,    DSREQn<float>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the number of items contained in the sketch";
                    desc.examples.push_back("datasketch_req_n(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::FLOAT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::DOUBLE]},LogicalType::UBIGINT
                ,    DSREQn<double>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the number of items contained in the sketch";
                    desc.examples.push_back("datasketch_req_n(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::DOUBLE] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UTINYINT]},LogicalType::UBIGINT
                ,    DSREQn<uint8_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the number of items contained in the sketch";
                    desc.examples.push_back("datasketch_req_n(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UTINYINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::USMALLINT]},LogicalType::UBIGINT
                ,    DSREQn<uint16_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the number of items contained in the sketch";
                    desc.examples.push_back("datasketch_req_n(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::USMALLINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UINTEGER]},LogicalType::UBIGINT
                ,    DSREQn<uint32_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the number of items contained in the sketch";
                    desc.examples.push_back("datasketch_req_n(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UINTEGER] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UBIGINT]},LogicalType::UBIGINT
                ,    DSREQn<uint64_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the number of items contained in the sketch";
                    desc.examples.push_back("datasketch_req_n(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UBIGINT] };
                    fn_descs.push_back(std::move(desc));
                }
        

        // Every scalar function deserializes a sketch, which throws on invalid input.
        fs.SetFallible();
        CreateScalarFunctionInfo info(std::move(fs));
        for (auto &d : fn_descs) {
            info.descriptions.push_back(std::move(d));
        }

        DSSetCategories(info, {"sketch", "req"});

        DSRegister(loader, info);


      }
      {
        ScalarFunctionSet fs("datasketch_req_is_estimation_mode");
        std::vector<FunctionDescription> fn_descs;
        
            
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::TINYINT]},LogicalType::BOOLEAN
                ,    DSREQis_estimation_mode<int8_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return a boolean indicating if the sketch is in estimation mode";
                    desc.examples.push_back("datasketch_req_is_estimation_mode(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::TINYINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::SMALLINT]},LogicalType::BOOLEAN
                ,    DSREQis_estimation_mode<int16_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return a boolean indicating if the sketch is in estimation mode";
                    desc.examples.push_back("datasketch_req_is_estimation_mode(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::SMALLINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::INTEGER]},LogicalType::BOOLEAN
                ,    DSREQis_estimation_mode<int32_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return a boolean indicating if the sketch is in estimation mode";
                    desc.examples.push_back("datasketch_req_is_estimation_mode(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::INTEGER] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::BIGINT]},LogicalType::BOOLEAN
                ,    DSREQis_estimation_mode<int64_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return a boolean indicating if the sketch is in estimation mode";
                    desc.examples.push_back("datasketch_req_is_estimation_mode(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::BIGINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::FLOAT]},LogicalType::BOOLEAN
                ,    DSREQis_estimation_mode<float>));
                {
                    FunctionDescription desc;
                    desc.description = "Return a boolean indicating if the sketch is in estimation mode";
                    desc.examples.push_back("datasketch_req_is_estimation_mode(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::FLOAT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::DOUBLE]},LogicalType::BOOLEAN
                ,    DSREQis_estimation_mode<double>));
                {
                    FunctionDescription desc;
                    desc.description = "Return a boolean indicating if the sketch is in estimation mode";
                    desc.examples.push_back("datasketch_req_is_estimation_mode(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::DOUBLE] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UTINYINT]},LogicalType::BOOLEAN
                ,    DSREQis_estimation_mode<uint8_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return a boolean indicating if the sketch is in estimation mode";
                    desc.examples.push_back("datasketch_req_is_estimation_mode(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UTINYINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::USMALLINT]},LogicalType::BOOLEAN
                ,    DSREQis_estimation_mode<uint16_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return a boolean indicating if the sketch is in estimation mode";
                    desc.examples.push_back("datasketch_req_is_estimation_mode(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::USMALLINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UINTEGER]},LogicalType::BOOLEAN
                ,    DSREQis_estimation_mode<uint32_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return a boolean indicating if the sketch is in estimation mode";
                    desc.examples.push_back("datasketch_req_is_estimation_mode(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UINTEGER] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UBIGINT]},LogicalType::BOOLEAN
                ,    DSREQis_estimation_mode<uint64_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return a boolean indicating if the sketch is in estimation mode";
                    desc.examples.push_back("datasketch_req_is_estimation_mode(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UBIGINT] };
                    fn_descs.push_back(std::move(desc));
                }
        

        // Every scalar function deserializes a sketch, which throws on invalid input.
        fs.SetFallible();
        CreateScalarFunctionInfo info(std::move(fs));
        for (auto &d : fn_descs) {
            info.descriptions.push_back(std::move(d));
        }

        DSSetCategories(info, {"sketch", "req"});

        DSRegister(loader, info);


      }
      {
        ScalarFunctionSet fs("datasketch_req_num_retained");
        std::vector<FunctionDescription> fn_descs;
        
            
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::TINYINT]},LogicalType::UBIGINT
                ,    DSREQnum_retained<int8_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the number of retained items in the sketch";
                    desc.examples.push_back("datasketch_req_num_retained(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::TINYINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::SMALLINT]},LogicalType::UBIGINT
                ,    DSREQnum_retained<int16_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the number of retained items in the sketch";
                    desc.examples.push_back("datasketch_req_num_retained(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::SMALLINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::INTEGER]},LogicalType::UBIGINT
                ,    DSREQnum_retained<int32_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the number of retained items in the sketch";
                    desc.examples.push_back("datasketch_req_num_retained(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::INTEGER] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::BIGINT]},LogicalType::UBIGINT
                ,    DSREQnum_retained<int64_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the number of retained items in the sketch";
                    desc.examples.push_back("datasketch_req_num_retained(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::BIGINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::FLOAT]},LogicalType::UBIGINT
                ,    DSREQnum_retained<float>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the number of retained items in the sketch";
                    desc.examples.push_back("datasketch_req_num_retained(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::FLOAT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::DOUBLE]},LogicalType::UBIGINT
                ,    DSREQnum_retained<double>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the number of retained items in the sketch";
                    desc.examples.push_back("datasketch_req_num_retained(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::DOUBLE] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UTINYINT]},LogicalType::UBIGINT
                ,    DSREQnum_retained<uint8_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the number of retained items in the sketch";
                    desc.examples.push_back("datasketch_req_num_retained(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UTINYINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::USMALLINT]},LogicalType::UBIGINT
                ,    DSREQnum_retained<uint16_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the number of retained items in the sketch";
                    desc.examples.push_back("datasketch_req_num_retained(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::USMALLINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UINTEGER]},LogicalType::UBIGINT
                ,    DSREQnum_retained<uint32_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the number of retained items in the sketch";
                    desc.examples.push_back("datasketch_req_num_retained(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UINTEGER] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UBIGINT]},LogicalType::UBIGINT
                ,    DSREQnum_retained<uint64_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the number of retained items in the sketch";
                    desc.examples.push_back("datasketch_req_num_retained(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UBIGINT] };
                    fn_descs.push_back(std::move(desc));
                }
        

        // Every scalar function deserializes a sketch, which throws on invalid input.
        fs.SetFallible();
        CreateScalarFunctionInfo info(std::move(fs));
        for (auto &d : fn_descs) {
            info.descriptions.push_back(std::move(d));
        }

        DSSetCategories(info, {"sketch", "req"});

        DSRegister(loader, info);


      }
      {
        ScalarFunctionSet fs("datasketch_req_min_item");
        std::vector<FunctionDescription> fn_descs;
        
            
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::TINYINT]},LogicalType::TINYINT
                ,    DSREQmin_item<int8_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the minimum item in the sketch";
                    desc.examples.push_back("datasketch_req_min_item(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::TINYINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::SMALLINT]},LogicalType::SMALLINT
                ,    DSREQmin_item<int16_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the minimum item in the sketch";
                    desc.examples.push_back("datasketch_req_min_item(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::SMALLINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::INTEGER]},LogicalType::INTEGER
                ,    DSREQmin_item<int32_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the minimum item in the sketch";
                    desc.examples.push_back("datasketch_req_min_item(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::INTEGER] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::BIGINT]},LogicalType::BIGINT
                ,    DSREQmin_item<int64_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the minimum item in the sketch";
                    desc.examples.push_back("datasketch_req_min_item(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::BIGINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::FLOAT]},LogicalType::FLOAT
                ,    DSREQmin_item<float>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the minimum item in the sketch";
                    desc.examples.push_back("datasketch_req_min_item(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::FLOAT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::DOUBLE]},LogicalType::DOUBLE
                ,    DSREQmin_item<double>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the minimum item in the sketch";
                    desc.examples.push_back("datasketch_req_min_item(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::DOUBLE] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UTINYINT]},LogicalType::UTINYINT
                ,    DSREQmin_item<uint8_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the minimum item in the sketch";
                    desc.examples.push_back("datasketch_req_min_item(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UTINYINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::USMALLINT]},LogicalType::USMALLINT
                ,    DSREQmin_item<uint16_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the minimum item in the sketch";
                    desc.examples.push_back("datasketch_req_min_item(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::USMALLINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UINTEGER]},LogicalType::UINTEGER
                ,    DSREQmin_item<uint32_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the minimum item in the sketch";
                    desc.examples.push_back("datasketch_req_min_item(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UINTEGER] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UBIGINT]},LogicalType::UBIGINT
                ,    DSREQmin_item<uint64_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the minimum item in the sketch";
                    desc.examples.push_back("datasketch_req_min_item(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UBIGINT] };
                    fn_descs.push_back(std::move(desc));
                }
        

        // Every scalar function deserializes a sketch, which throws on invalid input.
        fs.SetFallible();
        CreateScalarFunctionInfo info(std::move(fs));
        for (auto &d : fn_descs) {
            info.descriptions.push_back(std::move(d));
        }

        DSSetCategories(info, {"sketch", "req"});

        DSRegister(loader, info);


      }
      {
        ScalarFunctionSet fs("datasketch_req_max_item");
        std::vector<FunctionDescription> fn_descs;
        
            
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::TINYINT]},LogicalType::TINYINT
                ,    DSREQmax_item<int8_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the maximum item in the sketch";
                    desc.examples.push_back("datasketch_req_max_item(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::TINYINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::SMALLINT]},LogicalType::SMALLINT
                ,    DSREQmax_item<int16_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the maximum item in the sketch";
                    desc.examples.push_back("datasketch_req_max_item(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::SMALLINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::INTEGER]},LogicalType::INTEGER
                ,    DSREQmax_item<int32_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the maximum item in the sketch";
                    desc.examples.push_back("datasketch_req_max_item(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::INTEGER] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::BIGINT]},LogicalType::BIGINT
                ,    DSREQmax_item<int64_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the maximum item in the sketch";
                    desc.examples.push_back("datasketch_req_max_item(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::BIGINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::FLOAT]},LogicalType::FLOAT
                ,    DSREQmax_item<float>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the maximum item in the sketch";
                    desc.examples.push_back("datasketch_req_max_item(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::FLOAT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::DOUBLE]},LogicalType::DOUBLE
                ,    DSREQmax_item<double>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the maximum item in the sketch";
                    desc.examples.push_back("datasketch_req_max_item(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::DOUBLE] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UTINYINT]},LogicalType::UTINYINT
                ,    DSREQmax_item<uint8_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the maximum item in the sketch";
                    desc.examples.push_back("datasketch_req_max_item(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UTINYINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::USMALLINT]},LogicalType::USMALLINT
                ,    DSREQmax_item<uint16_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the maximum item in the sketch";
                    desc.examples.push_back("datasketch_req_max_item(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::USMALLINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UINTEGER]},LogicalType::UINTEGER
                ,    DSREQmax_item<uint32_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the maximum item in the sketch";
                    desc.examples.push_back("datasketch_req_max_item(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UINTEGER] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UBIGINT]},LogicalType::UBIGINT
                ,    DSREQmax_item<uint64_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the maximum item in the sketch";
                    desc.examples.push_back("datasketch_req_max_item(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UBIGINT] };
                    fn_descs.push_back(std::move(desc));
                }
        

        // Every scalar function deserializes a sketch, which throws on invalid input.
        fs.SetFallible();
        CreateScalarFunctionInfo info(std::move(fs));
        for (auto &d : fn_descs) {
            info.descriptions.push_back(std::move(d));
        }

        DSSetCategories(info, {"sketch", "req"});

        DSRegister(loader, info);


      }
      {
        ScalarFunctionSet fs("datasketch_req_rank_lower_bound");
        std::vector<FunctionDescription> fn_descs;
        
            
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::TINYINT],LogicalType::DOUBLE,LogicalType::UTINYINT},LogicalType::DOUBLE
                ,    DSREQrank_lower_bound<int8_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return an approximate lower bound of the given normalized rank, with a confidence of num_std_dev standard deviations (1, 2 or 3)";
                    desc.examples.push_back("datasketch_req_rank_lower_bound(sketch, 0.5, 2)");
                    desc.parameter_names = { "sketch","rank","num_std_dev" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::TINYINT],LogicalType::DOUBLE,LogicalType::UTINYINT };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::SMALLINT],LogicalType::DOUBLE,LogicalType::UTINYINT},LogicalType::DOUBLE
                ,    DSREQrank_lower_bound<int16_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return an approximate lower bound of the given normalized rank, with a confidence of num_std_dev standard deviations (1, 2 or 3)";
                    desc.examples.push_back("datasketch_req_rank_lower_bound(sketch, 0.5, 2)");
                    desc.parameter_names = { "sketch","rank","num_std_dev" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::SMALLINT],LogicalType::DOUBLE,LogicalType::UTINYINT };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::INTEGER],LogicalType::DOUBLE,LogicalType::UTINYINT},LogicalType::DOUBLE
                ,    DSREQrank_lower_bound<int32_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return an approximate lower bound of the given normalized rank, with a confidence of num_std_dev standard deviations (1, 2 or 3)";
                    desc.examples.push_back("datasketch_req_rank_lower_bound(sketch, 0.5, 2)");
                    desc.parameter_names = { "sketch","rank","num_std_dev" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::INTEGER],LogicalType::DOUBLE,LogicalType::UTINYINT };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::BIGINT],LogicalType::DOUBLE,LogicalType::UTINYINT},LogicalType::DOUBLE
                ,    DSREQrank_lower_bound<int64_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return an approximate lower bound of the given normalized rank, with a confidence of num_std_dev standard deviations (1, 2 or 3)";
                    desc.examples.push_back("datasketch_req_rank_lower_bound(sketch, 0.5, 2)");
                    desc.parameter_names = { "sketch","rank","num_std_dev" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::BIGINT],LogicalType::DOUBLE,LogicalType::UTINYINT };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::FLOAT],LogicalType::DOUBLE,LogicalType::UTINYINT},LogicalType::DOUBLE
                ,    DSREQrank_lower_bound<float>));
                {
                    FunctionDescription desc;
                    desc.description = "Return an approximate lower bound of the given normalized rank, with a confidence of num_std_dev standard deviations (1, 2 or 3)";
                    desc.examples.push_back("datasketch_req_rank_lower_bound(sketch, 0.5, 2)");
                    desc.parameter_names = { "sketch","rank","num_std_dev" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::FLOAT],LogicalType::DOUBLE,LogicalType::UTINYINT };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::DOUBLE],LogicalType::DOUBLE,LogicalType::UTINYINT},LogicalType::DOUBLE
                ,    DSREQrank_lower_bound<double>));
                {
                    FunctionDescription desc;
                    desc.description = "Return an approximate lower bound of the given normalized rank, with a confidence of num_std_dev standard deviations (1, 2 or 3)";
                    desc.examples.push_back("datasketch_req_rank_lower_bound(sketch, 0.5, 2)");
                    desc.parameter_names = { "sketch","rank","num_std_dev" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::DOUBLE],LogicalType::DOUBLE,LogicalType::UTINYINT };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UTINYINT],LogicalType::DOUBLE,LogicalType::UTINYINT},LogicalType::DOUBLE
                ,    DSREQrank_lower_bound<uint8_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return an approximate lower bound of the given normalized rank, with a confidence of num_std_dev standard deviations (1, 2 or 3)";
                    desc.examples.push_back("datasketch_req_rank_lower_bound(sketch, 0.5, 2)");
                    desc.parameter_names = { "sketch","rank","num_std_dev" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UTINYINT],LogicalType::DOUBLE,LogicalType::UTINYINT };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::USMALLINT],LogicalType::DOUBLE,LogicalType::UTINYINT},LogicalType::DOUBLE
                ,    DSREQrank_lower_bound<uint16_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return an approximate lower bound of the given normalized rank, with a confidence of num_std_dev standard deviations (1, 2 or 3)";
                    desc.examples.push_back("datasketch_req_rank_lower_bound(sketch, 0.5, 2)");
                    desc.parameter_names = { "sketch","rank","num_std_dev" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::USMALLINT],LogicalType::DOUBLE,LogicalType::UTINYINT };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UINTEGER],LogicalType::DOUBLE,LogicalType::UTINYINT},LogicalType::DOUBLE
                ,    DSREQrank_lower_bound<uint32_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return an approximate lower bound of the given normalized rank, with a confidence of num_std_dev standard deviations (1, 2 or 3)";
                    desc.examples.push_back("datasketch_req_rank_lower_bound(sketch, 0.5, 2)");
                    desc.parameter_names = { "sketch","rank","num_std_dev" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UINTEGER],LogicalType::DOUBLE,LogicalType::UTINYINT };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UBIGINT],LogicalType::DOUBLE,LogicalType::UTINYINT},LogicalType::DOUBLE
                ,    DSREQrank_lower_bound<uint64_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return an approximate lower bound of the given normalized rank, with a confidence of num_std_dev standard deviations (1, 2 or 3)";
                    desc.examples.push_back("datasketch_req_rank_lower_bound(sketch, 0.5, 2)");
                    desc.parameter_names = { "sketch","rank","num_std_dev" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UBIGINT],LogicalType::DOUBLE,LogicalType::UTINYINT };
                    fn_descs.push_back(std::move(desc));
                }
        

        // Every scalar function deserializes a sketch, which throws on invalid input.
        fs.SetFallible();
        CreateScalarFunctionInfo info(std::move(fs));
        for (auto &d : fn_descs) {
            info.descriptions.push_back(std::move(d));
        }

        DSSetCategories(info, {"sketch", "req"});

        DSRegister(loader, info);


      }
      {
        ScalarFunctionSet fs("datasketch_req_rank_upper_bound");
        std::vector<FunctionDescription> fn_descs;
        
            
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::TINYINT],LogicalType::DOUBLE,LogicalType::UTINYINT},LogicalType::DOUBLE
                ,    DSREQrank_upper_bound<int8_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return an approximate upper bound of the given normalized rank, with a confidence of num_std_dev standard deviations (1, 2 or 3)";
                    desc.examples.push_back("datasketch_req_rank_upper_bound(sketch, 0.5, 2)");
                    desc.parameter_names = { "sketch","rank","num_std_dev" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::TINYINT],LogicalType::DOUBLE,LogicalType::UTINYINT };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::SMALLINT],LogicalType::DOUBLE,LogicalType::UTINYINT},LogicalType::DOUBLE
                ,    DSREQrank_upper_bound<int16_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return an approximate upper bound of the given normalized rank, with a confidence of num_std_dev standard deviations (1, 2 or 3)";
                    desc.examples.push_back("datasketch_req_rank_upper_bound(sketch, 0.5, 2)");
                    desc.parameter_names = { "sketch","rank","num_std_dev" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::SMALLINT],LogicalType::DOUBLE,LogicalType::UTINYINT };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::INTEGER],LogicalType::DOUBLE,LogicalType::UTINYINT},LogicalType::DOUBLE
                ,    DSREQrank_upper_bound<int32_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return an approximate upper bound of the given normalized rank, with a confidence of num_std_dev standard deviations (1, 2 or 3)";
                    desc.examples.push_back("datasketch_req_rank_upper_bound(sketch, 0.5, 2)");
                    desc.parameter_names = { "sketch","rank","num_std_dev" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::INTEGER],LogicalType::DOUBLE,LogicalType::UTINYINT };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::BIGINT],LogicalType::DOUBLE,LogicalType::UTINYINT},LogicalType::DOUBLE
                ,    DSREQrank_upper_bound<int64_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return an approximate upper bound of the given normalized rank, with a confidence of num_std_dev standard deviations (1, 2 or 3)";
                    desc.examples.push_back("datasketch_req_rank_upper_bound(sketch, 0.5, 2)");
                    desc.parameter_names = { "sketch","rank","num_std_dev" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::BIGINT],LogicalType::DOUBLE,LogicalType::UTINYINT };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::FLOAT],LogicalType::DOUBLE,LogicalType::UTINYINT},LogicalType::DOUBLE
                ,    DSREQrank_upper_bound<float>));
                {
                    FunctionDescription desc;
                    desc.description = "Return an approximate upper bound of the given normalized rank, with a confidence of num_std_dev standard deviations (1, 2 or 3)";
                    desc.examples.push_back("datasketch_req_rank_upper_bound(sketch, 0.5, 2)");
                    desc.parameter_names = { "sketch","rank","num_std_dev" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::FLOAT],LogicalType::DOUBLE,LogicalType::UTINYINT };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::DOUBLE],LogicalType::DOUBLE,LogicalType::UTINYINT},LogicalType::DOUBLE
                ,    DSREQrank_upper_bound<double>));
                {
                    FunctionDescription desc;
                    desc.description = "Return an approximate upper bound of the given normalized rank, with a confidence of num_std_dev standard deviations (1, 2 or 3)";
                    desc.examples.push_back("datasketch_req_rank_upper_bound(sketch, 0.5, 2)");
                    desc.parameter_names = { "sketch","rank","num_std_dev" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::DOUBLE],LogicalType::DOUBLE,LogicalType::UTINYINT };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UTINYINT],LogicalType::DOUBLE,LogicalType::UTINYINT},LogicalType::DOUBLE
                ,    DSREQrank_upper_bound<uint8_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return an approximate upper bound of the given normalized rank, with a confidence of num_std_dev standard deviations (1, 2 or 3)";
                    desc.examples.push_back("datasketch_req_rank_upper_bound(sketch, 0.5, 2)");
                    desc.parameter_names = { "sketch","rank","num_std_dev" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UTINYINT],LogicalType::DOUBLE,LogicalType::UTINYINT };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::USMALLINT],LogicalType::DOUBLE,LogicalType::UTINYINT},LogicalType::DOUBLE
                ,    DSREQrank_upper_bound<uint16_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return an approximate upper bound of the given normalized rank, with a confidence of num_std_dev standard deviations (1, 2 or 3)";
                    desc.examples.push_back("datasketch_req_rank_upper_bound(sketch, 0.5, 2)");
                    desc.parameter_names = { "sketch","rank","num_std_dev" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::USMALLINT],LogicalType::DOUBLE,LogicalType::UTINYINT };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UINTEGER],LogicalType::DOUBLE,LogicalType::UTINYINT},LogicalType::DOUBLE
                ,    DSREQrank_upper_bound<uint32_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return an approximate upper bound of the given normalized rank, with a confidence of num_std_dev standard deviations (1, 2 or 3)";
                    desc.examples.push_back("datasketch_req_rank_upper_bound(sketch, 0.5, 2)");
                    desc.parameter_names = { "sketch","rank","num_std_dev" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UINTEGER],LogicalType::DOUBLE,LogicalType::UTINYINT };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UBIGINT],LogicalType::DOUBLE,LogicalType::UTINYINT},LogicalType::DOUBLE
                ,    DSREQrank_upper_bound<uint64_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return an approximate upper bound of the given normalized rank, with a confidence of num_std_dev standard deviations (1, 2 or 3)";
                    desc.examples.push_back("datasketch_req_rank_upper_bound(sketch, 0.5, 2)");
                    desc.parameter_names = { "sketch","rank","num_std_dev" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UBIGINT],LogicalType::DOUBLE,LogicalType::UTINYINT };
                    fn_descs.push_back(std::move(desc));
                }
        

        // Every scalar function deserializes a sketch, which throws on invalid input.
        fs.SetFallible();
        CreateScalarFunctionInfo info(std::move(fs));
        for (auto &d : fn_descs) {
            info.descriptions.push_back(std::move(d));
        }

        DSSetCategories(info, {"sketch", "req"});

        DSRegister(loader, info);


      }
      {
        ScalarFunctionSet fs("datasketch_req_is_hra");
        std::vector<FunctionDescription> fn_descs;
        
            
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::TINYINT]},LogicalType::BOOLEAN
                ,    DSREQis_hra<int8_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return true if the sketch favors accuracy at high ranks (HRA), false if it favors low ranks (LRA)";
                    desc.examples.push_back("datasketch_req_is_hra(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::TINYINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::SMALLINT]},LogicalType::BOOLEAN
                ,    DSREQis_hra<int16_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return true if the sketch favors accuracy at high ranks (HRA), false if it favors low ranks (LRA)";
                    desc.examples.push_back("datasketch_req_is_hra(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::SMALLINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::INTEGER]},LogicalType::BOOLEAN
                ,    DSREQis_hra<int32_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return true if the sketch favors accuracy at high ranks (HRA), false if it favors low ranks (LRA)";
                    desc.examples.push_back("datasketch_req_is_hra(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::INTEGER] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::BIGINT]},LogicalType::BOOLEAN
                ,    DSREQis_hra<int64_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return true if the sketch favors accuracy at high ranks (HRA), false if it favors low ranks (LRA)";
                    desc.examples.push_back("datasketch_req_is_hra(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::BIGINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::FLOAT]},LogicalType::BOOLEAN
                ,    DSREQis_hra<float>));
                {
                    FunctionDescription desc;
                    desc.description = "Return true if the sketch favors accuracy at high ranks (HRA), false if it favors low ranks (LRA)";
                    desc.examples.push_back("datasketch_req_is_hra(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::FLOAT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::DOUBLE]},LogicalType::BOOLEAN
                ,    DSREQis_hra<double>));
                {
                    FunctionDescription desc;
                    desc.description = "Return true if the sketch favors accuracy at high ranks (HRA), false if it favors low ranks (LRA)";
                    desc.examples.push_back("datasketch_req_is_hra(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::DOUBLE] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UTINYINT]},LogicalType::BOOLEAN
                ,    DSREQis_hra<uint8_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return true if the sketch favors accuracy at high ranks (HRA), false if it favors low ranks (LRA)";
                    desc.examples.push_back("datasketch_req_is_hra(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UTINYINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::USMALLINT]},LogicalType::BOOLEAN
                ,    DSREQis_hra<uint16_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return true if the sketch favors accuracy at high ranks (HRA), false if it favors low ranks (LRA)";
                    desc.examples.push_back("datasketch_req_is_hra(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::USMALLINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UINTEGER]},LogicalType::BOOLEAN
                ,    DSREQis_hra<uint32_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return true if the sketch favors accuracy at high ranks (HRA), false if it favors low ranks (LRA)";
                    desc.examples.push_back("datasketch_req_is_hra(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UINTEGER] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UBIGINT]},LogicalType::BOOLEAN
                ,    DSREQis_hra<uint64_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return true if the sketch favors accuracy at high ranks (HRA), false if it favors low ranks (LRA)";
                    desc.examples.push_back("datasketch_req_is_hra(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UBIGINT] };
                    fn_descs.push_back(std::move(desc));
                }
        

        // Every scalar function deserializes a sketch, which throws on invalid input.
        fs.SetFallible();
        CreateScalarFunctionInfo info(std::move(fs));
        for (auto &d : fn_descs) {
            info.descriptions.push_back(std::move(d));
        }

        DSSetCategories(info, {"sketch", "req"});

        DSRegister(loader, info);


      }
      {
        ScalarFunctionSet fs("datasketch_req_quantiles");
        std::vector<FunctionDescription> fn_descs;
        
            
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::TINYINT],LogicalType::LIST(LogicalType::DOUBLE),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::TINYINT)
                ,    DSREQquantiles<int8_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the quantiles (items) at a list of normalized ranks in [0, 1]. A NULL rank yields a NULL quantile";
                    desc.examples.push_back("datasketch_req_quantiles(sketch, [0.25, 0.5, 0.75], true)");
                    desc.parameter_names = { "sketch","ranks","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::TINYINT],LogicalType::LIST(LogicalType::DOUBLE),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::SMALLINT],LogicalType::LIST(LogicalType::DOUBLE),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::SMALLINT)
                ,    DSREQquantiles<int16_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the quantiles (items) at a list of normalized ranks in [0, 1]. A NULL rank yields a NULL quantile";
                    desc.examples.push_back("datasketch_req_quantiles(sketch, [0.25, 0.5, 0.75], true)");
                    desc.parameter_names = { "sketch","ranks","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::SMALLINT],LogicalType::LIST(LogicalType::DOUBLE),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::INTEGER],LogicalType::LIST(LogicalType::DOUBLE),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::INTEGER)
                ,    DSREQquantiles<int32_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the quantiles (items) at a list of normalized ranks in [0, 1]. A NULL rank yields a NULL quantile";
                    desc.examples.push_back("datasketch_req_quantiles(sketch, [0.25, 0.5, 0.75], true)");
                    desc.parameter_names = { "sketch","ranks","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::INTEGER],LogicalType::LIST(LogicalType::DOUBLE),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::BIGINT],LogicalType::LIST(LogicalType::DOUBLE),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::BIGINT)
                ,    DSREQquantiles<int64_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the quantiles (items) at a list of normalized ranks in [0, 1]. A NULL rank yields a NULL quantile";
                    desc.examples.push_back("datasketch_req_quantiles(sketch, [0.25, 0.5, 0.75], true)");
                    desc.parameter_names = { "sketch","ranks","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::BIGINT],LogicalType::LIST(LogicalType::DOUBLE),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::FLOAT],LogicalType::LIST(LogicalType::DOUBLE),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::FLOAT)
                ,    DSREQquantiles<float>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the quantiles (items) at a list of normalized ranks in [0, 1]. A NULL rank yields a NULL quantile";
                    desc.examples.push_back("datasketch_req_quantiles(sketch, [0.25, 0.5, 0.75], true)");
                    desc.parameter_names = { "sketch","ranks","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::FLOAT],LogicalType::LIST(LogicalType::DOUBLE),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::DOUBLE],LogicalType::LIST(LogicalType::DOUBLE),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::DOUBLE)
                ,    DSREQquantiles<double>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the quantiles (items) at a list of normalized ranks in [0, 1]. A NULL rank yields a NULL quantile";
                    desc.examples.push_back("datasketch_req_quantiles(sketch, [0.25, 0.5, 0.75], true)");
                    desc.parameter_names = { "sketch","ranks","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::DOUBLE],LogicalType::LIST(LogicalType::DOUBLE),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UTINYINT],LogicalType::LIST(LogicalType::DOUBLE),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::UTINYINT)
                ,    DSREQquantiles<uint8_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the quantiles (items) at a list of normalized ranks in [0, 1]. A NULL rank yields a NULL quantile";
                    desc.examples.push_back("datasketch_req_quantiles(sketch, [0.25, 0.5, 0.75], true)");
                    desc.parameter_names = { "sketch","ranks","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UTINYINT],LogicalType::LIST(LogicalType::DOUBLE),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::USMALLINT],LogicalType::LIST(LogicalType::DOUBLE),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::USMALLINT)
                ,    DSREQquantiles<uint16_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the quantiles (items) at a list of normalized ranks in [0, 1]. A NULL rank yields a NULL quantile";
                    desc.examples.push_back("datasketch_req_quantiles(sketch, [0.25, 0.5, 0.75], true)");
                    desc.parameter_names = { "sketch","ranks","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::USMALLINT],LogicalType::LIST(LogicalType::DOUBLE),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UINTEGER],LogicalType::LIST(LogicalType::DOUBLE),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::UINTEGER)
                ,    DSREQquantiles<uint32_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the quantiles (items) at a list of normalized ranks in [0, 1]. A NULL rank yields a NULL quantile";
                    desc.examples.push_back("datasketch_req_quantiles(sketch, [0.25, 0.5, 0.75], true)");
                    desc.parameter_names = { "sketch","ranks","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UINTEGER],LogicalType::LIST(LogicalType::DOUBLE),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UBIGINT],LogicalType::LIST(LogicalType::DOUBLE),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::UBIGINT)
                ,    DSREQquantiles<uint64_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the quantiles (items) at a list of normalized ranks in [0, 1]. A NULL rank yields a NULL quantile";
                    desc.examples.push_back("datasketch_req_quantiles(sketch, [0.25, 0.5, 0.75], true)");
                    desc.parameter_names = { "sketch","ranks","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UBIGINT],LogicalType::LIST(LogicalType::DOUBLE),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
        

        // Every scalar function deserializes a sketch, which throws on invalid input.
        fs.SetFallible();
        CreateScalarFunctionInfo info(std::move(fs));
        for (auto &d : fn_descs) {
            info.descriptions.push_back(std::move(d));
        }

        DSSetCategories(info, {"sketch", "req"});

        DSRegister(loader, info);


      }
      {
        ScalarFunctionSet fs("datasketch_req_ranks");
        std::vector<FunctionDescription> fn_descs;
        
            
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::TINYINT],LogicalType::LIST(LogicalType::TINYINT),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::DOUBLE)
                ,    DSREQranks<int8_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the normalized ranks of a list of items. A NULL item yields a NULL rank";
                    desc.examples.push_back("datasketch_req_ranks(sketch, [10, 20, 30], true)");
                    desc.parameter_names = { "sketch","items","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::TINYINT],LogicalType::LIST(LogicalType::TINYINT),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::SMALLINT],LogicalType::LIST(LogicalType::SMALLINT),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::DOUBLE)
                ,    DSREQranks<int16_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the normalized ranks of a list of items. A NULL item yields a NULL rank";
                    desc.examples.push_back("datasketch_req_ranks(sketch, [10, 20, 30], true)");
                    desc.parameter_names = { "sketch","items","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::SMALLINT],LogicalType::LIST(LogicalType::SMALLINT),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::INTEGER],LogicalType::LIST(LogicalType::INTEGER),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::DOUBLE)
                ,    DSREQranks<int32_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the normalized ranks of a list of items. A NULL item yields a NULL rank";
                    desc.examples.push_back("datasketch_req_ranks(sketch, [10, 20, 30], true)");
                    desc.parameter_names = { "sketch","items","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::INTEGER],LogicalType::LIST(LogicalType::INTEGER),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::BIGINT],LogicalType::LIST(LogicalType::BIGINT),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::DOUBLE)
                ,    DSREQranks<int64_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the normalized ranks of a list of items. A NULL item yields a NULL rank";
                    desc.examples.push_back("datasketch_req_ranks(sketch, [10, 20, 30], true)");
                    desc.parameter_names = { "sketch","items","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::BIGINT],LogicalType::LIST(LogicalType::BIGINT),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::FLOAT],LogicalType::LIST(LogicalType::FLOAT),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::DOUBLE)
                ,    DSREQranks<float>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the normalized ranks of a list of items. A NULL item yields a NULL rank";
                    desc.examples.push_back("datasketch_req_ranks(sketch, [10, 20, 30], true)");
                    desc.parameter_names = { "sketch","items","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::FLOAT],LogicalType::LIST(LogicalType::FLOAT),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::DOUBLE],LogicalType::LIST(LogicalType::DOUBLE),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::DOUBLE)
                ,    DSREQranks<double>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the normalized ranks of a list of items. A NULL item yields a NULL rank";
                    desc.examples.push_back("datasketch_req_ranks(sketch, [10, 20, 30], true)");
                    desc.parameter_names = { "sketch","items","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::DOUBLE],LogicalType::LIST(LogicalType::DOUBLE),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UTINYINT],LogicalType::LIST(LogicalType::UTINYINT),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::DOUBLE)
                ,    DSREQranks<uint8_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the normalized ranks of a list of items. A NULL item yields a NULL rank";
                    desc.examples.push_back("datasketch_req_ranks(sketch, [10, 20, 30], true)");
                    desc.parameter_names = { "sketch","items","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UTINYINT],LogicalType::LIST(LogicalType::UTINYINT),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::USMALLINT],LogicalType::LIST(LogicalType::USMALLINT),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::DOUBLE)
                ,    DSREQranks<uint16_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the normalized ranks of a list of items. A NULL item yields a NULL rank";
                    desc.examples.push_back("datasketch_req_ranks(sketch, [10, 20, 30], true)");
                    desc.parameter_names = { "sketch","items","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::USMALLINT],LogicalType::LIST(LogicalType::USMALLINT),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UINTEGER],LogicalType::LIST(LogicalType::UINTEGER),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::DOUBLE)
                ,    DSREQranks<uint32_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the normalized ranks of a list of items. A NULL item yields a NULL rank";
                    desc.examples.push_back("datasketch_req_ranks(sketch, [10, 20, 30], true)");
                    desc.parameter_names = { "sketch","items","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UINTEGER],LogicalType::LIST(LogicalType::UINTEGER),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UBIGINT],LogicalType::LIST(LogicalType::UBIGINT),LogicalType::BOOLEAN},LogicalType::LIST(LogicalType::DOUBLE)
                ,    DSREQranks<uint64_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the normalized ranks of a list of items. A NULL item yields a NULL rank";
                    desc.examples.push_back("datasketch_req_ranks(sketch, [10, 20, 30], true)");
                    desc.parameter_names = { "sketch","items","inclusive" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UBIGINT],LogicalType::LIST(LogicalType::UBIGINT),LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
        

        // Every scalar function deserializes a sketch, which throws on invalid input.
        fs.SetFallible();
        CreateScalarFunctionInfo info(std::move(fs));
        for (auto &d : fn_descs) {
            info.descriptions.push_back(std::move(d));
        }

        DSSetCategories(info, {"sketch", "req"});

        DSRegister(loader, info);


      }
      {
        ScalarFunctionSet fs("datasketch_req_sorted_view");
        std::vector<FunctionDescription> fn_descs;
        
            
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::TINYINT]},LogicalType::LIST(LogicalType::STRUCT(child_list_t<LogicalType>{{"item", LogicalType::TINYINT}, {"weight", LogicalType::UBIGINT}, {"cumulative_weight", LogicalType::UBIGINT}}))
                ,    DSREQsorted_view<int8_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the sketch's sorted view: one entry per retained item in ascending order (equal items can appear more than once), with the item's weight and the inclusive cumulative weight (the number of input items less than or equal to it; estimated once the sketch is in estimation mode). Divide cumulative_weight by datasketch_req_n(sketch) for the normalized rank. An empty sketch yields an empty list";
                    desc.examples.push_back("datasketch_req_sorted_view(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::TINYINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::SMALLINT]},LogicalType::LIST(LogicalType::STRUCT(child_list_t<LogicalType>{{"item", LogicalType::SMALLINT}, {"weight", LogicalType::UBIGINT}, {"cumulative_weight", LogicalType::UBIGINT}}))
                ,    DSREQsorted_view<int16_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the sketch's sorted view: one entry per retained item in ascending order (equal items can appear more than once), with the item's weight and the inclusive cumulative weight (the number of input items less than or equal to it; estimated once the sketch is in estimation mode). Divide cumulative_weight by datasketch_req_n(sketch) for the normalized rank. An empty sketch yields an empty list";
                    desc.examples.push_back("datasketch_req_sorted_view(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::SMALLINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::INTEGER]},LogicalType::LIST(LogicalType::STRUCT(child_list_t<LogicalType>{{"item", LogicalType::INTEGER}, {"weight", LogicalType::UBIGINT}, {"cumulative_weight", LogicalType::UBIGINT}}))
                ,    DSREQsorted_view<int32_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the sketch's sorted view: one entry per retained item in ascending order (equal items can appear more than once), with the item's weight and the inclusive cumulative weight (the number of input items less than or equal to it; estimated once the sketch is in estimation mode). Divide cumulative_weight by datasketch_req_n(sketch) for the normalized rank. An empty sketch yields an empty list";
                    desc.examples.push_back("datasketch_req_sorted_view(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::INTEGER] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::BIGINT]},LogicalType::LIST(LogicalType::STRUCT(child_list_t<LogicalType>{{"item", LogicalType::BIGINT}, {"weight", LogicalType::UBIGINT}, {"cumulative_weight", LogicalType::UBIGINT}}))
                ,    DSREQsorted_view<int64_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the sketch's sorted view: one entry per retained item in ascending order (equal items can appear more than once), with the item's weight and the inclusive cumulative weight (the number of input items less than or equal to it; estimated once the sketch is in estimation mode). Divide cumulative_weight by datasketch_req_n(sketch) for the normalized rank. An empty sketch yields an empty list";
                    desc.examples.push_back("datasketch_req_sorted_view(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::BIGINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::FLOAT]},LogicalType::LIST(LogicalType::STRUCT(child_list_t<LogicalType>{{"item", LogicalType::FLOAT}, {"weight", LogicalType::UBIGINT}, {"cumulative_weight", LogicalType::UBIGINT}}))
                ,    DSREQsorted_view<float>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the sketch's sorted view: one entry per retained item in ascending order (equal items can appear more than once), with the item's weight and the inclusive cumulative weight (the number of input items less than or equal to it; estimated once the sketch is in estimation mode). Divide cumulative_weight by datasketch_req_n(sketch) for the normalized rank. An empty sketch yields an empty list";
                    desc.examples.push_back("datasketch_req_sorted_view(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::FLOAT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::DOUBLE]},LogicalType::LIST(LogicalType::STRUCT(child_list_t<LogicalType>{{"item", LogicalType::DOUBLE}, {"weight", LogicalType::UBIGINT}, {"cumulative_weight", LogicalType::UBIGINT}}))
                ,    DSREQsorted_view<double>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the sketch's sorted view: one entry per retained item in ascending order (equal items can appear more than once), with the item's weight and the inclusive cumulative weight (the number of input items less than or equal to it; estimated once the sketch is in estimation mode). Divide cumulative_weight by datasketch_req_n(sketch) for the normalized rank. An empty sketch yields an empty list";
                    desc.examples.push_back("datasketch_req_sorted_view(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::DOUBLE] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UTINYINT]},LogicalType::LIST(LogicalType::STRUCT(child_list_t<LogicalType>{{"item", LogicalType::UTINYINT}, {"weight", LogicalType::UBIGINT}, {"cumulative_weight", LogicalType::UBIGINT}}))
                ,    DSREQsorted_view<uint8_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the sketch's sorted view: one entry per retained item in ascending order (equal items can appear more than once), with the item's weight and the inclusive cumulative weight (the number of input items less than or equal to it; estimated once the sketch is in estimation mode). Divide cumulative_weight by datasketch_req_n(sketch) for the normalized rank. An empty sketch yields an empty list";
                    desc.examples.push_back("datasketch_req_sorted_view(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UTINYINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::USMALLINT]},LogicalType::LIST(LogicalType::STRUCT(child_list_t<LogicalType>{{"item", LogicalType::USMALLINT}, {"weight", LogicalType::UBIGINT}, {"cumulative_weight", LogicalType::UBIGINT}}))
                ,    DSREQsorted_view<uint16_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the sketch's sorted view: one entry per retained item in ascending order (equal items can appear more than once), with the item's weight and the inclusive cumulative weight (the number of input items less than or equal to it; estimated once the sketch is in estimation mode). Divide cumulative_weight by datasketch_req_n(sketch) for the normalized rank. An empty sketch yields an empty list";
                    desc.examples.push_back("datasketch_req_sorted_view(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::USMALLINT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UINTEGER]},LogicalType::LIST(LogicalType::STRUCT(child_list_t<LogicalType>{{"item", LogicalType::UINTEGER}, {"weight", LogicalType::UBIGINT}, {"cumulative_weight", LogicalType::UBIGINT}}))
                ,    DSREQsorted_view<uint32_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the sketch's sorted view: one entry per retained item in ascending order (equal items can appear more than once), with the item's weight and the inclusive cumulative weight (the number of input items less than or equal to it; estimated once the sketch is in estimation mode). Divide cumulative_weight by datasketch_req_n(sketch) for the normalized rank. An empty sketch yields an empty list";
                    desc.examples.push_back("datasketch_req_sorted_view(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UINTEGER] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UBIGINT]},LogicalType::LIST(LogicalType::STRUCT(child_list_t<LogicalType>{{"item", LogicalType::UBIGINT}, {"weight", LogicalType::UBIGINT}, {"cumulative_weight", LogicalType::UBIGINT}}))
                ,    DSREQsorted_view<uint64_t>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the sketch's sorted view: one entry per retained item in ascending order (equal items can appear more than once), with the item's weight and the inclusive cumulative weight (the number of input items less than or equal to it; estimated once the sketch is in estimation mode). Divide cumulative_weight by datasketch_req_n(sketch) for the normalized rank. An empty sketch yields an empty list";
                    desc.examples.push_back("datasketch_req_sorted_view(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::UBIGINT] };
                    fn_descs.push_back(std::move(desc));
                }
        

        // Every scalar function deserializes a sketch, which throws on invalid input.
        fs.SetFallible();
        CreateScalarFunctionInfo info(std::move(fs));
        for (auto &d : fn_descs) {
            info.descriptions.push_back(std::move(d));
        }

        DSSetCategories(info, {"sketch", "req"});

        DSRegister(loader, info);


      }

      // This funciton creates the sketches.
      {
      AggregateFunctionSet sketch("datasketch_req");
      std::vector<FunctionDescription> agg_descs;
      
        
            {
                auto fun = DSREQCreateAggregate<int8_t>(LogicalType::TINYINT, sketch_map_types[LogicalTypeId::TINYINT]);
                fun.SetBindCallback(DSREQBind);
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                DSPrependArgument(fun, LogicalType::INTEGER);
                sketch.AddFunction(fun);

                FunctionDescription desc;
                desc.description = "Creates a sketch_req data sketch by aggregating values";
                desc.examples.push_back("datasketch_req(k, data)");
                desc.parameter_names = {"k", "data"};
                desc.parameter_types = { LogicalType::INTEGER, LogicalType::TINYINT };
                agg_descs.push_back(std::move(desc));
            }
            
            {
                auto fun = DSREQCreateAggregate<int8_t>(LogicalType::TINYINT, sketch_map_types[LogicalTypeId::TINYINT]);
                fun.SetBindCallback(DSREQBind);
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                DSSetArguments(fun, { LogicalType::INTEGER, LogicalType::TINYINT, LogicalType::BOOLEAN });
                sketch.AddFunction(fun);

                FunctionDescription desc;
                desc.description = "Creates a sketch_req data sketch by aggregating values. hra (a constant) selects high-rank accuracy (true, the default) or low-rank accuracy (false)";
                desc.examples.push_back("datasketch_req(k, data, false)");
                desc.parameter_names = {"k", "data", "hra"};
                desc.parameter_types = { LogicalType::INTEGER, LogicalType::TINYINT, LogicalType::BOOLEAN };
                agg_descs.push_back(std::move(desc));
            }
            
            {
                auto fun = DSREQMergeAggregate<int8_t>(LogicalType::TINYINT, sketch_map_types[LogicalTypeId::TINYINT]);
                fun.SetBindCallback(DSREQBind);
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                DSPrependArgument(fun, LogicalType::INTEGER);
                sketch.AddFunction(fun);

                FunctionDescription desc;
                desc.description = "Creates a sketch_req data sketch by merging other REQ data sketches";
                desc.examples.push_back("datasketch_req(k, sketch)");
                desc.parameter_names = {"k", "sketch"};
                desc.parameter_types = { LogicalType::INTEGER, sketch_map_types[LogicalTypeId::TINYINT] };
                agg_descs.push_back(std::move(desc));
            }
        
        
            {
                auto fun = DSREQCreateAggregate<int16_t>(LogicalType::SMALLINT, sketch_map_types[LogicalTypeId::SMALLINT]);
                fun.SetBindCallback(DSREQBind);
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                DSPrependArgument(fun, LogicalType::INTEGER);
                sketch.AddFunction(fun);

                FunctionDescription desc;
                desc.description = "Creates a sketch_req data sketch by aggregating values";
                desc.examples.push_back("datasketch_req(k, data)");
                desc.parameter_names = {"k", "data"};
                desc.parameter_types = { LogicalType::INTEGER, LogicalType::SMALLINT };
                agg_descs.push_back(std::move(desc));
            }
            
            {
                auto fun = DSREQCreateAggregate<int16_t>(LogicalType::SMALLINT, sketch_map_types[LogicalTypeId::SMALLINT]);
                fun.SetBindCallback(DSREQBind);
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                DSSetArguments(fun, { LogicalType::INTEGER, LogicalType::SMALLINT, LogicalType::BOOLEAN });
                sketch.AddFunction(fun);

                FunctionDescription desc;
                desc.description = "Creates a sketch_req data sketch by aggregating values. hra (a constant) selects high-rank accuracy (true, the default) or low-rank accuracy (false)";
                desc.examples.push_back("datasketch_req(k, data, false)");
                desc.parameter_names = {"k", "data", "hra"};
                desc.parameter_types = { LogicalType::INTEGER, LogicalType::SMALLINT, LogicalType::BOOLEAN };
                agg_descs.push_back(std::move(desc));
            }
            
            {
                auto fun = DSREQMergeAggregate<int16_t>(LogicalType::SMALLINT, sketch_map_types[LogicalTypeId::SMALLINT]);
                fun.SetBindCallback(DSREQBind);
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                DSPrependArgument(fun, LogicalType::INTEGER);
                sketch.AddFunction(fun);

                FunctionDescription desc;
                desc.description = "Creates a sketch_req data sketch by merging other REQ data sketches";
                desc.examples.push_back("datasketch_req(k, sketch)");
                desc.parameter_names = {"k", "sketch"};
                desc.parameter_types = { LogicalType::INTEGER, sketch_map_types[LogicalTypeId::SMALLINT] };
                agg_descs.push_back(std::move(desc));
            }
        
        
            {
                auto fun = DSREQCreateAggregate<int32_t>(LogicalType::INTEGER, sketch_map_types[LogicalTypeId::INTEGER]);
                fun.SetBindCallback(DSREQBind);
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                DSPrependArgument(fun, LogicalType::INTEGER);
                sketch.AddFunction(fun);

                FunctionDescription desc;
                desc.description = "Creates a sketch_req data sketch by aggregating values";
                desc.examples.push_back("datasketch_req(k, data)");
                desc.parameter_names = {"k", "data"};
                desc.parameter_types = { LogicalType::INTEGER, LogicalType::INTEGER };
                agg_descs.push_back(std::move(desc));
            }
            
            {
                auto fun = DSREQCreateAggregate<int32_t>(LogicalType::INTEGER, sketch_map_types[LogicalTypeId::INTEGER]);
                fun.SetBindCallback(DSREQBind);
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                DSSetArguments(fun, { LogicalType::INTEGER, LogicalType::INTEGER, LogicalType::BOOLEAN });
                sketch.AddFunction(fun);

                FunctionDescription desc;
                desc.description = "Creates a sketch_req data sketch by aggregating values. hra (a constant) selects high-rank accuracy (true, the default) or low-rank accuracy (false)";
                desc.examples.push_back("datasketch_req(k, data, false)");
                desc.parameter_names = {"k", "data", "hra"};
                desc.parameter_types = { LogicalType::INTEGER, LogicalType::INTEGER, LogicalType::BOOLEAN };
                agg_descs.push_back(std::move(desc));
            }
            
            {
                auto fun = DSREQMergeAggregate<int32_t>(LogicalType::INTEGER, sketch_map_types[LogicalTypeId::INTEGER]);
                fun.SetBindCallback(DSREQBind);
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                DSPrependArgument(fun, LogicalType::INTEGER);
                sketch.AddFunction(fun);

                FunctionDescription desc;
                desc.description = "Creates a sketch_req data sketch by merging other REQ data sketches";
                desc.examples.push_back("datasketch_req(k, sketch)");
                desc.parameter_names = {"k", "sketch"};
                desc.parameter_types = { LogicalType::INTEGER, sketch_map_types[LogicalTypeId::INTEGER] };
                agg_descs.push_back(std::move(desc));
            }
        
        
            {
                auto fun = DSREQCreateAggregate<int64_t>(LogicalType::BIGINT, sketch_map_types[LogicalTypeId::BIGINT]);
                fun.SetBindCallback(DSREQBind);
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                DSPrependArgument(fun, LogicalType::INTEGER);
                sketch.AddFunction(fun);

                FunctionDescription desc;
                desc.description = "Creates a sketch_req data sketch by aggregating values";
                desc.examples.push_back("datasketch_req(k, data)");
                desc.parameter_names = {"k", "data"};
                desc.parameter_types = { LogicalType::INTEGER, LogicalType::BIGINT };
                agg_descs.push_back(std::move(desc));
            }
            
            {
                auto fun = DSREQCreateAggregate<int64_t>(LogicalType::BIGINT, sketch_map_types[LogicalTypeId::BIGINT]);
                fun.SetBindCallback(DSREQBind);
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                DSSetArguments(fun, { LogicalType::INTEGER, LogicalType::BIGINT, LogicalType::BOOLEAN });
                sketch.AddFunction(fun);

                FunctionDescription desc;
                desc.description = "Creates a sketch_req data sketch by aggregating values. hra (a constant) selects high-rank accuracy (true, the default) or low-rank accuracy (false)";
                desc.examples.push_back("datasketch_req(k, data, false)");
                desc.parameter_names = {"k", "data", "hra"};
                desc.parameter_types = { LogicalType::INTEGER, LogicalType::BIGINT, LogicalType::BOOLEAN };
                agg_descs.push_back(std::move(desc));
            }
            
            {
                auto fun = DSREQMergeAggregate<int64_t>(LogicalType::BIGINT, sketch_map_types[LogicalTypeId::BIGINT]);
                fun.SetBindCallback(DSREQBind);
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                DSPrependArgument(fun, LogicalType::INTEGER);
                sketch.AddFunction(fun);

                FunctionDescription desc;
                desc.description = "Creates a sketch_req data sketch by merging other REQ data sketches";
                desc.examples.push_back("datasketch_req(k, sketch)");
                desc.parameter_names = {"k", "sketch"};
                desc.parameter_types = { LogicalType::INTEGER, sketch_map_types[LogicalTypeId::BIGINT] };
                agg_descs.push_back(std::move(desc));
            }
        
        
            {
                auto fun = DSREQCreateAggregate<float>(LogicalType::FLOAT, sketch_map_types[LogicalTypeId::FLOAT]);
                fun.SetBindCallback(DSREQBind);
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                DSPrependArgument(fun, LogicalType::INTEGER);
                sketch.AddFunction(fun);

                FunctionDescription desc;
                desc.description = "Creates a sketch_req data sketch by aggregating values";
                desc.examples.push_back("datasketch_req(k, data)");
                desc.parameter_names = {"k", "data"};
                desc.parameter_types = { LogicalType::INTEGER, LogicalType::FLOAT };
                agg_descs.push_back(std::move(desc));
            }
            
            {
                auto fun = DSREQCreateAggregate<float>(LogicalType::FLOAT, sketch_map_types[LogicalTypeId::FLOAT]);
                fun.SetBindCallback(DSREQBind);
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                DSSetArguments(fun, { LogicalType::INTEGER, LogicalType::FLOAT, LogicalType::BOOLEAN });
                sketch.AddFunction(fun);

                FunctionDescription desc;
                desc.description = "Creates a sketch_req data sketch by aggregating values. hra (a constant) selects high-rank accuracy (true, the default) or low-rank accuracy (false)";
                desc.examples.push_back("datasketch_req(k, data, false)");
                desc.parameter_names = {"k", "data", "hra"};
                desc.parameter_types = { LogicalType::INTEGER, LogicalType::FLOAT, LogicalType::BOOLEAN };
                agg_descs.push_back(std::move(desc));
            }
            
            {
                auto fun = DSREQMergeAggregate<float>(LogicalType::FLOAT, sketch_map_types[LogicalTypeId::FLOAT]);
                fun.SetBindCallback(DSREQBind);
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                DSPrependArgument(fun, LogicalType::INTEGER);
                sketch.AddFunction(fun);

                FunctionDescription desc;
                desc.description = "Creates a sketch_req data sketch by merging other REQ data sketches";
                desc.examples.push_back("datasketch_req(k, sketch)");
                desc.parameter_names = {"k", "sketch"};
                desc.parameter_types = { LogicalType::INTEGER, sketch_map_types[LogicalTypeId::FLOAT] };
                agg_descs.push_back(std::move(desc));
            }
        
        
            {
                auto fun = DSREQCreateAggregate<double>(LogicalType::DOUBLE, sketch_map_types[LogicalTypeId::DOUBLE]);
                fun.SetBindCallback(DSREQBind);
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                DSPrependArgument(fun, LogicalType::INTEGER);
                sketch.AddFunction(fun);

                FunctionDescription desc;
                desc.description = "Creates a sketch_req data sketch by aggregating values";
                desc.examples.push_back("datasketch_req(k, data)");
                desc.parameter_names = {"k", "data"};
                desc.parameter_types = { LogicalType::INTEGER, LogicalType::DOUBLE };
                agg_descs.push_back(std::move(desc));
            }
            
            {
                auto fun = DSREQCreateAggregate<double>(LogicalType::DOUBLE, sketch_map_types[LogicalTypeId::DOUBLE]);
                fun.SetBindCallback(DSREQBind);
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                DSSetArguments(fun, { LogicalType::INTEGER, LogicalType::DOUBLE, LogicalType::BOOLEAN });
                sketch.AddFunction(fun);

                FunctionDescription desc;
                desc.description = "Creates a sketch_req data sketch by aggregating values. hra (a constant) selects high-rank accuracy (true, the default) or low-rank accuracy (false)";
                desc.examples.push_back("datasketch_req(k, data, false)");
                desc.parameter_names = {"k", "data", "hra"};
                desc.parameter_types = { LogicalType::INTEGER, LogicalType::DOUBLE, LogicalType::BOOLEAN };
                agg_descs.push_back(std::move(desc));
            }
            
            {
                auto fun = DSREQMergeAggregate<double>(LogicalType::DOUBLE, sketch_map_types[LogicalTypeId::DOUBLE]);
                fun.SetBindCallback(DSREQBind);
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                DSPrependArgument(fun, LogicalType::INTEGER);
                sketch.AddFunction(fun);

                FunctionDescription desc;
                desc.description = "Creates a sketch_req data sketch by merging other REQ data sketches";
                desc.examples.push_back("datasketch_req(k, sketch)");
                desc.parameter_names = {"k", "sketch"};
                desc.parameter_types = { LogicalType::INTEGER, sketch_map_types[LogicalTypeId::DOUBLE] };
                agg_descs.push_back(std::move(desc));
            }
        
        
            {
                auto fun = DSREQCreateAggregate<uint8_t>(LogicalType::UTINYINT, sketch_map_types[LogicalTypeId::UTINYINT]);
                fun.SetBindCallback(DSREQBind);
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                DSPrependArgument(fun, LogicalType::INTEGER);
                sketch.AddFunction(fun);

                FunctionDescription desc;
                desc.description = "Creates a sketch_req data sketch by aggregating values";
                desc.examples.push_back("datasketch_req(k, data)");
                desc.parameter_names = {"k", "data"};
                desc.parameter_types = { LogicalType::INTEGER, LogicalType::UTINYINT };
                agg_descs.push_back(std::move(desc));
            }
            
            {
                auto fun = DSREQCreateAggregate<uint8_t>(LogicalType::UTINYINT, sketch_map_types[LogicalTypeId::UTINYINT]);
                fun.SetBindCallback(DSREQBind);
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                DSSetArguments(fun, { LogicalType::INTEGER, LogicalType::UTINYINT, LogicalType::BOOLEAN });
                sketch.AddFunction(fun);

                FunctionDescription desc;
                desc.description = "Creates a sketch_req data sketch by aggregating values. hra (a constant) selects high-rank accuracy (true, the default) or low-rank accuracy (false)";
                desc.examples.push_back("datasketch_req(k, data, false)");
                desc.parameter_names = {"k", "data", "hra"};
                desc.parameter_types = { LogicalType::INTEGER, LogicalType::UTINYINT, LogicalType::BOOLEAN };
                agg_descs.push_back(std::move(desc));
            }
            
            {
                auto fun = DSREQMergeAggregate<uint8_t>(LogicalType::UTINYINT, sketch_map_types[LogicalTypeId::UTINYINT]);
                fun.SetBindCallback(DSREQBind);
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                DSPrependArgument(fun, LogicalType::INTEGER);
                sketch.AddFunction(fun);

                FunctionDescription desc;
                desc.description = "Creates a sketch_req data sketch by merging other REQ data sketches";
                desc.examples.push_back("datasketch_req(k, sketch)");
                desc.parameter_names = {"k", "sketch"};
                desc.parameter_types = { LogicalType::INTEGER, sketch_map_types[LogicalTypeId::UTINYINT] };
                agg_descs.push_back(std::move(desc));
            }
        
        
            {
                auto fun = DSREQCreateAggregate<uint16_t>(LogicalType::USMALLINT, sketch_map_types[LogicalTypeId::USMALLINT]);
                fun.SetBindCallback(DSREQBind);
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                DSPrependArgument(fun, LogicalType::INTEGER);
                sketch.AddFunction(fun);

                FunctionDescription desc;
                desc.description = "Creates a sketch_req data sketch by aggregating values";
                desc.examples.push_back("datasketch_req(k, data)");
                desc.parameter_names = {"k", "data"};
                desc.parameter_types = { LogicalType::INTEGER, LogicalType::USMALLINT };
                agg_descs.push_back(std::move(desc));
            }
            
            {
                auto fun = DSREQCreateAggregate<uint16_t>(LogicalType::USMALLINT, sketch_map_types[LogicalTypeId::USMALLINT]);
                fun.SetBindCallback(DSREQBind);
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                DSSetArguments(fun, { LogicalType::INTEGER, LogicalType::USMALLINT, LogicalType::BOOLEAN });
                sketch.AddFunction(fun);

                FunctionDescription desc;
                desc.description = "Creates a sketch_req data sketch by aggregating values. hra (a constant) selects high-rank accuracy (true, the default) or low-rank accuracy (false)";
                desc.examples.push_back("datasketch_req(k, data, false)");
                desc.parameter_names = {"k", "data", "hra"};
                desc.parameter_types = { LogicalType::INTEGER, LogicalType::USMALLINT, LogicalType::BOOLEAN };
                agg_descs.push_back(std::move(desc));
            }
            
            {
                auto fun = DSREQMergeAggregate<uint16_t>(LogicalType::USMALLINT, sketch_map_types[LogicalTypeId::USMALLINT]);
                fun.SetBindCallback(DSREQBind);
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                DSPrependArgument(fun, LogicalType::INTEGER);
                sketch.AddFunction(fun);

                FunctionDescription desc;
                desc.description = "Creates a sketch_req data sketch by merging other REQ data sketches";
                desc.examples.push_back("datasketch_req(k, sketch)");
                desc.parameter_names = {"k", "sketch"};
                desc.parameter_types = { LogicalType::INTEGER, sketch_map_types[LogicalTypeId::USMALLINT] };
                agg_descs.push_back(std::move(desc));
            }
        
        
            {
                auto fun = DSREQCreateAggregate<uint32_t>(LogicalType::UINTEGER, sketch_map_types[LogicalTypeId::UINTEGER]);
                fun.SetBindCallback(DSREQBind);
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                DSPrependArgument(fun, LogicalType::INTEGER);
                sketch.AddFunction(fun);

                FunctionDescription desc;
                desc.description = "Creates a sketch_req data sketch by aggregating values";
                desc.examples.push_back("datasketch_req(k, data)");
                desc.parameter_names = {"k", "data"};
                desc.parameter_types = { LogicalType::INTEGER, LogicalType::UINTEGER };
                agg_descs.push_back(std::move(desc));
            }
            
            {
                auto fun = DSREQCreateAggregate<uint32_t>(LogicalType::UINTEGER, sketch_map_types[LogicalTypeId::UINTEGER]);
                fun.SetBindCallback(DSREQBind);
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                DSSetArguments(fun, { LogicalType::INTEGER, LogicalType::UINTEGER, LogicalType::BOOLEAN });
                sketch.AddFunction(fun);

                FunctionDescription desc;
                desc.description = "Creates a sketch_req data sketch by aggregating values. hra (a constant) selects high-rank accuracy (true, the default) or low-rank accuracy (false)";
                desc.examples.push_back("datasketch_req(k, data, false)");
                desc.parameter_names = {"k", "data", "hra"};
                desc.parameter_types = { LogicalType::INTEGER, LogicalType::UINTEGER, LogicalType::BOOLEAN };
                agg_descs.push_back(std::move(desc));
            }
            
            {
                auto fun = DSREQMergeAggregate<uint32_t>(LogicalType::UINTEGER, sketch_map_types[LogicalTypeId::UINTEGER]);
                fun.SetBindCallback(DSREQBind);
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                DSPrependArgument(fun, LogicalType::INTEGER);
                sketch.AddFunction(fun);

                FunctionDescription desc;
                desc.description = "Creates a sketch_req data sketch by merging other REQ data sketches";
                desc.examples.push_back("datasketch_req(k, sketch)");
                desc.parameter_names = {"k", "sketch"};
                desc.parameter_types = { LogicalType::INTEGER, sketch_map_types[LogicalTypeId::UINTEGER] };
                agg_descs.push_back(std::move(desc));
            }
        
        
            {
                auto fun = DSREQCreateAggregate<uint64_t>(LogicalType::UBIGINT, sketch_map_types[LogicalTypeId::UBIGINT]);
                fun.SetBindCallback(DSREQBind);
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                DSPrependArgument(fun, LogicalType::INTEGER);
                sketch.AddFunction(fun);

                FunctionDescription desc;
                desc.description = "Creates a sketch_req data sketch by aggregating values";
                desc.examples.push_back("datasketch_req(k, data)");
                desc.parameter_names = {"k", "data"};
                desc.parameter_types = { LogicalType::INTEGER, LogicalType::UBIGINT };
                agg_descs.push_back(std::move(desc));
            }
            
            {
                auto fun = DSREQCreateAggregate<uint64_t>(LogicalType::UBIGINT, sketch_map_types[LogicalTypeId::UBIGINT]);
                fun.SetBindCallback(DSREQBind);
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                DSSetArguments(fun, { LogicalType::INTEGER, LogicalType::UBIGINT, LogicalType::BOOLEAN });
                sketch.AddFunction(fun);

                FunctionDescription desc;
                desc.description = "Creates a sketch_req data sketch by aggregating values. hra (a constant) selects high-rank accuracy (true, the default) or low-rank accuracy (false)";
                desc.examples.push_back("datasketch_req(k, data, false)");
                desc.parameter_names = {"k", "data", "hra"};
                desc.parameter_types = { LogicalType::INTEGER, LogicalType::UBIGINT, LogicalType::BOOLEAN };
                agg_descs.push_back(std::move(desc));
            }
            
            {
                auto fun = DSREQMergeAggregate<uint64_t>(LogicalType::UBIGINT, sketch_map_types[LogicalTypeId::UBIGINT]);
                fun.SetBindCallback(DSREQBind);
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                DSPrependArgument(fun, LogicalType::INTEGER);
                sketch.AddFunction(fun);

                FunctionDescription desc;
                desc.description = "Creates a sketch_req data sketch by merging other REQ data sketches";
                desc.examples.push_back("datasketch_req(k, sketch)");
                desc.parameter_names = {"k", "sketch"};
                desc.parameter_types = { LogicalType::INTEGER, sketch_map_types[LogicalTypeId::UBIGINT] };
                agg_descs.push_back(std::move(desc));
            }
        
      CreateAggregateFunctionInfo sketch_info(sketch);
      for (auto &d : agg_descs) {
          sketch_info.descriptions.push_back(std::move(d));
      }

      DSSetCategories(sketch_info, {"sketch", "req"});

      DSRegister(loader, sketch_info);
      }



      


  }
    


template <class T>

static inline void DSTDigestis_empty(DataChunk &args, ExpressionState &state, Vector &result)
{
    // Get the references to the incoming vectors.
    D_ASSERT(args.ColumnCount() == 1);

    
        auto &sketch_vector = args.data[0];

    
        
        UnaryExecutor::Execute
        <string_t,bool>
        (
        sketch_vector,result,
        [&](string_t sketch_data) {

            
            auto sketch = [&]() {
                try {
                    return datasketches::tdigest<DSItem<T>>::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
                } catch (const std::exception &e) {
                    throw InvalidInputException("Failed to deserialize TDigest sketch: %s", e.what());
                }
            }();
return sketch.is_empty();
        });

}

    


template <class T>

static inline void DSTDigestk(DataChunk &args, ExpressionState &state, Vector &result)
{
    // Get the references to the incoming vectors.
    D_ASSERT(args.ColumnCount() == 1);

    
        auto &sketch_vector = args.data[0];

    
        
        UnaryExecutor::Execute
        <string_t,uint16_t>
        (
        sketch_vector,result,
        [&](string_t sketch_data) {

            
            auto sketch = [&]() {
                try {
                    return datasketches::tdigest<DSItem<T>>::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
                } catch (const std::exception &e) {
                    throw InvalidInputException("Failed to deserialize TDigest sketch: %s", e.what());
                }
            }();
return sketch.get_k();
        });

}

    


template <class T>

static inline void DSTDigestcdf(DataChunk &args, ExpressionState &state, Vector &result)
{
    // Get the references to the incoming vectors.
    D_ASSERT(args.ColumnCount() == 2);

    
        auto &sketch_vector = args.data[0];
        auto &split_points_vector = args.data[1];

    
        
                    auto &split_points_list_children = ListVector::GetChild(split_points_vector);

                    UnifiedVectorFormat split_points_children_unified;
                    split_points_list_children.ToUnifiedFormat(split_points_children_unified);

                    const T *split_points_list_children_data = UnifiedVectorFormat::GetData<T>(split_points_children_unified);
                    
        BinaryExecutor::Execute
        <string_t,list_entry_t,list_entry_t>
        (
        sketch_vector,split_points_vector,result,
        [&](string_t sketch_data,list_entry_t split_points_data) {

            
            auto sketch = [&]() {
                try {
                    return datasketches::tdigest<DSItem<T>>::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
                } catch (const std::exception &e) {
                    throw InvalidInputException("Failed to deserialize TDigest sketch: %s", e.what());
                }
            }();

                    std::vector<DSItem<T>> passing_points(split_points_data.length);
                    for (idx_t i = 0; i < split_points_data.length; i++)
                    {
                        passing_points[i] = DSItemIn(split_points_list_children_data[split_points_children_unified.sel->get_index(i + split_points_data.offset)]);
                    }
                    
auto cdf_result = sketch.get_CDF(passing_points.data(), split_points_data.length);
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
                
        });

}

    


template <class T>

static inline void DSTDigestpmf(DataChunk &args, ExpressionState &state, Vector &result)
{
    // Get the references to the incoming vectors.
    D_ASSERT(args.ColumnCount() == 2);

    
        auto &sketch_vector = args.data[0];
        auto &split_points_vector = args.data[1];

    
        
                    auto &split_points_list_children = ListVector::GetChild(split_points_vector);

                    UnifiedVectorFormat split_points_children_unified;
                    split_points_list_children.ToUnifiedFormat(split_points_children_unified);

                    const T *split_points_list_children_data = UnifiedVectorFormat::GetData<T>(split_points_children_unified);
                    
        BinaryExecutor::Execute
        <string_t,list_entry_t,list_entry_t>
        (
        sketch_vector,split_points_vector,result,
        [&](string_t sketch_data,list_entry_t split_points_data) {

            
            auto sketch = [&]() {
                try {
                    return datasketches::tdigest<DSItem<T>>::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
                } catch (const std::exception &e) {
                    throw InvalidInputException("Failed to deserialize TDigest sketch: %s", e.what());
                }
            }();

                    std::vector<DSItem<T>> passing_points(split_points_data.length);
                    for (idx_t i = 0; i < split_points_data.length; i++)
                    {
                        passing_points[i] = DSItemIn(split_points_list_children_data[split_points_children_unified.sel->get_index(i + split_points_data.offset)]);
                    }
                    
auto pmf_result = sketch.get_PMF(passing_points.data(), split_points_data.length);
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
                
        });

}

    


template <class T>

static inline void DSTDigestdescribe(DataChunk &args, ExpressionState &state, Vector &result)
{
    // Get the references to the incoming vectors.
    D_ASSERT(args.ColumnCount() == 2);

    
        auto &sketch_vector = args.data[0];
        auto &include_centroids_vector = args.data[1];

    
        
        BinaryExecutor::Execute
        <string_t,bool,string_t>
        (
        sketch_vector,include_centroids_vector,result,
        [&](string_t sketch_data,bool include_centroids_data) {

            
            auto sketch = [&]() {
                try {
                    return datasketches::tdigest<DSItem<T>>::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
                } catch (const std::exception &e) {
                    throw InvalidInputException("Failed to deserialize TDigest sketch: %s", e.what());
                }
            }();
return StringVector::AddString(result, sketch.to_string(include_centroids_data));
        });

}

    


template <class T>

static inline void DSTDigestrank(DataChunk &args, ExpressionState &state, Vector &result)
{
    // Get the references to the incoming vectors.
    D_ASSERT(args.ColumnCount() == 2);

    
        auto &sketch_vector = args.data[0];
        auto &item_vector = args.data[1];

    
        
        BinaryExecutor::Execute
        <string_t,T,double>
        (
        sketch_vector,item_vector,result,
        [&](string_t sketch_data,T item_data) {

            
            auto sketch = [&]() {
                try {
                    return datasketches::tdigest<DSItem<T>>::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
                } catch (const std::exception &e) {
                    throw InvalidInputException("Failed to deserialize TDigest sketch: %s", e.what());
                }
            }();
return sketch.get_rank(DSItemIn(item_data));
        });

}

    


template <class T>

static inline void DSTDigesttotal_weight(DataChunk &args, ExpressionState &state, Vector &result)
{
    // Get the references to the incoming vectors.
    D_ASSERT(args.ColumnCount() == 1);

    
        auto &sketch_vector = args.data[0];

    
        
        UnaryExecutor::Execute
        <string_t,uint64_t>
        (
        sketch_vector,result,
        [&](string_t sketch_data) {

            
            auto sketch = [&]() {
                try {
                    return datasketches::tdigest<DSItem<T>>::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
                } catch (const std::exception &e) {
                    throw InvalidInputException("Failed to deserialize TDigest sketch: %s", e.what());
                }
            }();
return sketch.get_total_weight();
        });

}

    


template <class T>

static inline void DSTDigestquantile(DataChunk &args, ExpressionState &state, Vector &result)
{
    // Get the references to the incoming vectors.
    D_ASSERT(args.ColumnCount() == 2);

    
        auto &sketch_vector = args.data[0];
        auto &rank_vector = args.data[1];

    
        
        BinaryExecutor::Execute
        <string_t,double,T>
        (
        sketch_vector,rank_vector,result,
        [&](string_t sketch_data,double rank_data) {

            
            auto sketch = [&]() {
                try {
                    return datasketches::tdigest<DSItem<T>>::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
                } catch (const std::exception &e) {
                    throw InvalidInputException("Failed to deserialize TDigest sketch: %s", e.what());
                }
            }();
return DSItemOut<T>(result, sketch.get_quantile(rank_data));
        });

}

    


template <class T>

static inline void DSTDigestmin_value(DataChunk &args, ExpressionState &state, Vector &result)
{
    // Get the references to the incoming vectors.
    D_ASSERT(args.ColumnCount() == 1);

    
        auto &sketch_vector = args.data[0];

    
        
        UnaryExecutor::Execute
        <string_t,T>
        (
        sketch_vector,result,
        [&](string_t sketch_data) {

            
            auto sketch = [&]() {
                try {
                    return datasketches::tdigest<DSItem<T>>::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
                } catch (const std::exception &e) {
                    throw InvalidInputException("Failed to deserialize TDigest sketch: %s", e.what());
                }
            }();
return sketch.get_min_value();
        });

}

    


template <class T>

static inline void DSTDigestmax_value(DataChunk &args, ExpressionState &state, Vector &result)
{
    // Get the references to the incoming vectors.
    D_ASSERT(args.ColumnCount() == 1);

    
        auto &sketch_vector = args.data[0];

    
        
        UnaryExecutor::Execute
        <string_t,T>
        (
        sketch_vector,result,
        [&](string_t sketch_data) {

            
            auto sketch = [&]() {
                try {
                    return datasketches::tdigest<DSItem<T>>::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
                } catch (const std::exception &e) {
                    throw InvalidInputException("Failed to deserialize TDigest sketch: %s", e.what());
                }
            }();
return sketch.get_max_value();
        });

}

    


    
    template <typename T>
    auto static DSTDigestMergeAggregate(const LogicalType &type, const LogicalType &result_type) -> AggregateFunction
    
    {
        
        auto fun = AggregateFunction::UnaryAggregate<DSTDigestState<T>, string_t, string_t, DSQuantilesMergeOperation<DSTDigestBindData>, AggregateDestructorType::LEGACY>(result_type, result_type);
        DSReadDataArgumentAt<1, DSTDigestState<T>, string_t, DSQuantilesMergeOperation<DSTDigestBindData>>(fun);
        return fun;
        
    }



    template <typename T>
    auto static DSTDigestCreateAggregate(const LogicalType &type, const LogicalType &result_type) -> AggregateFunction
    {
        
        auto fun = AggregateFunction::UnaryAggregate<DSTDigestState<T>, T, string_t, DSQuantilesCreateOperation<DSTDigestBindData>, AggregateDestructorType::LEGACY>(type, result_type);
        DSReadDataArgumentAt<1, DSTDigestState<T>, T, DSQuantilesCreateOperation<DSTDigestBindData>>(fun);
        return fun;
        
    }


  void LoadTDigestSketch(ExtensionLoader &loader) {


      
        std::unordered_map<LogicalTypeId, LogicalType> sketch_map_types;
        
            sketch_map_types.insert({ LogicalTypeId::FLOAT, CreateTDigestSketchType(loader, LogicalType(LogicalTypeId::FLOAT))});
            sketch_map_types.insert({ LogicalTypeId::DOUBLE, CreateTDigestSketchType(loader, LogicalType(LogicalTypeId::DOUBLE))});
      

      
      {
        ScalarFunctionSet fs("datasketch_tdigest_is_empty");
        std::vector<FunctionDescription> fn_descs;
        
            
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::FLOAT]},LogicalType::BOOLEAN
                ,    DSTDigestis_empty<float>));
                {
                    FunctionDescription desc;
                    desc.description = "Return a boolean indicating if the sketch is empty";
                    desc.examples.push_back("datasketch_tdigest_is_empty(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::FLOAT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::DOUBLE]},LogicalType::BOOLEAN
                ,    DSTDigestis_empty<double>));
                {
                    FunctionDescription desc;
                    desc.description = "Return a boolean indicating if the sketch is empty";
                    desc.examples.push_back("datasketch_tdigest_is_empty(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::DOUBLE] };
                    fn_descs.push_back(std::move(desc));
                }
        

        // Every scalar function deserializes a sketch, which throws on invalid input.
        fs.SetFallible();
        CreateScalarFunctionInfo info(std::move(fs));
        for (auto &d : fn_descs) {
            info.descriptions.push_back(std::move(d));
        }

        DSSetCategories(info, {"sketch", "tdigest"});

        DSRegister(loader, info);


      }
      {
        ScalarFunctionSet fs("datasketch_tdigest_k");
        std::vector<FunctionDescription> fn_descs;
        
            
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::FLOAT]},LogicalType::USMALLINT
                ,    DSTDigestk<float>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the value of K for this sketch";
                    desc.examples.push_back("datasketch_tdigest_k(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::FLOAT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::DOUBLE]},LogicalType::USMALLINT
                ,    DSTDigestk<double>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the value of K for this sketch";
                    desc.examples.push_back("datasketch_tdigest_k(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::DOUBLE] };
                    fn_descs.push_back(std::move(desc));
                }
        

        // Every scalar function deserializes a sketch, which throws on invalid input.
        fs.SetFallible();
        CreateScalarFunctionInfo info(std::move(fs));
        for (auto &d : fn_descs) {
            info.descriptions.push_back(std::move(d));
        }

        DSSetCategories(info, {"sketch", "tdigest"});

        DSRegister(loader, info);


      }
      {
        ScalarFunctionSet fs("datasketch_tdigest_cdf");
        std::vector<FunctionDescription> fn_descs;
        
            
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::FLOAT],LogicalType::LIST(LogicalType::FLOAT)},LogicalType::LIST(LogicalType::DOUBLE)
                ,    DSTDigestcdf<float>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the Cumulative Distribution Function (CDF) of the sketch for a series of points";
                    desc.examples.push_back("datasketch_tdigest_cdf(sketch, points)");
                    desc.parameter_names = { "sketch","split_points" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::FLOAT],LogicalType::LIST(LogicalType::FLOAT) };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::DOUBLE],LogicalType::LIST(LogicalType::DOUBLE)},LogicalType::LIST(LogicalType::DOUBLE)
                ,    DSTDigestcdf<double>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the Cumulative Distribution Function (CDF) of the sketch for a series of points";
                    desc.examples.push_back("datasketch_tdigest_cdf(sketch, points)");
                    desc.parameter_names = { "sketch","split_points" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::DOUBLE],LogicalType::LIST(LogicalType::DOUBLE) };
                    fn_descs.push_back(std::move(desc));
                }
        

        // Every scalar function deserializes a sketch, which throws on invalid input.
        fs.SetFallible();
        CreateScalarFunctionInfo info(std::move(fs));
        for (auto &d : fn_descs) {
            info.descriptions.push_back(std::move(d));
        }

        DSSetCategories(info, {"sketch", "tdigest"});

        DSRegister(loader, info);


      }
      {
        ScalarFunctionSet fs("datasketch_tdigest_pmf");
        std::vector<FunctionDescription> fn_descs;
        
            
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::FLOAT],LogicalType::LIST(LogicalType::FLOAT)},LogicalType::LIST(LogicalType::DOUBLE)
                ,    DSTDigestpmf<float>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the Probability Mass Function (PMF) of the sketch for a series of points";
                    desc.examples.push_back("datasketch_tdigest_pmf(sketch, points)");
                    desc.parameter_names = { "sketch","split_points" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::FLOAT],LogicalType::LIST(LogicalType::FLOAT) };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::DOUBLE],LogicalType::LIST(LogicalType::DOUBLE)},LogicalType::LIST(LogicalType::DOUBLE)
                ,    DSTDigestpmf<double>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the Probability Mass Function (PMF) of the sketch for a series of points";
                    desc.examples.push_back("datasketch_tdigest_pmf(sketch, points)");
                    desc.parameter_names = { "sketch","split_points" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::DOUBLE],LogicalType::LIST(LogicalType::DOUBLE) };
                    fn_descs.push_back(std::move(desc));
                }
        

        // Every scalar function deserializes a sketch, which throws on invalid input.
        fs.SetFallible();
        CreateScalarFunctionInfo info(std::move(fs));
        for (auto &d : fn_descs) {
            info.descriptions.push_back(std::move(d));
        }

        DSSetCategories(info, {"sketch", "tdigest"});

        DSRegister(loader, info);


      }
      {
        ScalarFunctionSet fs("datasketch_tdigest_describe");
        std::vector<FunctionDescription> fn_descs;
        
            
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::FLOAT],LogicalType::BOOLEAN},LogicalType::VARCHAR
                ,    DSTDigestdescribe<float>));
                {
                    FunctionDescription desc;
                    desc.description = "Return a description of this sketch";
                    desc.examples.push_back("datasketch_tdigest_describe(sketch, include_centroids)");
                    desc.parameter_names = { "sketch","include_centroids" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::FLOAT],LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::DOUBLE],LogicalType::BOOLEAN},LogicalType::VARCHAR
                ,    DSTDigestdescribe<double>));
                {
                    FunctionDescription desc;
                    desc.description = "Return a description of this sketch";
                    desc.examples.push_back("datasketch_tdigest_describe(sketch, include_centroids)");
                    desc.parameter_names = { "sketch","include_centroids" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::DOUBLE],LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
        

        // Every scalar function deserializes a sketch, which throws on invalid input.
        fs.SetFallible();
        CreateScalarFunctionInfo info(std::move(fs));
        for (auto &d : fn_descs) {
            info.descriptions.push_back(std::move(d));
        }

        DSSetCategories(info, {"sketch", "tdigest"});

        DSRegister(loader, info);


      }
      {
        ScalarFunctionSet fs("datasketch_tdigest_rank");
        std::vector<FunctionDescription> fn_descs;
        
            
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::FLOAT],LogicalType::FLOAT},LogicalType::DOUBLE
                ,    DSTDigestrank<float>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the rank of an item in the sketch";
                    desc.examples.push_back("datasketch_tdigest_rank(sketch, item)");
                    desc.parameter_names = { "sketch","item" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::FLOAT],LogicalType::FLOAT };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::DOUBLE],LogicalType::DOUBLE},LogicalType::DOUBLE
                ,    DSTDigestrank<double>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the rank of an item in the sketch";
                    desc.examples.push_back("datasketch_tdigest_rank(sketch, item)");
                    desc.parameter_names = { "sketch","item" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::DOUBLE],LogicalType::DOUBLE };
                    fn_descs.push_back(std::move(desc));
                }
        

        // Every scalar function deserializes a sketch, which throws on invalid input.
        fs.SetFallible();
        CreateScalarFunctionInfo info(std::move(fs));
        for (auto &d : fn_descs) {
            info.descriptions.push_back(std::move(d));
        }

        DSSetCategories(info, {"sketch", "tdigest"});

        DSRegister(loader, info);


      }
      {
        ScalarFunctionSet fs("datasketch_tdigest_total_weight");
        std::vector<FunctionDescription> fn_descs;
        
            
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::FLOAT]},LogicalType::UBIGINT
                ,    DSTDigesttotal_weight<float>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the total weight of this sketch";
                    desc.examples.push_back("datasketch_tdigest_total_weight(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::FLOAT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::DOUBLE]},LogicalType::UBIGINT
                ,    DSTDigesttotal_weight<double>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the total weight of this sketch";
                    desc.examples.push_back("datasketch_tdigest_total_weight(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::DOUBLE] };
                    fn_descs.push_back(std::move(desc));
                }
        

        // Every scalar function deserializes a sketch, which throws on invalid input.
        fs.SetFallible();
        CreateScalarFunctionInfo info(std::move(fs));
        for (auto &d : fn_descs) {
            info.descriptions.push_back(std::move(d));
        }

        DSSetCategories(info, {"sketch", "tdigest"});

        DSRegister(loader, info);


      }
      {
        ScalarFunctionSet fs("datasketch_tdigest_quantile");
        std::vector<FunctionDescription> fn_descs;
        
            
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::FLOAT],LogicalType::DOUBLE},LogicalType::FLOAT
                ,    DSTDigestquantile<float>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the quantile of a rank in the sketch";
                    desc.examples.push_back("datasketch_tdigest_quantile(sketch, rank)");
                    desc.parameter_names = { "sketch","rank" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::FLOAT],LogicalType::DOUBLE };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::DOUBLE],LogicalType::DOUBLE},LogicalType::DOUBLE
                ,    DSTDigestquantile<double>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the quantile of a rank in the sketch";
                    desc.examples.push_back("datasketch_tdigest_quantile(sketch, rank)");
                    desc.parameter_names = { "sketch","rank" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::DOUBLE],LogicalType::DOUBLE };
                    fn_descs.push_back(std::move(desc));
                }
        

        // Every scalar function deserializes a sketch, which throws on invalid input.
        fs.SetFallible();
        CreateScalarFunctionInfo info(std::move(fs));
        for (auto &d : fn_descs) {
            info.descriptions.push_back(std::move(d));
        }

        DSSetCategories(info, {"sketch", "tdigest"});

        DSRegister(loader, info);


      }
      {
        ScalarFunctionSet fs("datasketch_tdigest_min_value");
        std::vector<FunctionDescription> fn_descs;
        
            
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::FLOAT]},LogicalType::FLOAT
                ,    DSTDigestmin_value<float>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the minimum value seen by the sketch";
                    desc.examples.push_back("datasketch_tdigest_min_value(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::FLOAT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::DOUBLE]},LogicalType::DOUBLE
                ,    DSTDigestmin_value<double>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the minimum value seen by the sketch";
                    desc.examples.push_back("datasketch_tdigest_min_value(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::DOUBLE] };
                    fn_descs.push_back(std::move(desc));
                }
        

        // Every scalar function deserializes a sketch, which throws on invalid input.
        fs.SetFallible();
        CreateScalarFunctionInfo info(std::move(fs));
        for (auto &d : fn_descs) {
            info.descriptions.push_back(std::move(d));
        }

        DSSetCategories(info, {"sketch", "tdigest"});

        DSRegister(loader, info);


      }
      {
        ScalarFunctionSet fs("datasketch_tdigest_max_value");
        std::vector<FunctionDescription> fn_descs;
        
            
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::FLOAT]},LogicalType::FLOAT
                ,    DSTDigestmax_value<float>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the maximum value seen by the sketch";
                    desc.examples.push_back("datasketch_tdigest_max_value(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::FLOAT] };
                    fn_descs.push_back(std::move(desc));
                }
                fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::DOUBLE]},LogicalType::DOUBLE
                ,    DSTDigestmax_value<double>));
                {
                    FunctionDescription desc;
                    desc.description = "Return the maximum value seen by the sketch";
                    desc.examples.push_back("datasketch_tdigest_max_value(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_map_types[LogicalTypeId::DOUBLE] };
                    fn_descs.push_back(std::move(desc));
                }
        

        // Every scalar function deserializes a sketch, which throws on invalid input.
        fs.SetFallible();
        CreateScalarFunctionInfo info(std::move(fs));
        for (auto &d : fn_descs) {
            info.descriptions.push_back(std::move(d));
        }

        DSSetCategories(info, {"sketch", "tdigest"});

        DSRegister(loader, info);


      }

      // This funciton creates the sketches.
      {
      AggregateFunctionSet sketch("datasketch_tdigest");
      std::vector<FunctionDescription> agg_descs;
      
        
            {
                auto fun = DSTDigestCreateAggregate<float>(LogicalType::FLOAT, sketch_map_types[LogicalTypeId::FLOAT]);
                fun.SetBindCallback(DSTDigestBind);
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                DSPrependArgument(fun, LogicalType::INTEGER);
                sketch.AddFunction(fun);

                FunctionDescription desc;
                desc.description = "Creates a sketch_tdigest data sketch by aggregating values";
                desc.examples.push_back("datasketch_tdigest(k, data)");
                desc.parameter_names = {"k", "data"};
                desc.parameter_types = { LogicalType::INTEGER, LogicalType::FLOAT };
                agg_descs.push_back(std::move(desc));
            }
            
            {
                auto fun = DSTDigestMergeAggregate<float>(LogicalType::FLOAT, sketch_map_types[LogicalTypeId::FLOAT]);
                fun.SetBindCallback(DSTDigestBind);
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                DSPrependArgument(fun, LogicalType::INTEGER);
                sketch.AddFunction(fun);

                FunctionDescription desc;
                desc.description = "Creates a sketch_tdigest data sketch by merging other TDigest data sketches";
                desc.examples.push_back("datasketch_tdigest(k, sketch)");
                desc.parameter_names = {"k", "sketch"};
                desc.parameter_types = { LogicalType::INTEGER, sketch_map_types[LogicalTypeId::FLOAT] };
                agg_descs.push_back(std::move(desc));
            }
        
        
            {
                auto fun = DSTDigestCreateAggregate<double>(LogicalType::DOUBLE, sketch_map_types[LogicalTypeId::DOUBLE]);
                fun.SetBindCallback(DSTDigestBind);
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                DSPrependArgument(fun, LogicalType::INTEGER);
                sketch.AddFunction(fun);

                FunctionDescription desc;
                desc.description = "Creates a sketch_tdigest data sketch by aggregating values";
                desc.examples.push_back("datasketch_tdigest(k, data)");
                desc.parameter_names = {"k", "data"};
                desc.parameter_types = { LogicalType::INTEGER, LogicalType::DOUBLE };
                agg_descs.push_back(std::move(desc));
            }
            
            {
                auto fun = DSTDigestMergeAggregate<double>(LogicalType::DOUBLE, sketch_map_types[LogicalTypeId::DOUBLE]);
                fun.SetBindCallback(DSTDigestBind);
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                DSPrependArgument(fun, LogicalType::INTEGER);
                sketch.AddFunction(fun);

                FunctionDescription desc;
                desc.description = "Creates a sketch_tdigest data sketch by merging other TDigest data sketches";
                desc.examples.push_back("datasketch_tdigest(k, sketch)");
                desc.parameter_names = {"k", "sketch"};
                desc.parameter_types = { LogicalType::INTEGER, sketch_map_types[LogicalTypeId::DOUBLE] };
                agg_descs.push_back(std::move(desc));
            }
        
      CreateAggregateFunctionInfo sketch_info(sketch);
      for (auto &d : agg_descs) {
          sketch_info.descriptions.push_back(std::move(d));
      }

      DSSetCategories(sketch_info, {"sketch", "tdigest"});

      DSRegister(loader, sketch_info);
      }



      


  }
    


static inline void DSHLLis_empty(DataChunk &args, ExpressionState &state, Vector &result)
{
    // Get the references to the incoming vectors.
    D_ASSERT(args.ColumnCount() == 1);

    
        auto &sketch_vector = args.data[0];

    
        
        UnaryExecutor::Execute
        <string_t,bool>
        (
        sketch_vector,result,
        [&](string_t sketch_data) {

            
            auto sketch = [&]() {
                try {
                    return datasketches::hll_sketch::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
                } catch (const std::exception &e) {
                    throw InvalidInputException("Failed to deserialize HLL sketch: %s", e.what());
                }
            }();
return sketch.is_empty();
        });

}

    


static inline void DSHLLdescribe(DataChunk &args, ExpressionState &state, Vector &result)
{
    // Get the references to the incoming vectors.
    D_ASSERT(args.ColumnCount() == 3);

    
        auto &sketch_vector = args.data[0];
        auto &summary_vector = args.data[1];
        auto &detail_vector = args.data[2];

    
        
        TernaryExecutor::Execute
        <string_t,bool,bool,string_t>
        (
        sketch_vector,summary_vector,detail_vector,result,
        [&](string_t sketch_data,bool summary_data,bool detail_data) {

            
            auto sketch = [&]() {
                try {
                    return datasketches::hll_sketch::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
                } catch (const std::exception &e) {
                    throw InvalidInputException("Failed to deserialize HLL sketch: %s", e.what());
                }
            }();
return StringVector::AddString(result, sketch.to_string(summary_data, detail_data, false, false));
        });

}

    


static inline void DSHLLlg_config_k(DataChunk &args, ExpressionState &state, Vector &result)
{
    // Get the references to the incoming vectors.
    D_ASSERT(args.ColumnCount() == 1);

    
        auto &sketch_vector = args.data[0];

    
        
        UnaryExecutor::Execute
        <string_t,uint8_t>
        (
        sketch_vector,result,
        [&](string_t sketch_data) {

            
            auto sketch = [&]() {
                try {
                    return datasketches::hll_sketch::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
                } catch (const std::exception &e) {
                    throw InvalidInputException("Failed to deserialize HLL sketch: %s", e.what());
                }
            }();
return sketch.get_lg_config_k();
        });

}

    


static inline void DSHLLis_compact(DataChunk &args, ExpressionState &state, Vector &result)
{
    // Get the references to the incoming vectors.
    D_ASSERT(args.ColumnCount() == 1);

    
        auto &sketch_vector = args.data[0];

    
        
        UnaryExecutor::Execute
        <string_t,bool>
        (
        sketch_vector,result,
        [&](string_t sketch_data) {

            
            auto sketch = [&]() {
                try {
                    return datasketches::hll_sketch::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
                } catch (const std::exception &e) {
                    throw InvalidInputException("Failed to deserialize HLL sketch: %s", e.what());
                }
            }();
return sketch.is_compact();
        });

}

    


static inline void DSHLLcomposite_estimate(DataChunk &args, ExpressionState &state, Vector &result)
{
    // Get the references to the incoming vectors.
    D_ASSERT(args.ColumnCount() == 1);

    
        auto &sketch_vector = args.data[0];

    
        
        UnaryExecutor::Execute
        <string_t,double>
        (
        sketch_vector,result,
        [&](string_t sketch_data) {

            
            auto sketch = [&]() {
                try {
                    return datasketches::hll_sketch::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
                } catch (const std::exception &e) {
                    throw InvalidInputException("Failed to deserialize HLL sketch: %s", e.what());
                }
            }();
return sketch.get_composite_estimate();
        });

}

    


static inline void DSHLLtarget_type(DataChunk &args, ExpressionState &state, Vector &result)
{
    // Get the references to the incoming vectors.
    D_ASSERT(args.ColumnCount() == 1);

    
        auto &sketch_vector = args.data[0];

    
        
        UnaryExecutor::Execute
        <string_t,string_t>
        (
        sketch_vector,result,
        [&](string_t sketch_data) {

            
            auto sketch = [&]() {
                try {
                    return datasketches::hll_sketch::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
                } catch (const std::exception &e) {
                    throw InvalidInputException("Failed to deserialize HLL sketch: %s", e.what());
                }
            }();
return StringVector::AddString(result, DSHLLTargetTypeName(sketch.get_target_type()));
        });

}

    


static inline void DSHLLmerge(DataChunk &args, ExpressionState &state, Vector &result)
{
    // Get the references to the incoming vectors.
    D_ASSERT(args.ColumnCount() == 2);

    
        auto &sketch_a_vector = args.data[0];
        auto &sketch_b_vector = args.data[1];

    
        
        BinaryExecutor::Execute
        <string_t,string_t,string_t>
        (
        sketch_a_vector,sketch_b_vector,result,
        [&](string_t sketch_a_data,string_t sketch_b_data) {

            
            auto sketch_a = [&]() {
                try {
                    return datasketches::hll_sketch::deserialize(sketch_a_data.GetDataUnsafe(), sketch_a_data.GetSize());
                } catch (const std::exception &e) {
                    throw InvalidInputException("Failed to deserialize HLL sketch: %s", e.what());
                }
            }();

            auto sketch_b = [&]() {
                try {
                    return datasketches::hll_sketch::deserialize(sketch_b_data.GetDataUnsafe(), sketch_b_data.GetSize());
                } catch (const std::exception &e) {
                    throw InvalidInputException("Failed to deserialize HLL sketch: %s", e.what());
                }
            }();

                datasketches::hll_union u(std::max(sketch_a.get_lg_config_k(), sketch_b.get_lg_config_k()));
                u.update(sketch_a);
                u.update(sketch_b);
                auto serialized_data = u.get_result(sketch_a.get_target_type()).serialize_updatable();
                return StringVector::AddStringOrBlob(result, std::string(serialized_data.begin(), serialized_data.end()));
                
        });

}

    


static inline void DSHLLestimate(DataChunk &args, ExpressionState &state, Vector &result)
{
    // Get the references to the incoming vectors.
    D_ASSERT(args.ColumnCount() == 1);

    
        auto &sketch_vector = args.data[0];

    
        
        UnaryExecutor::Execute
        <string_t,double>
        (
        sketch_vector,result,
        [&](string_t sketch_data) {

            
            auto sketch = [&]() {
                try {
                    return datasketches::hll_sketch::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
                } catch (const std::exception &e) {
                    throw InvalidInputException("Failed to deserialize HLL sketch: %s", e.what());
                }
            }();
return sketch.get_estimate();
        });

}

    


static inline void DSHLLlower_bound(DataChunk &args, ExpressionState &state, Vector &result)
{
    // Get the references to the incoming vectors.
    D_ASSERT(args.ColumnCount() == 2);

    
        auto &sketch_vector = args.data[0];
        auto &std_dev_vector = args.data[1];

    
        
        BinaryExecutor::Execute
        <string_t,uint8_t,double>
        (
        sketch_vector,std_dev_vector,result,
        [&](string_t sketch_data,uint8_t std_dev_data) {

            
            auto sketch = [&]() {
                try {
                    return datasketches::hll_sketch::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
                } catch (const std::exception &e) {
                    throw InvalidInputException("Failed to deserialize HLL sketch: %s", e.what());
                }
            }();
return sketch.get_lower_bound(std_dev_data);
        });

}

    


static inline void DSHLLupper_bound(DataChunk &args, ExpressionState &state, Vector &result)
{
    // Get the references to the incoming vectors.
    D_ASSERT(args.ColumnCount() == 2);

    
        auto &sketch_vector = args.data[0];
        auto &std_dev_vector = args.data[1];

    
        
        BinaryExecutor::Execute
        <string_t,uint8_t,double>
        (
        sketch_vector,std_dev_vector,result,
        [&](string_t sketch_data,uint8_t std_dev_data) {

            
            auto sketch = [&]() {
                try {
                    return datasketches::hll_sketch::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
                } catch (const std::exception &e) {
                    throw InvalidInputException("Failed to deserialize HLL sketch: %s", e.what());
                }
            }();
return sketch.get_upper_bound(std_dev_data);
        });

}

    


    
    auto static DSHLLMergeAggregate(const LogicalType &result_type) -> AggregateFunction
    
    {
        
        auto fun = AggregateFunction::UnaryAggregate<DSHLLState, string_t, string_t, DSHLLMergeOperation<DSHLLBindData>, AggregateDestructorType::LEGACY>(result_type, result_type);
        DSReadDataArgumentAt<1, DSHLLState, string_t, DSHLLMergeOperation<DSHLLBindData>>(fun);
        return fun;
        
    }



    template <typename T>
    auto static DSHLLCreateAggregate(const LogicalType &type, const LogicalType &result_type) -> AggregateFunction
    {
        
        auto fun = AggregateFunction::UnaryAggregate<DSHLLState, T, string_t, DSHLLCreateOperation<DSHLLBindData>, AggregateDestructorType::LEGACY>(type, result_type);
        DSReadDataArgumentAt<1, DSHLLState, T, DSHLLCreateOperation<DSHLLBindData>>(fun);
        return fun;
        
    }


  void LoadHLLSketch(ExtensionLoader &loader) {


      
        auto sketch_type = CreateHLLCountingSketchType(loader);
      

      
      {
        ScalarFunctionSet fs("datasketch_hll_is_empty");
        std::vector<FunctionDescription> fn_descs;
        
                fs.AddFunction(ScalarFunction(
                {sketch_type},LogicalType::BOOLEAN
                ,    DSHLLis_empty));
                {
                    FunctionDescription desc;
                    desc.description = "Return a boolean indicating if the sketch is empty";
                    desc.examples.push_back("datasketch_hll_is_empty(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_type };
                    fn_descs.push_back(std::move(desc));
                }
        

        // Every scalar function deserializes a sketch, which throws on invalid input.
        fs.SetFallible();
        CreateScalarFunctionInfo info(std::move(fs));
        for (auto &d : fn_descs) {
            info.descriptions.push_back(std::move(d));
        }

        DSSetCategories(info, {"sketch", "hll"});

        DSRegister(loader, info);


      }
      {
        ScalarFunctionSet fs("datasketch_hll_describe");
        std::vector<FunctionDescription> fn_descs;
        
                fs.AddFunction(ScalarFunction(
                {sketch_type,LogicalType::BOOLEAN,LogicalType::BOOLEAN},LogicalType::VARCHAR
                ,    DSHLLdescribe));
                {
                    FunctionDescription desc;
                    desc.description = "Return a string representation of the sketch";
                    desc.examples.push_back("datasketch_hll_describe(sketch, include_summary, include_detail)");
                    desc.parameter_names = { "sketch","summary","detail" };
                    desc.parameter_types = { sketch_type,LogicalType::BOOLEAN,LogicalType::BOOLEAN };
                    fn_descs.push_back(std::move(desc));
                }
        

        // Every scalar function deserializes a sketch, which throws on invalid input.
        fs.SetFallible();
        CreateScalarFunctionInfo info(std::move(fs));
        for (auto &d : fn_descs) {
            info.descriptions.push_back(std::move(d));
        }

        DSSetCategories(info, {"sketch", "hll"});

        DSRegister(loader, info);


      }
      {
        ScalarFunctionSet fs("datasketch_hll_lg_config_k");
        std::vector<FunctionDescription> fn_descs;
        
                fs.AddFunction(ScalarFunction(
                {sketch_type},LogicalType::UTINYINT
                ,    DSHLLlg_config_k));
                {
                    FunctionDescription desc;
                    desc.description = "Return the value of log base 2 K for this sketch";
                    desc.examples.push_back("datasketch_hll_lg_config_k(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_type };
                    fn_descs.push_back(std::move(desc));
                }
        

        // Every scalar function deserializes a sketch, which throws on invalid input.
        fs.SetFallible();
        CreateScalarFunctionInfo info(std::move(fs));
        for (auto &d : fn_descs) {
            info.descriptions.push_back(std::move(d));
        }

        DSSetCategories(info, {"sketch", "hll"});

        DSRegister(loader, info);


      }
      {
        ScalarFunctionSet fs("datasketch_hll_is_compact");
        std::vector<FunctionDescription> fn_descs;
        
                fs.AddFunction(ScalarFunction(
                {sketch_type},LogicalType::BOOLEAN
                ,    DSHLLis_compact));
                {
                    FunctionDescription desc;
                    desc.description = "Return whether the sketch is in compact form";
                    desc.examples.push_back("datasketch_hll_is_compact(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_type };
                    fn_descs.push_back(std::move(desc));
                }
        

        // Every scalar function deserializes a sketch, which throws on invalid input.
        fs.SetFallible();
        CreateScalarFunctionInfo info(std::move(fs));
        for (auto &d : fn_descs) {
            info.descriptions.push_back(std::move(d));
        }

        DSSetCategories(info, {"sketch", "hll"});

        DSRegister(loader, info);


      }
      {
        ScalarFunctionSet fs("datasketch_hll_composite_estimate");
        std::vector<FunctionDescription> fn_descs;
        
                fs.AddFunction(ScalarFunction(
                {sketch_type},LogicalType::DOUBLE
                ,    DSHLLcomposite_estimate));
                {
                    FunctionDescription desc;
                    desc.description = "Return the composite estimate of the number of distinct items, computed from the registers alone (the raw HLL estimate blended with linear counting). datasketch_hll_estimate prefers the HIP estimator, which is only available for sketches that were not built by a union";
                    desc.examples.push_back("datasketch_hll_composite_estimate(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_type };
                    fn_descs.push_back(std::move(desc));
                }
        

        // Every scalar function deserializes a sketch, which throws on invalid input.
        fs.SetFallible();
        CreateScalarFunctionInfo info(std::move(fs));
        for (auto &d : fn_descs) {
            info.descriptions.push_back(std::move(d));
        }

        DSSetCategories(info, {"sketch", "hll"});

        DSRegister(loader, info);


      }
      {
        ScalarFunctionSet fs("datasketch_hll_target_type");
        std::vector<FunctionDescription> fn_descs;
        
                fs.AddFunction(ScalarFunction(
                {sketch_type},LogicalType::VARCHAR
                ,    DSHLLtarget_type));
                {
                    FunctionDescription desc;
                    desc.description = "Return the sketch's register encoding: 'HLL_4', 'HLL_6' or 'HLL_8'";
                    desc.examples.push_back("datasketch_hll_target_type(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_type };
                    fn_descs.push_back(std::move(desc));
                }
        

        // Every scalar function deserializes a sketch, which throws on invalid input.
        fs.SetFallible();
        CreateScalarFunctionInfo info(std::move(fs));
        for (auto &d : fn_descs) {
            info.descriptions.push_back(std::move(d));
        }

        DSSetCategories(info, {"sketch", "hll"});

        DSRegister(loader, info);


      }
      {
        ScalarFunctionSet fs("datasketch_hll_merge");
        std::vector<FunctionDescription> fn_descs;
        
                fs.AddFunction(ScalarFunction(
                {sketch_type,sketch_type},sketch_type
                ,    DSHLLmerge));
                {
                    FunctionDescription desc;
                    desc.description = "Return the union of two HLL sketches as a new sketch with the first sketch's target type. The union runs at the larger lg_config_k of the two; if either input is in HLL (dense) mode the result is reduced to the smaller lg_config_k";
                    desc.examples.push_back("datasketch_hll_merge(sketch_a, sketch_b)");
                    desc.parameter_names = { "sketch_a","sketch_b" };
                    desc.parameter_types = { sketch_type,sketch_type };
                    fn_descs.push_back(std::move(desc));
                }
        

        // Every scalar function deserializes a sketch, which throws on invalid input.
        fs.SetFallible();
        CreateScalarFunctionInfo info(std::move(fs));
        for (auto &d : fn_descs) {
            info.descriptions.push_back(std::move(d));
        }

        DSSetCategories(info, {"sketch", "hll"});

        DSRegister(loader, info);


      }
      {
        ScalarFunctionSet fs("datasketch_hll_estimate");
        std::vector<FunctionDescription> fn_descs;
        
                fs.AddFunction(ScalarFunction(
                {sketch_type},LogicalType::DOUBLE
                ,    DSHLLestimate));
                {
                    FunctionDescription desc;
                    desc.description = "Return the estimate of the number of distinct items seen by the sketch";
                    desc.examples.push_back("datasketch_hll_estimate(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_type };
                    fn_descs.push_back(std::move(desc));
                }
        

        // Every scalar function deserializes a sketch, which throws on invalid input.
        fs.SetFallible();
        CreateScalarFunctionInfo info(std::move(fs));
        for (auto &d : fn_descs) {
            info.descriptions.push_back(std::move(d));
        }

        DSSetCategories(info, {"sketch", "hll"});

        DSRegister(loader, info);


      }
      {
        ScalarFunctionSet fs("datasketch_hll_lower_bound");
        std::vector<FunctionDescription> fn_descs;
        
                fs.AddFunction(ScalarFunction(
                {sketch_type,LogicalType::UTINYINT},LogicalType::DOUBLE
                ,    DSHLLlower_bound));
                {
                    FunctionDescription desc;
                    desc.description = "Return the lower bound of the number of distinct items seen by the sketch";
                    desc.examples.push_back("datasketch_hll_lower_bound(sketch, std_dev)");
                    desc.parameter_names = { "sketch","std_dev" };
                    desc.parameter_types = { sketch_type,LogicalType::UTINYINT };
                    fn_descs.push_back(std::move(desc));
                }
        

        // Every scalar function deserializes a sketch, which throws on invalid input.
        fs.SetFallible();
        CreateScalarFunctionInfo info(std::move(fs));
        for (auto &d : fn_descs) {
            info.descriptions.push_back(std::move(d));
        }

        DSSetCategories(info, {"sketch", "hll"});

        DSRegister(loader, info);


      }
      {
        ScalarFunctionSet fs("datasketch_hll_upper_bound");
        std::vector<FunctionDescription> fn_descs;
        
                fs.AddFunction(ScalarFunction(
                {sketch_type,LogicalType::UTINYINT},LogicalType::DOUBLE
                ,    DSHLLupper_bound));
                {
                    FunctionDescription desc;
                    desc.description = "Return the upper bound of the number of distinct items seen by the sketch";
                    desc.examples.push_back("datasketch_hll_upper_bound(sketch, std_dev)");
                    desc.parameter_names = { "sketch","std_dev" };
                    desc.parameter_types = { sketch_type,LogicalType::UTINYINT };
                    fn_descs.push_back(std::move(desc));
                }
        

        // Every scalar function deserializes a sketch, which throws on invalid input.
        fs.SetFallible();
        CreateScalarFunctionInfo info(std::move(fs));
        for (auto &d : fn_descs) {
            info.descriptions.push_back(std::move(d));
        }

        DSSetCategories(info, {"sketch", "hll"});

        DSRegister(loader, info);


      }

      // This funciton creates the sketches.
      {
      AggregateFunctionSet sketch("datasketch_hll");
      std::vector<FunctionDescription> agg_descs;
      
        
            {
                auto fun = DSHLLCreateAggregate<int8_t>(LogicalType::TINYINT, sketch_type);
                fun.SetBindCallback(DSHLLBind);
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                DSPrependArgument(fun, LogicalType::INTEGER);
                sketch.AddFunction(fun);

                FunctionDescription desc;
                desc.description = "Creates a sketch_hll data sketch by aggregating values";
                desc.examples.push_back("datasketch_hll(k, data)");
                desc.parameter_names = {"k", "data"};
                desc.parameter_types = { LogicalType::INTEGER, LogicalType::TINYINT };
                agg_descs.push_back(std::move(desc));
            }
            
            {
                auto fun = DSHLLCreateAggregate<int8_t>(LogicalType::TINYINT, sketch_type);
                fun.SetBindCallback(DSHLLBind);
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                DSSetArguments(fun, { LogicalType::INTEGER, LogicalType::TINYINT, LogicalType::VARCHAR });
                sketch.AddFunction(fun);

                FunctionDescription desc;
                desc.description = "Creates a sketch_hll data sketch by aggregating values. target_type (a constant) selects the register encoding: 'HLL_4' (the default, smallest), 'HLL_6' or 'HLL_8' (largest, fastest)";
                desc.examples.push_back("datasketch_hll(k, data, 'HLL_8')");
                desc.parameter_names = {"k", "data", "target_type"};
                desc.parameter_types = { LogicalType::INTEGER, LogicalType::TINYINT, LogicalType::VARCHAR };
                agg_descs.push_back(std::move(desc));
            }
            
        
        
            {
                auto fun = DSHLLCreateAggregate<int16_t>(LogicalType::SMALLINT, sketch_type);
                fun.SetBindCallback(DSHLLBind);
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                DSPrependArgument(fun, LogicalType::INTEGER);
                sketch.AddFunction(fun);

                FunctionDescription desc;
                desc.description = "Creates a sketch_hll data sketch by aggregating values";
                desc.examples.push_back("datasketch_hll(k, data)");
                desc.parameter_names = {"k", "data"};
                desc.parameter_types = { LogicalType::INTEGER, LogicalType::SMALLINT };
                agg_descs.push_back(std::move(desc));
            }
            
            {
                auto fun = DSHLLCreateAggregate<int16_t>(LogicalType::SMALLINT, sketch_type);
                fun.SetBindCallback(DSHLLBind);
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                DSSetArguments(fun, { LogicalType::INTEGER, LogicalType::SMALLINT, LogicalType::VARCHAR });
                sketch.AddFunction(fun);

                FunctionDescription desc;
                desc.description = "Creates a sketch_hll data sketch by aggregating values. target_type (a constant) selects the register encoding: 'HLL_4' (the default, smallest), 'HLL_6' or 'HLL_8' (largest, fastest)";
                desc.examples.push_back("datasketch_hll(k, data, 'HLL_8')");
                desc.parameter_names = {"k", "data", "target_type"};
                desc.parameter_types = { LogicalType::INTEGER, LogicalType::SMALLINT, LogicalType::VARCHAR };
                agg_descs.push_back(std::move(desc));
            }
            
        
        
            {
                auto fun = DSHLLCreateAggregate<int32_t>(LogicalType::INTEGER, sketch_type);
                fun.SetBindCallback(DSHLLBind);
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                DSPrependArgument(fun, LogicalType::INTEGER);
                sketch.AddFunction(fun);

                FunctionDescription desc;
                desc.description = "Creates a sketch_hll data sketch by aggregating values";
                desc.examples.push_back("datasketch_hll(k, data)");
                desc.parameter_names = {"k", "data"};
                desc.parameter_types = { LogicalType::INTEGER, LogicalType::INTEGER };
                agg_descs.push_back(std::move(desc));
            }
            
            {
                auto fun = DSHLLCreateAggregate<int32_t>(LogicalType::INTEGER, sketch_type);
                fun.SetBindCallback(DSHLLBind);
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                DSSetArguments(fun, { LogicalType::INTEGER, LogicalType::INTEGER, LogicalType::VARCHAR });
                sketch.AddFunction(fun);

                FunctionDescription desc;
                desc.description = "Creates a sketch_hll data sketch by aggregating values. target_type (a constant) selects the register encoding: 'HLL_4' (the default, smallest), 'HLL_6' or 'HLL_8' (largest, fastest)";
                desc.examples.push_back("datasketch_hll(k, data, 'HLL_8')");
                desc.parameter_names = {"k", "data", "target_type"};
                desc.parameter_types = { LogicalType::INTEGER, LogicalType::INTEGER, LogicalType::VARCHAR };
                agg_descs.push_back(std::move(desc));
            }
            
        
        
            {
                auto fun = DSHLLCreateAggregate<int64_t>(LogicalType::BIGINT, sketch_type);
                fun.SetBindCallback(DSHLLBind);
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                DSPrependArgument(fun, LogicalType::INTEGER);
                sketch.AddFunction(fun);

                FunctionDescription desc;
                desc.description = "Creates a sketch_hll data sketch by aggregating values";
                desc.examples.push_back("datasketch_hll(k, data)");
                desc.parameter_names = {"k", "data"};
                desc.parameter_types = { LogicalType::INTEGER, LogicalType::BIGINT };
                agg_descs.push_back(std::move(desc));
            }
            
            {
                auto fun = DSHLLCreateAggregate<int64_t>(LogicalType::BIGINT, sketch_type);
                fun.SetBindCallback(DSHLLBind);
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                DSSetArguments(fun, { LogicalType::INTEGER, LogicalType::BIGINT, LogicalType::VARCHAR });
                sketch.AddFunction(fun);

                FunctionDescription desc;
                desc.description = "Creates a sketch_hll data sketch by aggregating values. target_type (a constant) selects the register encoding: 'HLL_4' (the default, smallest), 'HLL_6' or 'HLL_8' (largest, fastest)";
                desc.examples.push_back("datasketch_hll(k, data, 'HLL_8')");
                desc.parameter_names = {"k", "data", "target_type"};
                desc.parameter_types = { LogicalType::INTEGER, LogicalType::BIGINT, LogicalType::VARCHAR };
                agg_descs.push_back(std::move(desc));
            }
            
        
        
            {
                auto fun = DSHLLCreateAggregate<float>(LogicalType::FLOAT, sketch_type);
                fun.SetBindCallback(DSHLLBind);
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                DSPrependArgument(fun, LogicalType::INTEGER);
                sketch.AddFunction(fun);

                FunctionDescription desc;
                desc.description = "Creates a sketch_hll data sketch by aggregating values";
                desc.examples.push_back("datasketch_hll(k, data)");
                desc.parameter_names = {"k", "data"};
                desc.parameter_types = { LogicalType::INTEGER, LogicalType::FLOAT };
                agg_descs.push_back(std::move(desc));
            }
            
            {
                auto fun = DSHLLCreateAggregate<float>(LogicalType::FLOAT, sketch_type);
                fun.SetBindCallback(DSHLLBind);
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                DSSetArguments(fun, { LogicalType::INTEGER, LogicalType::FLOAT, LogicalType::VARCHAR });
                sketch.AddFunction(fun);

                FunctionDescription desc;
                desc.description = "Creates a sketch_hll data sketch by aggregating values. target_type (a constant) selects the register encoding: 'HLL_4' (the default, smallest), 'HLL_6' or 'HLL_8' (largest, fastest)";
                desc.examples.push_back("datasketch_hll(k, data, 'HLL_8')");
                desc.parameter_names = {"k", "data", "target_type"};
                desc.parameter_types = { LogicalType::INTEGER, LogicalType::FLOAT, LogicalType::VARCHAR };
                agg_descs.push_back(std::move(desc));
            }
            
        
        
            {
                auto fun = DSHLLCreateAggregate<double>(LogicalType::DOUBLE, sketch_type);
                fun.SetBindCallback(DSHLLBind);
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                DSPrependArgument(fun, LogicalType::INTEGER);
                sketch.AddFunction(fun);

                FunctionDescription desc;
                desc.description = "Creates a sketch_hll data sketch by aggregating values";
                desc.examples.push_back("datasketch_hll(k, data)");
                desc.parameter_names = {"k", "data"};
                desc.parameter_types = { LogicalType::INTEGER, LogicalType::DOUBLE };
                agg_descs.push_back(std::move(desc));
            }
            
            {
                auto fun = DSHLLCreateAggregate<double>(LogicalType::DOUBLE, sketch_type);
                fun.SetBindCallback(DSHLLBind);
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                DSSetArguments(fun, { LogicalType::INTEGER, LogicalType::DOUBLE, LogicalType::VARCHAR });
                sketch.AddFunction(fun);

                FunctionDescription desc;
                desc.description = "Creates a sketch_hll data sketch by aggregating values. target_type (a constant) selects the register encoding: 'HLL_4' (the default, smallest), 'HLL_6' or 'HLL_8' (largest, fastest)";
                desc.examples.push_back("datasketch_hll(k, data, 'HLL_8')");
                desc.parameter_names = {"k", "data", "target_type"};
                desc.parameter_types = { LogicalType::INTEGER, LogicalType::DOUBLE, LogicalType::VARCHAR };
                agg_descs.push_back(std::move(desc));
            }
            
        
        
            {
                auto fun = DSHLLCreateAggregate<uint8_t>(LogicalType::UTINYINT, sketch_type);
                fun.SetBindCallback(DSHLLBind);
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                DSPrependArgument(fun, LogicalType::INTEGER);
                sketch.AddFunction(fun);

                FunctionDescription desc;
                desc.description = "Creates a sketch_hll data sketch by aggregating values";
                desc.examples.push_back("datasketch_hll(k, data)");
                desc.parameter_names = {"k", "data"};
                desc.parameter_types = { LogicalType::INTEGER, LogicalType::UTINYINT };
                agg_descs.push_back(std::move(desc));
            }
            
            {
                auto fun = DSHLLCreateAggregate<uint8_t>(LogicalType::UTINYINT, sketch_type);
                fun.SetBindCallback(DSHLLBind);
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                DSSetArguments(fun, { LogicalType::INTEGER, LogicalType::UTINYINT, LogicalType::VARCHAR });
                sketch.AddFunction(fun);

                FunctionDescription desc;
                desc.description = "Creates a sketch_hll data sketch by aggregating values. target_type (a constant) selects the register encoding: 'HLL_4' (the default, smallest), 'HLL_6' or 'HLL_8' (largest, fastest)";
                desc.examples.push_back("datasketch_hll(k, data, 'HLL_8')");
                desc.parameter_names = {"k", "data", "target_type"};
                desc.parameter_types = { LogicalType::INTEGER, LogicalType::UTINYINT, LogicalType::VARCHAR };
                agg_descs.push_back(std::move(desc));
            }
            
        
        
            {
                auto fun = DSHLLCreateAggregate<uint16_t>(LogicalType::USMALLINT, sketch_type);
                fun.SetBindCallback(DSHLLBind);
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                DSPrependArgument(fun, LogicalType::INTEGER);
                sketch.AddFunction(fun);

                FunctionDescription desc;
                desc.description = "Creates a sketch_hll data sketch by aggregating values";
                desc.examples.push_back("datasketch_hll(k, data)");
                desc.parameter_names = {"k", "data"};
                desc.parameter_types = { LogicalType::INTEGER, LogicalType::USMALLINT };
                agg_descs.push_back(std::move(desc));
            }
            
            {
                auto fun = DSHLLCreateAggregate<uint16_t>(LogicalType::USMALLINT, sketch_type);
                fun.SetBindCallback(DSHLLBind);
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                DSSetArguments(fun, { LogicalType::INTEGER, LogicalType::USMALLINT, LogicalType::VARCHAR });
                sketch.AddFunction(fun);

                FunctionDescription desc;
                desc.description = "Creates a sketch_hll data sketch by aggregating values. target_type (a constant) selects the register encoding: 'HLL_4' (the default, smallest), 'HLL_6' or 'HLL_8' (largest, fastest)";
                desc.examples.push_back("datasketch_hll(k, data, 'HLL_8')");
                desc.parameter_names = {"k", "data", "target_type"};
                desc.parameter_types = { LogicalType::INTEGER, LogicalType::USMALLINT, LogicalType::VARCHAR };
                agg_descs.push_back(std::move(desc));
            }
            
        
        
            {
                auto fun = DSHLLCreateAggregate<uint32_t>(LogicalType::UINTEGER, sketch_type);
                fun.SetBindCallback(DSHLLBind);
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                DSPrependArgument(fun, LogicalType::INTEGER);
                sketch.AddFunction(fun);

                FunctionDescription desc;
                desc.description = "Creates a sketch_hll data sketch by aggregating values";
                desc.examples.push_back("datasketch_hll(k, data)");
                desc.parameter_names = {"k", "data"};
                desc.parameter_types = { LogicalType::INTEGER, LogicalType::UINTEGER };
                agg_descs.push_back(std::move(desc));
            }
            
            {
                auto fun = DSHLLCreateAggregate<uint32_t>(LogicalType::UINTEGER, sketch_type);
                fun.SetBindCallback(DSHLLBind);
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                DSSetArguments(fun, { LogicalType::INTEGER, LogicalType::UINTEGER, LogicalType::VARCHAR });
                sketch.AddFunction(fun);

                FunctionDescription desc;
                desc.description = "Creates a sketch_hll data sketch by aggregating values. target_type (a constant) selects the register encoding: 'HLL_4' (the default, smallest), 'HLL_6' or 'HLL_8' (largest, fastest)";
                desc.examples.push_back("datasketch_hll(k, data, 'HLL_8')");
                desc.parameter_names = {"k", "data", "target_type"};
                desc.parameter_types = { LogicalType::INTEGER, LogicalType::UINTEGER, LogicalType::VARCHAR };
                agg_descs.push_back(std::move(desc));
            }
            
        
        
            {
                auto fun = DSHLLCreateAggregate<uint64_t>(LogicalType::UBIGINT, sketch_type);
                fun.SetBindCallback(DSHLLBind);
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                DSPrependArgument(fun, LogicalType::INTEGER);
                sketch.AddFunction(fun);

                FunctionDescription desc;
                desc.description = "Creates a sketch_hll data sketch by aggregating values";
                desc.examples.push_back("datasketch_hll(k, data)");
                desc.parameter_names = {"k", "data"};
                desc.parameter_types = { LogicalType::INTEGER, LogicalType::UBIGINT };
                agg_descs.push_back(std::move(desc));
            }
            
            {
                auto fun = DSHLLCreateAggregate<uint64_t>(LogicalType::UBIGINT, sketch_type);
                fun.SetBindCallback(DSHLLBind);
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                DSSetArguments(fun, { LogicalType::INTEGER, LogicalType::UBIGINT, LogicalType::VARCHAR });
                sketch.AddFunction(fun);

                FunctionDescription desc;
                desc.description = "Creates a sketch_hll data sketch by aggregating values. target_type (a constant) selects the register encoding: 'HLL_4' (the default, smallest), 'HLL_6' or 'HLL_8' (largest, fastest)";
                desc.examples.push_back("datasketch_hll(k, data, 'HLL_8')");
                desc.parameter_names = {"k", "data", "target_type"};
                desc.parameter_types = { LogicalType::INTEGER, LogicalType::UBIGINT, LogicalType::VARCHAR };
                agg_descs.push_back(std::move(desc));
            }
            
        
        
            {
                auto fun = DSHLLCreateAggregate<string_t>(LogicalType::VARCHAR, sketch_type);
                fun.SetBindCallback(DSHLLBind);
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                DSPrependArgument(fun, LogicalType::INTEGER);
                sketch.AddFunction(fun);

                FunctionDescription desc;
                desc.description = "Creates a sketch_hll data sketch by aggregating values";
                desc.examples.push_back("datasketch_hll(k, data)");
                desc.parameter_names = {"k", "data"};
                desc.parameter_types = { LogicalType::INTEGER, LogicalType::VARCHAR };
                agg_descs.push_back(std::move(desc));
            }
            
            {
                auto fun = DSHLLCreateAggregate<string_t>(LogicalType::VARCHAR, sketch_type);
                fun.SetBindCallback(DSHLLBind);
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                DSSetArguments(fun, { LogicalType::INTEGER, LogicalType::VARCHAR, LogicalType::VARCHAR });
                sketch.AddFunction(fun);

                FunctionDescription desc;
                desc.description = "Creates a sketch_hll data sketch by aggregating values. target_type (a constant) selects the register encoding: 'HLL_4' (the default, smallest), 'HLL_6' or 'HLL_8' (largest, fastest)";
                desc.examples.push_back("datasketch_hll(k, data, 'HLL_8')");
                desc.parameter_names = {"k", "data", "target_type"};
                desc.parameter_types = { LogicalType::INTEGER, LogicalType::VARCHAR, LogicalType::VARCHAR };
                agg_descs.push_back(std::move(desc));
            }
            
        
        
            {
                auto fun = DSHLLCreateAggregate<string_t>(LogicalType::BLOB, sketch_type);
                fun.SetBindCallback(DSHLLBind);
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                DSPrependArgument(fun, LogicalType::INTEGER);
                sketch.AddFunction(fun);

                FunctionDescription desc;
                desc.description = "Creates a sketch_hll data sketch by aggregating values";
                desc.examples.push_back("datasketch_hll(k, data)");
                desc.parameter_names = {"k", "data"};
                desc.parameter_types = { LogicalType::INTEGER, LogicalType::BLOB };
                agg_descs.push_back(std::move(desc));
            }
            
            {
                auto fun = DSHLLCreateAggregate<string_t>(LogicalType::BLOB, sketch_type);
                fun.SetBindCallback(DSHLLBind);
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                DSSetArguments(fun, { LogicalType::INTEGER, LogicalType::BLOB, LogicalType::VARCHAR });
                sketch.AddFunction(fun);

                FunctionDescription desc;
                desc.description = "Creates a sketch_hll data sketch by aggregating values. target_type (a constant) selects the register encoding: 'HLL_4' (the default, smallest), 'HLL_6' or 'HLL_8' (largest, fastest)";
                desc.examples.push_back("datasketch_hll(k, data, 'HLL_8')");
                desc.parameter_names = {"k", "data", "target_type"};
                desc.parameter_types = { LogicalType::INTEGER, LogicalType::BLOB, LogicalType::VARCHAR };
                agg_descs.push_back(std::move(desc));
            }
            
        
      CreateAggregateFunctionInfo sketch_info(sketch);
      for (auto &d : agg_descs) {
          sketch_info.descriptions.push_back(std::move(d));
      }

      DSSetCategories(sketch_info, {"sketch", "hll"});

      DSRegister(loader, sketch_info);
      }



      
      {
      AggregateFunctionSet sketch("datasketch_hll_union");
      auto fun = DSHLLMergeAggregate(sketch_type);
      fun.SetBindCallback(DSHLLBind);
      fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
      DSPrependArgument(fun, LogicalType::INTEGER);
      sketch.AddFunction(fun);
      
      DSSetArguments(fun, { LogicalType::INTEGER, sketch_type, LogicalType::VARCHAR });
      sketch.AddFunction(fun);
      
      CreateAggregateFunctionInfo sketch_info(sketch);

    {
        FunctionDescription desc;
        desc.description = "Creates a sketch_HLL data sketch by aggregating other HLL data sketches";
        desc.examples.push_back("datasketch_hll_union(k, sketch)");
        desc.parameter_names = {"k", "sketch"};
        desc.parameter_types = { LogicalType::INTEGER, sketch_type };
        sketch_info.descriptions.push_back(desc);
    }
      
    {
        FunctionDescription desc;
        desc.description = "Creates a sketch_HLL data sketch by aggregating other HLL data sketches. target_type (a constant) selects the result's register encoding: 'HLL_4' (the default), 'HLL_6' or 'HLL_8'";
        desc.examples.push_back("datasketch_hll_union(k, sketch, 'HLL_8')");
        desc.parameter_names = {"k", "sketch", "target_type"};
        desc.parameter_types = { LogicalType::INTEGER, sketch_type, LogicalType::VARCHAR };
        sketch_info.descriptions.push_back(desc);
    }
      

      DSSetCategories(sketch_info, {"sketch", "hll"});

      DSRegister(loader, sketch_info);
      }
      


  }
    


static inline void DSCPCis_empty(DataChunk &args, ExpressionState &state, Vector &result)
{
    // Get the references to the incoming vectors.
    D_ASSERT(args.ColumnCount() == 1);

    
        auto &sketch_vector = args.data[0];

    
        
        UnaryExecutor::Execute
        <string_t,bool>
        (
        sketch_vector,result,
        [&](string_t sketch_data) {

            
            auto sketch = [&]() {
                try {
                    return datasketches::cpc_sketch::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
                } catch (const std::exception &e) {
                    throw InvalidInputException("Failed to deserialize CPC sketch: %s", e.what());
                }
            }();
return sketch.is_empty();
        });

}

    


static inline void DSCPCmerge(DataChunk &args, ExpressionState &state, Vector &result)
{
    // Get the references to the incoming vectors.
    D_ASSERT(args.ColumnCount() == 2);

    
        auto &sketch_a_vector = args.data[0];
        auto &sketch_b_vector = args.data[1];

    
        
        BinaryExecutor::Execute
        <string_t,string_t,string_t>
        (
        sketch_a_vector,sketch_b_vector,result,
        [&](string_t sketch_a_data,string_t sketch_b_data) {

            
            auto sketch_a = [&]() {
                try {
                    return datasketches::cpc_sketch::deserialize(sketch_a_data.GetDataUnsafe(), sketch_a_data.GetSize());
                } catch (const std::exception &e) {
                    throw InvalidInputException("Failed to deserialize CPC sketch: %s", e.what());
                }
            }();

            auto sketch_b = [&]() {
                try {
                    return datasketches::cpc_sketch::deserialize(sketch_b_data.GetDataUnsafe(), sketch_b_data.GetSize());
                } catch (const std::exception &e) {
                    throw InvalidInputException("Failed to deserialize CPC sketch: %s", e.what());
                }
            }();

                datasketches::cpc_union u(std::max(sketch_a.get_lg_k(), sketch_b.get_lg_k()));
                u.update(sketch_a);
                u.update(sketch_b);
                auto serialized_data = u.get_result().serialize();
                return StringVector::AddStringOrBlob(result, std::string(serialized_data.begin(), serialized_data.end()));
                
        });

}

    


static inline void DSCPCdescribe(DataChunk &args, ExpressionState &state, Vector &result)
{
    // Get the references to the incoming vectors.
    D_ASSERT(args.ColumnCount() == 1);

    
        auto &sketch_vector = args.data[0];

    
        
        UnaryExecutor::Execute
        <string_t,string_t>
        (
        sketch_vector,result,
        [&](string_t sketch_data) {

            
            auto sketch = [&]() {
                try {
                    return datasketches::cpc_sketch::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
                } catch (const std::exception &e) {
                    throw InvalidInputException("Failed to deserialize CPC sketch: %s", e.what());
                }
            }();
return StringVector::AddString(result, sketch.to_string());
        });

}

    


static inline void DSCPCestimate(DataChunk &args, ExpressionState &state, Vector &result)
{
    // Get the references to the incoming vectors.
    D_ASSERT(args.ColumnCount() == 1);

    
        auto &sketch_vector = args.data[0];

    
        
        UnaryExecutor::Execute
        <string_t,double>
        (
        sketch_vector,result,
        [&](string_t sketch_data) {

            
            auto sketch = [&]() {
                try {
                    return datasketches::cpc_sketch::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
                } catch (const std::exception &e) {
                    throw InvalidInputException("Failed to deserialize CPC sketch: %s", e.what());
                }
            }();
return sketch.get_estimate();
        });

}

    


static inline void DSCPClower_bound(DataChunk &args, ExpressionState &state, Vector &result)
{
    // Get the references to the incoming vectors.
    D_ASSERT(args.ColumnCount() == 2);

    
        auto &sketch_vector = args.data[0];
        auto &std_dev_vector = args.data[1];

    
        
        BinaryExecutor::Execute
        <string_t,uint8_t,double>
        (
        sketch_vector,std_dev_vector,result,
        [&](string_t sketch_data,uint8_t std_dev_data) {

            
            auto sketch = [&]() {
                try {
                    return datasketches::cpc_sketch::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
                } catch (const std::exception &e) {
                    throw InvalidInputException("Failed to deserialize CPC sketch: %s", e.what());
                }
            }();
return sketch.get_lower_bound(std_dev_data);
        });

}

    


static inline void DSCPCupper_bound(DataChunk &args, ExpressionState &state, Vector &result)
{
    // Get the references to the incoming vectors.
    D_ASSERT(args.ColumnCount() == 2);

    
        auto &sketch_vector = args.data[0];
        auto &std_dev_vector = args.data[1];

    
        
        BinaryExecutor::Execute
        <string_t,uint8_t,double>
        (
        sketch_vector,std_dev_vector,result,
        [&](string_t sketch_data,uint8_t std_dev_data) {

            
            auto sketch = [&]() {
                try {
                    return datasketches::cpc_sketch::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
                } catch (const std::exception &e) {
                    throw InvalidInputException("Failed to deserialize CPC sketch: %s", e.what());
                }
            }();
return sketch.get_upper_bound(std_dev_data);
        });

}

    


    
    auto static DSCPCMergeAggregate(const LogicalType &result_type) -> AggregateFunction
    
    {
        
        auto fun = AggregateFunction::UnaryAggregate<DSCPCState, string_t, string_t, DSCPCMergeOperation<DSCPCBindData>, AggregateDestructorType::LEGACY>(result_type, result_type);
        DSReadDataArgumentAt<1, DSCPCState, string_t, DSCPCMergeOperation<DSCPCBindData>>(fun);
        return fun;
        
    }



    template <typename T>
    auto static DSCPCCreateAggregate(const LogicalType &type, const LogicalType &result_type) -> AggregateFunction
    {
        
        auto fun = AggregateFunction::UnaryAggregate<DSCPCState, T, string_t, DSCPCCreateOperation<DSCPCBindData>, AggregateDestructorType::LEGACY>(type, result_type);
        DSReadDataArgumentAt<1, DSCPCState, T, DSCPCCreateOperation<DSCPCBindData>>(fun);
        return fun;
        
    }


  void LoadCPCSketch(ExtensionLoader &loader) {


      
        auto sketch_type = CreateCPCCountingSketchType(loader);
      

      
      {
        ScalarFunctionSet fs("datasketch_cpc_is_empty");
        std::vector<FunctionDescription> fn_descs;
        
                fs.AddFunction(ScalarFunction(
                {sketch_type},LogicalType::BOOLEAN
                ,    DSCPCis_empty));
                {
                    FunctionDescription desc;
                    desc.description = "Return a boolean indicating if the sketch is empty";
                    desc.examples.push_back("datasketch_cpc_is_empty(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_type };
                    fn_descs.push_back(std::move(desc));
                }
        

        // Every scalar function deserializes a sketch, which throws on invalid input.
        fs.SetFallible();
        CreateScalarFunctionInfo info(std::move(fs));
        for (auto &d : fn_descs) {
            info.descriptions.push_back(std::move(d));
        }

        DSSetCategories(info, {"sketch", "cpc"});

        DSRegister(loader, info);


      }
      {
        ScalarFunctionSet fs("datasketch_cpc_merge");
        std::vector<FunctionDescription> fn_descs;
        
                fs.AddFunction(ScalarFunction(
                {sketch_type,sketch_type},sketch_type
                ,    DSCPCmerge));
                {
                    FunctionDescription desc;
                    desc.description = "Return the union of two CPC sketches as a new sketch. The result uses the smaller lg_k of the two inputs";
                    desc.examples.push_back("datasketch_cpc_merge(sketch_a, sketch_b)");
                    desc.parameter_names = { "sketch_a","sketch_b" };
                    desc.parameter_types = { sketch_type,sketch_type };
                    fn_descs.push_back(std::move(desc));
                }
        

        // Every scalar function deserializes a sketch, which throws on invalid input.
        fs.SetFallible();
        CreateScalarFunctionInfo info(std::move(fs));
        for (auto &d : fn_descs) {
            info.descriptions.push_back(std::move(d));
        }

        DSSetCategories(info, {"sketch", "cpc"});

        DSRegister(loader, info);


      }
      {
        ScalarFunctionSet fs("datasketch_cpc_describe");
        std::vector<FunctionDescription> fn_descs;
        
                fs.AddFunction(ScalarFunction(
                {sketch_type},LogicalType::VARCHAR
                ,    DSCPCdescribe));
                {
                    FunctionDescription desc;
                    desc.description = "Return a string representation of the sketch";
                    desc.examples.push_back("datasketch_cpc_describe(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_type };
                    fn_descs.push_back(std::move(desc));
                }
        

        // Every scalar function deserializes a sketch, which throws on invalid input.
        fs.SetFallible();
        CreateScalarFunctionInfo info(std::move(fs));
        for (auto &d : fn_descs) {
            info.descriptions.push_back(std::move(d));
        }

        DSSetCategories(info, {"sketch", "cpc"});

        DSRegister(loader, info);


      }
      {
        ScalarFunctionSet fs("datasketch_cpc_estimate");
        std::vector<FunctionDescription> fn_descs;
        
                fs.AddFunction(ScalarFunction(
                {sketch_type},LogicalType::DOUBLE
                ,    DSCPCestimate));
                {
                    FunctionDescription desc;
                    desc.description = "Return the estimate of the number of distinct items seen by the sketch";
                    desc.examples.push_back("datasketch_cpc_estimate(sketch)");
                    desc.parameter_names = { "sketch" };
                    desc.parameter_types = { sketch_type };
                    fn_descs.push_back(std::move(desc));
                }
        

        // Every scalar function deserializes a sketch, which throws on invalid input.
        fs.SetFallible();
        CreateScalarFunctionInfo info(std::move(fs));
        for (auto &d : fn_descs) {
            info.descriptions.push_back(std::move(d));
        }

        DSSetCategories(info, {"sketch", "cpc"});

        DSRegister(loader, info);


      }
      {
        ScalarFunctionSet fs("datasketch_cpc_lower_bound");
        std::vector<FunctionDescription> fn_descs;
        
                fs.AddFunction(ScalarFunction(
                {sketch_type,LogicalType::UTINYINT},LogicalType::DOUBLE
                ,    DSCPClower_bound));
                {
                    FunctionDescription desc;
                    desc.description = "Return the lower bound of the number of distinct items seen by the sketch";
                    desc.examples.push_back("datasketch_cpc_lower_bound(sketch, std_dev)");
                    desc.parameter_names = { "sketch","std_dev" };
                    desc.parameter_types = { sketch_type,LogicalType::UTINYINT };
                    fn_descs.push_back(std::move(desc));
                }
        

        // Every scalar function deserializes a sketch, which throws on invalid input.
        fs.SetFallible();
        CreateScalarFunctionInfo info(std::move(fs));
        for (auto &d : fn_descs) {
            info.descriptions.push_back(std::move(d));
        }

        DSSetCategories(info, {"sketch", "cpc"});

        DSRegister(loader, info);


      }
      {
        ScalarFunctionSet fs("datasketch_cpc_upper_bound");
        std::vector<FunctionDescription> fn_descs;
        
                fs.AddFunction(ScalarFunction(
                {sketch_type,LogicalType::UTINYINT},LogicalType::DOUBLE
                ,    DSCPCupper_bound));
                {
                    FunctionDescription desc;
                    desc.description = "Return the upper bound of the number of distinct items seen by the sketch";
                    desc.examples.push_back("datasketch_cpc_upper_bound(sketch, std_dev)");
                    desc.parameter_names = { "sketch","std_dev" };
                    desc.parameter_types = { sketch_type,LogicalType::UTINYINT };
                    fn_descs.push_back(std::move(desc));
                }
        

        // Every scalar function deserializes a sketch, which throws on invalid input.
        fs.SetFallible();
        CreateScalarFunctionInfo info(std::move(fs));
        for (auto &d : fn_descs) {
            info.descriptions.push_back(std::move(d));
        }

        DSSetCategories(info, {"sketch", "cpc"});

        DSRegister(loader, info);


      }

      // This funciton creates the sketches.
      {
      AggregateFunctionSet sketch("datasketch_cpc");
      std::vector<FunctionDescription> agg_descs;
      
        
            {
                auto fun = DSCPCCreateAggregate<int8_t>(LogicalType::TINYINT, sketch_type);
                fun.SetBindCallback(DSCPCBind);
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                DSPrependArgument(fun, LogicalType::INTEGER);
                sketch.AddFunction(fun);

                FunctionDescription desc;
                desc.description = "Creates a sketch_cpc data sketch by aggregating values";
                desc.examples.push_back("datasketch_cpc(k, data)");
                desc.parameter_names = {"k", "data"};
                desc.parameter_types = { LogicalType::INTEGER, LogicalType::TINYINT };
                agg_descs.push_back(std::move(desc));
            }
            
        
        
            {
                auto fun = DSCPCCreateAggregate<int16_t>(LogicalType::SMALLINT, sketch_type);
                fun.SetBindCallback(DSCPCBind);
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                DSPrependArgument(fun, LogicalType::INTEGER);
                sketch.AddFunction(fun);

                FunctionDescription desc;
                desc.description = "Creates a sketch_cpc data sketch by aggregating values";
                desc.examples.push_back("datasketch_cpc(k, data)");
                desc.parameter_names = {"k", "data"};
                desc.parameter_types = { LogicalType::INTEGER, LogicalType::SMALLINT };
                agg_descs.push_back(std::move(desc));
            }
            
        
        
            {
                auto fun = DSCPCCreateAggregate<int32_t>(LogicalType::INTEGER, sketch_type);
                fun.SetBindCallback(DSCPCBind);
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                DSPrependArgument(fun, LogicalType::INTEGER);
                sketch.AddFunction(fun);

                FunctionDescription desc;
                desc.description = "Creates a sketch_cpc data sketch by aggregating values";
                desc.examples.push_back("datasketch_cpc(k, data)");
                desc.parameter_names = {"k", "data"};
                desc.parameter_types = { LogicalType::INTEGER, LogicalType::INTEGER };
                agg_descs.push_back(std::move(desc));
            }
            
        
        
            {
                auto fun = DSCPCCreateAggregate<int64_t>(LogicalType::BIGINT, sketch_type);
                fun.SetBindCallback(DSCPCBind);
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                DSPrependArgument(fun, LogicalType::INTEGER);
                sketch.AddFunction(fun);

                FunctionDescription desc;
                desc.description = "Creates a sketch_cpc data sketch by aggregating values";
                desc.examples.push_back("datasketch_cpc(k, data)");
                desc.parameter_names = {"k", "data"};
                desc.parameter_types = { LogicalType::INTEGER, LogicalType::BIGINT };
                agg_descs.push_back(std::move(desc));
            }
            
        
        
            {
                auto fun = DSCPCCreateAggregate<float>(LogicalType::FLOAT, sketch_type);
                fun.SetBindCallback(DSCPCBind);
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                DSPrependArgument(fun, LogicalType::INTEGER);
                sketch.AddFunction(fun);

                FunctionDescription desc;
                desc.description = "Creates a sketch_cpc data sketch by aggregating values";
                desc.examples.push_back("datasketch_cpc(k, data)");
                desc.parameter_names = {"k", "data"};
                desc.parameter_types = { LogicalType::INTEGER, LogicalType::FLOAT };
                agg_descs.push_back(std::move(desc));
            }
            
        
        
            {
                auto fun = DSCPCCreateAggregate<double>(LogicalType::DOUBLE, sketch_type);
                fun.SetBindCallback(DSCPCBind);
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                DSPrependArgument(fun, LogicalType::INTEGER);
                sketch.AddFunction(fun);

                FunctionDescription desc;
                desc.description = "Creates a sketch_cpc data sketch by aggregating values";
                desc.examples.push_back("datasketch_cpc(k, data)");
                desc.parameter_names = {"k", "data"};
                desc.parameter_types = { LogicalType::INTEGER, LogicalType::DOUBLE };
                agg_descs.push_back(std::move(desc));
            }
            
        
        
            {
                auto fun = DSCPCCreateAggregate<uint8_t>(LogicalType::UTINYINT, sketch_type);
                fun.SetBindCallback(DSCPCBind);
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                DSPrependArgument(fun, LogicalType::INTEGER);
                sketch.AddFunction(fun);

                FunctionDescription desc;
                desc.description = "Creates a sketch_cpc data sketch by aggregating values";
                desc.examples.push_back("datasketch_cpc(k, data)");
                desc.parameter_names = {"k", "data"};
                desc.parameter_types = { LogicalType::INTEGER, LogicalType::UTINYINT };
                agg_descs.push_back(std::move(desc));
            }
            
        
        
            {
                auto fun = DSCPCCreateAggregate<uint16_t>(LogicalType::USMALLINT, sketch_type);
                fun.SetBindCallback(DSCPCBind);
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                DSPrependArgument(fun, LogicalType::INTEGER);
                sketch.AddFunction(fun);

                FunctionDescription desc;
                desc.description = "Creates a sketch_cpc data sketch by aggregating values";
                desc.examples.push_back("datasketch_cpc(k, data)");
                desc.parameter_names = {"k", "data"};
                desc.parameter_types = { LogicalType::INTEGER, LogicalType::USMALLINT };
                agg_descs.push_back(std::move(desc));
            }
            
        
        
            {
                auto fun = DSCPCCreateAggregate<uint32_t>(LogicalType::UINTEGER, sketch_type);
                fun.SetBindCallback(DSCPCBind);
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                DSPrependArgument(fun, LogicalType::INTEGER);
                sketch.AddFunction(fun);

                FunctionDescription desc;
                desc.description = "Creates a sketch_cpc data sketch by aggregating values";
                desc.examples.push_back("datasketch_cpc(k, data)");
                desc.parameter_names = {"k", "data"};
                desc.parameter_types = { LogicalType::INTEGER, LogicalType::UINTEGER };
                agg_descs.push_back(std::move(desc));
            }
            
        
        
            {
                auto fun = DSCPCCreateAggregate<uint64_t>(LogicalType::UBIGINT, sketch_type);
                fun.SetBindCallback(DSCPCBind);
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                DSPrependArgument(fun, LogicalType::INTEGER);
                sketch.AddFunction(fun);

                FunctionDescription desc;
                desc.description = "Creates a sketch_cpc data sketch by aggregating values";
                desc.examples.push_back("datasketch_cpc(k, data)");
                desc.parameter_names = {"k", "data"};
                desc.parameter_types = { LogicalType::INTEGER, LogicalType::UBIGINT };
                agg_descs.push_back(std::move(desc));
            }
            
        
        
            {
                auto fun = DSCPCCreateAggregate<string_t>(LogicalType::VARCHAR, sketch_type);
                fun.SetBindCallback(DSCPCBind);
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                DSPrependArgument(fun, LogicalType::INTEGER);
                sketch.AddFunction(fun);

                FunctionDescription desc;
                desc.description = "Creates a sketch_cpc data sketch by aggregating values";
                desc.examples.push_back("datasketch_cpc(k, data)");
                desc.parameter_names = {"k", "data"};
                desc.parameter_types = { LogicalType::INTEGER, LogicalType::VARCHAR };
                agg_descs.push_back(std::move(desc));
            }
            
        
        
            {
                auto fun = DSCPCCreateAggregate<string_t>(LogicalType::BLOB, sketch_type);
                fun.SetBindCallback(DSCPCBind);
                fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
                DSPrependArgument(fun, LogicalType::INTEGER);
                sketch.AddFunction(fun);

                FunctionDescription desc;
                desc.description = "Creates a sketch_cpc data sketch by aggregating values";
                desc.examples.push_back("datasketch_cpc(k, data)");
                desc.parameter_names = {"k", "data"};
                desc.parameter_types = { LogicalType::INTEGER, LogicalType::BLOB };
                agg_descs.push_back(std::move(desc));
            }
            
        
      CreateAggregateFunctionInfo sketch_info(sketch);
      for (auto &d : agg_descs) {
          sketch_info.descriptions.push_back(std::move(d));
      }

      DSSetCategories(sketch_info, {"sketch", "cpc"});

      DSRegister(loader, sketch_info);
      }



      
      {
      AggregateFunctionSet sketch("datasketch_cpc_union");
      auto fun = DSCPCMergeAggregate(sketch_type);
      fun.SetBindCallback(DSCPCBind);
      fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
      DSPrependArgument(fun, LogicalType::INTEGER);
      sketch.AddFunction(fun);
      
      CreateAggregateFunctionInfo sketch_info(sketch);

    {
        FunctionDescription desc;
        desc.description = "Creates a sketch_CPC data sketch by aggregating other CPC data sketches";
        desc.examples.push_back("datasketch_cpc_union(k, sketch)");
        desc.parameter_names = {"k", "sketch"};
        desc.parameter_types = { LogicalType::INTEGER, sketch_type };
        sketch_info.descriptions.push_back(desc);
    }
      

      DSSetCategories(sketch_info, {"sketch", "cpc"});

      DSRegister(loader, sketch_info);
      }
      


  }


}