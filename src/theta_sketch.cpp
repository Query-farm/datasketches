#include "datasketches_extension.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/parsed_data/create_aggregate_function_info.hpp"

// Apache DataSketches Headers
#include <DataSketches/theta_sketch.hpp>
#include <DataSketches/theta_union.hpp>
#include <DataSketches/theta_intersection.hpp>
#include <DataSketches/theta_a_not_b.hpp>

// Use the duckdb namespace to avoid verbose prefixing
using namespace duckdb;

namespace duckdb_datasketches {

// ============================================================
// 1. Helpers & Bind Data
// ============================================================

static std::string toLowerCase(const std::string& input) {
    std::string result = input;
    std::transform(result.begin(), result.end(), result.begin(), [](unsigned char c) {
        return std::tolower(c);
    });
    return result;
}

struct DSThetaBindData : public FunctionData {
    DSThetaBindData() {}
    explicit DSThetaBindData(uint8_t lg_k) : lg_k(lg_k) {}

    unique_ptr<FunctionData> Copy() const override {
        return make_uniq<DSThetaBindData>(lg_k);
    }

    bool Equals(const FunctionData &other_p) const override {
        auto &other = other_p.Cast<DSThetaBindData>();
        return lg_k == other.lg_k;
    }

    uint8_t lg_k;
};

// Bind function for: datasketch_theta(lg_k, column)
unique_ptr<FunctionData> DSThetaBindWithK(ClientContext &context, AggregateFunction &function,
                                          vector<unique_ptr<Expression>> &arguments) {
    if (arguments[0]->HasParameter()) {
        throw ParameterNotResolvedException();
    }
    if (!arguments[0]->IsFoldable()) {
        throw BinderException("Theta Sketch lg_k must be a constant integer");
    }

    Value k_val = ExpressionExecutor::EvaluateScalar(context, *arguments[0]);
    if (k_val.IsNull()) {
        throw BinderException("Theta Sketch lg_k cannot be NULL");
    }

    auto lg_k = (uint8_t)k_val.GetValue<int32_t>();

    // Remove the 'lg_k' argument so the aggregate operation only sees the data column
    Function::EraseArgument(function, arguments, 0);
    return make_uniq<DSThetaBindData>(lg_k);
}

// Bind function for: datasketch_theta(column)
unique_ptr<FunctionData> DSThetaBindDefault(ClientContext &context, AggregateFunction &function,
                                            vector<unique_ptr<Expression>> &arguments) {
    // Default to lg_k = 12 (K = 4096)
    return make_uniq<DSThetaBindData>(12);
}

// ============================================================
// 2. State & Operations
// ============================================================

struct DSThetaState {
    datasketches::update_theta_sketch* update_sketch = nullptr;
    datasketches::theta_union* union_sketch = nullptr;

    ~DSThetaState() {
        if (update_sketch) delete update_sketch;
        if (union_sketch) delete union_sketch;
    }

    void CreateUpdateSketch(uint8_t lg_k) {
        if (!update_sketch) {
            datasketches::update_theta_sketch::builder b;
            b.set_lg_k(lg_k);
            update_sketch = new datasketches::update_theta_sketch(b.build());
        }
    }

    void CreateUnionSketch(uint8_t lg_k) {
        if (!union_sketch) {
            datasketches::theta_union::builder b;
            b.set_lg_k(lg_k);
            union_sketch = new datasketches::theta_union(b.build());
        }
    }
};

struct DSThetaOperationBase {
    template <class STATE>
    static void Initialize(STATE &state) {
        state.update_sketch = nullptr;
        state.union_sketch = nullptr;
    }
    template <class STATE>
    static void Destroy(STATE &state, AggregateInputData &aggr_input_data) {
        if (state.update_sketch) delete state.update_sketch;
        if (state.union_sketch) delete state.union_sketch;
    }
    static bool IgnoreNull() { return true; }
};

struct DSThetaCreateOperation : DSThetaOperationBase {
    template <class A_TYPE, class STATE, class OP>
    static void Operation(STATE &state, const A_TYPE &a_data, AggregateUnaryInput &idata) {
        auto &bind_data = idata.input.bind_data->template Cast<DSThetaBindData>();
        state.CreateUpdateSketch(bind_data.lg_k);

        if constexpr (std::is_same_v<A_TYPE, duckdb::string_t>) {
            state.update_sketch->update(a_data.GetData(), a_data.GetSize());
        } else {
            state.update_sketch->update(a_data);
        }
    }

    template <class INPUT_TYPE, class STATE, class OP>
    static void ConstantOperation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input, idx_t count) {
        for (idx_t i = 0; i < count; i++) {
            Operation<INPUT_TYPE, STATE, OP>(state, input, unary_input);
        }
    }

    template <class STATE, class OP>
    static void Combine(const STATE &source, STATE &target, AggregateInputData &aggr_input_data) {
        if (!source.update_sketch) return;

        if (!target.union_sketch) {
             uint8_t lg_k = target.update_sketch ? target.update_sketch->get_lg_k() : 12;
             target.CreateUnionSketch(lg_k);
             if (target.update_sketch) {
                 target.union_sketch->update(*target.update_sketch);
                 delete target.update_sketch;
                 target.update_sketch = nullptr;
             }
        }
        target.union_sketch->update(*source.update_sketch);
    }

    template <class T, class STATE>
    static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
        if (state.union_sketch) {
             auto compact = state.union_sketch->get_result();
             auto serialized = compact.serialize();
             target = StringVector::AddStringOrBlob(finalize_data.result, std::string(serialized.begin(), serialized.end()));
        } else if (state.update_sketch) {
             auto compact = state.update_sketch->compact();
             auto serialized = compact.serialize();
             target = StringVector::AddStringOrBlob(finalize_data.result, std::string(serialized.begin(), serialized.end()));
        } else {
             finalize_data.ReturnNull();
        }
    }
};

// ============================================================
// 3. Scalar Functions
// ============================================================

static void DSThetaIntersect(DataChunk &args, ExpressionState &state, Vector &result) {
    BinaryExecutor::Execute<string_t, string_t, string_t>(
        args.data[0], args.data[1], result, args.size(),
        [&](string_t a_blob, string_t b_blob) {
            auto sketch_a = datasketches::compact_theta_sketch::deserialize(a_blob.GetDataUnsafe(), a_blob.GetSize());
            auto sketch_b = datasketches::compact_theta_sketch::deserialize(b_blob.GetDataUnsafe(), b_blob.GetSize());

            datasketches::theta_intersection intersection;
            intersection.update(sketch_a);
            intersection.update(sketch_b);

            auto result_sketch = intersection.get_result();
            auto serialized = result_sketch.serialize();
            return StringVector::AddStringOrBlob(result, std::string(serialized.begin(), serialized.end()));
        });
}

static void DSThetaANotB(DataChunk &args, ExpressionState &state, Vector &result) {
    BinaryExecutor::Execute<string_t, string_t, string_t>(
        args.data[0], args.data[1], result, args.size(),
        [&](string_t a_blob, string_t b_blob) {
            auto sketch_a = datasketches::compact_theta_sketch::deserialize(a_blob.GetDataUnsafe(), a_blob.GetSize());
            auto sketch_b = datasketches::compact_theta_sketch::deserialize(b_blob.GetDataUnsafe(), b_blob.GetSize());

            datasketches::theta_a_not_b a_not_b;
            auto result_sketch = a_not_b.compute(sketch_a, sketch_b);

            auto serialized = result_sketch.serialize();
            return StringVector::AddStringOrBlob(result, std::string(serialized.begin(), serialized.end()));
        });
}

static void DSThetaEstimate(DataChunk &args, ExpressionState &state, Vector &result) {
    UnaryExecutor::Execute<string_t, double>(
        args.data[0], result, args.size(),
        [&](string_t sketch_blob) {
            auto sketch = datasketches::compact_theta_sketch::deserialize(sketch_blob.GetDataUnsafe(), sketch_blob.GetSize());
            return sketch.get_estimate();
        });
}

static void DSThetaLowerBound(DataChunk &args, ExpressionState &state, Vector &result) {
    BinaryExecutor::Execute<string_t, int32_t, double>(
        args.data[0], args.data[1], result, args.size(),
        [&](string_t sketch_blob, int32_t num_std_devs) {
            auto sketch = datasketches::compact_theta_sketch::deserialize(sketch_blob.GetDataUnsafe(), sketch_blob.GetSize());
            return sketch.get_lower_bound(static_cast<uint8_t>(num_std_devs));
        });
}

static void DSThetaUpperBound(DataChunk &args, ExpressionState &state, Vector &result) {
    BinaryExecutor::Execute<string_t, int32_t, double>(
        args.data[0], args.data[1], result, args.size(),
        [&](string_t sketch_blob, int32_t num_std_devs) {
            auto sketch = datasketches::compact_theta_sketch::deserialize(sketch_blob.GetDataUnsafe(), sketch_blob.GetSize());
            return sketch.get_upper_bound(static_cast<uint8_t>(num_std_devs));
        });
}

static void DSThetaDescribe(DataChunk &args, ExpressionState &state, Vector &result) {
    UnaryExecutor::Execute<string_t, string_t>(
        args.data[0], result, args.size(),
        [&](string_t sketch_blob) {
            auto sketch = datasketches::compact_theta_sketch::deserialize(sketch_blob.GetDataUnsafe(), sketch_blob.GetSize());
            // passing 'false' to to_string prevents printing all internal hash values, which would be huge
            return StringVector::AddString(result, sketch.to_string(false));
        });
}

// ============================================================
// 4. Type Creation & Registration Helpers
// ============================================================

static LogicalType CreateThetaSketchType(ExtensionLoader &loader) {
    auto new_type = LogicalType(LogicalTypeId::BLOB);
    auto new_type_name = "sketch_theta";
    auto type_info = CreateTypeInfo(new_type_name, LogicalType::BLOB);
    type_info.temporary = false;
    type_info.internal = true;
    type_info.comment = "Sketch type for Theta Sketch";
    new_type.SetAlias(new_type_name);

    auto &system_catalog = Catalog::GetSystemCatalog(loader.GetDatabaseInstance());
    auto data = CatalogTransaction::GetSystemTransaction(loader.GetDatabaseInstance());
    system_catalog.CreateType(data, type_info);

    loader.RegisterCastFunction(LogicalType::BLOB, new_type, DefaultCasts::ReinterpretCast, 1);
    loader.RegisterCastFunction(new_type, LogicalType::BLOB, DefaultCasts::ReinterpretCast, 1);
    return new_type;
}

template <typename T>
static void RegisterThetaAggregates(AggregateFunctionSet &set, const LogicalType &input_type, const LogicalType &result_type) {
    // Overload 1: datasketch_theta(column)
    auto fun_default = AggregateFunction::UnaryAggregateDestructor<DSThetaState, T, string_t, DSThetaCreateOperation, AggregateDestructorType::LEGACY>(
        input_type, result_type);
    fun_default.bind = DSThetaBindDefault;
    fun_default.order_dependent = AggregateOrderDependent::NOT_ORDER_DEPENDENT;
    set.AddFunction(fun_default);

    // Overload 2: datasketch_theta(lg_k, column)
    auto fun_with_k = AggregateFunction::UnaryAggregateDestructor<DSThetaState, T, string_t, DSThetaCreateOperation, AggregateDestructorType::LEGACY>(
        input_type, result_type);
    fun_with_k.bind = DSThetaBindWithK;
    fun_with_k.order_dependent = AggregateOrderDependent::NOT_ORDER_DEPENDENT;
    fun_with_k.arguments.insert(fun_with_k.arguments.begin(), LogicalType::INTEGER);
    set.AddFunction(fun_with_k);
}

// ============================================================
// 5. Main Loader
// ============================================================

void LoadThetaSketch(ExtensionLoader &loader) {
    auto sketch_type = CreateThetaSketchType(loader);

    // 1. Register Aggregates (Build)
    AggregateFunctionSet sketch_agg("datasketch_theta");

    RegisterThetaAggregates<int8_t>(sketch_agg, LogicalType::TINYINT, sketch_type);
    RegisterThetaAggregates<int16_t>(sketch_agg, LogicalType::SMALLINT, sketch_type);
    RegisterThetaAggregates<int32_t>(sketch_agg, LogicalType::INTEGER, sketch_type);
    RegisterThetaAggregates<int64_t>(sketch_agg, LogicalType::BIGINT, sketch_type);
    RegisterThetaAggregates<float>(sketch_agg, LogicalType::FLOAT, sketch_type);
    RegisterThetaAggregates<double>(sketch_agg, LogicalType::DOUBLE, sketch_type);
    RegisterThetaAggregates<uint8_t>(sketch_agg, LogicalType::UTINYINT, sketch_type);
    RegisterThetaAggregates<uint16_t>(sketch_agg, LogicalType::USMALLINT, sketch_type);
    RegisterThetaAggregates<uint32_t>(sketch_agg, LogicalType::UINTEGER, sketch_type);
    RegisterThetaAggregates<uint64_t>(sketch_agg, LogicalType::UBIGINT, sketch_type);
    RegisterThetaAggregates<string_t>(sketch_agg, LogicalType::VARCHAR, sketch_type);
    RegisterThetaAggregates<string_t>(sketch_agg, LogicalType::BLOB, sketch_type);

    CreateAggregateFunctionInfo agg_info(sketch_agg);
    {
        FunctionDescription desc;
        desc.description = "Creates a Theta Sketch from raw data.";
        desc.examples.push_back("datasketch_theta(column)");
        desc.examples.push_back("datasketch_theta(lg_k, column)");
        agg_info.descriptions.push_back(desc);
    }
    loader.RegisterFunction(agg_info);

    // 2. Register Scalar Functions

    // Intersection
    {
        ScalarFunctionSet intersect_set("datasketch_theta_intersect");
        intersect_set.AddFunction(ScalarFunction({sketch_type, sketch_type}, sketch_type, DSThetaIntersect));
        CreateScalarFunctionInfo intersect_info(std::move(intersect_set));
        loader.RegisterFunction(intersect_info);
    }

    // Difference (A NOT B)
    {
        ScalarFunctionSet diff_set("datasketch_theta_a_not_b");
        diff_set.AddFunction(ScalarFunction({sketch_type, sketch_type}, sketch_type, DSThetaANotB));
        CreateScalarFunctionInfo diff_info(std::move(diff_set));
        loader.RegisterFunction(diff_info);
    }

    // Estimate
    {
        ScalarFunctionSet est_set("datasketch_theta_estimate");
        est_set.AddFunction(ScalarFunction({sketch_type}, LogicalType::DOUBLE, DSThetaEstimate));
        CreateScalarFunctionInfo est_info(std::move(est_set));
        loader.RegisterFunction(est_info);
    }

	// Lower Bound
    {
        ScalarFunctionSet lb_set("datasketch_theta_lower_bound");
        lb_set.AddFunction(ScalarFunction({sketch_type, LogicalType::INTEGER}, LogicalType::DOUBLE, DSThetaLowerBound));
        CreateScalarFunctionInfo info(std::move(lb_set));
        loader.RegisterFunction(info);
    }

    // Upper Bound
    {
        ScalarFunctionSet ub_set("datasketch_theta_upper_bound");
        ub_set.AddFunction(ScalarFunction({sketch_type, LogicalType::INTEGER}, LogicalType::DOUBLE, DSThetaUpperBound));
        CreateScalarFunctionInfo info(std::move(ub_set));
        loader.RegisterFunction(info);
    }

    // Describe
    {
        ScalarFunctionSet desc_set("datasketch_theta_describe");
        desc_set.AddFunction(ScalarFunction({sketch_type}, LogicalType::VARCHAR, DSThetaDescribe));
        CreateScalarFunctionInfo info(std::move(desc_set));
        loader.RegisterFunction(info);
    }
}

} // namespace duckdb_datasketches

