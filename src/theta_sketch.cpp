#include "datasketches_extension.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/parsed_data/create_aggregate_function_info.hpp"

// Apache DataSketches Headers
#include <DataSketches/theta_sketch.hpp>
#include <DataSketches/theta_union.hpp>
#include <DataSketches/theta_intersection.hpp>
#include <DataSketches/theta_a_not_b.hpp>

using namespace duckdb;

namespace duckdb_datasketches {

// ============================================================
// 1. Helpers & Bind Data
// ============================================================

struct DSThetaBindData : public FunctionData {
    DSThetaBindData() : lg_k(12) {}
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

unique_ptr<FunctionData> DSThetaBindWithK(ClientContext &context, AggregateFunction &function,
                                          vector<unique_ptr<Expression>> &arguments) {
    if (arguments[0]->HasParameter()) throw ParameterNotResolvedException();
    if (!arguments[0]->IsFoldable()) throw BinderException("Theta Sketch lg_k must be constant");

    Value k_val = ExpressionExecutor::EvaluateScalar(context, *arguments[0]);
    if (k_val.IsNull()) throw BinderException("Theta Sketch lg_k cannot be NULL");

    auto lg_k = (uint8_t)k_val.GetValue<int32_t>();
    Function::EraseArgument(function, arguments, 0);
    return make_uniq<DSThetaBindData>(lg_k);
}

unique_ptr<FunctionData> DSThetaBindDefault(ClientContext &context, AggregateFunction &function,
                                            vector<unique_ptr<Expression>> &arguments) {
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
    template <class STATE> static void Initialize(STATE &state) {
        state.update_sketch = nullptr;
        state.union_sketch = nullptr;
    }
    template <class STATE> static void Destroy(STATE &state, AggregateInputData &) {
        if (state.update_sketch) delete state.update_sketch;
        if (state.union_sketch) delete state.union_sketch;
    }
    static bool IgnoreNull() { return true; }

    template <class STATE, class OP>
    static void Combine(const STATE &source, STATE &target, AggregateInputData &aggr_input_data) {
        if (!source.update_sketch && !source.union_sketch) return;

        if (!target.union_sketch) {
             auto &bind_data = aggr_input_data.bind_data->template Cast<DSThetaBindData>();
             target.CreateUnionSketch(bind_data.lg_k);
             if (target.update_sketch) {
                 target.union_sketch->update(*target.update_sketch);
                 delete target.update_sketch;
                 target.update_sketch = nullptr;
             }
        }
        if (source.update_sketch) target.union_sketch->update(*source.update_sketch);
        if (source.union_sketch) target.union_sketch->update(source.union_sketch->get_result());
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
             auto &bind_data = finalize_data.input.bind_data->template Cast<DSThetaBindData>();
             datasketches::update_theta_sketch::builder b;
             b.set_lg_k(bind_data.lg_k);
             auto empty_sketch = b.build();
             auto compact = empty_sketch.compact();
             auto serialized = compact.serialize();
             target = StringVector::AddStringOrBlob(finalize_data.result, std::string(serialized.begin(), serialized.end()));
        }
    }
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
};

struct DSThetaMergeOperation : DSThetaOperationBase {
    template <class A_TYPE, class STATE, class OP>
    static void Operation(STATE &state, const A_TYPE &a_data, AggregateUnaryInput &idata) {
        auto &bind_data = idata.input.bind_data->template Cast<DSThetaBindData>();
        state.CreateUnionSketch(bind_data.lg_k);
        auto sketch = datasketches::compact_theta_sketch::deserialize(a_data.GetDataUnsafe(), a_data.GetSize());
        state.union_sketch->update(sketch);
    }

    template <class INPUT_TYPE, class STATE, class OP>
    static void ConstantOperation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input, idx_t count) {
        for (idx_t i = 0; i < count; i++) {
            Operation<INPUT_TYPE, STATE, OP>(state, input, unary_input);
        }
    }
};

// ============================================================
// 3. Scalar Functions
// ============================================================

static void DSThetaUnion(DataChunk &args, ExpressionState &state, Vector &result) {
    BinaryExecutor::Execute<string_t, string_t, string_t>(
        args.data[0], args.data[1], result, args.size(),
        [&](string_t a_blob, string_t b_blob) {
            datasketches::theta_union::builder b;
            b.set_lg_k(12);
            auto union_obj = b.build();
            auto sketch_a = datasketches::compact_theta_sketch::deserialize(a_blob.GetDataUnsafe(), a_blob.GetSize());
            auto sketch_b = datasketches::compact_theta_sketch::deserialize(b_blob.GetDataUnsafe(), b_blob.GetSize());
            union_obj.update(sketch_a);
            union_obj.update(sketch_b);
            auto res = union_obj.get_result();
            auto serialized = res.serialize();
            return StringVector::AddStringOrBlob(result, std::string(serialized.begin(), serialized.end()));
        });
}

static void DSThetaIntersect(DataChunk &args, ExpressionState &state, Vector &result) {
    BinaryExecutor::Execute<string_t, string_t, string_t>(
        args.data[0], args.data[1], result, args.size(),
        [&](string_t a_blob, string_t b_blob) {
            auto sketch_a = datasketches::compact_theta_sketch::deserialize(a_blob.GetDataUnsafe(), a_blob.GetSize());
            auto sketch_b = datasketches::compact_theta_sketch::deserialize(b_blob.GetDataUnsafe(), b_blob.GetSize());
            datasketches::theta_intersection intersection;
            intersection.update(sketch_a);
            intersection.update(sketch_b);
            auto res = intersection.get_result();
            auto serialized = res.serialize();
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
            auto res = a_not_b.compute(sketch_a, sketch_b);
            auto serialized = res.serialize();
            return StringVector::AddStringOrBlob(result, std::string(serialized.begin(), serialized.end()));
        });
}

static void DSThetaEstimate(DataChunk &args, ExpressionState &state, Vector &result) {
    UnaryExecutor::Execute<string_t, double>(args.data[0], result, args.size(),
        [&](string_t sketch_blob) {
            return datasketches::compact_theta_sketch::deserialize(sketch_blob.GetDataUnsafe(), sketch_blob.GetSize()).get_estimate();
        });
}

static void DSThetaLowerBound(DataChunk &args, ExpressionState &state, Vector &result) {
    BinaryExecutor::Execute<string_t, int32_t, double>(args.data[0], args.data[1], result, args.size(),
        [&](string_t sketch_blob, int32_t num_std_devs) {
            return datasketches::compact_theta_sketch::deserialize(sketch_blob.GetDataUnsafe(), sketch_blob.GetSize()).get_lower_bound(static_cast<uint8_t>(num_std_devs));
        });
}

static void DSThetaUpperBound(DataChunk &args, ExpressionState &state, Vector &result) {
    BinaryExecutor::Execute<string_t, int32_t, double>(args.data[0], args.data[1], result, args.size(),
        [&](string_t sketch_blob, int32_t num_std_devs) {
            return datasketches::compact_theta_sketch::deserialize(sketch_blob.GetDataUnsafe(), sketch_blob.GetSize()).get_upper_bound(static_cast<uint8_t>(num_std_devs));
        });
}

static void DSThetaDescribe(DataChunk &args, ExpressionState &state, Vector &result) {
    UnaryExecutor::Execute<string_t, string_t>(args.data[0], result, args.size(),
        [&](string_t sketch_blob) {
            return StringVector::AddString(result, datasketches::compact_theta_sketch::deserialize(sketch_blob.GetDataUnsafe(), sketch_blob.GetSize()).to_string(false));
        });
}

// --- METADATA FUNCTIONS

static void DSThetaIsEmpty(DataChunk &args, ExpressionState &state, Vector &result) {
    UnaryExecutor::Execute<string_t, bool>(args.data[0], result, args.size(),
        [&](string_t sketch_blob) {
            return datasketches::compact_theta_sketch::deserialize(sketch_blob.GetDataUnsafe(), sketch_blob.GetSize()).is_empty();
        });
}

static void DSThetaIsEstimation(DataChunk &args, ExpressionState &state, Vector &result) {
    UnaryExecutor::Execute<string_t, bool>(args.data[0], result, args.size(),
        [&](string_t sketch_blob) {
            return datasketches::compact_theta_sketch::deserialize(sketch_blob.GetDataUnsafe(), sketch_blob.GetSize()).is_estimation_mode();
        });
}

static void DSThetaGetTheta(DataChunk &args, ExpressionState &state, Vector &result) {
    UnaryExecutor::Execute<string_t, double>(args.data[0], result, args.size(),
        [&](string_t sketch_blob) {
            return datasketches::compact_theta_sketch::deserialize(sketch_blob.GetDataUnsafe(), sketch_blob.GetSize()).get_theta();
        });
}

static void DSThetaNumRetained(DataChunk &args, ExpressionState &state, Vector &result) {
    UnaryExecutor::Execute<string_t, int64_t>(args.data[0], result, args.size(),
        [&](string_t sketch_blob) {
            return (int64_t)datasketches::compact_theta_sketch::deserialize(sketch_blob.GetDataUnsafe(), sketch_blob.GetSize()).get_num_retained();
        });
}

static void DSThetaGetSeed(DataChunk &args, ExpressionState &state, Vector &result) {
    // Note: Compact sketches typically store the Seed HASH, not the full seed.
    UnaryExecutor::Execute<string_t, int64_t>(args.data[0], result, args.size(),
        [&](string_t sketch_blob) {
            return (int64_t)datasketches::compact_theta_sketch::deserialize(sketch_blob.GetDataUnsafe(), sketch_blob.GetSize()).get_seed_hash();
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
    auto fun_default = AggregateFunction::UnaryAggregateDestructor<DSThetaState, T, string_t, DSThetaCreateOperation, AggregateDestructorType::LEGACY>(
        input_type, result_type);
    fun_default.bind = DSThetaBindDefault;
	fun_default.order_dependent = AggregateOrderDependent::NOT_ORDER_DEPENDENT;
    set.AddFunction(fun_default);

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
    AggregateFunctionSet sketch_agg("datasketch_theta");

    // 1. RAW DATA - Register specific types
    // IMPORTANT: DO NOT register LogicalType::BLOB here!
    // If we do, it shadows the Merge operation for "sketch_theta".
    RegisterThetaAggregates<int8_t>(sketch_agg, LogicalType::TINYINT, sketch_type);
    RegisterThetaAggregates<int16_t>(sketch_agg, LogicalType::SMALLINT, sketch_type);
    RegisterThetaAggregates<int32_t>(sketch_agg, LogicalType::INTEGER, sketch_type);
    RegisterThetaAggregates<int64_t>(sketch_agg, LogicalType::BIGINT, sketch_type);
    RegisterThetaAggregates<float>(sketch_agg, LogicalType::FLOAT, sketch_type);
    RegisterThetaAggregates<double>(sketch_agg, LogicalType::DOUBLE, sketch_type);
    RegisterThetaAggregates<string_t>(sketch_agg, LogicalType::VARCHAR, sketch_type);

    // 2. MERGE SKETCHES (sketch_theta / BLOB)
    auto fun_merge = AggregateFunction::UnaryAggregateDestructor<DSThetaState, string_t, string_t, DSThetaMergeOperation, AggregateDestructorType::LEGACY>(
        sketch_type, sketch_type);
    fun_merge.bind = DSThetaBindDefault;
    fun_merge.arguments = {sketch_type};
    sketch_agg.AddFunction(fun_merge);

    auto fun_merge_k = AggregateFunction::UnaryAggregateDestructor<DSThetaState, string_t, string_t, DSThetaMergeOperation, AggregateDestructorType::LEGACY>(
        sketch_type, sketch_type);
    fun_merge_k.bind = DSThetaBindWithK;
    fun_merge_k.arguments = {LogicalType::INTEGER, sketch_type};
    sketch_agg.AddFunction(fun_merge_k);

    loader.RegisterFunction(CreateAggregateFunctionInfo(sketch_agg));

    // --- SCALAR FUNCTIONS ---
    loader.RegisterFunction(CreateScalarFunctionInfo(ScalarFunction("datasketch_theta_intersect", {sketch_type, sketch_type}, sketch_type, DSThetaIntersect)));
    loader.RegisterFunction(CreateScalarFunctionInfo(ScalarFunction("datasketch_theta_union", {sketch_type, sketch_type}, sketch_type, DSThetaUnion)));
    loader.RegisterFunction(CreateScalarFunctionInfo(ScalarFunction("datasketch_theta_a_not_b", {sketch_type, sketch_type}, sketch_type, DSThetaANotB)));
    loader.RegisterFunction(CreateScalarFunctionInfo(ScalarFunction("datasketch_theta_estimate", {sketch_type}, LogicalType::DOUBLE, DSThetaEstimate)));
    loader.RegisterFunction(CreateScalarFunctionInfo(ScalarFunction("datasketch_theta_lower_bound", {sketch_type, LogicalType::INTEGER}, LogicalType::DOUBLE, DSThetaLowerBound)));
    loader.RegisterFunction(CreateScalarFunctionInfo(ScalarFunction("datasketch_theta_upper_bound", {sketch_type, LogicalType::INTEGER}, LogicalType::DOUBLE, DSThetaUpperBound)));
    loader.RegisterFunction(CreateScalarFunctionInfo(ScalarFunction("datasketch_theta_describe", {sketch_type}, LogicalType::VARCHAR, DSThetaDescribe)));

    // Metadata
    loader.RegisterFunction(CreateScalarFunctionInfo(ScalarFunction("datasketch_theta_is_empty", {sketch_type}, LogicalType::BOOLEAN, DSThetaIsEmpty)));
    loader.RegisterFunction(CreateScalarFunctionInfo(ScalarFunction("datasketch_theta_is_estimation_mode", {sketch_type}, LogicalType::BOOLEAN, DSThetaIsEstimation)));
    loader.RegisterFunction(CreateScalarFunctionInfo(ScalarFunction("datasketch_theta_get_theta", {sketch_type}, LogicalType::DOUBLE, DSThetaGetTheta)));
    loader.RegisterFunction(CreateScalarFunctionInfo(ScalarFunction("datasketch_theta_num_retained", {sketch_type}, LogicalType::BIGINT, DSThetaNumRetained)));
    loader.RegisterFunction(CreateScalarFunctionInfo(ScalarFunction("datasketch_theta_get_seed", {sketch_type}, LogicalType::BIGINT, DSThetaGetSeed)));
}

} // namespace duckdb_datasketches



