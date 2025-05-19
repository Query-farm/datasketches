#include "datasketches_extension.hpp"

#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/parsed_data/create_aggregate_function_info.hpp"
#include "duckdb/function/scalar_function.hpp"

#include <DataSketches/quantiles_sketch.hpp>
#include <DataSketches/kll_sketch.hpp>
#include <DataSketches/req_sketch.hpp>
#include <DataSketches/tdigest.hpp>
#include <DataSketches/hll.hpp>
#include <DataSketches/cpc_sketch.hpp>
#include <DataSketches/cpc_union.hpp>

using namespace duckdb;
namespace duckdb_datasketches
{

    static std::string toLowerCase(const std::string &input)
    {
        std::string result = input;
        std::transform(result.begin(), result.end(), result.begin(), [](unsigned char c)
                       { return std::tolower(c); });
        return result;
    }

    struct DSQuantilesBindData : public FunctionData
    {
        DSQuantilesBindData()
        {
        }
        explicit DSQuantilesBindData(int32_t k) : k(k)
        {
        }

        unique_ptr<FunctionData> Copy() const override
        {
            return make_uniq<DSQuantilesBindData>(k);
        }

        bool Equals(const FunctionData &other_p) const override
        {
            auto &other = other_p.Cast<DSQuantilesBindData>();
            return k == other.k;
        }

        int32_t k;
    };

    unique_ptr<FunctionData> DSQuantilesBind(ClientContext &context, AggregateFunction &function,
                                             vector<unique_ptr<Expression>> &arguments)
    {
        if (arguments[0]->HasParameter())
        {
            throw ParameterNotResolvedException();
        }
        if (!arguments[0]->IsFoldable())
        {
            throw BinderException("Quantiles can only take a constant K value");
        }
        Value k_val = ExpressionExecutor::EvaluateScalar(context, *arguments[0]);
        if (k_val.IsNull())
        {
            throw BinderException("Quantiles K value cannot be NULL");
        }

        auto actual_k = k_val.GetValue<int32_t>();

        Function::EraseArgument(function, arguments, 0);
        return make_uniq<DSQuantilesBindData>(actual_k);
    }

    template <class T>

    struct DSQuantilesState
    {

        datasketches::quantiles_sketch<T> *sketch = nullptr;

        ~DSQuantilesState()
        {
            if (sketch)
            {
                delete sketch;
            }
        }

        void CreateSketch(int32_t k)
        {
            D_ASSERT(!sketch);
            D_ASSERT(k > 0);
            D_ASSERT(k <= 32768);
            sketch = new datasketches::quantiles_sketch<T>(k);
        }

        void CreateSketch(const DSQuantilesState &existing)
        {
            if (existing.sketch)
            {

                sketch = new datasketches::quantiles_sketch<T>(*existing.sketch);
            }
        }

        datasketches::quantiles_sketch<T> deserialize_sketch(const string_t &data)
        {
            return datasketches::quantiles_sketch<T>::deserialize(data.GetDataUnsafe(), data.GetSize());
        }
    };

    static LogicalType CreateQuantilesSketchType(DatabaseInstance &instance, LogicalType embedded_type)
    {
        auto new_type = LogicalType(LogicalTypeId::BLOB);
        auto type_suffix = toLowerCase(embedded_type.ToString());
        auto new_type_name = "sketch_quantiles_" + type_suffix;
        new_type.SetAlias(new_type_name);
        auto type_info = CreateTypeInfo(new_type_name, new_type);
        type_info.temporary = false;
        type_info.internal = true;
        type_info.comment = "Sketch type for Quantiles sketch with embedded type " + embedded_type.ToString();
        auto &system_catalog = Catalog::GetSystemCatalog(instance);
        auto data = CatalogTransaction::GetSystemTransaction(instance);
        system_catalog.CreateType(data, type_info);
        ExtensionUtil::RegisterCastFunction(instance, LogicalType::BLOB, new_type, DefaultCasts::ReinterpretCast, 1);
        ExtensionUtil::RegisterCastFunction(instance, new_type, LogicalType::BLOB, DefaultCasts::ReinterpretCast, 1);
        return new_type;
    }

    struct DSKLLBindData : public FunctionData
    {
        DSKLLBindData()
        {
        }
        explicit DSKLLBindData(int32_t k) : k(k)
        {
        }

        unique_ptr<FunctionData> Copy() const override
        {
            return make_uniq<DSKLLBindData>(k);
        }

        bool Equals(const FunctionData &other_p) const override
        {
            auto &other = other_p.Cast<DSKLLBindData>();
            return k == other.k;
        }

        int32_t k;
    };

    unique_ptr<FunctionData> DSKLLBind(ClientContext &context, AggregateFunction &function,
                                       vector<unique_ptr<Expression>> &arguments)
    {
        if (arguments[0]->HasParameter())
        {
            throw ParameterNotResolvedException();
        }
        if (!arguments[0]->IsFoldable())
        {
            throw BinderException("KLL can only take a constant K value");
        }
        Value k_val = ExpressionExecutor::EvaluateScalar(context, *arguments[0]);
        if (k_val.IsNull())
        {
            throw BinderException("KLL K value cannot be NULL");
        }

        auto actual_k = k_val.GetValue<int32_t>();

        Function::EraseArgument(function, arguments, 0);
        return make_uniq<DSKLLBindData>(actual_k);
    }

    template <class T>

    struct DSKLLState
    {

        datasketches::kll_sketch<T> *sketch = nullptr;

        ~DSKLLState()
        {
            if (sketch)
            {
                delete sketch;
            }
        }

        void CreateSketch(int32_t k)
        {
            D_ASSERT(!sketch);
            D_ASSERT(k > 0);
            D_ASSERT(k <= 32768);
            sketch = new datasketches::kll_sketch<T>(k);
        }

        void CreateSketch(const DSKLLState &existing)
        {
            if (existing.sketch)
            {

                sketch = new datasketches::kll_sketch<T>(*existing.sketch);
            }
        }

        datasketches::kll_sketch<T> deserialize_sketch(const string_t &data)
        {
            return datasketches::kll_sketch<T>::deserialize(data.GetDataUnsafe(), data.GetSize());
        }
    };

    static LogicalType CreateKLLSketchType(DatabaseInstance &instance, LogicalType embedded_type)
    {
        auto new_type = LogicalType(LogicalTypeId::BLOB);
        auto type_suffix = toLowerCase(embedded_type.ToString());
        auto new_type_name = "sketch_kll_" + type_suffix;
        new_type.SetAlias(new_type_name);
        auto type_info = CreateTypeInfo(new_type_name, new_type);
        type_info.temporary = false;
        type_info.internal = true;
        type_info.comment = "Sketch type for KLL sketch with embedded type " + embedded_type.ToString();
        auto &system_catalog = Catalog::GetSystemCatalog(instance);
        auto data = CatalogTransaction::GetSystemTransaction(instance);
        system_catalog.CreateType(data, type_info);
        ExtensionUtil::RegisterCastFunction(instance, LogicalType::BLOB, new_type, DefaultCasts::ReinterpretCast, 1);
        ExtensionUtil::RegisterCastFunction(instance, new_type, LogicalType::BLOB, DefaultCasts::ReinterpretCast, 1);
        return new_type;
    }

    struct DSREQBindData : public FunctionData
    {
        DSREQBindData()
        {
        }
        explicit DSREQBindData(int32_t k) : k(k)
        {
        }

        unique_ptr<FunctionData> Copy() const override
        {
            return make_uniq<DSREQBindData>(k);
        }

        bool Equals(const FunctionData &other_p) const override
        {
            auto &other = other_p.Cast<DSREQBindData>();
            return k == other.k;
        }

        int32_t k;
    };

    unique_ptr<FunctionData> DSREQBind(ClientContext &context, AggregateFunction &function,
                                       vector<unique_ptr<Expression>> &arguments)
    {
        if (arguments[0]->HasParameter())
        {
            throw ParameterNotResolvedException();
        }
        if (!arguments[0]->IsFoldable())
        {
            throw BinderException("REQ can only take a constant K value");
        }
        Value k_val = ExpressionExecutor::EvaluateScalar(context, *arguments[0]);
        if (k_val.IsNull())
        {
            throw BinderException("REQ K value cannot be NULL");
        }

        auto actual_k = k_val.GetValue<int32_t>();

        Function::EraseArgument(function, arguments, 0);
        return make_uniq<DSREQBindData>(actual_k);
    }

    template <class T>

    struct DSREQState
    {

        datasketches::req_sketch<T> *sketch = nullptr;

        ~DSREQState()
        {
            if (sketch)
            {
                delete sketch;
            }
        }

        void CreateSketch(int32_t k)
        {
            D_ASSERT(!sketch);
            D_ASSERT(k >= 4);
            D_ASSERT(k <= 1024);
            sketch = new datasketches::req_sketch<T>(k);
        }

        void CreateSketch(const DSREQState &existing)
        {
            if (existing.sketch)
            {

                sketch = new datasketches::req_sketch<T>(*existing.sketch);
            }
        }

        datasketches::req_sketch<T> deserialize_sketch(const string_t &data)
        {
            return datasketches::req_sketch<T>::deserialize(data.GetDataUnsafe(), data.GetSize());
        }
    };

    static LogicalType CreateREQSketchType(DatabaseInstance &instance, LogicalType embedded_type)
    {
        auto new_type = LogicalType(LogicalTypeId::BLOB);
        auto type_suffix = toLowerCase(embedded_type.ToString());
        auto new_type_name = "sketch_req_" + type_suffix;
        new_type.SetAlias(new_type_name);
        auto type_info = CreateTypeInfo(new_type_name, new_type);
        type_info.temporary = false;
        type_info.internal = true;
        type_info.comment = "Sketch type for REQ sketch with embedded type " + embedded_type.ToString();
        auto &system_catalog = Catalog::GetSystemCatalog(instance);
        auto data = CatalogTransaction::GetSystemTransaction(instance);
        system_catalog.CreateType(data, type_info);
        ExtensionUtil::RegisterCastFunction(instance, LogicalType::BLOB, new_type, DefaultCasts::ReinterpretCast, 1);
        ExtensionUtil::RegisterCastFunction(instance, new_type, LogicalType::BLOB, DefaultCasts::ReinterpretCast, 1);
        return new_type;
    }

    struct DSTDigestBindData : public FunctionData
    {
        DSTDigestBindData()
        {
        }
        explicit DSTDigestBindData(int32_t k) : k(k)
        {
        }

        unique_ptr<FunctionData> Copy() const override
        {
            return make_uniq<DSTDigestBindData>(k);
        }

        bool Equals(const FunctionData &other_p) const override
        {
            auto &other = other_p.Cast<DSTDigestBindData>();
            return k == other.k;
        }

        int32_t k;
    };

    unique_ptr<FunctionData> DSTDigestBind(ClientContext &context, AggregateFunction &function,
                                           vector<unique_ptr<Expression>> &arguments)
    {
        if (arguments[0]->HasParameter())
        {
            throw ParameterNotResolvedException();
        }
        if (!arguments[0]->IsFoldable())
        {
            throw BinderException("TDigest can only take a constant K value");
        }
        Value k_val = ExpressionExecutor::EvaluateScalar(context, *arguments[0]);
        if (k_val.IsNull())
        {
            throw BinderException("TDigest K value cannot be NULL");
        }

        auto actual_k = k_val.GetValue<int32_t>();

        Function::EraseArgument(function, arguments, 0);
        return make_uniq<DSTDigestBindData>(actual_k);
    }

    template <class T>

    struct DSTDigestState
    {

        datasketches::tdigest<T> *sketch = nullptr;

        ~DSTDigestState()
        {
            if (sketch)
            {
                delete sketch;
            }
        }

        void CreateSketch(uint16_t k)
        {
            D_ASSERT(!sketch);
            sketch = new datasketches::tdigest<T>(k);
        }

        void CreateSketch(const DSTDigestState &existing)
        {
            if (existing.sketch)
            {

                sketch = new datasketches::tdigest<T>(*existing.sketch);
            }
        }

        datasketches::tdigest<T> deserialize_sketch(const string_t &data)
        {
            return datasketches::tdigest<T>::deserialize(data.GetDataUnsafe(), data.GetSize());
        }
    };

    static LogicalType CreateTDigestSketchType(DatabaseInstance &instance, LogicalType embedded_type)
    {
        auto new_type = LogicalType(LogicalTypeId::BLOB);
        auto type_suffix = toLowerCase(embedded_type.ToString());
        auto new_type_name = "sketch_tdigest_" + type_suffix;
        new_type.SetAlias(new_type_name);
        auto type_info = CreateTypeInfo(new_type_name, new_type);
        type_info.temporary = false;
        type_info.internal = true;
        type_info.comment = "Sketch type for TDigest sketch with embedded type " + embedded_type.ToString();
        auto &system_catalog = Catalog::GetSystemCatalog(instance);
        auto data = CatalogTransaction::GetSystemTransaction(instance);
        system_catalog.CreateType(data, type_info);
        ExtensionUtil::RegisterCastFunction(instance, LogicalType::BLOB, new_type, DefaultCasts::ReinterpretCast, 1);
        ExtensionUtil::RegisterCastFunction(instance, new_type, LogicalType::BLOB, DefaultCasts::ReinterpretCast, 1);
        return new_type;
    }

    struct DSHLLBindData : public FunctionData
    {
        DSHLLBindData()
        {
        }
        explicit DSHLLBindData(int32_t k) : k(k)
        {
        }

        unique_ptr<FunctionData> Copy() const override
        {
            return make_uniq<DSHLLBindData>(k);
        }

        bool Equals(const FunctionData &other_p) const override
        {
            auto &other = other_p.Cast<DSHLLBindData>();
            return k == other.k;
        }

        int32_t k;
    };

    unique_ptr<FunctionData> DSHLLBind(ClientContext &context, AggregateFunction &function,
                                       vector<unique_ptr<Expression>> &arguments)
    {
        if (arguments[0]->HasParameter())
        {
            throw ParameterNotResolvedException();
        }
        if (!arguments[0]->IsFoldable())
        {
            throw BinderException("HLL can only take a constant K value");
        }
        Value k_val = ExpressionExecutor::EvaluateScalar(context, *arguments[0]);
        if (k_val.IsNull())
        {
            throw BinderException("HLL K value cannot be NULL");
        }

        auto actual_k = k_val.GetValue<int32_t>();

        Function::EraseArgument(function, arguments, 0);
        return make_uniq<DSHLLBindData>(actual_k);
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

        void CreateSketch(uint16_t k)
        {
            D_ASSERT(!sketch);
            sketch = new datasketches::hll_sketch(k);
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
            return datasketches::hll_sketch::deserialize(data.GetDataUnsafe(), data.GetSize());
        }
    };

    static LogicalType CreateHLLCountingSketchType(DatabaseInstance &instance)
    {
        auto new_type = LogicalType(LogicalTypeId::BLOB);
        auto new_type_name = "sketch_hll";
        auto type_info = CreateTypeInfo(new_type_name, LogicalType::BLOB);
        type_info.temporary = false;
        type_info.internal = true;
        type_info.comment = "Sketch type for HLL sketch";
        new_type.SetAlias(new_type_name);
        auto &system_catalog = Catalog::GetSystemCatalog(instance);
        auto data = CatalogTransaction::GetSystemTransaction(instance);
        system_catalog.CreateType(data, type_info);
        ExtensionUtil::RegisterCastFunction(instance, LogicalType::BLOB, new_type, DefaultCasts::ReinterpretCast, 1);
        ExtensionUtil::RegisterCastFunction(instance, new_type, LogicalType::BLOB, DefaultCasts::ReinterpretCast, 1);
        return new_type;
    }

    struct DSCPCBindData : public FunctionData
    {
        DSCPCBindData()
        {
        }
        explicit DSCPCBindData(int32_t k) : k(k)
        {
        }

        unique_ptr<FunctionData> Copy() const override
        {
            return make_uniq<DSCPCBindData>(k);
        }

        bool Equals(const FunctionData &other_p) const override
        {
            auto &other = other_p.Cast<DSCPCBindData>();
            return k == other.k;
        }

        int32_t k;
    };

    unique_ptr<FunctionData> DSCPCBind(ClientContext &context, AggregateFunction &function,
                                       vector<unique_ptr<Expression>> &arguments)
    {
        if (arguments[0]->HasParameter())
        {
            throw ParameterNotResolvedException();
        }
        if (!arguments[0]->IsFoldable())
        {
            throw BinderException("CPC can only take a constant K value");
        }
        Value k_val = ExpressionExecutor::EvaluateScalar(context, *arguments[0]);
        if (k_val.IsNull())
        {
            throw BinderException("CPC K value cannot be NULL");
        }

        auto actual_k = k_val.GetValue<int32_t>();

        Function::EraseArgument(function, arguments, 0);
        return make_uniq<DSCPCBindData>(actual_k);
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

        void CreateSketch(uint8_t k)
        {
            D_ASSERT(!sketch);
            sketch = new datasketches::cpc_sketch(k);
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
            return datasketches::cpc_sketch::deserialize(data.GetDataUnsafe(), data.GetSize());
        }
    };

    static LogicalType CreateCPCCountingSketchType(DatabaseInstance &instance)
    {
        auto new_type = LogicalType(LogicalTypeId::BLOB);
        auto new_type_name = "sketch_cpc";
        auto type_info = CreateTypeInfo(new_type_name, LogicalType::BLOB);
        type_info.temporary = false;
        type_info.internal = true;
        type_info.comment = "Sketch type for CPC sketch";
        new_type.SetAlias(new_type_name);
        auto &system_catalog = Catalog::GetSystemCatalog(instance);
        auto data = CatalogTransaction::GetSystemTransaction(instance);
        system_catalog.CreateType(data, type_info);
        ExtensionUtil::RegisterCastFunction(instance, LogicalType::BLOB, new_type, DefaultCasts::ReinterpretCast, 1);
        ExtensionUtil::RegisterCastFunction(instance, new_type, LogicalType::BLOB, DefaultCasts::ReinterpretCast, 1);
        return new_type;
    }

    struct DSSketchOperationBase
    {
        template <class STATE>
        static void Initialize(STATE &state)
        {
            state.sketch = nullptr;
        }

        template <class STATE>
        static void Destroy(STATE &state, AggregateInputData &aggr_input_data)
        {
            if (state.sketch)
            {
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
            if (!state.sketch)
            {
                auto &bind_data = idata.input.bind_data->template Cast<BIND_DATA_TYPE>();
                state.CreateSketch(bind_data.k);
            }

            // this is a sketch in b_data, so we need to deserialize it.
            state.sketch->merge(state.deserialize_sketch(a_data));
        }

        template <class INPUT_TYPE, class STATE, class OP>
        static void ConstantOperation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input,
                                      idx_t count)
        {
            for (idx_t i = 0; i < count; i++)
            {
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
                target.sketch->merge(*source.sketch);
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
                state.CreateSketch(bind_data.k);
            }

            state.sketch->update(a_data);
        }

        template <class INPUT_TYPE, class STATE, class OP>
        static void ConstantOperation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input,
                                      idx_t count)
        {
            for (idx_t i = 0; i < count; i++)
            {
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
                target.sketch->merge(*source.sketch);
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
                state.CreateSketch(bind_data.k);
            }

            if constexpr (std::is_same_v<A_TYPE, duckdb::string_t>)
            {
                state.sketch->update(a_data.GetData(), a_data.GetSize());
            }
            else
            {
                state.sketch->update(a_data);
            }
        }

        template <class INPUT_TYPE, class STATE, class OP>
        static void ConstantOperation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input,
                                      idx_t count)
        {
            for (idx_t i = 0; i < count; i++)
            {
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
                datasketches::hll_union u(target.sketch->get_lg_config_k());
                u.update(*target.sketch);
                if (source.sketch)
                {
                    u.update(*source.sketch);
                }
                *target.sketch = u.get_result(datasketches::target_hll_type::HLL_4);
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
                state.CreateSketch(bind_data.k);
            }

            auto a_sketch = state.deserialize_sketch(a_data);

            datasketches::hll_union u(bind_data.k);
            if (state.sketch)
            {
                u.update(*state.sketch);
            }
            u.update(a_sketch);

            *state.sketch = u.get_result(datasketches::target_hll_type::HLL_4);
        }

        template <class INPUT_TYPE, class STATE, class OP>
        static void ConstantOperation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input,
                                      idx_t count)
        {
            for (idx_t i = 0; i < count; i++)
            {
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
                datasketches::hll_union u(target.sketch->get_lg_config_k());
                if (source.sketch)
                {
                    u.update(*source.sketch);
                }
                u.update(*target.sketch);

                *target.sketch = u.get_result(datasketches::target_hll_type::HLL_4);
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
                state.CreateSketch(bind_data.k);
            }

            auto a_sketch = state.deserialize_sketch(a_data);
            datasketches::cpc_union u(bind_data.k);
            if (state.sketch)
            {
                u.update(*state.sketch);
            }
            u.update(a_sketch);

            *state.sketch = u.get_result();
        }

        template <class INPUT_TYPE, class STATE, class OP>
        static void ConstantOperation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input,
                                      idx_t count)
        {
            for (idx_t i = 0; i < count; i++)
            {
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
                if (source.sketch)
                {
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
                state.CreateSketch(bind_data.k);
            }

            if constexpr (std::is_same_v<A_TYPE, duckdb::string_t>)
            {
                state.sketch->update(a_data.GetData(), a_data.GetSize());
            }
            else
            {
                state.sketch->update(a_data);
            }
        }

        template <class INPUT_TYPE, class STATE, class OP>
        static void ConstantOperation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input,
                                      idx_t count)
        {
            for (idx_t i = 0; i < count; i++)
            {
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
                if (source.sketch)
                {
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

        UnaryExecutor::Execute<string_t, bool>(
            sketch_vector, result, args.size(),
            [&](string_t sketch_data)
            {
                auto sketch = datasketches::quantiles_sketch<T>::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
                return sketch.is_empty();
            });
    }

    template <class T>

    static inline void DSQuantilesk(DataChunk &args, ExpressionState &state, Vector &result)
    {
        // Get the references to the incoming vectors.
        D_ASSERT(args.ColumnCount() == 1);

        auto &sketch_vector = args.data[0];

        UnaryExecutor::Execute<string_t, uint16_t>(
            sketch_vector, result, args.size(),
            [&](string_t sketch_data)
            {
                auto sketch = datasketches::quantiles_sketch<T>::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
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

        UnifiedVectorFormat unified_split_points;
        split_points_vector.ToUnifiedFormat(args.size(), unified_split_points);

        // auto split_points_list_entries = UnifiedVectorFormat::GetData<list_entry_t>(unified_split_points);
        //         auto split_points_validitiy = FlatVector::Validity(split_points_vector);

        auto &split_points_list_children = ListVector::GetEntry(split_points_vector);

        UnifiedVectorFormat split_points_children_unified;
        split_points_list_children.ToUnifiedFormat(args.size(), split_points_children_unified);

        const T *split_points_list_children_data = UnifiedVectorFormat::GetData<T>(split_points_children_unified);

        TernaryExecutor::Execute<string_t, list_entry_t, bool, list_entry_t>(
            sketch_vector, split_points_vector, inclusive_vector, result, args.size(),
            [&](string_t sketch_data, list_entry_t split_points_data, bool inclusive_data)
            {
                auto sketch = datasketches::quantiles_sketch<T>::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());

                T *passing_points = (T *)malloc(sizeof(T) * split_points_data.length);
                for (idx_t i = 0; i < split_points_data.length; i++)
                {
                    passing_points[i] = split_points_list_children_data[i + split_points_data.offset];
                }

                auto cdf_result = sketch.get_CDF(passing_points, split_points_data.length, inclusive_data);
                free(passing_points);
                auto current_size = ListVector::GetListSize(result);
                auto new_size = current_size + cdf_result.size();
                if (ListVector::GetListCapacity(result) < new_size)
                {
                    ListVector::Reserve(result, new_size);
                }

                auto &child_entry = ListVector::GetEntry(result);
                auto child_vals = FlatVector::GetData<T>(child_entry);
                // auto &child_validity = FlatVector::Validity(child_entry);
                for (idx_t i = 0; i < cdf_result.size(); i++)
                {
                    child_vals[current_size + i] = cdf_result[i];
                }
                ListVector::SetListSize(result, new_size);
                return list_entry_t{current_size, cdf_result.size()};
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

        UnifiedVectorFormat unified_split_points;
        split_points_vector.ToUnifiedFormat(args.size(), unified_split_points);

        // auto split_points_list_entries = UnifiedVectorFormat::GetData<list_entry_t>(unified_split_points);
        //         auto split_points_validitiy = FlatVector::Validity(split_points_vector);

        auto &split_points_list_children = ListVector::GetEntry(split_points_vector);

        UnifiedVectorFormat split_points_children_unified;
        split_points_list_children.ToUnifiedFormat(args.size(), split_points_children_unified);

        const T *split_points_list_children_data = UnifiedVectorFormat::GetData<T>(split_points_children_unified);

        TernaryExecutor::Execute<string_t, list_entry_t, bool, list_entry_t>(
            sketch_vector, split_points_vector, inclusive_vector, result, args.size(),
            [&](string_t sketch_data, list_entry_t split_points_data, bool inclusive_data)
            {
                auto sketch = datasketches::quantiles_sketch<T>::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());

                T *passing_points = (T *)malloc(sizeof(T) * split_points_data.length);
                for (idx_t i = 0; i < split_points_data.length; i++)
                {
                    passing_points[i] = split_points_list_children_data[i + split_points_data.offset];
                }

                auto pmf_result = sketch.get_PMF(passing_points, split_points_data.length, inclusive_data);
                free(passing_points);

                auto current_size = ListVector::GetListSize(result);
                auto new_size = current_size + pmf_result.size();
                if (ListVector::GetListCapacity(result) < new_size)
                {
                    ListVector::Reserve(result, new_size);
                }

                auto &child_entry = ListVector::GetEntry(result);
                auto child_vals = FlatVector::GetData<double>(child_entry);
                // auto &child_validity = FlatVector::Validity(child_entry);
                for (idx_t i = 0; i < pmf_result.size(); i++)
                {
                    child_vals[current_size + i] = pmf_result[i];
                }
                ListVector::SetListSize(result, new_size);
                return list_entry_t{current_size, pmf_result.size()};
            });
    }

    template <class T>

    static inline void DSQuantilesnormalized_rank_error(DataChunk &args, ExpressionState &state, Vector &result)
    {
        // Get the references to the incoming vectors.
        D_ASSERT(args.ColumnCount() == 2);

        auto &sketch_vector = args.data[0];
        auto &is_pmf_vector = args.data[1];

        BinaryExecutor::Execute<string_t, bool, double>(
            sketch_vector, is_pmf_vector, result, args.size(),
            [&](string_t sketch_data, bool is_pmf_data)
            {
                auto sketch = datasketches::quantiles_sketch<T>::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
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

        TernaryExecutor::Execute<string_t, bool, bool, string_t>(
            sketch_vector, include_levels_vector, include_items_vector, result, args.size(),
            [&](string_t sketch_data, bool include_levels_data, bool include_items_data)
            {
                auto sketch = datasketches::quantiles_sketch<T>::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
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

        TernaryExecutor::Execute<string_t, T, bool, double>(
            sketch_vector, item_vector, inclusive_vector, result, args.size(),
            [&](string_t sketch_data, T item_data, bool inclusive_data)
            {
                auto sketch = datasketches::quantiles_sketch<T>::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
                return sketch.get_rank(item_data, inclusive_data);
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

        TernaryExecutor::Execute<string_t, double, bool, T>(
            sketch_vector, rank_vector, inclusive_vector, result, args.size(),
            [&](string_t sketch_data, double rank_data, bool inclusive_data)
            {
                auto sketch = datasketches::quantiles_sketch<T>::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
                return sketch.get_quantile(rank_data, inclusive_data);
            });
    }

    template <class T>

    static inline void DSQuantilesn(DataChunk &args, ExpressionState &state, Vector &result)
    {
        // Get the references to the incoming vectors.
        D_ASSERT(args.ColumnCount() == 1);

        auto &sketch_vector = args.data[0];

        UnaryExecutor::Execute<string_t, uint64_t>(
            sketch_vector, result, args.size(),
            [&](string_t sketch_data)
            {
                auto sketch = datasketches::quantiles_sketch<T>::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
                return sketch.get_n();
            });
    }

    template <class T>

    static inline void DSQuantilesis_estimation_mode(DataChunk &args, ExpressionState &state, Vector &result)
    {
        // Get the references to the incoming vectors.
        D_ASSERT(args.ColumnCount() == 1);

        auto &sketch_vector = args.data[0];

        UnaryExecutor::Execute<string_t, bool>(
            sketch_vector, result, args.size(),
            [&](string_t sketch_data)
            {
                auto sketch = datasketches::quantiles_sketch<T>::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
                return sketch.is_estimation_mode();
            });
    }

    template <class T>

    static inline void DSQuantilesnum_retained(DataChunk &args, ExpressionState &state, Vector &result)
    {
        // Get the references to the incoming vectors.
        D_ASSERT(args.ColumnCount() == 1);

        auto &sketch_vector = args.data[0];

        UnaryExecutor::Execute<string_t, uint64_t>(
            sketch_vector, result, args.size(),
            [&](string_t sketch_data)
            {
                auto sketch = datasketches::quantiles_sketch<T>::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
                return sketch.get_num_retained();
            });
    }

    template <class T>

    static inline void DSQuantilesmin_item(DataChunk &args, ExpressionState &state, Vector &result)
    {
        // Get the references to the incoming vectors.
        D_ASSERT(args.ColumnCount() == 1);

        auto &sketch_vector = args.data[0];

        UnaryExecutor::Execute<string_t, T>(
            sketch_vector, result, args.size(),
            [&](string_t sketch_data)
            {
                auto sketch = datasketches::quantiles_sketch<T>::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
                return sketch.get_min_item();
            });
    }

    template <class T>

    static inline void DSQuantilesmax_item(DataChunk &args, ExpressionState &state, Vector &result)
    {
        // Get the references to the incoming vectors.
        D_ASSERT(args.ColumnCount() == 1);

        auto &sketch_vector = args.data[0];

        UnaryExecutor::Execute<string_t, T>(
            sketch_vector, result, args.size(),
            [&](string_t sketch_data)
            {
                auto sketch = datasketches::quantiles_sketch<T>::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
                return sketch.get_max_item();
            });
    }

    template <typename T>
    auto static DSQuantilesMergeAggregate(const LogicalType &type, const LogicalType &result_type) -> AggregateFunction

    {

        return AggregateFunction::UnaryAggregateDestructor<DSQuantilesState<T>, string_t, string_t, DSQuantilesMergeOperation<DSQuantilesBindData>, AggregateDestructorType::LEGACY>(
            result_type, result_type);
    }

    template <typename T>
    auto static DSQuantilesCreateAggregate(const LogicalType &type, const LogicalType &result_type) -> AggregateFunction
    {

        return AggregateFunction::UnaryAggregateDestructor<DSQuantilesState<T>, T, string_t, DSQuantilesCreateOperation<DSQuantilesBindData>, AggregateDestructorType::LEGACY>(
            type, result_type);
    }

    void LoadQuantilesSketch(DatabaseInstance &instance)
    {
        auto &system_catalog = Catalog::GetSystemCatalog(instance);
        auto data = CatalogTransaction::GetSystemTransaction(instance);

        std::unordered_map<LogicalTypeId, LogicalType> sketch_map_types;

        sketch_map_types.insert({LogicalTypeId::TINYINT, CreateQuantilesSketchType(instance, LogicalType(LogicalTypeId::TINYINT))});
        sketch_map_types.insert({LogicalTypeId::SMALLINT, CreateQuantilesSketchType(instance, LogicalType(LogicalTypeId::SMALLINT))});
        sketch_map_types.insert({LogicalTypeId::INTEGER, CreateQuantilesSketchType(instance, LogicalType(LogicalTypeId::INTEGER))});
        sketch_map_types.insert({LogicalTypeId::BIGINT, CreateQuantilesSketchType(instance, LogicalType(LogicalTypeId::BIGINT))});
        sketch_map_types.insert({LogicalTypeId::FLOAT, CreateQuantilesSketchType(instance, LogicalType(LogicalTypeId::FLOAT))});
        sketch_map_types.insert({LogicalTypeId::DOUBLE, CreateQuantilesSketchType(instance, LogicalType(LogicalTypeId::DOUBLE))});
        sketch_map_types.insert({LogicalTypeId::UTINYINT, CreateQuantilesSketchType(instance, LogicalType(LogicalTypeId::UTINYINT))});
        sketch_map_types.insert({LogicalTypeId::USMALLINT, CreateQuantilesSketchType(instance, LogicalType(LogicalTypeId::USMALLINT))});
        sketch_map_types.insert({LogicalTypeId::UINTEGER, CreateQuantilesSketchType(instance, LogicalType(LogicalTypeId::UINTEGER))});
        sketch_map_types.insert({LogicalTypeId::UBIGINT, CreateQuantilesSketchType(instance, LogicalType(LogicalTypeId::UBIGINT))});

        {
            ScalarFunctionSet fs("datasketch_quantiles_is_empty");

            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::TINYINT]}, LogicalType::BOOLEAN, DSQuantilesis_empty<int8_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::SMALLINT]}, LogicalType::BOOLEAN, DSQuantilesis_empty<int16_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::INTEGER]}, LogicalType::BOOLEAN, DSQuantilesis_empty<int32_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::BIGINT]}, LogicalType::BOOLEAN, DSQuantilesis_empty<int64_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::FLOAT]}, LogicalType::BOOLEAN, DSQuantilesis_empty<float>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::DOUBLE]}, LogicalType::BOOLEAN, DSQuantilesis_empty<double>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UTINYINT]}, LogicalType::BOOLEAN, DSQuantilesis_empty<uint8_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::USMALLINT]}, LogicalType::BOOLEAN, DSQuantilesis_empty<uint16_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UINTEGER]}, LogicalType::BOOLEAN, DSQuantilesis_empty<uint32_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UBIGINT]}, LogicalType::BOOLEAN, DSQuantilesis_empty<uint64_t>));

            CreateScalarFunctionInfo info(std::move(fs));

            {
                FunctionDescription desc;
                desc.description = "Return a boolean indicating if the sketch is empty";
                desc.examples.push_back("datasketch_quantiles_is_empty(sketch)");
                info.descriptions.push_back(desc);
            }

            system_catalog.CreateFunction(data, info);
        }
        {
            ScalarFunctionSet fs("datasketch_quantiles_k");

            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::TINYINT]}, LogicalType::USMALLINT, DSQuantilesk<int8_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::SMALLINT]}, LogicalType::USMALLINT, DSQuantilesk<int16_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::INTEGER]}, LogicalType::USMALLINT, DSQuantilesk<int32_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::BIGINT]}, LogicalType::USMALLINT, DSQuantilesk<int64_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::FLOAT]}, LogicalType::USMALLINT, DSQuantilesk<float>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::DOUBLE]}, LogicalType::USMALLINT, DSQuantilesk<double>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UTINYINT]}, LogicalType::USMALLINT, DSQuantilesk<uint8_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::USMALLINT]}, LogicalType::USMALLINT, DSQuantilesk<uint16_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UINTEGER]}, LogicalType::USMALLINT, DSQuantilesk<uint32_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UBIGINT]}, LogicalType::USMALLINT, DSQuantilesk<uint64_t>));

            CreateScalarFunctionInfo info(std::move(fs));

            {
                FunctionDescription desc;
                desc.description = "Return the value of K for this sketch";
                desc.examples.push_back("datasketch_quantiles_k(sketch)");
                info.descriptions.push_back(desc);
            }

            system_catalog.CreateFunction(data, info);
        }
        {
            ScalarFunctionSet fs("datasketch_quantiles_cdf");

            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::TINYINT], LogicalType::LIST(LogicalType::TINYINT), LogicalType::BOOLEAN}, LogicalType::LIST(LogicalType::TINYINT), DSQuantilescdf<int8_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::SMALLINT], LogicalType::LIST(LogicalType::SMALLINT), LogicalType::BOOLEAN}, LogicalType::LIST(LogicalType::SMALLINT), DSQuantilescdf<int16_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::INTEGER], LogicalType::LIST(LogicalType::INTEGER), LogicalType::BOOLEAN}, LogicalType::LIST(LogicalType::INTEGER), DSQuantilescdf<int32_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::BIGINT], LogicalType::LIST(LogicalType::BIGINT), LogicalType::BOOLEAN}, LogicalType::LIST(LogicalType::BIGINT), DSQuantilescdf<int64_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::FLOAT], LogicalType::LIST(LogicalType::FLOAT), LogicalType::BOOLEAN}, LogicalType::LIST(LogicalType::FLOAT), DSQuantilescdf<float>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::DOUBLE], LogicalType::LIST(LogicalType::DOUBLE), LogicalType::BOOLEAN}, LogicalType::LIST(LogicalType::DOUBLE), DSQuantilescdf<double>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UTINYINT], LogicalType::LIST(LogicalType::UTINYINT), LogicalType::BOOLEAN}, LogicalType::LIST(LogicalType::UTINYINT), DSQuantilescdf<uint8_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::USMALLINT], LogicalType::LIST(LogicalType::USMALLINT), LogicalType::BOOLEAN}, LogicalType::LIST(LogicalType::USMALLINT), DSQuantilescdf<uint16_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UINTEGER], LogicalType::LIST(LogicalType::UINTEGER), LogicalType::BOOLEAN}, LogicalType::LIST(LogicalType::UINTEGER), DSQuantilescdf<uint32_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UBIGINT], LogicalType::LIST(LogicalType::UBIGINT), LogicalType::BOOLEAN}, LogicalType::LIST(LogicalType::UBIGINT), DSQuantilescdf<uint64_t>));

            CreateScalarFunctionInfo info(std::move(fs));

            {
                FunctionDescription desc;
                desc.description = "Return the Cumulative Distribution Function (CDF) of the sketch for a series of points";
                desc.examples.push_back("datasketch_quantiles_cdf(sketch, points, inclusive)");
                info.descriptions.push_back(desc);
            }

            system_catalog.CreateFunction(data, info);
        }
        {
            ScalarFunctionSet fs("datasketch_quantiles_pmf");

            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::TINYINT], LogicalType::LIST(LogicalType::TINYINT), LogicalType::BOOLEAN}, LogicalType::LIST(LogicalType::TINYINT), DSQuantilespmf<int8_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::SMALLINT], LogicalType::LIST(LogicalType::SMALLINT), LogicalType::BOOLEAN}, LogicalType::LIST(LogicalType::SMALLINT), DSQuantilespmf<int16_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::INTEGER], LogicalType::LIST(LogicalType::INTEGER), LogicalType::BOOLEAN}, LogicalType::LIST(LogicalType::INTEGER), DSQuantilespmf<int32_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::BIGINT], LogicalType::LIST(LogicalType::BIGINT), LogicalType::BOOLEAN}, LogicalType::LIST(LogicalType::BIGINT), DSQuantilespmf<int64_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::FLOAT], LogicalType::LIST(LogicalType::FLOAT), LogicalType::BOOLEAN}, LogicalType::LIST(LogicalType::FLOAT), DSQuantilespmf<float>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::DOUBLE], LogicalType::LIST(LogicalType::DOUBLE), LogicalType::BOOLEAN}, LogicalType::LIST(LogicalType::DOUBLE), DSQuantilespmf<double>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UTINYINT], LogicalType::LIST(LogicalType::UTINYINT), LogicalType::BOOLEAN}, LogicalType::LIST(LogicalType::UTINYINT), DSQuantilespmf<uint8_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::USMALLINT], LogicalType::LIST(LogicalType::USMALLINT), LogicalType::BOOLEAN}, LogicalType::LIST(LogicalType::USMALLINT), DSQuantilespmf<uint16_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UINTEGER], LogicalType::LIST(LogicalType::UINTEGER), LogicalType::BOOLEAN}, LogicalType::LIST(LogicalType::UINTEGER), DSQuantilespmf<uint32_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UBIGINT], LogicalType::LIST(LogicalType::UBIGINT), LogicalType::BOOLEAN}, LogicalType::LIST(LogicalType::UBIGINT), DSQuantilespmf<uint64_t>));

            CreateScalarFunctionInfo info(std::move(fs));

            {
                FunctionDescription desc;
                desc.description = "Return the Probability Mass Function (PMF) of the sketch for a series of points";
                desc.examples.push_back("datasketch_quantiles_pmf(sketch, points, inclusive)");
                info.descriptions.push_back(desc);
            }

            system_catalog.CreateFunction(data, info);
        }
        {
            ScalarFunctionSet fs("datasketch_quantiles_normalized_rank_error");

            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::TINYINT], LogicalType::BOOLEAN}, LogicalType::DOUBLE, DSQuantilesnormalized_rank_error<int8_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::SMALLINT], LogicalType::BOOLEAN}, LogicalType::DOUBLE, DSQuantilesnormalized_rank_error<int16_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::INTEGER], LogicalType::BOOLEAN}, LogicalType::DOUBLE, DSQuantilesnormalized_rank_error<int32_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::BIGINT], LogicalType::BOOLEAN}, LogicalType::DOUBLE, DSQuantilesnormalized_rank_error<int64_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::FLOAT], LogicalType::BOOLEAN}, LogicalType::DOUBLE, DSQuantilesnormalized_rank_error<float>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::DOUBLE], LogicalType::BOOLEAN}, LogicalType::DOUBLE, DSQuantilesnormalized_rank_error<double>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UTINYINT], LogicalType::BOOLEAN}, LogicalType::DOUBLE, DSQuantilesnormalized_rank_error<uint8_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::USMALLINT], LogicalType::BOOLEAN}, LogicalType::DOUBLE, DSQuantilesnormalized_rank_error<uint16_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UINTEGER], LogicalType::BOOLEAN}, LogicalType::DOUBLE, DSQuantilesnormalized_rank_error<uint32_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UBIGINT], LogicalType::BOOLEAN}, LogicalType::DOUBLE, DSQuantilesnormalized_rank_error<uint64_t>));

            CreateScalarFunctionInfo info(std::move(fs));

            {
                FunctionDescription desc;
                desc.description = "Return the normalized rank error of the sketch";
                desc.examples.push_back("datasketch_quantiles_normalized_rank_error(sketch, is_pmf)");
                info.descriptions.push_back(desc);
            }

            system_catalog.CreateFunction(data, info);
        }
        {
            ScalarFunctionSet fs("datasketch_quantiles_describe");

            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::TINYINT], LogicalType::BOOLEAN, LogicalType::BOOLEAN}, LogicalType::VARCHAR, DSQuantilesdescribe<int8_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::SMALLINT], LogicalType::BOOLEAN, LogicalType::BOOLEAN}, LogicalType::VARCHAR, DSQuantilesdescribe<int16_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::INTEGER], LogicalType::BOOLEAN, LogicalType::BOOLEAN}, LogicalType::VARCHAR, DSQuantilesdescribe<int32_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::BIGINT], LogicalType::BOOLEAN, LogicalType::BOOLEAN}, LogicalType::VARCHAR, DSQuantilesdescribe<int64_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::FLOAT], LogicalType::BOOLEAN, LogicalType::BOOLEAN}, LogicalType::VARCHAR, DSQuantilesdescribe<float>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::DOUBLE], LogicalType::BOOLEAN, LogicalType::BOOLEAN}, LogicalType::VARCHAR, DSQuantilesdescribe<double>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UTINYINT], LogicalType::BOOLEAN, LogicalType::BOOLEAN}, LogicalType::VARCHAR, DSQuantilesdescribe<uint8_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::USMALLINT], LogicalType::BOOLEAN, LogicalType::BOOLEAN}, LogicalType::VARCHAR, DSQuantilesdescribe<uint16_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UINTEGER], LogicalType::BOOLEAN, LogicalType::BOOLEAN}, LogicalType::VARCHAR, DSQuantilesdescribe<uint32_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UBIGINT], LogicalType::BOOLEAN, LogicalType::BOOLEAN}, LogicalType::VARCHAR, DSQuantilesdescribe<uint64_t>));

            CreateScalarFunctionInfo info(std::move(fs));

            {
                FunctionDescription desc;
                desc.description = "Return a description of this sketch";
                desc.examples.push_back("datasketch_quantiles_describe(sketch, include_levels, include_items)");
                info.descriptions.push_back(desc);
            }

            system_catalog.CreateFunction(data, info);
        }
        {
            ScalarFunctionSet fs("datasketch_quantiles_rank");

            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::TINYINT], LogicalType::TINYINT, LogicalType::BOOLEAN}, LogicalType::DOUBLE, DSQuantilesrank<int8_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::SMALLINT], LogicalType::SMALLINT, LogicalType::BOOLEAN}, LogicalType::DOUBLE, DSQuantilesrank<int16_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::INTEGER], LogicalType::INTEGER, LogicalType::BOOLEAN}, LogicalType::DOUBLE, DSQuantilesrank<int32_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::BIGINT], LogicalType::BIGINT, LogicalType::BOOLEAN}, LogicalType::DOUBLE, DSQuantilesrank<int64_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::FLOAT], LogicalType::FLOAT, LogicalType::BOOLEAN}, LogicalType::DOUBLE, DSQuantilesrank<float>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::DOUBLE], LogicalType::DOUBLE, LogicalType::BOOLEAN}, LogicalType::DOUBLE, DSQuantilesrank<double>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UTINYINT], LogicalType::UTINYINT, LogicalType::BOOLEAN}, LogicalType::DOUBLE, DSQuantilesrank<uint8_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::USMALLINT], LogicalType::USMALLINT, LogicalType::BOOLEAN}, LogicalType::DOUBLE, DSQuantilesrank<uint16_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UINTEGER], LogicalType::UINTEGER, LogicalType::BOOLEAN}, LogicalType::DOUBLE, DSQuantilesrank<uint32_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UBIGINT], LogicalType::UBIGINT, LogicalType::BOOLEAN}, LogicalType::DOUBLE, DSQuantilesrank<uint64_t>));

            CreateScalarFunctionInfo info(std::move(fs));

            {
                FunctionDescription desc;
                desc.description = "Return the rank of an item in the sketch";
                desc.examples.push_back("datasketch_quantiles_rank(sketch, item, inclusive)");
                info.descriptions.push_back(desc);
            }

            system_catalog.CreateFunction(data, info);
        }
        {
            ScalarFunctionSet fs("datasketch_quantiles_quantile");

            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::TINYINT], LogicalType::DOUBLE, LogicalType::BOOLEAN}, LogicalType::TINYINT, DSQuantilesquantile<int8_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::SMALLINT], LogicalType::DOUBLE, LogicalType::BOOLEAN}, LogicalType::SMALLINT, DSQuantilesquantile<int16_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::INTEGER], LogicalType::DOUBLE, LogicalType::BOOLEAN}, LogicalType::INTEGER, DSQuantilesquantile<int32_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::BIGINT], LogicalType::DOUBLE, LogicalType::BOOLEAN}, LogicalType::BIGINT, DSQuantilesquantile<int64_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::FLOAT], LogicalType::DOUBLE, LogicalType::BOOLEAN}, LogicalType::FLOAT, DSQuantilesquantile<float>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::DOUBLE], LogicalType::DOUBLE, LogicalType::BOOLEAN}, LogicalType::DOUBLE, DSQuantilesquantile<double>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UTINYINT], LogicalType::DOUBLE, LogicalType::BOOLEAN}, LogicalType::UTINYINT, DSQuantilesquantile<uint8_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::USMALLINT], LogicalType::DOUBLE, LogicalType::BOOLEAN}, LogicalType::USMALLINT, DSQuantilesquantile<uint16_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UINTEGER], LogicalType::DOUBLE, LogicalType::BOOLEAN}, LogicalType::UINTEGER, DSQuantilesquantile<uint32_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UBIGINT], LogicalType::DOUBLE, LogicalType::BOOLEAN}, LogicalType::UBIGINT, DSQuantilesquantile<uint64_t>));

            CreateScalarFunctionInfo info(std::move(fs));

            {
                FunctionDescription desc;
                desc.description = "Return the quantile of a rank in the sketch";
                desc.examples.push_back("datasketch_quantiles_rank(sketch, rank, inclusive)");
                info.descriptions.push_back(desc);
            }

            system_catalog.CreateFunction(data, info);
        }
        {
            ScalarFunctionSet fs("datasketch_quantiles_n");

            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::TINYINT]}, LogicalType::UBIGINT, DSQuantilesn<int8_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::SMALLINT]}, LogicalType::UBIGINT, DSQuantilesn<int16_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::INTEGER]}, LogicalType::UBIGINT, DSQuantilesn<int32_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::BIGINT]}, LogicalType::UBIGINT, DSQuantilesn<int64_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::FLOAT]}, LogicalType::UBIGINT, DSQuantilesn<float>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::DOUBLE]}, LogicalType::UBIGINT, DSQuantilesn<double>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UTINYINT]}, LogicalType::UBIGINT, DSQuantilesn<uint8_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::USMALLINT]}, LogicalType::UBIGINT, DSQuantilesn<uint16_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UINTEGER]}, LogicalType::UBIGINT, DSQuantilesn<uint32_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UBIGINT]}, LogicalType::UBIGINT, DSQuantilesn<uint64_t>));

            CreateScalarFunctionInfo info(std::move(fs));

            {
                FunctionDescription desc;
                desc.description = "Return the number of items contained in the sketch";
                desc.examples.push_back("datasketch_quantiles_rank(sketch)");
                info.descriptions.push_back(desc);
            }

            system_catalog.CreateFunction(data, info);
        }
        {
            ScalarFunctionSet fs("datasketch_quantiles_is_estimation_mode");

            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::TINYINT]}, LogicalType::BOOLEAN, DSQuantilesis_estimation_mode<int8_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::SMALLINT]}, LogicalType::BOOLEAN, DSQuantilesis_estimation_mode<int16_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::INTEGER]}, LogicalType::BOOLEAN, DSQuantilesis_estimation_mode<int32_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::BIGINT]}, LogicalType::BOOLEAN, DSQuantilesis_estimation_mode<int64_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::FLOAT]}, LogicalType::BOOLEAN, DSQuantilesis_estimation_mode<float>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::DOUBLE]}, LogicalType::BOOLEAN, DSQuantilesis_estimation_mode<double>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UTINYINT]}, LogicalType::BOOLEAN, DSQuantilesis_estimation_mode<uint8_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::USMALLINT]}, LogicalType::BOOLEAN, DSQuantilesis_estimation_mode<uint16_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UINTEGER]}, LogicalType::BOOLEAN, DSQuantilesis_estimation_mode<uint32_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UBIGINT]}, LogicalType::BOOLEAN, DSQuantilesis_estimation_mode<uint64_t>));

            CreateScalarFunctionInfo info(std::move(fs));

            {
                FunctionDescription desc;
                desc.description = "Return a boolean indicating if the sketch is in estimation mode";
                desc.examples.push_back("datasketch_quantiles_is_estimation_mode(sketch)");
                info.descriptions.push_back(desc);
            }

            system_catalog.CreateFunction(data, info);
        }
        {
            ScalarFunctionSet fs("datasketch_quantiles_num_retained");

            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::TINYINT]}, LogicalType::UBIGINT, DSQuantilesnum_retained<int8_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::SMALLINT]}, LogicalType::UBIGINT, DSQuantilesnum_retained<int16_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::INTEGER]}, LogicalType::UBIGINT, DSQuantilesnum_retained<int32_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::BIGINT]}, LogicalType::UBIGINT, DSQuantilesnum_retained<int64_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::FLOAT]}, LogicalType::UBIGINT, DSQuantilesnum_retained<float>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::DOUBLE]}, LogicalType::UBIGINT, DSQuantilesnum_retained<double>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UTINYINT]}, LogicalType::UBIGINT, DSQuantilesnum_retained<uint8_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::USMALLINT]}, LogicalType::UBIGINT, DSQuantilesnum_retained<uint16_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UINTEGER]}, LogicalType::UBIGINT, DSQuantilesnum_retained<uint32_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UBIGINT]}, LogicalType::UBIGINT, DSQuantilesnum_retained<uint64_t>));

            CreateScalarFunctionInfo info(std::move(fs));

            {
                FunctionDescription desc;
                desc.description = "Return the number of retained items in the sketch";
                desc.examples.push_back("datasketch_quantiles_num_retained(sketch)");
                info.descriptions.push_back(desc);
            }

            system_catalog.CreateFunction(data, info);
        }
        {
            ScalarFunctionSet fs("datasketch_quantiles_min_item");

            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::TINYINT]}, LogicalType::TINYINT, DSQuantilesmin_item<int8_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::SMALLINT]}, LogicalType::SMALLINT, DSQuantilesmin_item<int16_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::INTEGER]}, LogicalType::INTEGER, DSQuantilesmin_item<int32_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::BIGINT]}, LogicalType::BIGINT, DSQuantilesmin_item<int64_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::FLOAT]}, LogicalType::FLOAT, DSQuantilesmin_item<float>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::DOUBLE]}, LogicalType::DOUBLE, DSQuantilesmin_item<double>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UTINYINT]}, LogicalType::UTINYINT, DSQuantilesmin_item<uint8_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::USMALLINT]}, LogicalType::USMALLINT, DSQuantilesmin_item<uint16_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UINTEGER]}, LogicalType::UINTEGER, DSQuantilesmin_item<uint32_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UBIGINT]}, LogicalType::UBIGINT, DSQuantilesmin_item<uint64_t>));

            CreateScalarFunctionInfo info(std::move(fs));

            {
                FunctionDescription desc;
                desc.description = "Return the minimum item in the sketch";
                desc.examples.push_back("datasketch_quantiles_min_item(sketch)");
                info.descriptions.push_back(desc);
            }

            system_catalog.CreateFunction(data, info);
        }
        {
            ScalarFunctionSet fs("datasketch_quantiles_max_item");

            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::TINYINT]}, LogicalType::TINYINT, DSQuantilesmax_item<int8_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::SMALLINT]}, LogicalType::SMALLINT, DSQuantilesmax_item<int16_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::INTEGER]}, LogicalType::INTEGER, DSQuantilesmax_item<int32_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::BIGINT]}, LogicalType::BIGINT, DSQuantilesmax_item<int64_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::FLOAT]}, LogicalType::FLOAT, DSQuantilesmax_item<float>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::DOUBLE]}, LogicalType::DOUBLE, DSQuantilesmax_item<double>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UTINYINT]}, LogicalType::UTINYINT, DSQuantilesmax_item<uint8_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::USMALLINT]}, LogicalType::USMALLINT, DSQuantilesmax_item<uint16_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UINTEGER]}, LogicalType::UINTEGER, DSQuantilesmax_item<uint32_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UBIGINT]}, LogicalType::UBIGINT, DSQuantilesmax_item<uint64_t>));

            CreateScalarFunctionInfo info(std::move(fs));

            {
                FunctionDescription desc;
                desc.description = "Return the maxium item in the sketch";
                desc.examples.push_back("datasketch_quantiles_max_item(sketch)");
                info.descriptions.push_back(desc);
            }

            system_catalog.CreateFunction(data, info);
        }

        // This funciton creates the sketches.
        {
            AggregateFunctionSet sketch("datasketch_quantiles");

            {
                auto fun = DSQuantilesCreateAggregate<int8_t>(LogicalType::TINYINT, sketch_map_types[LogicalTypeId::TINYINT]);
                fun.bind = DSQuantilesBind;
                fun.arguments.insert(fun.arguments.begin(), LogicalType::INTEGER);
                sketch.AddFunction(fun);
            }
            // sketch.AddFunction(DSQuantilesCreateAggregate<int8_t>(LogicalType::TINYINT, sketch_map_types[LogicalTypeId::TINYINT]));
            {
                auto fun = DSQuantilesMergeAggregate<int8_t>(LogicalType::TINYINT, sketch_map_types[LogicalTypeId::TINYINT]);
                fun.bind = DSQuantilesBind;
                fun.arguments.insert(fun.arguments.begin(), LogicalType::INTEGER);
                sketch.AddFunction(fun);
            }
            // sketch.AddFunction(DSQuantilesMergeAggregate<int8_t>(LogicalType::TINYINT, sketch_map_types[LogicalTypeId::TINYINT]));

            {
                auto fun = DSQuantilesCreateAggregate<int16_t>(LogicalType::SMALLINT, sketch_map_types[LogicalTypeId::SMALLINT]);
                fun.bind = DSQuantilesBind;
                fun.arguments.insert(fun.arguments.begin(), LogicalType::INTEGER);
                sketch.AddFunction(fun);
            }
            // sketch.AddFunction(DSQuantilesCreateAggregate<int16_t>(LogicalType::SMALLINT, sketch_map_types[LogicalTypeId::SMALLINT]));
            {
                auto fun = DSQuantilesMergeAggregate<int16_t>(LogicalType::SMALLINT, sketch_map_types[LogicalTypeId::SMALLINT]);
                fun.bind = DSQuantilesBind;
                fun.arguments.insert(fun.arguments.begin(), LogicalType::INTEGER);
                sketch.AddFunction(fun);
            }
            // sketch.AddFunction(DSQuantilesMergeAggregate<int16_t>(LogicalType::SMALLINT, sketch_map_types[LogicalTypeId::SMALLINT]));

            {
                auto fun = DSQuantilesCreateAggregate<int32_t>(LogicalType::INTEGER, sketch_map_types[LogicalTypeId::INTEGER]);
                fun.bind = DSQuantilesBind;
                fun.arguments.insert(fun.arguments.begin(), LogicalType::INTEGER);
                sketch.AddFunction(fun);
            }
            // sketch.AddFunction(DSQuantilesCreateAggregate<int32_t>(LogicalType::INTEGER, sketch_map_types[LogicalTypeId::INTEGER]));
            {
                auto fun = DSQuantilesMergeAggregate<int32_t>(LogicalType::INTEGER, sketch_map_types[LogicalTypeId::INTEGER]);
                fun.bind = DSQuantilesBind;
                fun.arguments.insert(fun.arguments.begin(), LogicalType::INTEGER);
                sketch.AddFunction(fun);
            }
            // sketch.AddFunction(DSQuantilesMergeAggregate<int32_t>(LogicalType::INTEGER, sketch_map_types[LogicalTypeId::INTEGER]));

            {
                auto fun = DSQuantilesCreateAggregate<int64_t>(LogicalType::BIGINT, sketch_map_types[LogicalTypeId::BIGINT]);
                fun.bind = DSQuantilesBind;
                fun.arguments.insert(fun.arguments.begin(), LogicalType::INTEGER);
                sketch.AddFunction(fun);
            }
            // sketch.AddFunction(DSQuantilesCreateAggregate<int64_t>(LogicalType::BIGINT, sketch_map_types[LogicalTypeId::BIGINT]));
            {
                auto fun = DSQuantilesMergeAggregate<int64_t>(LogicalType::BIGINT, sketch_map_types[LogicalTypeId::BIGINT]);
                fun.bind = DSQuantilesBind;
                fun.arguments.insert(fun.arguments.begin(), LogicalType::INTEGER);
                sketch.AddFunction(fun);
            }
            // sketch.AddFunction(DSQuantilesMergeAggregate<int64_t>(LogicalType::BIGINT, sketch_map_types[LogicalTypeId::BIGINT]));

            {
                auto fun = DSQuantilesCreateAggregate<float>(LogicalType::FLOAT, sketch_map_types[LogicalTypeId::FLOAT]);
                fun.bind = DSQuantilesBind;
                fun.arguments.insert(fun.arguments.begin(), LogicalType::INTEGER);
                sketch.AddFunction(fun);
            }
            // sketch.AddFunction(DSQuantilesCreateAggregate<float>(LogicalType::FLOAT, sketch_map_types[LogicalTypeId::FLOAT]));
            {
                auto fun = DSQuantilesMergeAggregate<float>(LogicalType::FLOAT, sketch_map_types[LogicalTypeId::FLOAT]);
                fun.bind = DSQuantilesBind;
                fun.arguments.insert(fun.arguments.begin(), LogicalType::INTEGER);
                sketch.AddFunction(fun);
            }
            // sketch.AddFunction(DSQuantilesMergeAggregate<float>(LogicalType::FLOAT, sketch_map_types[LogicalTypeId::FLOAT]));

            {
                auto fun = DSQuantilesCreateAggregate<double>(LogicalType::DOUBLE, sketch_map_types[LogicalTypeId::DOUBLE]);
                fun.bind = DSQuantilesBind;
                fun.arguments.insert(fun.arguments.begin(), LogicalType::INTEGER);
                sketch.AddFunction(fun);
            }
            // sketch.AddFunction(DSQuantilesCreateAggregate<double>(LogicalType::DOUBLE, sketch_map_types[LogicalTypeId::DOUBLE]));
            {
                auto fun = DSQuantilesMergeAggregate<double>(LogicalType::DOUBLE, sketch_map_types[LogicalTypeId::DOUBLE]);
                fun.bind = DSQuantilesBind;
                fun.arguments.insert(fun.arguments.begin(), LogicalType::INTEGER);
                sketch.AddFunction(fun);
            }
            // sketch.AddFunction(DSQuantilesMergeAggregate<double>(LogicalType::DOUBLE, sketch_map_types[LogicalTypeId::DOUBLE]));

            {
                auto fun = DSQuantilesCreateAggregate<uint8_t>(LogicalType::UTINYINT, sketch_map_types[LogicalTypeId::UTINYINT]);
                fun.bind = DSQuantilesBind;
                fun.arguments.insert(fun.arguments.begin(), LogicalType::INTEGER);
                sketch.AddFunction(fun);
            }
            // sketch.AddFunction(DSQuantilesCreateAggregate<uint8_t>(LogicalType::UTINYINT, sketch_map_types[LogicalTypeId::UTINYINT]));
            {
                auto fun = DSQuantilesMergeAggregate<uint8_t>(LogicalType::UTINYINT, sketch_map_types[LogicalTypeId::UTINYINT]);
                fun.bind = DSQuantilesBind;
                fun.arguments.insert(fun.arguments.begin(), LogicalType::INTEGER);
                sketch.AddFunction(fun);
            }
            // sketch.AddFunction(DSQuantilesMergeAggregate<uint8_t>(LogicalType::UTINYINT, sketch_map_types[LogicalTypeId::UTINYINT]));

            {
                auto fun = DSQuantilesCreateAggregate<uint16_t>(LogicalType::USMALLINT, sketch_map_types[LogicalTypeId::USMALLINT]);
                fun.bind = DSQuantilesBind;
                fun.arguments.insert(fun.arguments.begin(), LogicalType::INTEGER);
                sketch.AddFunction(fun);
            }
            // sketch.AddFunction(DSQuantilesCreateAggregate<uint16_t>(LogicalType::USMALLINT, sketch_map_types[LogicalTypeId::USMALLINT]));
            {
                auto fun = DSQuantilesMergeAggregate<uint16_t>(LogicalType::USMALLINT, sketch_map_types[LogicalTypeId::USMALLINT]);
                fun.bind = DSQuantilesBind;
                fun.arguments.insert(fun.arguments.begin(), LogicalType::INTEGER);
                sketch.AddFunction(fun);
            }
            // sketch.AddFunction(DSQuantilesMergeAggregate<uint16_t>(LogicalType::USMALLINT, sketch_map_types[LogicalTypeId::USMALLINT]));

            {
                auto fun = DSQuantilesCreateAggregate<uint32_t>(LogicalType::UINTEGER, sketch_map_types[LogicalTypeId::UINTEGER]);
                fun.bind = DSQuantilesBind;
                fun.arguments.insert(fun.arguments.begin(), LogicalType::INTEGER);
                sketch.AddFunction(fun);
            }
            // sketch.AddFunction(DSQuantilesCreateAggregate<uint32_t>(LogicalType::UINTEGER, sketch_map_types[LogicalTypeId::UINTEGER]));
            {
                auto fun = DSQuantilesMergeAggregate<uint32_t>(LogicalType::UINTEGER, sketch_map_types[LogicalTypeId::UINTEGER]);
                fun.bind = DSQuantilesBind;
                fun.arguments.insert(fun.arguments.begin(), LogicalType::INTEGER);
                sketch.AddFunction(fun);
            }
            // sketch.AddFunction(DSQuantilesMergeAggregate<uint32_t>(LogicalType::UINTEGER, sketch_map_types[LogicalTypeId::UINTEGER]));

            {
                auto fun = DSQuantilesCreateAggregate<uint64_t>(LogicalType::UBIGINT, sketch_map_types[LogicalTypeId::UBIGINT]);
                fun.bind = DSQuantilesBind;
                fun.arguments.insert(fun.arguments.begin(), LogicalType::INTEGER);
                sketch.AddFunction(fun);
            }
            // sketch.AddFunction(DSQuantilesCreateAggregate<uint64_t>(LogicalType::UBIGINT, sketch_map_types[LogicalTypeId::UBIGINT]));
            {
                auto fun = DSQuantilesMergeAggregate<uint64_t>(LogicalType::UBIGINT, sketch_map_types[LogicalTypeId::UBIGINT]);
                fun.bind = DSQuantilesBind;
                fun.arguments.insert(fun.arguments.begin(), LogicalType::INTEGER);
                sketch.AddFunction(fun);
            }
            // sketch.AddFunction(DSQuantilesMergeAggregate<uint64_t>(LogicalType::UBIGINT, sketch_map_types[LogicalTypeId::UBIGINT]));

            CreateAggregateFunctionInfo sketch_info(sketch);

            {
                FunctionDescription desc;
                desc.description = "Creates a sketch_quantiles data sketch by aggregating values or by aggregating other Quantiles data sketches";
                desc.examples.push_back("datasketch_quantiles(k, data)");
                sketch_info.descriptions.push_back(desc);
            }

            system_catalog.CreateFunction(data, sketch_info);
        }
    }

    template <class T>

    static inline void DSKLLis_empty(DataChunk &args, ExpressionState &state, Vector &result)
    {
        // Get the references to the incoming vectors.
        D_ASSERT(args.ColumnCount() == 1);

        auto &sketch_vector = args.data[0];

        UnaryExecutor::Execute<string_t, bool>(
            sketch_vector, result, args.size(),
            [&](string_t sketch_data)
            {
                auto sketch = datasketches::kll_sketch<T>::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
                return sketch.is_empty();
            });
    }

    template <class T>

    static inline void DSKLLk(DataChunk &args, ExpressionState &state, Vector &result)
    {
        // Get the references to the incoming vectors.
        D_ASSERT(args.ColumnCount() == 1);

        auto &sketch_vector = args.data[0];

        UnaryExecutor::Execute<string_t, uint16_t>(
            sketch_vector, result, args.size(),
            [&](string_t sketch_data)
            {
                auto sketch = datasketches::kll_sketch<T>::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
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

        UnifiedVectorFormat unified_split_points;
        split_points_vector.ToUnifiedFormat(args.size(), unified_split_points);

        // auto split_points_list_entries = UnifiedVectorFormat::GetData<list_entry_t>(unified_split_points);
        //         auto split_points_validitiy = FlatVector::Validity(split_points_vector);

        auto &split_points_list_children = ListVector::GetEntry(split_points_vector);

        UnifiedVectorFormat split_points_children_unified;
        split_points_list_children.ToUnifiedFormat(args.size(), split_points_children_unified);

        const T *split_points_list_children_data = UnifiedVectorFormat::GetData<T>(split_points_children_unified);

        TernaryExecutor::Execute<string_t, list_entry_t, bool, list_entry_t>(
            sketch_vector, split_points_vector, inclusive_vector, result, args.size(),
            [&](string_t sketch_data, list_entry_t split_points_data, bool inclusive_data)
            {
                auto sketch = datasketches::kll_sketch<T>::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());

                T *passing_points = (T *)malloc(sizeof(T) * split_points_data.length);
                for (idx_t i = 0; i < split_points_data.length; i++)
                {
                    passing_points[i] = split_points_list_children_data[i + split_points_data.offset];
                }

                auto cdf_result = sketch.get_CDF(passing_points, split_points_data.length, inclusive_data);
                free(passing_points);
                auto current_size = ListVector::GetListSize(result);
                auto new_size = current_size + cdf_result.size();
                if (ListVector::GetListCapacity(result) < new_size)
                {
                    ListVector::Reserve(result, new_size);
                }

                auto &child_entry = ListVector::GetEntry(result);
                auto child_vals = FlatVector::GetData<T>(child_entry);
                // auto &child_validity = FlatVector::Validity(child_entry);
                for (idx_t i = 0; i < cdf_result.size(); i++)
                {
                    child_vals[current_size + i] = cdf_result[i];
                }
                ListVector::SetListSize(result, new_size);
                return list_entry_t{current_size, cdf_result.size()};
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

        UnifiedVectorFormat unified_split_points;
        split_points_vector.ToUnifiedFormat(args.size(), unified_split_points);

        // auto split_points_list_entries = UnifiedVectorFormat::GetData<list_entry_t>(unified_split_points);
        //         auto split_points_validitiy = FlatVector::Validity(split_points_vector);

        auto &split_points_list_children = ListVector::GetEntry(split_points_vector);

        UnifiedVectorFormat split_points_children_unified;
        split_points_list_children.ToUnifiedFormat(args.size(), split_points_children_unified);

        const T *split_points_list_children_data = UnifiedVectorFormat::GetData<T>(split_points_children_unified);

        TernaryExecutor::Execute<string_t, list_entry_t, bool, list_entry_t>(
            sketch_vector, split_points_vector, inclusive_vector, result, args.size(),
            [&](string_t sketch_data, list_entry_t split_points_data, bool inclusive_data)
            {
                auto sketch = datasketches::kll_sketch<T>::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());

                T *passing_points = (T *)malloc(sizeof(T) * split_points_data.length);
                for (idx_t i = 0; i < split_points_data.length; i++)
                {
                    passing_points[i] = split_points_list_children_data[i + split_points_data.offset];
                }

                auto pmf_result = sketch.get_PMF(passing_points, split_points_data.length, inclusive_data);
                free(passing_points);

                auto current_size = ListVector::GetListSize(result);
                auto new_size = current_size + pmf_result.size();
                if (ListVector::GetListCapacity(result) < new_size)
                {
                    ListVector::Reserve(result, new_size);
                }

                auto &child_entry = ListVector::GetEntry(result);
                auto child_vals = FlatVector::GetData<double>(child_entry);
                // auto &child_validity = FlatVector::Validity(child_entry);
                for (idx_t i = 0; i < pmf_result.size(); i++)
                {
                    child_vals[current_size + i] = pmf_result[i];
                }
                ListVector::SetListSize(result, new_size);
                return list_entry_t{current_size, pmf_result.size()};
            });
    }

    template <class T>

    static inline void DSKLLnormalized_rank_error(DataChunk &args, ExpressionState &state, Vector &result)
    {
        // Get the references to the incoming vectors.
        D_ASSERT(args.ColumnCount() == 2);

        auto &sketch_vector = args.data[0];
        auto &is_pmf_vector = args.data[1];

        BinaryExecutor::Execute<string_t, bool, double>(
            sketch_vector, is_pmf_vector, result, args.size(),
            [&](string_t sketch_data, bool is_pmf_data)
            {
                auto sketch = datasketches::kll_sketch<T>::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
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

        TernaryExecutor::Execute<string_t, bool, bool, string_t>(
            sketch_vector, include_levels_vector, include_items_vector, result, args.size(),
            [&](string_t sketch_data, bool include_levels_data, bool include_items_data)
            {
                auto sketch = datasketches::kll_sketch<T>::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
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

        TernaryExecutor::Execute<string_t, T, bool, double>(
            sketch_vector, item_vector, inclusive_vector, result, args.size(),
            [&](string_t sketch_data, T item_data, bool inclusive_data)
            {
                auto sketch = datasketches::kll_sketch<T>::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
                return sketch.get_rank(item_data, inclusive_data);
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

        TernaryExecutor::Execute<string_t, double, bool, T>(
            sketch_vector, rank_vector, inclusive_vector, result, args.size(),
            [&](string_t sketch_data, double rank_data, bool inclusive_data)
            {
                auto sketch = datasketches::kll_sketch<T>::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
                return sketch.get_quantile(rank_data, inclusive_data);
            });
    }

    template <class T>

    static inline void DSKLLn(DataChunk &args, ExpressionState &state, Vector &result)
    {
        // Get the references to the incoming vectors.
        D_ASSERT(args.ColumnCount() == 1);

        auto &sketch_vector = args.data[0];

        UnaryExecutor::Execute<string_t, uint64_t>(
            sketch_vector, result, args.size(),
            [&](string_t sketch_data)
            {
                auto sketch = datasketches::kll_sketch<T>::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
                return sketch.get_n();
            });
    }

    template <class T>

    static inline void DSKLLis_estimation_mode(DataChunk &args, ExpressionState &state, Vector &result)
    {
        // Get the references to the incoming vectors.
        D_ASSERT(args.ColumnCount() == 1);

        auto &sketch_vector = args.data[0];

        UnaryExecutor::Execute<string_t, bool>(
            sketch_vector, result, args.size(),
            [&](string_t sketch_data)
            {
                auto sketch = datasketches::kll_sketch<T>::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
                return sketch.is_estimation_mode();
            });
    }

    template <class T>

    static inline void DSKLLnum_retained(DataChunk &args, ExpressionState &state, Vector &result)
    {
        // Get the references to the incoming vectors.
        D_ASSERT(args.ColumnCount() == 1);

        auto &sketch_vector = args.data[0];

        UnaryExecutor::Execute<string_t, uint64_t>(
            sketch_vector, result, args.size(),
            [&](string_t sketch_data)
            {
                auto sketch = datasketches::kll_sketch<T>::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
                return sketch.get_num_retained();
            });
    }

    template <class T>

    static inline void DSKLLmin_item(DataChunk &args, ExpressionState &state, Vector &result)
    {
        // Get the references to the incoming vectors.
        D_ASSERT(args.ColumnCount() == 1);

        auto &sketch_vector = args.data[0];

        UnaryExecutor::Execute<string_t, T>(
            sketch_vector, result, args.size(),
            [&](string_t sketch_data)
            {
                auto sketch = datasketches::kll_sketch<T>::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
                return sketch.get_min_item();
            });
    }

    template <class T>

    static inline void DSKLLmax_item(DataChunk &args, ExpressionState &state, Vector &result)
    {
        // Get the references to the incoming vectors.
        D_ASSERT(args.ColumnCount() == 1);

        auto &sketch_vector = args.data[0];

        UnaryExecutor::Execute<string_t, T>(
            sketch_vector, result, args.size(),
            [&](string_t sketch_data)
            {
                auto sketch = datasketches::kll_sketch<T>::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
                return sketch.get_max_item();
            });
    }

    template <typename T>
    auto static DSKLLMergeAggregate(const LogicalType &type, const LogicalType &result_type) -> AggregateFunction

    {

        return AggregateFunction::UnaryAggregateDestructor<DSKLLState<T>, string_t, string_t, DSQuantilesMergeOperation<DSKLLBindData>, AggregateDestructorType::LEGACY>(
            result_type, result_type);
    }

    template <typename T>
    auto static DSKLLCreateAggregate(const LogicalType &type, const LogicalType &result_type) -> AggregateFunction
    {

        return AggregateFunction::UnaryAggregateDestructor<DSKLLState<T>, T, string_t, DSQuantilesCreateOperation<DSKLLBindData>, AggregateDestructorType::LEGACY>(
            type, result_type);
    }

    void LoadKLLSketch(DatabaseInstance &instance)
    {
        auto &system_catalog = Catalog::GetSystemCatalog(instance);
        auto data = CatalogTransaction::GetSystemTransaction(instance);

        std::unordered_map<LogicalTypeId, LogicalType> sketch_map_types;

        sketch_map_types.insert({LogicalTypeId::TINYINT, CreateKLLSketchType(instance, LogicalType(LogicalTypeId::TINYINT))});
        sketch_map_types.insert({LogicalTypeId::SMALLINT, CreateKLLSketchType(instance, LogicalType(LogicalTypeId::SMALLINT))});
        sketch_map_types.insert({LogicalTypeId::INTEGER, CreateKLLSketchType(instance, LogicalType(LogicalTypeId::INTEGER))});
        sketch_map_types.insert({LogicalTypeId::BIGINT, CreateKLLSketchType(instance, LogicalType(LogicalTypeId::BIGINT))});
        sketch_map_types.insert({LogicalTypeId::FLOAT, CreateKLLSketchType(instance, LogicalType(LogicalTypeId::FLOAT))});
        sketch_map_types.insert({LogicalTypeId::DOUBLE, CreateKLLSketchType(instance, LogicalType(LogicalTypeId::DOUBLE))});
        sketch_map_types.insert({LogicalTypeId::UTINYINT, CreateKLLSketchType(instance, LogicalType(LogicalTypeId::UTINYINT))});
        sketch_map_types.insert({LogicalTypeId::USMALLINT, CreateKLLSketchType(instance, LogicalType(LogicalTypeId::USMALLINT))});
        sketch_map_types.insert({LogicalTypeId::UINTEGER, CreateKLLSketchType(instance, LogicalType(LogicalTypeId::UINTEGER))});
        sketch_map_types.insert({LogicalTypeId::UBIGINT, CreateKLLSketchType(instance, LogicalType(LogicalTypeId::UBIGINT))});

        {
            ScalarFunctionSet fs("datasketch_kll_is_empty");

            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::TINYINT]}, LogicalType::BOOLEAN, DSKLLis_empty<int8_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::SMALLINT]}, LogicalType::BOOLEAN, DSKLLis_empty<int16_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::INTEGER]}, LogicalType::BOOLEAN, DSKLLis_empty<int32_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::BIGINT]}, LogicalType::BOOLEAN, DSKLLis_empty<int64_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::FLOAT]}, LogicalType::BOOLEAN, DSKLLis_empty<float>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::DOUBLE]}, LogicalType::BOOLEAN, DSKLLis_empty<double>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UTINYINT]}, LogicalType::BOOLEAN, DSKLLis_empty<uint8_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::USMALLINT]}, LogicalType::BOOLEAN, DSKLLis_empty<uint16_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UINTEGER]}, LogicalType::BOOLEAN, DSKLLis_empty<uint32_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UBIGINT]}, LogicalType::BOOLEAN, DSKLLis_empty<uint64_t>));

            CreateScalarFunctionInfo info(std::move(fs));

            {
                FunctionDescription desc;
                desc.description = "Return a boolean indicating if the sketch is empty";
                desc.examples.push_back("datasketch_kll_is_empty(sketch)");
                info.descriptions.push_back(desc);
            }

            system_catalog.CreateFunction(data, info);
        }
        {
            ScalarFunctionSet fs("datasketch_kll_k");

            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::TINYINT]}, LogicalType::USMALLINT, DSKLLk<int8_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::SMALLINT]}, LogicalType::USMALLINT, DSKLLk<int16_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::INTEGER]}, LogicalType::USMALLINT, DSKLLk<int32_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::BIGINT]}, LogicalType::USMALLINT, DSKLLk<int64_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::FLOAT]}, LogicalType::USMALLINT, DSKLLk<float>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::DOUBLE]}, LogicalType::USMALLINT, DSKLLk<double>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UTINYINT]}, LogicalType::USMALLINT, DSKLLk<uint8_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::USMALLINT]}, LogicalType::USMALLINT, DSKLLk<uint16_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UINTEGER]}, LogicalType::USMALLINT, DSKLLk<uint32_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UBIGINT]}, LogicalType::USMALLINT, DSKLLk<uint64_t>));

            CreateScalarFunctionInfo info(std::move(fs));

            {
                FunctionDescription desc;
                desc.description = "Return the value of K for this sketch";
                desc.examples.push_back("datasketch_kll_k(sketch)");
                info.descriptions.push_back(desc);
            }

            system_catalog.CreateFunction(data, info);
        }
        {
            ScalarFunctionSet fs("datasketch_kll_cdf");

            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::TINYINT], LogicalType::LIST(LogicalType::TINYINT), LogicalType::BOOLEAN}, LogicalType::LIST(LogicalType::TINYINT), DSKLLcdf<int8_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::SMALLINT], LogicalType::LIST(LogicalType::SMALLINT), LogicalType::BOOLEAN}, LogicalType::LIST(LogicalType::SMALLINT), DSKLLcdf<int16_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::INTEGER], LogicalType::LIST(LogicalType::INTEGER), LogicalType::BOOLEAN}, LogicalType::LIST(LogicalType::INTEGER), DSKLLcdf<int32_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::BIGINT], LogicalType::LIST(LogicalType::BIGINT), LogicalType::BOOLEAN}, LogicalType::LIST(LogicalType::BIGINT), DSKLLcdf<int64_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::FLOAT], LogicalType::LIST(LogicalType::FLOAT), LogicalType::BOOLEAN}, LogicalType::LIST(LogicalType::FLOAT), DSKLLcdf<float>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::DOUBLE], LogicalType::LIST(LogicalType::DOUBLE), LogicalType::BOOLEAN}, LogicalType::LIST(LogicalType::DOUBLE), DSKLLcdf<double>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UTINYINT], LogicalType::LIST(LogicalType::UTINYINT), LogicalType::BOOLEAN}, LogicalType::LIST(LogicalType::UTINYINT), DSKLLcdf<uint8_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::USMALLINT], LogicalType::LIST(LogicalType::USMALLINT), LogicalType::BOOLEAN}, LogicalType::LIST(LogicalType::USMALLINT), DSKLLcdf<uint16_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UINTEGER], LogicalType::LIST(LogicalType::UINTEGER), LogicalType::BOOLEAN}, LogicalType::LIST(LogicalType::UINTEGER), DSKLLcdf<uint32_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UBIGINT], LogicalType::LIST(LogicalType::UBIGINT), LogicalType::BOOLEAN}, LogicalType::LIST(LogicalType::UBIGINT), DSKLLcdf<uint64_t>));

            CreateScalarFunctionInfo info(std::move(fs));

            {
                FunctionDescription desc;
                desc.description = "Return the Cumulative Distribution Function (CDF) of the sketch for a series of points";
                desc.examples.push_back("datasketch_kll_cdf(sketch, points, inclusive)");
                info.descriptions.push_back(desc);
            }

            system_catalog.CreateFunction(data, info);
        }
        {
            ScalarFunctionSet fs("datasketch_kll_pmf");

            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::TINYINT], LogicalType::LIST(LogicalType::TINYINT), LogicalType::BOOLEAN}, LogicalType::LIST(LogicalType::TINYINT), DSKLLpmf<int8_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::SMALLINT], LogicalType::LIST(LogicalType::SMALLINT), LogicalType::BOOLEAN}, LogicalType::LIST(LogicalType::SMALLINT), DSKLLpmf<int16_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::INTEGER], LogicalType::LIST(LogicalType::INTEGER), LogicalType::BOOLEAN}, LogicalType::LIST(LogicalType::INTEGER), DSKLLpmf<int32_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::BIGINT], LogicalType::LIST(LogicalType::BIGINT), LogicalType::BOOLEAN}, LogicalType::LIST(LogicalType::BIGINT), DSKLLpmf<int64_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::FLOAT], LogicalType::LIST(LogicalType::FLOAT), LogicalType::BOOLEAN}, LogicalType::LIST(LogicalType::FLOAT), DSKLLpmf<float>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::DOUBLE], LogicalType::LIST(LogicalType::DOUBLE), LogicalType::BOOLEAN}, LogicalType::LIST(LogicalType::DOUBLE), DSKLLpmf<double>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UTINYINT], LogicalType::LIST(LogicalType::UTINYINT), LogicalType::BOOLEAN}, LogicalType::LIST(LogicalType::UTINYINT), DSKLLpmf<uint8_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::USMALLINT], LogicalType::LIST(LogicalType::USMALLINT), LogicalType::BOOLEAN}, LogicalType::LIST(LogicalType::USMALLINT), DSKLLpmf<uint16_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UINTEGER], LogicalType::LIST(LogicalType::UINTEGER), LogicalType::BOOLEAN}, LogicalType::LIST(LogicalType::UINTEGER), DSKLLpmf<uint32_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UBIGINT], LogicalType::LIST(LogicalType::UBIGINT), LogicalType::BOOLEAN}, LogicalType::LIST(LogicalType::UBIGINT), DSKLLpmf<uint64_t>));

            CreateScalarFunctionInfo info(std::move(fs));

            {
                FunctionDescription desc;
                desc.description = "Return the Probability Mass Function (PMF) of the sketch for a series of points";
                desc.examples.push_back("datasketch_kll_pmf(sketch, points, inclusive)");
                info.descriptions.push_back(desc);
            }

            system_catalog.CreateFunction(data, info);
        }
        {
            ScalarFunctionSet fs("datasketch_kll_normalized_rank_error");

            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::TINYINT], LogicalType::BOOLEAN}, LogicalType::DOUBLE, DSKLLnormalized_rank_error<int8_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::SMALLINT], LogicalType::BOOLEAN}, LogicalType::DOUBLE, DSKLLnormalized_rank_error<int16_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::INTEGER], LogicalType::BOOLEAN}, LogicalType::DOUBLE, DSKLLnormalized_rank_error<int32_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::BIGINT], LogicalType::BOOLEAN}, LogicalType::DOUBLE, DSKLLnormalized_rank_error<int64_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::FLOAT], LogicalType::BOOLEAN}, LogicalType::DOUBLE, DSKLLnormalized_rank_error<float>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::DOUBLE], LogicalType::BOOLEAN}, LogicalType::DOUBLE, DSKLLnormalized_rank_error<double>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UTINYINT], LogicalType::BOOLEAN}, LogicalType::DOUBLE, DSKLLnormalized_rank_error<uint8_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::USMALLINT], LogicalType::BOOLEAN}, LogicalType::DOUBLE, DSKLLnormalized_rank_error<uint16_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UINTEGER], LogicalType::BOOLEAN}, LogicalType::DOUBLE, DSKLLnormalized_rank_error<uint32_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UBIGINT], LogicalType::BOOLEAN}, LogicalType::DOUBLE, DSKLLnormalized_rank_error<uint64_t>));

            CreateScalarFunctionInfo info(std::move(fs));

            {
                FunctionDescription desc;
                desc.description = "Return the normalized rank error of the sketch";
                desc.examples.push_back("datasketch_kll_normalized_rank_error(sketch, is_pmf)");
                info.descriptions.push_back(desc);
            }

            system_catalog.CreateFunction(data, info);
        }
        {
            ScalarFunctionSet fs("datasketch_kll_describe");

            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::TINYINT], LogicalType::BOOLEAN, LogicalType::BOOLEAN}, LogicalType::VARCHAR, DSKLLdescribe<int8_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::SMALLINT], LogicalType::BOOLEAN, LogicalType::BOOLEAN}, LogicalType::VARCHAR, DSKLLdescribe<int16_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::INTEGER], LogicalType::BOOLEAN, LogicalType::BOOLEAN}, LogicalType::VARCHAR, DSKLLdescribe<int32_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::BIGINT], LogicalType::BOOLEAN, LogicalType::BOOLEAN}, LogicalType::VARCHAR, DSKLLdescribe<int64_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::FLOAT], LogicalType::BOOLEAN, LogicalType::BOOLEAN}, LogicalType::VARCHAR, DSKLLdescribe<float>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::DOUBLE], LogicalType::BOOLEAN, LogicalType::BOOLEAN}, LogicalType::VARCHAR, DSKLLdescribe<double>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UTINYINT], LogicalType::BOOLEAN, LogicalType::BOOLEAN}, LogicalType::VARCHAR, DSKLLdescribe<uint8_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::USMALLINT], LogicalType::BOOLEAN, LogicalType::BOOLEAN}, LogicalType::VARCHAR, DSKLLdescribe<uint16_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UINTEGER], LogicalType::BOOLEAN, LogicalType::BOOLEAN}, LogicalType::VARCHAR, DSKLLdescribe<uint32_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UBIGINT], LogicalType::BOOLEAN, LogicalType::BOOLEAN}, LogicalType::VARCHAR, DSKLLdescribe<uint64_t>));

            CreateScalarFunctionInfo info(std::move(fs));

            {
                FunctionDescription desc;
                desc.description = "Return a description of this sketch";
                desc.examples.push_back("datasketch_kll_describe(sketch, include_levels, include_items)");
                info.descriptions.push_back(desc);
            }

            system_catalog.CreateFunction(data, info);
        }
        {
            ScalarFunctionSet fs("datasketch_kll_rank");

            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::TINYINT], LogicalType::TINYINT, LogicalType::BOOLEAN}, LogicalType::DOUBLE, DSKLLrank<int8_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::SMALLINT], LogicalType::SMALLINT, LogicalType::BOOLEAN}, LogicalType::DOUBLE, DSKLLrank<int16_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::INTEGER], LogicalType::INTEGER, LogicalType::BOOLEAN}, LogicalType::DOUBLE, DSKLLrank<int32_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::BIGINT], LogicalType::BIGINT, LogicalType::BOOLEAN}, LogicalType::DOUBLE, DSKLLrank<int64_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::FLOAT], LogicalType::FLOAT, LogicalType::BOOLEAN}, LogicalType::DOUBLE, DSKLLrank<float>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::DOUBLE], LogicalType::DOUBLE, LogicalType::BOOLEAN}, LogicalType::DOUBLE, DSKLLrank<double>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UTINYINT], LogicalType::UTINYINT, LogicalType::BOOLEAN}, LogicalType::DOUBLE, DSKLLrank<uint8_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::USMALLINT], LogicalType::USMALLINT, LogicalType::BOOLEAN}, LogicalType::DOUBLE, DSKLLrank<uint16_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UINTEGER], LogicalType::UINTEGER, LogicalType::BOOLEAN}, LogicalType::DOUBLE, DSKLLrank<uint32_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UBIGINT], LogicalType::UBIGINT, LogicalType::BOOLEAN}, LogicalType::DOUBLE, DSKLLrank<uint64_t>));

            CreateScalarFunctionInfo info(std::move(fs));

            {
                FunctionDescription desc;
                desc.description = "Return the rank of an item in the sketch";
                desc.examples.push_back("datasketch_kll_rank(sketch, item, inclusive)");
                info.descriptions.push_back(desc);
            }

            system_catalog.CreateFunction(data, info);
        }
        {
            ScalarFunctionSet fs("datasketch_kll_quantile");

            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::TINYINT], LogicalType::DOUBLE, LogicalType::BOOLEAN}, LogicalType::TINYINT, DSKLLquantile<int8_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::SMALLINT], LogicalType::DOUBLE, LogicalType::BOOLEAN}, LogicalType::SMALLINT, DSKLLquantile<int16_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::INTEGER], LogicalType::DOUBLE, LogicalType::BOOLEAN}, LogicalType::INTEGER, DSKLLquantile<int32_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::BIGINT], LogicalType::DOUBLE, LogicalType::BOOLEAN}, LogicalType::BIGINT, DSKLLquantile<int64_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::FLOAT], LogicalType::DOUBLE, LogicalType::BOOLEAN}, LogicalType::FLOAT, DSKLLquantile<float>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::DOUBLE], LogicalType::DOUBLE, LogicalType::BOOLEAN}, LogicalType::DOUBLE, DSKLLquantile<double>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UTINYINT], LogicalType::DOUBLE, LogicalType::BOOLEAN}, LogicalType::UTINYINT, DSKLLquantile<uint8_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::USMALLINT], LogicalType::DOUBLE, LogicalType::BOOLEAN}, LogicalType::USMALLINT, DSKLLquantile<uint16_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UINTEGER], LogicalType::DOUBLE, LogicalType::BOOLEAN}, LogicalType::UINTEGER, DSKLLquantile<uint32_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UBIGINT], LogicalType::DOUBLE, LogicalType::BOOLEAN}, LogicalType::UBIGINT, DSKLLquantile<uint64_t>));

            CreateScalarFunctionInfo info(std::move(fs));

            {
                FunctionDescription desc;
                desc.description = "Return the quantile of a rank in the sketch";
                desc.examples.push_back("datasketch_kll_rank(sketch, rank, inclusive)");
                info.descriptions.push_back(desc);
            }

            system_catalog.CreateFunction(data, info);
        }
        {
            ScalarFunctionSet fs("datasketch_kll_n");

            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::TINYINT]}, LogicalType::UBIGINT, DSKLLn<int8_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::SMALLINT]}, LogicalType::UBIGINT, DSKLLn<int16_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::INTEGER]}, LogicalType::UBIGINT, DSKLLn<int32_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::BIGINT]}, LogicalType::UBIGINT, DSKLLn<int64_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::FLOAT]}, LogicalType::UBIGINT, DSKLLn<float>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::DOUBLE]}, LogicalType::UBIGINT, DSKLLn<double>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UTINYINT]}, LogicalType::UBIGINT, DSKLLn<uint8_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::USMALLINT]}, LogicalType::UBIGINT, DSKLLn<uint16_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UINTEGER]}, LogicalType::UBIGINT, DSKLLn<uint32_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UBIGINT]}, LogicalType::UBIGINT, DSKLLn<uint64_t>));

            CreateScalarFunctionInfo info(std::move(fs));

            {
                FunctionDescription desc;
                desc.description = "Return the number of items contained in the sketch";
                desc.examples.push_back("datasketch_kll_rank(sketch)");
                info.descriptions.push_back(desc);
            }

            system_catalog.CreateFunction(data, info);
        }
        {
            ScalarFunctionSet fs("datasketch_kll_is_estimation_mode");

            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::TINYINT]}, LogicalType::BOOLEAN, DSKLLis_estimation_mode<int8_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::SMALLINT]}, LogicalType::BOOLEAN, DSKLLis_estimation_mode<int16_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::INTEGER]}, LogicalType::BOOLEAN, DSKLLis_estimation_mode<int32_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::BIGINT]}, LogicalType::BOOLEAN, DSKLLis_estimation_mode<int64_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::FLOAT]}, LogicalType::BOOLEAN, DSKLLis_estimation_mode<float>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::DOUBLE]}, LogicalType::BOOLEAN, DSKLLis_estimation_mode<double>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UTINYINT]}, LogicalType::BOOLEAN, DSKLLis_estimation_mode<uint8_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::USMALLINT]}, LogicalType::BOOLEAN, DSKLLis_estimation_mode<uint16_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UINTEGER]}, LogicalType::BOOLEAN, DSKLLis_estimation_mode<uint32_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UBIGINT]}, LogicalType::BOOLEAN, DSKLLis_estimation_mode<uint64_t>));

            CreateScalarFunctionInfo info(std::move(fs));

            {
                FunctionDescription desc;
                desc.description = "Return a boolean indicating if the sketch is in estimation mode";
                desc.examples.push_back("datasketch_kll_is_estimation_mode(sketch)");
                info.descriptions.push_back(desc);
            }

            system_catalog.CreateFunction(data, info);
        }
        {
            ScalarFunctionSet fs("datasketch_kll_num_retained");

            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::TINYINT]}, LogicalType::UBIGINT, DSKLLnum_retained<int8_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::SMALLINT]}, LogicalType::UBIGINT, DSKLLnum_retained<int16_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::INTEGER]}, LogicalType::UBIGINT, DSKLLnum_retained<int32_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::BIGINT]}, LogicalType::UBIGINT, DSKLLnum_retained<int64_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::FLOAT]}, LogicalType::UBIGINT, DSKLLnum_retained<float>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::DOUBLE]}, LogicalType::UBIGINT, DSKLLnum_retained<double>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UTINYINT]}, LogicalType::UBIGINT, DSKLLnum_retained<uint8_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::USMALLINT]}, LogicalType::UBIGINT, DSKLLnum_retained<uint16_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UINTEGER]}, LogicalType::UBIGINT, DSKLLnum_retained<uint32_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UBIGINT]}, LogicalType::UBIGINT, DSKLLnum_retained<uint64_t>));

            CreateScalarFunctionInfo info(std::move(fs));

            {
                FunctionDescription desc;
                desc.description = "Return the number of retained items in the sketch";
                desc.examples.push_back("datasketch_kll_num_retained(sketch)");
                info.descriptions.push_back(desc);
            }

            system_catalog.CreateFunction(data, info);
        }
        {
            ScalarFunctionSet fs("datasketch_kll_min_item");

            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::TINYINT]}, LogicalType::TINYINT, DSKLLmin_item<int8_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::SMALLINT]}, LogicalType::SMALLINT, DSKLLmin_item<int16_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::INTEGER]}, LogicalType::INTEGER, DSKLLmin_item<int32_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::BIGINT]}, LogicalType::BIGINT, DSKLLmin_item<int64_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::FLOAT]}, LogicalType::FLOAT, DSKLLmin_item<float>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::DOUBLE]}, LogicalType::DOUBLE, DSKLLmin_item<double>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UTINYINT]}, LogicalType::UTINYINT, DSKLLmin_item<uint8_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::USMALLINT]}, LogicalType::USMALLINT, DSKLLmin_item<uint16_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UINTEGER]}, LogicalType::UINTEGER, DSKLLmin_item<uint32_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UBIGINT]}, LogicalType::UBIGINT, DSKLLmin_item<uint64_t>));

            CreateScalarFunctionInfo info(std::move(fs));

            {
                FunctionDescription desc;
                desc.description = "Return the minimum item in the sketch";
                desc.examples.push_back("datasketch_kll_min_item(sketch)");
                info.descriptions.push_back(desc);
            }

            system_catalog.CreateFunction(data, info);
        }
        {
            ScalarFunctionSet fs("datasketch_kll_max_item");

            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::TINYINT]}, LogicalType::TINYINT, DSKLLmax_item<int8_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::SMALLINT]}, LogicalType::SMALLINT, DSKLLmax_item<int16_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::INTEGER]}, LogicalType::INTEGER, DSKLLmax_item<int32_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::BIGINT]}, LogicalType::BIGINT, DSKLLmax_item<int64_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::FLOAT]}, LogicalType::FLOAT, DSKLLmax_item<float>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::DOUBLE]}, LogicalType::DOUBLE, DSKLLmax_item<double>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UTINYINT]}, LogicalType::UTINYINT, DSKLLmax_item<uint8_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::USMALLINT]}, LogicalType::USMALLINT, DSKLLmax_item<uint16_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UINTEGER]}, LogicalType::UINTEGER, DSKLLmax_item<uint32_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UBIGINT]}, LogicalType::UBIGINT, DSKLLmax_item<uint64_t>));

            CreateScalarFunctionInfo info(std::move(fs));

            {
                FunctionDescription desc;
                desc.description = "Return the maxium item in the sketch";
                desc.examples.push_back("datasketch_kll_max_item(sketch)");
                info.descriptions.push_back(desc);
            }

            system_catalog.CreateFunction(data, info);
        }

        // This funciton creates the sketches.
        {
            AggregateFunctionSet sketch("datasketch_kll");

            {
                auto fun = DSKLLCreateAggregate<int8_t>(LogicalType::TINYINT, sketch_map_types[LogicalTypeId::TINYINT]);
                fun.bind = DSKLLBind;
                fun.arguments.insert(fun.arguments.begin(), LogicalType::INTEGER);
                sketch.AddFunction(fun);
            }
            // sketch.AddFunction(DSKLLCreateAggregate<int8_t>(LogicalType::TINYINT, sketch_map_types[LogicalTypeId::TINYINT]));
            {
                auto fun = DSKLLMergeAggregate<int8_t>(LogicalType::TINYINT, sketch_map_types[LogicalTypeId::TINYINT]);
                fun.bind = DSKLLBind;
                fun.arguments.insert(fun.arguments.begin(), LogicalType::INTEGER);
                sketch.AddFunction(fun);
            }
            // sketch.AddFunction(DSKLLMergeAggregate<int8_t>(LogicalType::TINYINT, sketch_map_types[LogicalTypeId::TINYINT]));

            {
                auto fun = DSKLLCreateAggregate<int16_t>(LogicalType::SMALLINT, sketch_map_types[LogicalTypeId::SMALLINT]);
                fun.bind = DSKLLBind;
                fun.arguments.insert(fun.arguments.begin(), LogicalType::INTEGER);
                sketch.AddFunction(fun);
            }
            // sketch.AddFunction(DSKLLCreateAggregate<int16_t>(LogicalType::SMALLINT, sketch_map_types[LogicalTypeId::SMALLINT]));
            {
                auto fun = DSKLLMergeAggregate<int16_t>(LogicalType::SMALLINT, sketch_map_types[LogicalTypeId::SMALLINT]);
                fun.bind = DSKLLBind;
                fun.arguments.insert(fun.arguments.begin(), LogicalType::INTEGER);
                sketch.AddFunction(fun);
            }
            // sketch.AddFunction(DSKLLMergeAggregate<int16_t>(LogicalType::SMALLINT, sketch_map_types[LogicalTypeId::SMALLINT]));

            {
                auto fun = DSKLLCreateAggregate<int32_t>(LogicalType::INTEGER, sketch_map_types[LogicalTypeId::INTEGER]);
                fun.bind = DSKLLBind;
                fun.arguments.insert(fun.arguments.begin(), LogicalType::INTEGER);
                sketch.AddFunction(fun);
            }
            // sketch.AddFunction(DSKLLCreateAggregate<int32_t>(LogicalType::INTEGER, sketch_map_types[LogicalTypeId::INTEGER]));
            {
                auto fun = DSKLLMergeAggregate<int32_t>(LogicalType::INTEGER, sketch_map_types[LogicalTypeId::INTEGER]);
                fun.bind = DSKLLBind;
                fun.arguments.insert(fun.arguments.begin(), LogicalType::INTEGER);
                sketch.AddFunction(fun);
            }
            // sketch.AddFunction(DSKLLMergeAggregate<int32_t>(LogicalType::INTEGER, sketch_map_types[LogicalTypeId::INTEGER]));

            {
                auto fun = DSKLLCreateAggregate<int64_t>(LogicalType::BIGINT, sketch_map_types[LogicalTypeId::BIGINT]);
                fun.bind = DSKLLBind;
                fun.arguments.insert(fun.arguments.begin(), LogicalType::INTEGER);
                sketch.AddFunction(fun);
            }
            // sketch.AddFunction(DSKLLCreateAggregate<int64_t>(LogicalType::BIGINT, sketch_map_types[LogicalTypeId::BIGINT]));
            {
                auto fun = DSKLLMergeAggregate<int64_t>(LogicalType::BIGINT, sketch_map_types[LogicalTypeId::BIGINT]);
                fun.bind = DSKLLBind;
                fun.arguments.insert(fun.arguments.begin(), LogicalType::INTEGER);
                sketch.AddFunction(fun);
            }
            // sketch.AddFunction(DSKLLMergeAggregate<int64_t>(LogicalType::BIGINT, sketch_map_types[LogicalTypeId::BIGINT]));

            {
                auto fun = DSKLLCreateAggregate<float>(LogicalType::FLOAT, sketch_map_types[LogicalTypeId::FLOAT]);
                fun.bind = DSKLLBind;
                fun.arguments.insert(fun.arguments.begin(), LogicalType::INTEGER);
                sketch.AddFunction(fun);
            }
            // sketch.AddFunction(DSKLLCreateAggregate<float>(LogicalType::FLOAT, sketch_map_types[LogicalTypeId::FLOAT]));
            {
                auto fun = DSKLLMergeAggregate<float>(LogicalType::FLOAT, sketch_map_types[LogicalTypeId::FLOAT]);
                fun.bind = DSKLLBind;
                fun.arguments.insert(fun.arguments.begin(), LogicalType::INTEGER);
                sketch.AddFunction(fun);
            }
            // sketch.AddFunction(DSKLLMergeAggregate<float>(LogicalType::FLOAT, sketch_map_types[LogicalTypeId::FLOAT]));

            {
                auto fun = DSKLLCreateAggregate<double>(LogicalType::DOUBLE, sketch_map_types[LogicalTypeId::DOUBLE]);
                fun.bind = DSKLLBind;
                fun.arguments.insert(fun.arguments.begin(), LogicalType::INTEGER);
                sketch.AddFunction(fun);
            }
            // sketch.AddFunction(DSKLLCreateAggregate<double>(LogicalType::DOUBLE, sketch_map_types[LogicalTypeId::DOUBLE]));
            {
                auto fun = DSKLLMergeAggregate<double>(LogicalType::DOUBLE, sketch_map_types[LogicalTypeId::DOUBLE]);
                fun.bind = DSKLLBind;
                fun.arguments.insert(fun.arguments.begin(), LogicalType::INTEGER);
                sketch.AddFunction(fun);
            }
            // sketch.AddFunction(DSKLLMergeAggregate<double>(LogicalType::DOUBLE, sketch_map_types[LogicalTypeId::DOUBLE]));

            {
                auto fun = DSKLLCreateAggregate<uint8_t>(LogicalType::UTINYINT, sketch_map_types[LogicalTypeId::UTINYINT]);
                fun.bind = DSKLLBind;
                fun.arguments.insert(fun.arguments.begin(), LogicalType::INTEGER);
                sketch.AddFunction(fun);
            }
            // sketch.AddFunction(DSKLLCreateAggregate<uint8_t>(LogicalType::UTINYINT, sketch_map_types[LogicalTypeId::UTINYINT]));
            {
                auto fun = DSKLLMergeAggregate<uint8_t>(LogicalType::UTINYINT, sketch_map_types[LogicalTypeId::UTINYINT]);
                fun.bind = DSKLLBind;
                fun.arguments.insert(fun.arguments.begin(), LogicalType::INTEGER);
                sketch.AddFunction(fun);
            }
            // sketch.AddFunction(DSKLLMergeAggregate<uint8_t>(LogicalType::UTINYINT, sketch_map_types[LogicalTypeId::UTINYINT]));

            {
                auto fun = DSKLLCreateAggregate<uint16_t>(LogicalType::USMALLINT, sketch_map_types[LogicalTypeId::USMALLINT]);
                fun.bind = DSKLLBind;
                fun.arguments.insert(fun.arguments.begin(), LogicalType::INTEGER);
                sketch.AddFunction(fun);
            }
            // sketch.AddFunction(DSKLLCreateAggregate<uint16_t>(LogicalType::USMALLINT, sketch_map_types[LogicalTypeId::USMALLINT]));
            {
                auto fun = DSKLLMergeAggregate<uint16_t>(LogicalType::USMALLINT, sketch_map_types[LogicalTypeId::USMALLINT]);
                fun.bind = DSKLLBind;
                fun.arguments.insert(fun.arguments.begin(), LogicalType::INTEGER);
                sketch.AddFunction(fun);
            }
            // sketch.AddFunction(DSKLLMergeAggregate<uint16_t>(LogicalType::USMALLINT, sketch_map_types[LogicalTypeId::USMALLINT]));

            {
                auto fun = DSKLLCreateAggregate<uint32_t>(LogicalType::UINTEGER, sketch_map_types[LogicalTypeId::UINTEGER]);
                fun.bind = DSKLLBind;
                fun.arguments.insert(fun.arguments.begin(), LogicalType::INTEGER);
                sketch.AddFunction(fun);
            }
            // sketch.AddFunction(DSKLLCreateAggregate<uint32_t>(LogicalType::UINTEGER, sketch_map_types[LogicalTypeId::UINTEGER]));
            {
                auto fun = DSKLLMergeAggregate<uint32_t>(LogicalType::UINTEGER, sketch_map_types[LogicalTypeId::UINTEGER]);
                fun.bind = DSKLLBind;
                fun.arguments.insert(fun.arguments.begin(), LogicalType::INTEGER);
                sketch.AddFunction(fun);
            }
            // sketch.AddFunction(DSKLLMergeAggregate<uint32_t>(LogicalType::UINTEGER, sketch_map_types[LogicalTypeId::UINTEGER]));

            {
                auto fun = DSKLLCreateAggregate<uint64_t>(LogicalType::UBIGINT, sketch_map_types[LogicalTypeId::UBIGINT]);
                fun.bind = DSKLLBind;
                fun.arguments.insert(fun.arguments.begin(), LogicalType::INTEGER);
                sketch.AddFunction(fun);
            }
            // sketch.AddFunction(DSKLLCreateAggregate<uint64_t>(LogicalType::UBIGINT, sketch_map_types[LogicalTypeId::UBIGINT]));
            {
                auto fun = DSKLLMergeAggregate<uint64_t>(LogicalType::UBIGINT, sketch_map_types[LogicalTypeId::UBIGINT]);
                fun.bind = DSKLLBind;
                fun.arguments.insert(fun.arguments.begin(), LogicalType::INTEGER);
                sketch.AddFunction(fun);
            }
            // sketch.AddFunction(DSKLLMergeAggregate<uint64_t>(LogicalType::UBIGINT, sketch_map_types[LogicalTypeId::UBIGINT]));

            CreateAggregateFunctionInfo sketch_info(sketch);

            {
                FunctionDescription desc;
                desc.description = "Creates a sketch_kll data sketch by aggregating values or by aggregating other KLL data sketches";
                desc.examples.push_back("datasketch_kll(k, data)");
                sketch_info.descriptions.push_back(desc);
            }

            system_catalog.CreateFunction(data, sketch_info);
        }
    }

    template <class T>

    static inline void DSREQis_empty(DataChunk &args, ExpressionState &state, Vector &result)
    {
        // Get the references to the incoming vectors.
        D_ASSERT(args.ColumnCount() == 1);

        auto &sketch_vector = args.data[0];

        UnaryExecutor::Execute<string_t, bool>(
            sketch_vector, result, args.size(),
            [&](string_t sketch_data)
            {
                auto sketch = datasketches::req_sketch<T>::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
                return sketch.is_empty();
            });
    }

    template <class T>

    static inline void DSREQk(DataChunk &args, ExpressionState &state, Vector &result)
    {
        // Get the references to the incoming vectors.
        D_ASSERT(args.ColumnCount() == 1);

        auto &sketch_vector = args.data[0];

        UnaryExecutor::Execute<string_t, uint16_t>(
            sketch_vector, result, args.size(),
            [&](string_t sketch_data)
            {
                auto sketch = datasketches::req_sketch<T>::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
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

        UnifiedVectorFormat unified_split_points;
        split_points_vector.ToUnifiedFormat(args.size(), unified_split_points);

        // auto split_points_list_entries = UnifiedVectorFormat::GetData<list_entry_t>(unified_split_points);
        //         auto split_points_validitiy = FlatVector::Validity(split_points_vector);

        auto &split_points_list_children = ListVector::GetEntry(split_points_vector);

        UnifiedVectorFormat split_points_children_unified;
        split_points_list_children.ToUnifiedFormat(args.size(), split_points_children_unified);

        const T *split_points_list_children_data = UnifiedVectorFormat::GetData<T>(split_points_children_unified);

        TernaryExecutor::Execute<string_t, list_entry_t, bool, list_entry_t>(
            sketch_vector, split_points_vector, inclusive_vector, result, args.size(),
            [&](string_t sketch_data, list_entry_t split_points_data, bool inclusive_data)
            {
                auto sketch = datasketches::req_sketch<T>::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());

                T *passing_points = (T *)malloc(sizeof(T) * split_points_data.length);
                for (idx_t i = 0; i < split_points_data.length; i++)
                {
                    passing_points[i] = split_points_list_children_data[i + split_points_data.offset];
                }

                auto cdf_result = sketch.get_CDF(passing_points, split_points_data.length, inclusive_data);
                free(passing_points);
                auto current_size = ListVector::GetListSize(result);
                auto new_size = current_size + cdf_result.size();
                if (ListVector::GetListCapacity(result) < new_size)
                {
                    ListVector::Reserve(result, new_size);
                }

                auto &child_entry = ListVector::GetEntry(result);
                auto child_vals = FlatVector::GetData<T>(child_entry);
                // auto &child_validity = FlatVector::Validity(child_entry);
                for (idx_t i = 0; i < cdf_result.size(); i++)
                {
                    child_vals[current_size + i] = cdf_result[i];
                }
                ListVector::SetListSize(result, new_size);
                return list_entry_t{current_size, cdf_result.size()};
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

        UnifiedVectorFormat unified_split_points;
        split_points_vector.ToUnifiedFormat(args.size(), unified_split_points);

        // auto split_points_list_entries = UnifiedVectorFormat::GetData<list_entry_t>(unified_split_points);
        //         auto split_points_validitiy = FlatVector::Validity(split_points_vector);

        auto &split_points_list_children = ListVector::GetEntry(split_points_vector);

        UnifiedVectorFormat split_points_children_unified;
        split_points_list_children.ToUnifiedFormat(args.size(), split_points_children_unified);

        const T *split_points_list_children_data = UnifiedVectorFormat::GetData<T>(split_points_children_unified);

        TernaryExecutor::Execute<string_t, list_entry_t, bool, list_entry_t>(
            sketch_vector, split_points_vector, inclusive_vector, result, args.size(),
            [&](string_t sketch_data, list_entry_t split_points_data, bool inclusive_data)
            {
                auto sketch = datasketches::req_sketch<T>::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());

                T *passing_points = (T *)malloc(sizeof(T) * split_points_data.length);
                for (idx_t i = 0; i < split_points_data.length; i++)
                {
                    passing_points[i] = split_points_list_children_data[i + split_points_data.offset];
                }

                auto pmf_result = sketch.get_PMF(passing_points, split_points_data.length, inclusive_data);
                free(passing_points);

                auto current_size = ListVector::GetListSize(result);
                auto new_size = current_size + pmf_result.size();
                if (ListVector::GetListCapacity(result) < new_size)
                {
                    ListVector::Reserve(result, new_size);
                }

                auto &child_entry = ListVector::GetEntry(result);
                auto child_vals = FlatVector::GetData<double>(child_entry);
                // auto &child_validity = FlatVector::Validity(child_entry);
                for (idx_t i = 0; i < pmf_result.size(); i++)
                {
                    child_vals[current_size + i] = pmf_result[i];
                }
                ListVector::SetListSize(result, new_size);
                return list_entry_t{current_size, pmf_result.size()};
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

        TernaryExecutor::Execute<string_t, bool, bool, string_t>(
            sketch_vector, include_levels_vector, include_items_vector, result, args.size(),
            [&](string_t sketch_data, bool include_levels_data, bool include_items_data)
            {
                auto sketch = datasketches::req_sketch<T>::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
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

        TernaryExecutor::Execute<string_t, T, bool, double>(
            sketch_vector, item_vector, inclusive_vector, result, args.size(),
            [&](string_t sketch_data, T item_data, bool inclusive_data)
            {
                auto sketch = datasketches::req_sketch<T>::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
                return sketch.get_rank(item_data, inclusive_data);
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

        TernaryExecutor::Execute<string_t, double, bool, T>(
            sketch_vector, rank_vector, inclusive_vector, result, args.size(),
            [&](string_t sketch_data, double rank_data, bool inclusive_data)
            {
                auto sketch = datasketches::req_sketch<T>::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
                return sketch.get_quantile(rank_data, inclusive_data);
            });
    }

    template <class T>

    static inline void DSREQn(DataChunk &args, ExpressionState &state, Vector &result)
    {
        // Get the references to the incoming vectors.
        D_ASSERT(args.ColumnCount() == 1);

        auto &sketch_vector = args.data[0];

        UnaryExecutor::Execute<string_t, uint64_t>(
            sketch_vector, result, args.size(),
            [&](string_t sketch_data)
            {
                auto sketch = datasketches::req_sketch<T>::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
                return sketch.get_n();
            });
    }

    template <class T>

    static inline void DSREQis_estimation_mode(DataChunk &args, ExpressionState &state, Vector &result)
    {
        // Get the references to the incoming vectors.
        D_ASSERT(args.ColumnCount() == 1);

        auto &sketch_vector = args.data[0];

        UnaryExecutor::Execute<string_t, bool>(
            sketch_vector, result, args.size(),
            [&](string_t sketch_data)
            {
                auto sketch = datasketches::req_sketch<T>::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
                return sketch.is_estimation_mode();
            });
    }

    template <class T>

    static inline void DSREQnum_retained(DataChunk &args, ExpressionState &state, Vector &result)
    {
        // Get the references to the incoming vectors.
        D_ASSERT(args.ColumnCount() == 1);

        auto &sketch_vector = args.data[0];

        UnaryExecutor::Execute<string_t, uint64_t>(
            sketch_vector, result, args.size(),
            [&](string_t sketch_data)
            {
                auto sketch = datasketches::req_sketch<T>::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
                return sketch.get_num_retained();
            });
    }

    template <class T>

    static inline void DSREQmin_item(DataChunk &args, ExpressionState &state, Vector &result)
    {
        // Get the references to the incoming vectors.
        D_ASSERT(args.ColumnCount() == 1);

        auto &sketch_vector = args.data[0];

        UnaryExecutor::Execute<string_t, T>(
            sketch_vector, result, args.size(),
            [&](string_t sketch_data)
            {
                auto sketch = datasketches::req_sketch<T>::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
                return sketch.get_min_item();
            });
    }

    template <class T>

    static inline void DSREQmax_item(DataChunk &args, ExpressionState &state, Vector &result)
    {
        // Get the references to the incoming vectors.
        D_ASSERT(args.ColumnCount() == 1);

        auto &sketch_vector = args.data[0];

        UnaryExecutor::Execute<string_t, T>(
            sketch_vector, result, args.size(),
            [&](string_t sketch_data)
            {
                auto sketch = datasketches::req_sketch<T>::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
                return sketch.get_max_item();
            });
    }

    template <typename T>
    auto static DSREQMergeAggregate(const LogicalType &type, const LogicalType &result_type) -> AggregateFunction

    {

        return AggregateFunction::UnaryAggregateDestructor<DSREQState<T>, string_t, string_t, DSQuantilesMergeOperation<DSREQBindData>, AggregateDestructorType::LEGACY>(
            result_type, result_type);
    }

    template <typename T>
    auto static DSREQCreateAggregate(const LogicalType &type, const LogicalType &result_type) -> AggregateFunction
    {

        return AggregateFunction::UnaryAggregateDestructor<DSREQState<T>, T, string_t, DSQuantilesCreateOperation<DSREQBindData>, AggregateDestructorType::LEGACY>(
            type, result_type);
    }

    void LoadREQSketch(DatabaseInstance &instance)
    {
        auto &system_catalog = Catalog::GetSystemCatalog(instance);
        auto data = CatalogTransaction::GetSystemTransaction(instance);

        std::unordered_map<LogicalTypeId, LogicalType> sketch_map_types;

        sketch_map_types.insert({LogicalTypeId::TINYINT, CreateREQSketchType(instance, LogicalType(LogicalTypeId::TINYINT))});
        sketch_map_types.insert({LogicalTypeId::SMALLINT, CreateREQSketchType(instance, LogicalType(LogicalTypeId::SMALLINT))});
        sketch_map_types.insert({LogicalTypeId::INTEGER, CreateREQSketchType(instance, LogicalType(LogicalTypeId::INTEGER))});
        sketch_map_types.insert({LogicalTypeId::BIGINT, CreateREQSketchType(instance, LogicalType(LogicalTypeId::BIGINT))});
        sketch_map_types.insert({LogicalTypeId::FLOAT, CreateREQSketchType(instance, LogicalType(LogicalTypeId::FLOAT))});
        sketch_map_types.insert({LogicalTypeId::DOUBLE, CreateREQSketchType(instance, LogicalType(LogicalTypeId::DOUBLE))});
        sketch_map_types.insert({LogicalTypeId::UTINYINT, CreateREQSketchType(instance, LogicalType(LogicalTypeId::UTINYINT))});
        sketch_map_types.insert({LogicalTypeId::USMALLINT, CreateREQSketchType(instance, LogicalType(LogicalTypeId::USMALLINT))});
        sketch_map_types.insert({LogicalTypeId::UINTEGER, CreateREQSketchType(instance, LogicalType(LogicalTypeId::UINTEGER))});
        sketch_map_types.insert({LogicalTypeId::UBIGINT, CreateREQSketchType(instance, LogicalType(LogicalTypeId::UBIGINT))});

        {
            ScalarFunctionSet fs("datasketch_req_is_empty");

            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::TINYINT]}, LogicalType::BOOLEAN, DSREQis_empty<int8_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::SMALLINT]}, LogicalType::BOOLEAN, DSREQis_empty<int16_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::INTEGER]}, LogicalType::BOOLEAN, DSREQis_empty<int32_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::BIGINT]}, LogicalType::BOOLEAN, DSREQis_empty<int64_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::FLOAT]}, LogicalType::BOOLEAN, DSREQis_empty<float>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::DOUBLE]}, LogicalType::BOOLEAN, DSREQis_empty<double>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UTINYINT]}, LogicalType::BOOLEAN, DSREQis_empty<uint8_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::USMALLINT]}, LogicalType::BOOLEAN, DSREQis_empty<uint16_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UINTEGER]}, LogicalType::BOOLEAN, DSREQis_empty<uint32_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UBIGINT]}, LogicalType::BOOLEAN, DSREQis_empty<uint64_t>));

            CreateScalarFunctionInfo info(std::move(fs));

            {
                FunctionDescription desc;
                desc.description = "Return a boolean indicating if the sketch is empty";
                desc.examples.push_back("datasketch_req_is_empty(sketch)");
                info.descriptions.push_back(desc);
            }

            system_catalog.CreateFunction(data, info);
        }
        {
            ScalarFunctionSet fs("datasketch_req_k");

            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::TINYINT]}, LogicalType::USMALLINT, DSREQk<int8_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::SMALLINT]}, LogicalType::USMALLINT, DSREQk<int16_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::INTEGER]}, LogicalType::USMALLINT, DSREQk<int32_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::BIGINT]}, LogicalType::USMALLINT, DSREQk<int64_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::FLOAT]}, LogicalType::USMALLINT, DSREQk<float>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::DOUBLE]}, LogicalType::USMALLINT, DSREQk<double>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UTINYINT]}, LogicalType::USMALLINT, DSREQk<uint8_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::USMALLINT]}, LogicalType::USMALLINT, DSREQk<uint16_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UINTEGER]}, LogicalType::USMALLINT, DSREQk<uint32_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UBIGINT]}, LogicalType::USMALLINT, DSREQk<uint64_t>));

            CreateScalarFunctionInfo info(std::move(fs));

            {
                FunctionDescription desc;
                desc.description = "Return the value of K for this sketch";
                desc.examples.push_back("datasketch_req_k(sketch)");
                info.descriptions.push_back(desc);
            }

            system_catalog.CreateFunction(data, info);
        }
        {
            ScalarFunctionSet fs("datasketch_req_cdf");

            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::TINYINT], LogicalType::LIST(LogicalType::TINYINT), LogicalType::BOOLEAN}, LogicalType::LIST(LogicalType::TINYINT), DSREQcdf<int8_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::SMALLINT], LogicalType::LIST(LogicalType::SMALLINT), LogicalType::BOOLEAN}, LogicalType::LIST(LogicalType::SMALLINT), DSREQcdf<int16_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::INTEGER], LogicalType::LIST(LogicalType::INTEGER), LogicalType::BOOLEAN}, LogicalType::LIST(LogicalType::INTEGER), DSREQcdf<int32_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::BIGINT], LogicalType::LIST(LogicalType::BIGINT), LogicalType::BOOLEAN}, LogicalType::LIST(LogicalType::BIGINT), DSREQcdf<int64_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::FLOAT], LogicalType::LIST(LogicalType::FLOAT), LogicalType::BOOLEAN}, LogicalType::LIST(LogicalType::FLOAT), DSREQcdf<float>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::DOUBLE], LogicalType::LIST(LogicalType::DOUBLE), LogicalType::BOOLEAN}, LogicalType::LIST(LogicalType::DOUBLE), DSREQcdf<double>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UTINYINT], LogicalType::LIST(LogicalType::UTINYINT), LogicalType::BOOLEAN}, LogicalType::LIST(LogicalType::UTINYINT), DSREQcdf<uint8_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::USMALLINT], LogicalType::LIST(LogicalType::USMALLINT), LogicalType::BOOLEAN}, LogicalType::LIST(LogicalType::USMALLINT), DSREQcdf<uint16_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UINTEGER], LogicalType::LIST(LogicalType::UINTEGER), LogicalType::BOOLEAN}, LogicalType::LIST(LogicalType::UINTEGER), DSREQcdf<uint32_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UBIGINT], LogicalType::LIST(LogicalType::UBIGINT), LogicalType::BOOLEAN}, LogicalType::LIST(LogicalType::UBIGINT), DSREQcdf<uint64_t>));

            CreateScalarFunctionInfo info(std::move(fs));

            {
                FunctionDescription desc;
                desc.description = "Return the Cumulative Distribution Function (CDF) of the sketch for a series of points";
                desc.examples.push_back("datasketch_req_cdf(sketch, points, inclusive)");
                info.descriptions.push_back(desc);
            }

            system_catalog.CreateFunction(data, info);
        }
        {
            ScalarFunctionSet fs("datasketch_req_pmf");

            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::TINYINT], LogicalType::LIST(LogicalType::TINYINT), LogicalType::BOOLEAN}, LogicalType::LIST(LogicalType::TINYINT), DSREQpmf<int8_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::SMALLINT], LogicalType::LIST(LogicalType::SMALLINT), LogicalType::BOOLEAN}, LogicalType::LIST(LogicalType::SMALLINT), DSREQpmf<int16_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::INTEGER], LogicalType::LIST(LogicalType::INTEGER), LogicalType::BOOLEAN}, LogicalType::LIST(LogicalType::INTEGER), DSREQpmf<int32_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::BIGINT], LogicalType::LIST(LogicalType::BIGINT), LogicalType::BOOLEAN}, LogicalType::LIST(LogicalType::BIGINT), DSREQpmf<int64_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::FLOAT], LogicalType::LIST(LogicalType::FLOAT), LogicalType::BOOLEAN}, LogicalType::LIST(LogicalType::FLOAT), DSREQpmf<float>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::DOUBLE], LogicalType::LIST(LogicalType::DOUBLE), LogicalType::BOOLEAN}, LogicalType::LIST(LogicalType::DOUBLE), DSREQpmf<double>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UTINYINT], LogicalType::LIST(LogicalType::UTINYINT), LogicalType::BOOLEAN}, LogicalType::LIST(LogicalType::UTINYINT), DSREQpmf<uint8_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::USMALLINT], LogicalType::LIST(LogicalType::USMALLINT), LogicalType::BOOLEAN}, LogicalType::LIST(LogicalType::USMALLINT), DSREQpmf<uint16_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UINTEGER], LogicalType::LIST(LogicalType::UINTEGER), LogicalType::BOOLEAN}, LogicalType::LIST(LogicalType::UINTEGER), DSREQpmf<uint32_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UBIGINT], LogicalType::LIST(LogicalType::UBIGINT), LogicalType::BOOLEAN}, LogicalType::LIST(LogicalType::UBIGINT), DSREQpmf<uint64_t>));

            CreateScalarFunctionInfo info(std::move(fs));

            {
                FunctionDescription desc;
                desc.description = "Return the Probability Mass Function (PMF) of the sketch for a series of points";
                desc.examples.push_back("datasketch_req_pmf(sketch, points, inclusive)");
                info.descriptions.push_back(desc);
            }

            system_catalog.CreateFunction(data, info);
        }
        {
            ScalarFunctionSet fs("datasketch_req_describe");

            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::TINYINT], LogicalType::BOOLEAN, LogicalType::BOOLEAN}, LogicalType::VARCHAR, DSREQdescribe<int8_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::SMALLINT], LogicalType::BOOLEAN, LogicalType::BOOLEAN}, LogicalType::VARCHAR, DSREQdescribe<int16_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::INTEGER], LogicalType::BOOLEAN, LogicalType::BOOLEAN}, LogicalType::VARCHAR, DSREQdescribe<int32_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::BIGINT], LogicalType::BOOLEAN, LogicalType::BOOLEAN}, LogicalType::VARCHAR, DSREQdescribe<int64_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::FLOAT], LogicalType::BOOLEAN, LogicalType::BOOLEAN}, LogicalType::VARCHAR, DSREQdescribe<float>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::DOUBLE], LogicalType::BOOLEAN, LogicalType::BOOLEAN}, LogicalType::VARCHAR, DSREQdescribe<double>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UTINYINT], LogicalType::BOOLEAN, LogicalType::BOOLEAN}, LogicalType::VARCHAR, DSREQdescribe<uint8_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::USMALLINT], LogicalType::BOOLEAN, LogicalType::BOOLEAN}, LogicalType::VARCHAR, DSREQdescribe<uint16_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UINTEGER], LogicalType::BOOLEAN, LogicalType::BOOLEAN}, LogicalType::VARCHAR, DSREQdescribe<uint32_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UBIGINT], LogicalType::BOOLEAN, LogicalType::BOOLEAN}, LogicalType::VARCHAR, DSREQdescribe<uint64_t>));

            CreateScalarFunctionInfo info(std::move(fs));

            {
                FunctionDescription desc;
                desc.description = "Return a description of this sketch";
                desc.examples.push_back("datasketch_req_describe(sketch, include_levels, include_items)");
                info.descriptions.push_back(desc);
            }

            system_catalog.CreateFunction(data, info);
        }
        {
            ScalarFunctionSet fs("datasketch_req_rank");

            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::TINYINT], LogicalType::TINYINT, LogicalType::BOOLEAN}, LogicalType::DOUBLE, DSREQrank<int8_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::SMALLINT], LogicalType::SMALLINT, LogicalType::BOOLEAN}, LogicalType::DOUBLE, DSREQrank<int16_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::INTEGER], LogicalType::INTEGER, LogicalType::BOOLEAN}, LogicalType::DOUBLE, DSREQrank<int32_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::BIGINT], LogicalType::BIGINT, LogicalType::BOOLEAN}, LogicalType::DOUBLE, DSREQrank<int64_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::FLOAT], LogicalType::FLOAT, LogicalType::BOOLEAN}, LogicalType::DOUBLE, DSREQrank<float>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::DOUBLE], LogicalType::DOUBLE, LogicalType::BOOLEAN}, LogicalType::DOUBLE, DSREQrank<double>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UTINYINT], LogicalType::UTINYINT, LogicalType::BOOLEAN}, LogicalType::DOUBLE, DSREQrank<uint8_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::USMALLINT], LogicalType::USMALLINT, LogicalType::BOOLEAN}, LogicalType::DOUBLE, DSREQrank<uint16_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UINTEGER], LogicalType::UINTEGER, LogicalType::BOOLEAN}, LogicalType::DOUBLE, DSREQrank<uint32_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UBIGINT], LogicalType::UBIGINT, LogicalType::BOOLEAN}, LogicalType::DOUBLE, DSREQrank<uint64_t>));

            CreateScalarFunctionInfo info(std::move(fs));

            {
                FunctionDescription desc;
                desc.description = "Return the rank of an item in the sketch";
                desc.examples.push_back("datasketch_req_rank(sketch, item, inclusive)");
                info.descriptions.push_back(desc);
            }

            system_catalog.CreateFunction(data, info);
        }
        {
            ScalarFunctionSet fs("datasketch_req_quantile");

            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::TINYINT], LogicalType::DOUBLE, LogicalType::BOOLEAN}, LogicalType::TINYINT, DSREQquantile<int8_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::SMALLINT], LogicalType::DOUBLE, LogicalType::BOOLEAN}, LogicalType::SMALLINT, DSREQquantile<int16_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::INTEGER], LogicalType::DOUBLE, LogicalType::BOOLEAN}, LogicalType::INTEGER, DSREQquantile<int32_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::BIGINT], LogicalType::DOUBLE, LogicalType::BOOLEAN}, LogicalType::BIGINT, DSREQquantile<int64_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::FLOAT], LogicalType::DOUBLE, LogicalType::BOOLEAN}, LogicalType::FLOAT, DSREQquantile<float>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::DOUBLE], LogicalType::DOUBLE, LogicalType::BOOLEAN}, LogicalType::DOUBLE, DSREQquantile<double>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UTINYINT], LogicalType::DOUBLE, LogicalType::BOOLEAN}, LogicalType::UTINYINT, DSREQquantile<uint8_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::USMALLINT], LogicalType::DOUBLE, LogicalType::BOOLEAN}, LogicalType::USMALLINT, DSREQquantile<uint16_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UINTEGER], LogicalType::DOUBLE, LogicalType::BOOLEAN}, LogicalType::UINTEGER, DSREQquantile<uint32_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UBIGINT], LogicalType::DOUBLE, LogicalType::BOOLEAN}, LogicalType::UBIGINT, DSREQquantile<uint64_t>));

            CreateScalarFunctionInfo info(std::move(fs));

            {
                FunctionDescription desc;
                desc.description = "Return the quantile of a rank in the sketch";
                desc.examples.push_back("datasketch_req_rank(sketch, rank, inclusive)");
                info.descriptions.push_back(desc);
            }

            system_catalog.CreateFunction(data, info);
        }
        {
            ScalarFunctionSet fs("datasketch_req_n");

            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::TINYINT]}, LogicalType::UBIGINT, DSREQn<int8_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::SMALLINT]}, LogicalType::UBIGINT, DSREQn<int16_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::INTEGER]}, LogicalType::UBIGINT, DSREQn<int32_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::BIGINT]}, LogicalType::UBIGINT, DSREQn<int64_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::FLOAT]}, LogicalType::UBIGINT, DSREQn<float>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::DOUBLE]}, LogicalType::UBIGINT, DSREQn<double>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UTINYINT]}, LogicalType::UBIGINT, DSREQn<uint8_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::USMALLINT]}, LogicalType::UBIGINT, DSREQn<uint16_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UINTEGER]}, LogicalType::UBIGINT, DSREQn<uint32_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UBIGINT]}, LogicalType::UBIGINT, DSREQn<uint64_t>));

            CreateScalarFunctionInfo info(std::move(fs));

            {
                FunctionDescription desc;
                desc.description = "Return the number of items contained in the sketch";
                desc.examples.push_back("datasketch_req_rank(sketch)");
                info.descriptions.push_back(desc);
            }

            system_catalog.CreateFunction(data, info);
        }
        {
            ScalarFunctionSet fs("datasketch_req_is_estimation_mode");

            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::TINYINT]}, LogicalType::BOOLEAN, DSREQis_estimation_mode<int8_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::SMALLINT]}, LogicalType::BOOLEAN, DSREQis_estimation_mode<int16_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::INTEGER]}, LogicalType::BOOLEAN, DSREQis_estimation_mode<int32_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::BIGINT]}, LogicalType::BOOLEAN, DSREQis_estimation_mode<int64_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::FLOAT]}, LogicalType::BOOLEAN, DSREQis_estimation_mode<float>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::DOUBLE]}, LogicalType::BOOLEAN, DSREQis_estimation_mode<double>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UTINYINT]}, LogicalType::BOOLEAN, DSREQis_estimation_mode<uint8_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::USMALLINT]}, LogicalType::BOOLEAN, DSREQis_estimation_mode<uint16_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UINTEGER]}, LogicalType::BOOLEAN, DSREQis_estimation_mode<uint32_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UBIGINT]}, LogicalType::BOOLEAN, DSREQis_estimation_mode<uint64_t>));

            CreateScalarFunctionInfo info(std::move(fs));

            {
                FunctionDescription desc;
                desc.description = "Return a boolean indicating if the sketch is in estimation mode";
                desc.examples.push_back("datasketch_req_is_estimation_mode(sketch)");
                info.descriptions.push_back(desc);
            }

            system_catalog.CreateFunction(data, info);
        }
        {
            ScalarFunctionSet fs("datasketch_req_num_retained");

            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::TINYINT]}, LogicalType::UBIGINT, DSREQnum_retained<int8_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::SMALLINT]}, LogicalType::UBIGINT, DSREQnum_retained<int16_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::INTEGER]}, LogicalType::UBIGINT, DSREQnum_retained<int32_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::BIGINT]}, LogicalType::UBIGINT, DSREQnum_retained<int64_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::FLOAT]}, LogicalType::UBIGINT, DSREQnum_retained<float>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::DOUBLE]}, LogicalType::UBIGINT, DSREQnum_retained<double>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UTINYINT]}, LogicalType::UBIGINT, DSREQnum_retained<uint8_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::USMALLINT]}, LogicalType::UBIGINT, DSREQnum_retained<uint16_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UINTEGER]}, LogicalType::UBIGINT, DSREQnum_retained<uint32_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UBIGINT]}, LogicalType::UBIGINT, DSREQnum_retained<uint64_t>));

            CreateScalarFunctionInfo info(std::move(fs));

            {
                FunctionDescription desc;
                desc.description = "Return the number of retained items in the sketch";
                desc.examples.push_back("datasketch_req_num_retained(sketch)");
                info.descriptions.push_back(desc);
            }

            system_catalog.CreateFunction(data, info);
        }
        {
            ScalarFunctionSet fs("datasketch_req_min_item");

            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::TINYINT]}, LogicalType::TINYINT, DSREQmin_item<int8_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::SMALLINT]}, LogicalType::SMALLINT, DSREQmin_item<int16_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::INTEGER]}, LogicalType::INTEGER, DSREQmin_item<int32_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::BIGINT]}, LogicalType::BIGINT, DSREQmin_item<int64_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::FLOAT]}, LogicalType::FLOAT, DSREQmin_item<float>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::DOUBLE]}, LogicalType::DOUBLE, DSREQmin_item<double>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UTINYINT]}, LogicalType::UTINYINT, DSREQmin_item<uint8_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::USMALLINT]}, LogicalType::USMALLINT, DSREQmin_item<uint16_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UINTEGER]}, LogicalType::UINTEGER, DSREQmin_item<uint32_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UBIGINT]}, LogicalType::UBIGINT, DSREQmin_item<uint64_t>));

            CreateScalarFunctionInfo info(std::move(fs));

            {
                FunctionDescription desc;
                desc.description = "Return the minimum item in the sketch";
                desc.examples.push_back("datasketch_req_min_item(sketch)");
                info.descriptions.push_back(desc);
            }

            system_catalog.CreateFunction(data, info);
        }
        {
            ScalarFunctionSet fs("datasketch_req_max_item");

            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::TINYINT]}, LogicalType::TINYINT, DSREQmax_item<int8_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::SMALLINT]}, LogicalType::SMALLINT, DSREQmax_item<int16_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::INTEGER]}, LogicalType::INTEGER, DSREQmax_item<int32_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::BIGINT]}, LogicalType::BIGINT, DSREQmax_item<int64_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::FLOAT]}, LogicalType::FLOAT, DSREQmax_item<float>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::DOUBLE]}, LogicalType::DOUBLE, DSREQmax_item<double>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UTINYINT]}, LogicalType::UTINYINT, DSREQmax_item<uint8_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::USMALLINT]}, LogicalType::USMALLINT, DSREQmax_item<uint16_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UINTEGER]}, LogicalType::UINTEGER, DSREQmax_item<uint32_t>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::UBIGINT]}, LogicalType::UBIGINT, DSREQmax_item<uint64_t>));

            CreateScalarFunctionInfo info(std::move(fs));

            {
                FunctionDescription desc;
                desc.description = "Return the maxium item in the sketch";
                desc.examples.push_back("datasketch_req_max_item(sketch)");
                info.descriptions.push_back(desc);
            }

            system_catalog.CreateFunction(data, info);
        }

        // This funciton creates the sketches.
        {
            AggregateFunctionSet sketch("datasketch_req");

            {
                auto fun = DSREQCreateAggregate<int8_t>(LogicalType::TINYINT, sketch_map_types[LogicalTypeId::TINYINT]);
                fun.bind = DSREQBind;
                fun.arguments.insert(fun.arguments.begin(), LogicalType::INTEGER);
                sketch.AddFunction(fun);
            }
            // sketch.AddFunction(DSREQCreateAggregate<int8_t>(LogicalType::TINYINT, sketch_map_types[LogicalTypeId::TINYINT]));
            {
                auto fun = DSREQMergeAggregate<int8_t>(LogicalType::TINYINT, sketch_map_types[LogicalTypeId::TINYINT]);
                fun.bind = DSREQBind;
                fun.arguments.insert(fun.arguments.begin(), LogicalType::INTEGER);
                sketch.AddFunction(fun);
            }
            // sketch.AddFunction(DSREQMergeAggregate<int8_t>(LogicalType::TINYINT, sketch_map_types[LogicalTypeId::TINYINT]));

            {
                auto fun = DSREQCreateAggregate<int16_t>(LogicalType::SMALLINT, sketch_map_types[LogicalTypeId::SMALLINT]);
                fun.bind = DSREQBind;
                fun.arguments.insert(fun.arguments.begin(), LogicalType::INTEGER);
                sketch.AddFunction(fun);
            }
            // sketch.AddFunction(DSREQCreateAggregate<int16_t>(LogicalType::SMALLINT, sketch_map_types[LogicalTypeId::SMALLINT]));
            {
                auto fun = DSREQMergeAggregate<int16_t>(LogicalType::SMALLINT, sketch_map_types[LogicalTypeId::SMALLINT]);
                fun.bind = DSREQBind;
                fun.arguments.insert(fun.arguments.begin(), LogicalType::INTEGER);
                sketch.AddFunction(fun);
            }
            // sketch.AddFunction(DSREQMergeAggregate<int16_t>(LogicalType::SMALLINT, sketch_map_types[LogicalTypeId::SMALLINT]));

            {
                auto fun = DSREQCreateAggregate<int32_t>(LogicalType::INTEGER, sketch_map_types[LogicalTypeId::INTEGER]);
                fun.bind = DSREQBind;
                fun.arguments.insert(fun.arguments.begin(), LogicalType::INTEGER);
                sketch.AddFunction(fun);
            }
            // sketch.AddFunction(DSREQCreateAggregate<int32_t>(LogicalType::INTEGER, sketch_map_types[LogicalTypeId::INTEGER]));
            {
                auto fun = DSREQMergeAggregate<int32_t>(LogicalType::INTEGER, sketch_map_types[LogicalTypeId::INTEGER]);
                fun.bind = DSREQBind;
                fun.arguments.insert(fun.arguments.begin(), LogicalType::INTEGER);
                sketch.AddFunction(fun);
            }
            // sketch.AddFunction(DSREQMergeAggregate<int32_t>(LogicalType::INTEGER, sketch_map_types[LogicalTypeId::INTEGER]));

            {
                auto fun = DSREQCreateAggregate<int64_t>(LogicalType::BIGINT, sketch_map_types[LogicalTypeId::BIGINT]);
                fun.bind = DSREQBind;
                fun.arguments.insert(fun.arguments.begin(), LogicalType::INTEGER);
                sketch.AddFunction(fun);
            }
            // sketch.AddFunction(DSREQCreateAggregate<int64_t>(LogicalType::BIGINT, sketch_map_types[LogicalTypeId::BIGINT]));
            {
                auto fun = DSREQMergeAggregate<int64_t>(LogicalType::BIGINT, sketch_map_types[LogicalTypeId::BIGINT]);
                fun.bind = DSREQBind;
                fun.arguments.insert(fun.arguments.begin(), LogicalType::INTEGER);
                sketch.AddFunction(fun);
            }
            // sketch.AddFunction(DSREQMergeAggregate<int64_t>(LogicalType::BIGINT, sketch_map_types[LogicalTypeId::BIGINT]));

            {
                auto fun = DSREQCreateAggregate<float>(LogicalType::FLOAT, sketch_map_types[LogicalTypeId::FLOAT]);
                fun.bind = DSREQBind;
                fun.arguments.insert(fun.arguments.begin(), LogicalType::INTEGER);
                sketch.AddFunction(fun);
            }
            // sketch.AddFunction(DSREQCreateAggregate<float>(LogicalType::FLOAT, sketch_map_types[LogicalTypeId::FLOAT]));
            {
                auto fun = DSREQMergeAggregate<float>(LogicalType::FLOAT, sketch_map_types[LogicalTypeId::FLOAT]);
                fun.bind = DSREQBind;
                fun.arguments.insert(fun.arguments.begin(), LogicalType::INTEGER);
                sketch.AddFunction(fun);
            }
            // sketch.AddFunction(DSREQMergeAggregate<float>(LogicalType::FLOAT, sketch_map_types[LogicalTypeId::FLOAT]));

            {
                auto fun = DSREQCreateAggregate<double>(LogicalType::DOUBLE, sketch_map_types[LogicalTypeId::DOUBLE]);
                fun.bind = DSREQBind;
                fun.arguments.insert(fun.arguments.begin(), LogicalType::INTEGER);
                sketch.AddFunction(fun);
            }
            // sketch.AddFunction(DSREQCreateAggregate<double>(LogicalType::DOUBLE, sketch_map_types[LogicalTypeId::DOUBLE]));
            {
                auto fun = DSREQMergeAggregate<double>(LogicalType::DOUBLE, sketch_map_types[LogicalTypeId::DOUBLE]);
                fun.bind = DSREQBind;
                fun.arguments.insert(fun.arguments.begin(), LogicalType::INTEGER);
                sketch.AddFunction(fun);
            }
            // sketch.AddFunction(DSREQMergeAggregate<double>(LogicalType::DOUBLE, sketch_map_types[LogicalTypeId::DOUBLE]));

            {
                auto fun = DSREQCreateAggregate<uint8_t>(LogicalType::UTINYINT, sketch_map_types[LogicalTypeId::UTINYINT]);
                fun.bind = DSREQBind;
                fun.arguments.insert(fun.arguments.begin(), LogicalType::INTEGER);
                sketch.AddFunction(fun);
            }
            // sketch.AddFunction(DSREQCreateAggregate<uint8_t>(LogicalType::UTINYINT, sketch_map_types[LogicalTypeId::UTINYINT]));
            {
                auto fun = DSREQMergeAggregate<uint8_t>(LogicalType::UTINYINT, sketch_map_types[LogicalTypeId::UTINYINT]);
                fun.bind = DSREQBind;
                fun.arguments.insert(fun.arguments.begin(), LogicalType::INTEGER);
                sketch.AddFunction(fun);
            }
            // sketch.AddFunction(DSREQMergeAggregate<uint8_t>(LogicalType::UTINYINT, sketch_map_types[LogicalTypeId::UTINYINT]));

            {
                auto fun = DSREQCreateAggregate<uint16_t>(LogicalType::USMALLINT, sketch_map_types[LogicalTypeId::USMALLINT]);
                fun.bind = DSREQBind;
                fun.arguments.insert(fun.arguments.begin(), LogicalType::INTEGER);
                sketch.AddFunction(fun);
            }
            // sketch.AddFunction(DSREQCreateAggregate<uint16_t>(LogicalType::USMALLINT, sketch_map_types[LogicalTypeId::USMALLINT]));
            {
                auto fun = DSREQMergeAggregate<uint16_t>(LogicalType::USMALLINT, sketch_map_types[LogicalTypeId::USMALLINT]);
                fun.bind = DSREQBind;
                fun.arguments.insert(fun.arguments.begin(), LogicalType::INTEGER);
                sketch.AddFunction(fun);
            }
            // sketch.AddFunction(DSREQMergeAggregate<uint16_t>(LogicalType::USMALLINT, sketch_map_types[LogicalTypeId::USMALLINT]));

            {
                auto fun = DSREQCreateAggregate<uint32_t>(LogicalType::UINTEGER, sketch_map_types[LogicalTypeId::UINTEGER]);
                fun.bind = DSREQBind;
                fun.arguments.insert(fun.arguments.begin(), LogicalType::INTEGER);
                sketch.AddFunction(fun);
            }
            // sketch.AddFunction(DSREQCreateAggregate<uint32_t>(LogicalType::UINTEGER, sketch_map_types[LogicalTypeId::UINTEGER]));
            {
                auto fun = DSREQMergeAggregate<uint32_t>(LogicalType::UINTEGER, sketch_map_types[LogicalTypeId::UINTEGER]);
                fun.bind = DSREQBind;
                fun.arguments.insert(fun.arguments.begin(), LogicalType::INTEGER);
                sketch.AddFunction(fun);
            }
            // sketch.AddFunction(DSREQMergeAggregate<uint32_t>(LogicalType::UINTEGER, sketch_map_types[LogicalTypeId::UINTEGER]));

            {
                auto fun = DSREQCreateAggregate<uint64_t>(LogicalType::UBIGINT, sketch_map_types[LogicalTypeId::UBIGINT]);
                fun.bind = DSREQBind;
                fun.arguments.insert(fun.arguments.begin(), LogicalType::INTEGER);
                sketch.AddFunction(fun);
            }
            // sketch.AddFunction(DSREQCreateAggregate<uint64_t>(LogicalType::UBIGINT, sketch_map_types[LogicalTypeId::UBIGINT]));
            {
                auto fun = DSREQMergeAggregate<uint64_t>(LogicalType::UBIGINT, sketch_map_types[LogicalTypeId::UBIGINT]);
                fun.bind = DSREQBind;
                fun.arguments.insert(fun.arguments.begin(), LogicalType::INTEGER);
                sketch.AddFunction(fun);
            }
            // sketch.AddFunction(DSREQMergeAggregate<uint64_t>(LogicalType::UBIGINT, sketch_map_types[LogicalTypeId::UBIGINT]));

            CreateAggregateFunctionInfo sketch_info(sketch);

            {
                FunctionDescription desc;
                desc.description = "Creates a sketch_req data sketch by aggregating values or by aggregating other REQ data sketches";
                desc.examples.push_back("datasketch_req(k, data)");
                sketch_info.descriptions.push_back(desc);
            }

            system_catalog.CreateFunction(data, sketch_info);
        }
    }

    template <class T>

    static inline void DSTDigestis_empty(DataChunk &args, ExpressionState &state, Vector &result)
    {
        // Get the references to the incoming vectors.
        D_ASSERT(args.ColumnCount() == 1);

        auto &sketch_vector = args.data[0];

        UnaryExecutor::Execute<string_t, bool>(
            sketch_vector, result, args.size(),
            [&](string_t sketch_data)
            {
                auto sketch = datasketches::tdigest<T>::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
                return sketch.is_empty();
            });
    }

    template <class T>

    static inline void DSTDigestk(DataChunk &args, ExpressionState &state, Vector &result)
    {
        // Get the references to the incoming vectors.
        D_ASSERT(args.ColumnCount() == 1);

        auto &sketch_vector = args.data[0];

        UnaryExecutor::Execute<string_t, uint16_t>(
            sketch_vector, result, args.size(),
            [&](string_t sketch_data)
            {
                auto sketch = datasketches::tdigest<T>::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
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

        UnifiedVectorFormat unified_split_points;
        split_points_vector.ToUnifiedFormat(args.size(), unified_split_points);

        // auto split_points_list_entries = UnifiedVectorFormat::GetData<list_entry_t>(unified_split_points);
        //         auto split_points_validitiy = FlatVector::Validity(split_points_vector);

        auto &split_points_list_children = ListVector::GetEntry(split_points_vector);

        UnifiedVectorFormat split_points_children_unified;
        split_points_list_children.ToUnifiedFormat(args.size(), split_points_children_unified);

        const T *split_points_list_children_data = UnifiedVectorFormat::GetData<T>(split_points_children_unified);

        BinaryExecutor::Execute<string_t, list_entry_t, list_entry_t>(
            sketch_vector, split_points_vector, result, args.size(),
            [&](string_t sketch_data, list_entry_t split_points_data)
            {
                auto sketch = datasketches::tdigest<T>::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());

                T *passing_points = (T *)malloc(sizeof(T) * split_points_data.length);
                for (idx_t i = 0; i < split_points_data.length; i++)
                {
                    passing_points[i] = split_points_list_children_data[i + split_points_data.offset];
                }

                auto cdf_result = sketch.get_CDF(passing_points, split_points_data.length);
                free(passing_points);
                auto current_size = ListVector::GetListSize(result);
                auto new_size = current_size + cdf_result.size();
                if (ListVector::GetListCapacity(result) < new_size)
                {
                    ListVector::Reserve(result, new_size);
                }

                auto &child_entry = ListVector::GetEntry(result);
                auto child_vals = FlatVector::GetData<T>(child_entry);
                // auto &child_validity = FlatVector::Validity(child_entry);
                for (idx_t i = 0; i < cdf_result.size(); i++)
                {
                    child_vals[current_size + i] = cdf_result[i];
                }
                ListVector::SetListSize(result, new_size);
                return list_entry_t{current_size, cdf_result.size()};
            });
    }

    template <class T>

    static inline void DSTDigestpmf(DataChunk &args, ExpressionState &state, Vector &result)
    {
        // Get the references to the incoming vectors.
        D_ASSERT(args.ColumnCount() == 2);

        auto &sketch_vector = args.data[0];
        auto &split_points_vector = args.data[1];

        UnifiedVectorFormat unified_split_points;
        split_points_vector.ToUnifiedFormat(args.size(), unified_split_points);

        // auto split_points_list_entries = UnifiedVectorFormat::GetData<list_entry_t>(unified_split_points);
        //         auto split_points_validitiy = FlatVector::Validity(split_points_vector);

        auto &split_points_list_children = ListVector::GetEntry(split_points_vector);

        UnifiedVectorFormat split_points_children_unified;
        split_points_list_children.ToUnifiedFormat(args.size(), split_points_children_unified);

        const T *split_points_list_children_data = UnifiedVectorFormat::GetData<T>(split_points_children_unified);

        BinaryExecutor::Execute<string_t, list_entry_t, list_entry_t>(
            sketch_vector, split_points_vector, result, args.size(),
            [&](string_t sketch_data, list_entry_t split_points_data)
            {
                auto sketch = datasketches::tdigest<T>::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());

                T *passing_points = (T *)malloc(sizeof(T) * split_points_data.length);
                for (idx_t i = 0; i < split_points_data.length; i++)
                {
                    passing_points[i] = split_points_list_children_data[i + split_points_data.offset];
                }

                auto pmf_result = sketch.get_PMF(passing_points, split_points_data.length);
                free(passing_points);

                auto current_size = ListVector::GetListSize(result);
                auto new_size = current_size + pmf_result.size();
                if (ListVector::GetListCapacity(result) < new_size)
                {
                    ListVector::Reserve(result, new_size);
                }

                auto &child_entry = ListVector::GetEntry(result);
                auto child_vals = FlatVector::GetData<double>(child_entry);
                // auto &child_validity = FlatVector::Validity(child_entry);
                for (idx_t i = 0; i < pmf_result.size(); i++)
                {
                    child_vals[current_size + i] = pmf_result[i];
                }
                ListVector::SetListSize(result, new_size);
                return list_entry_t{current_size, pmf_result.size()};
            });
    }

    template <class T>

    static inline void DSTDigestdescribe(DataChunk &args, ExpressionState &state, Vector &result)
    {
        // Get the references to the incoming vectors.
        D_ASSERT(args.ColumnCount() == 2);

        auto &sketch_vector = args.data[0];
        auto &include_centroids_vector = args.data[1];

        BinaryExecutor::Execute<string_t, bool, string_t>(
            sketch_vector, include_centroids_vector, result, args.size(),
            [&](string_t sketch_data, bool include_centroids_data)
            {
                auto sketch = datasketches::tdigest<T>::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
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

        BinaryExecutor::Execute<string_t, T, double>(
            sketch_vector, item_vector, result, args.size(),
            [&](string_t sketch_data, T item_data)
            {
                auto sketch = datasketches::tdigest<T>::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
                return sketch.get_rank(item_data);
            });
    }

    template <class T>

    static inline void DSTDigesttotal_weight(DataChunk &args, ExpressionState &state, Vector &result)
    {
        // Get the references to the incoming vectors.
        D_ASSERT(args.ColumnCount() == 1);

        auto &sketch_vector = args.data[0];

        UnaryExecutor::Execute<string_t, uint64_t>(
            sketch_vector, result, args.size(),
            [&](string_t sketch_data)
            {
                auto sketch = datasketches::tdigest<T>::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
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

        BinaryExecutor::Execute<string_t, double, T>(
            sketch_vector, rank_vector, result, args.size(),
            [&](string_t sketch_data, double rank_data)
            {
                auto sketch = datasketches::tdigest<T>::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
                return sketch.get_quantile(rank_data);
            });
    }

    template <typename T>
    auto static DSTDigestMergeAggregate(const LogicalType &type, const LogicalType &result_type) -> AggregateFunction

    {

        return AggregateFunction::UnaryAggregateDestructor<DSTDigestState<T>, string_t, string_t, DSQuantilesMergeOperation<DSTDigestBindData>, AggregateDestructorType::LEGACY>(
            result_type, result_type);
    }

    template <typename T>
    auto static DSTDigestCreateAggregate(const LogicalType &type, const LogicalType &result_type) -> AggregateFunction
    {

        return AggregateFunction::UnaryAggregateDestructor<DSTDigestState<T>, T, string_t, DSQuantilesCreateOperation<DSTDigestBindData>, AggregateDestructorType::LEGACY>(
            type, result_type);
    }

    void LoadTDigestSketch(DatabaseInstance &instance)
    {
        auto &system_catalog = Catalog::GetSystemCatalog(instance);
        auto data = CatalogTransaction::GetSystemTransaction(instance);

        std::unordered_map<LogicalTypeId, LogicalType> sketch_map_types;

        sketch_map_types.insert({LogicalTypeId::FLOAT, CreateTDigestSketchType(instance, LogicalType(LogicalTypeId::FLOAT))});
        sketch_map_types.insert({LogicalTypeId::DOUBLE, CreateTDigestSketchType(instance, LogicalType(LogicalTypeId::DOUBLE))});

        {
            ScalarFunctionSet fs("datasketch_tdigest_is_empty");

            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::FLOAT]}, LogicalType::BOOLEAN, DSTDigestis_empty<float>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::DOUBLE]}, LogicalType::BOOLEAN, DSTDigestis_empty<double>));

            CreateScalarFunctionInfo info(std::move(fs));

            {
                FunctionDescription desc;
                desc.description = "Return a boolean indicating if the sketch is empty";
                desc.examples.push_back("datasketch_tdigest_is_empty(sketch)");
                info.descriptions.push_back(desc);
            }

            system_catalog.CreateFunction(data, info);
        }
        {
            ScalarFunctionSet fs("datasketch_tdigest_k");

            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::FLOAT]}, LogicalType::USMALLINT, DSTDigestk<float>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::DOUBLE]}, LogicalType::USMALLINT, DSTDigestk<double>));

            CreateScalarFunctionInfo info(std::move(fs));

            {
                FunctionDescription desc;
                desc.description = "Return the value of K for this sketch";
                desc.examples.push_back("datasketch_tdigest_k(sketch)");
                info.descriptions.push_back(desc);
            }

            system_catalog.CreateFunction(data, info);
        }
        {
            ScalarFunctionSet fs("datasketch_tdigest_cdf");

            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::FLOAT], LogicalType::LIST(LogicalType::FLOAT)}, LogicalType::LIST(LogicalType::FLOAT), DSTDigestcdf<float>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::DOUBLE], LogicalType::LIST(LogicalType::DOUBLE)}, LogicalType::LIST(LogicalType::DOUBLE), DSTDigestcdf<double>));

            CreateScalarFunctionInfo info(std::move(fs));

            {
                FunctionDescription desc;
                desc.description = "Return the Cumulative Distribution Function (CDF) of the sketch for a series of points";
                desc.examples.push_back("datasketch_tdigest_cdf(sketch, points)");
                info.descriptions.push_back(desc);
            }

            system_catalog.CreateFunction(data, info);
        }
        {
            ScalarFunctionSet fs("datasketch_tdigest_pmf");

            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::FLOAT], LogicalType::LIST(LogicalType::FLOAT)}, LogicalType::LIST(LogicalType::FLOAT), DSTDigestpmf<float>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::DOUBLE], LogicalType::LIST(LogicalType::DOUBLE)}, LogicalType::LIST(LogicalType::DOUBLE), DSTDigestpmf<double>));

            CreateScalarFunctionInfo info(std::move(fs));

            {
                FunctionDescription desc;
                desc.description = "Return the Probability Mass Function (PMF) of the sketch for a series of points";
                desc.examples.push_back("datasketch_tdigest_pmf(sketch, points)");
                info.descriptions.push_back(desc);
            }

            system_catalog.CreateFunction(data, info);
        }
        {
            ScalarFunctionSet fs("datasketch_tdigest_describe");

            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::FLOAT], LogicalType::BOOLEAN}, LogicalType::VARCHAR, DSTDigestdescribe<float>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::DOUBLE], LogicalType::BOOLEAN}, LogicalType::VARCHAR, DSTDigestdescribe<double>));

            CreateScalarFunctionInfo info(std::move(fs));

            {
                FunctionDescription desc;
                desc.description = "Return a description of this sketch";
                desc.examples.push_back("datasketch_tdigest_describe(sketch, include_centroids)");
                info.descriptions.push_back(desc);
            }

            system_catalog.CreateFunction(data, info);
        }
        {
            ScalarFunctionSet fs("datasketch_tdigest_rank");

            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::FLOAT], LogicalType::FLOAT}, LogicalType::DOUBLE, DSTDigestrank<float>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::DOUBLE], LogicalType::DOUBLE}, LogicalType::DOUBLE, DSTDigestrank<double>));

            CreateScalarFunctionInfo info(std::move(fs));

            {
                FunctionDescription desc;
                desc.description = "Return the rank of an item in the sketch";
                desc.examples.push_back("datasketch_tdigest_rank(sketch, item)");
                info.descriptions.push_back(desc);
            }

            system_catalog.CreateFunction(data, info);
        }
        {
            ScalarFunctionSet fs("datasketch_tdigest_total_weight");

            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::FLOAT]}, LogicalType::UBIGINT, DSTDigesttotal_weight<float>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::DOUBLE]}, LogicalType::UBIGINT, DSTDigesttotal_weight<double>));

            CreateScalarFunctionInfo info(std::move(fs));

            {
                FunctionDescription desc;
                desc.description = "Return the total weight of this sketch";
                desc.examples.push_back("datasketch_tdigest_total_weight(sketch)");
                info.descriptions.push_back(desc);
            }

            system_catalog.CreateFunction(data, info);
        }
        {
            ScalarFunctionSet fs("datasketch_tdigest_quantile");

            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::FLOAT], LogicalType::DOUBLE}, LogicalType::FLOAT, DSTDigestquantile<float>));
            fs.AddFunction(ScalarFunction(
                {sketch_map_types[LogicalTypeId::DOUBLE], LogicalType::DOUBLE}, LogicalType::DOUBLE, DSTDigestquantile<double>));

            CreateScalarFunctionInfo info(std::move(fs));

            {
                FunctionDescription desc;
                desc.description = "Return the quantile of a rank in the sketch";
                desc.examples.push_back("datasketch_tdigest_quantile(sketch, rank)");
                info.descriptions.push_back(desc);
            }

            system_catalog.CreateFunction(data, info);
        }

        // This funciton creates the sketches.
        {
            AggregateFunctionSet sketch("datasketch_tdigest");

            {
                auto fun = DSTDigestCreateAggregate<float>(LogicalType::FLOAT, sketch_map_types[LogicalTypeId::FLOAT]);
                fun.bind = DSTDigestBind;
                fun.arguments.insert(fun.arguments.begin(), LogicalType::INTEGER);
                sketch.AddFunction(fun);
            }
            // sketch.AddFunction(DSTDigestCreateAggregate<float>(LogicalType::FLOAT, sketch_map_types[LogicalTypeId::FLOAT]));
            {
                auto fun = DSTDigestMergeAggregate<float>(LogicalType::FLOAT, sketch_map_types[LogicalTypeId::FLOAT]);
                fun.bind = DSTDigestBind;
                fun.arguments.insert(fun.arguments.begin(), LogicalType::INTEGER);
                sketch.AddFunction(fun);
            }
            // sketch.AddFunction(DSTDigestMergeAggregate<float>(LogicalType::FLOAT, sketch_map_types[LogicalTypeId::FLOAT]));

            {
                auto fun = DSTDigestCreateAggregate<double>(LogicalType::DOUBLE, sketch_map_types[LogicalTypeId::DOUBLE]);
                fun.bind = DSTDigestBind;
                fun.arguments.insert(fun.arguments.begin(), LogicalType::INTEGER);
                sketch.AddFunction(fun);
            }
            // sketch.AddFunction(DSTDigestCreateAggregate<double>(LogicalType::DOUBLE, sketch_map_types[LogicalTypeId::DOUBLE]));
            {
                auto fun = DSTDigestMergeAggregate<double>(LogicalType::DOUBLE, sketch_map_types[LogicalTypeId::DOUBLE]);
                fun.bind = DSTDigestBind;
                fun.arguments.insert(fun.arguments.begin(), LogicalType::INTEGER);
                sketch.AddFunction(fun);
            }
            // sketch.AddFunction(DSTDigestMergeAggregate<double>(LogicalType::DOUBLE, sketch_map_types[LogicalTypeId::DOUBLE]));

            CreateAggregateFunctionInfo sketch_info(sketch);

            {
                FunctionDescription desc;
                desc.description = "Creates a sketch_tdigest data sketch by aggregating values or by aggregating other TDigest data sketches";
                desc.examples.push_back("datasketch_tdigest(k, data)");
                sketch_info.descriptions.push_back(desc);
            }

            system_catalog.CreateFunction(data, sketch_info);
        }
    }

    static inline void DSHLLis_empty(DataChunk &args, ExpressionState &state, Vector &result)
    {
        // Get the references to the incoming vectors.
        D_ASSERT(args.ColumnCount() == 1);

        auto &sketch_vector = args.data[0];

        UnaryExecutor::Execute<string_t, bool>(
            sketch_vector, result, args.size(),
            [&](string_t sketch_data)
            {
                auto sketch = datasketches::hll_sketch::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
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

        TernaryExecutor::Execute<string_t, bool, bool, string_t>(
            sketch_vector, summary_vector, detail_vector, result, args.size(),
            [&](string_t sketch_data, bool summary_data, bool detail_data)
            {
                auto sketch = datasketches::hll_sketch::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
                return StringVector::AddString(result, sketch.to_string(summary_data, detail_data, false, false));
            });
    }

    static inline void DSHLLlg_config_k(DataChunk &args, ExpressionState &state, Vector &result)
    {
        // Get the references to the incoming vectors.
        D_ASSERT(args.ColumnCount() == 1);

        auto &sketch_vector = args.data[0];

        UnaryExecutor::Execute<string_t, uint8_t>(
            sketch_vector, result, args.size(),
            [&](string_t sketch_data)
            {
                auto sketch = datasketches::hll_sketch::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
                return sketch.get_lg_config_k();
            });
    }

    static inline void DSHLLis_compact(DataChunk &args, ExpressionState &state, Vector &result)
    {
        // Get the references to the incoming vectors.
        D_ASSERT(args.ColumnCount() == 1);

        auto &sketch_vector = args.data[0];

        UnaryExecutor::Execute<string_t, bool>(
            sketch_vector, result, args.size(),
            [&](string_t sketch_data)
            {
                auto sketch = datasketches::hll_sketch::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
                return sketch.is_compact();
            });
    }

    static inline void DSHLLestimate(DataChunk &args, ExpressionState &state, Vector &result)
    {
        // Get the references to the incoming vectors.
        D_ASSERT(args.ColumnCount() == 1);

        auto &sketch_vector = args.data[0];

        UnaryExecutor::Execute<string_t, double>(
            sketch_vector, result, args.size(),
            [&](string_t sketch_data)
            {
                auto sketch = datasketches::hll_sketch::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
                return sketch.get_estimate();
            });
    }

    static inline void DSHLLlower_bound(DataChunk &args, ExpressionState &state, Vector &result)
    {
        // Get the references to the incoming vectors.
        D_ASSERT(args.ColumnCount() == 2);

        auto &sketch_vector = args.data[0];
        auto &std_dev_vector = args.data[1];

        BinaryExecutor::Execute<string_t, uint8_t, double>(
            sketch_vector, std_dev_vector, result, args.size(),
            [&](string_t sketch_data, uint8_t std_dev_data)
            {
                auto sketch = datasketches::hll_sketch::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
                return sketch.get_lower_bound(std_dev_data);
            });
    }

    static inline void DSHLLupper_bound(DataChunk &args, ExpressionState &state, Vector &result)
    {
        // Get the references to the incoming vectors.
        D_ASSERT(args.ColumnCount() == 2);

        auto &sketch_vector = args.data[0];
        auto &std_dev_vector = args.data[1];

        BinaryExecutor::Execute<string_t, uint8_t, double>(
            sketch_vector, std_dev_vector, result, args.size(),
            [&](string_t sketch_data, uint8_t std_dev_data)
            {
                auto sketch = datasketches::hll_sketch::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
                return sketch.get_upper_bound(std_dev_data);
            });
    }

    auto static DSHLLMergeAggregate(const LogicalType &result_type) -> AggregateFunction

    {

        return AggregateFunction::UnaryAggregateDestructor<DSHLLState, string_t, string_t, DSHLLMergeOperation<DSHLLBindData>, AggregateDestructorType::LEGACY>(
            result_type, result_type);
    }

    template <typename T>
    auto static DSHLLCreateAggregate(const LogicalType &type, const LogicalType &result_type) -> AggregateFunction
    {

        return AggregateFunction::UnaryAggregateDestructor<DSHLLState, T, string_t, DSHLLCreateOperation<DSHLLBindData>, AggregateDestructorType::LEGACY>(
            type, result_type);
    }

    void LoadHLLSketch(DatabaseInstance &instance)
    {
        auto &system_catalog = Catalog::GetSystemCatalog(instance);
        auto data = CatalogTransaction::GetSystemTransaction(instance);

        auto sketch_type = CreateHLLCountingSketchType(instance);

        {
            ScalarFunctionSet fs("datasketch_hll_is_empty");

            fs.AddFunction(ScalarFunction(
                {sketch_type}, LogicalType::BOOLEAN, DSHLLis_empty));

            CreateScalarFunctionInfo info(std::move(fs));

            {
                FunctionDescription desc;
                desc.description = "Return a boolean indicating if the sketch is empty";
                desc.examples.push_back("datasketch_hll_is_empty(sketch)");
                info.descriptions.push_back(desc);
            }

            system_catalog.CreateFunction(data, info);
        }
        {
            ScalarFunctionSet fs("datasketch_hll_describe");

            fs.AddFunction(ScalarFunction(
                {sketch_type, LogicalType::BOOLEAN, LogicalType::BOOLEAN}, LogicalType::VARCHAR, DSHLLdescribe));

            CreateScalarFunctionInfo info(std::move(fs));

            {
                FunctionDescription desc;
                desc.description = "Return a string representation of the sketch";
                desc.examples.push_back("datasketch_hll_describe(sketch, include_summary, include_detail)");
                info.descriptions.push_back(desc);
            }

            system_catalog.CreateFunction(data, info);
        }
        {
            ScalarFunctionSet fs("datasketch_hll_lg_config_k");

            fs.AddFunction(ScalarFunction(
                {sketch_type}, LogicalType::UTINYINT, DSHLLlg_config_k));

            CreateScalarFunctionInfo info(std::move(fs));

            {
                FunctionDescription desc;
                desc.description = "Return the value of log base 2 K for this sketch";
                desc.examples.push_back("datasketch_hll_lg_config_k(sketch)");
                info.descriptions.push_back(desc);
            }

            system_catalog.CreateFunction(data, info);
        }
        {
            ScalarFunctionSet fs("datasketch_hll_is_compact");

            fs.AddFunction(ScalarFunction(
                {sketch_type}, LogicalType::BOOLEAN, DSHLLis_compact));

            CreateScalarFunctionInfo info(std::move(fs));

            {
                FunctionDescription desc;
                desc.description = "Return whether the sketch is in compact form";
                desc.examples.push_back("datasketch_hll_is_compact(sketch)");
                info.descriptions.push_back(desc);
            }

            system_catalog.CreateFunction(data, info);
        }
        {
            ScalarFunctionSet fs("datasketch_hll_estimate");

            fs.AddFunction(ScalarFunction(
                {sketch_type}, LogicalType::DOUBLE, DSHLLestimate));

            CreateScalarFunctionInfo info(std::move(fs));

            {
                FunctionDescription desc;
                desc.description = "Return the estimate of the number of distinct items seen by the sketch";
                desc.examples.push_back("datasketch_hll_estimate(sketch)");
                info.descriptions.push_back(desc);
            }

            system_catalog.CreateFunction(data, info);
        }
        {
            ScalarFunctionSet fs("datasketch_hll_lower_bound");

            fs.AddFunction(ScalarFunction(
                {sketch_type, LogicalType::UTINYINT}, LogicalType::DOUBLE, DSHLLlower_bound));

            CreateScalarFunctionInfo info(std::move(fs));

            {
                FunctionDescription desc;
                desc.description = "Return the lower bound of the number of distinct items seen by the sketch";
                desc.examples.push_back("datasketch_hll_lower_bound(sketch, std_dev)");
                info.descriptions.push_back(desc);
            }

            system_catalog.CreateFunction(data, info);
        }
        {
            ScalarFunctionSet fs("datasketch_hll_upper_bound");

            fs.AddFunction(ScalarFunction(
                {sketch_type, LogicalType::UTINYINT}, LogicalType::DOUBLE, DSHLLupper_bound));

            CreateScalarFunctionInfo info(std::move(fs));

            {
                FunctionDescription desc;
                desc.description = "Return the upper bound of the number of distinct items seen by the sketch";
                desc.examples.push_back("datasketch_hll_upper_bound(sketch, std_dev)");
                info.descriptions.push_back(desc);
            }

            system_catalog.CreateFunction(data, info);
        }

        // This funciton creates the sketches.
        {
            AggregateFunctionSet sketch("datasketch_hll");

            {
                auto fun = DSHLLCreateAggregate<int8_t>(LogicalType::TINYINT, sketch_type);
                fun.bind = DSHLLBind;
                fun.arguments.insert(fun.arguments.begin(), LogicalType::INTEGER);
                sketch.AddFunction(fun);
            }

            {
                auto fun = DSHLLCreateAggregate<int16_t>(LogicalType::SMALLINT, sketch_type);
                fun.bind = DSHLLBind;
                fun.arguments.insert(fun.arguments.begin(), LogicalType::INTEGER);
                sketch.AddFunction(fun);
            }

            {
                auto fun = DSHLLCreateAggregate<int32_t>(LogicalType::INTEGER, sketch_type);
                fun.bind = DSHLLBind;
                fun.arguments.insert(fun.arguments.begin(), LogicalType::INTEGER);
                sketch.AddFunction(fun);
            }

            {
                auto fun = DSHLLCreateAggregate<int64_t>(LogicalType::BIGINT, sketch_type);
                fun.bind = DSHLLBind;
                fun.arguments.insert(fun.arguments.begin(), LogicalType::INTEGER);
                sketch.AddFunction(fun);
            }

            {
                auto fun = DSHLLCreateAggregate<float>(LogicalType::FLOAT, sketch_type);
                fun.bind = DSHLLBind;
                fun.arguments.insert(fun.arguments.begin(), LogicalType::INTEGER);
                sketch.AddFunction(fun);
            }

            {
                auto fun = DSHLLCreateAggregate<double>(LogicalType::DOUBLE, sketch_type);
                fun.bind = DSHLLBind;
                fun.arguments.insert(fun.arguments.begin(), LogicalType::INTEGER);
                sketch.AddFunction(fun);
            }

            {
                auto fun = DSHLLCreateAggregate<uint8_t>(LogicalType::UTINYINT, sketch_type);
                fun.bind = DSHLLBind;
                fun.arguments.insert(fun.arguments.begin(), LogicalType::INTEGER);
                sketch.AddFunction(fun);
            }

            {
                auto fun = DSHLLCreateAggregate<uint16_t>(LogicalType::USMALLINT, sketch_type);
                fun.bind = DSHLLBind;
                fun.arguments.insert(fun.arguments.begin(), LogicalType::INTEGER);
                sketch.AddFunction(fun);
            }

            {
                auto fun = DSHLLCreateAggregate<uint32_t>(LogicalType::UINTEGER, sketch_type);
                fun.bind = DSHLLBind;
                fun.arguments.insert(fun.arguments.begin(), LogicalType::INTEGER);
                sketch.AddFunction(fun);
            }

            {
                auto fun = DSHLLCreateAggregate<uint64_t>(LogicalType::UBIGINT, sketch_type);
                fun.bind = DSHLLBind;
                fun.arguments.insert(fun.arguments.begin(), LogicalType::INTEGER);
                sketch.AddFunction(fun);
            }

            {
                auto fun = DSHLLCreateAggregate<string_t>(LogicalType::VARCHAR, sketch_type);
                fun.bind = DSHLLBind;
                fun.arguments.insert(fun.arguments.begin(), LogicalType::INTEGER);
                sketch.AddFunction(fun);
            }

            {
                auto fun = DSHLLCreateAggregate<string_t>(LogicalType::BLOB, sketch_type);
                fun.bind = DSHLLBind;
                fun.arguments.insert(fun.arguments.begin(), LogicalType::INTEGER);
                sketch.AddFunction(fun);
            }

            CreateAggregateFunctionInfo sketch_info(sketch);

            {
                FunctionDescription desc;
                desc.description = "Creates a sketch_hll data sketch by aggregating values or by aggregating other HLL data sketches";
                desc.examples.push_back("datasketch_hll(k, data)");
                sketch_info.descriptions.push_back(desc);
            }

            system_catalog.CreateFunction(data, sketch_info);
        }

        {
            AggregateFunctionSet sketch("datasketch_hll_union");
            auto fun = DSHLLMergeAggregate(sketch_type);
            fun.bind = DSHLLBind;
            fun.arguments.insert(fun.arguments.begin(), LogicalType::INTEGER);
            sketch.AddFunction(fun);
            CreateAggregateFunctionInfo sketch_info(sketch);

            {
                FunctionDescription desc;
                desc.description = "Creates a sketch_HLL data sketch by aggregating other HLL data sketches";
                desc.examples.push_back("datasketch_hll_union(k, data)");
                sketch_info.descriptions.push_back(desc);
            }

            system_catalog.CreateFunction(data, sketch_info);
        }
    }

    static inline void DSCPCis_empty(DataChunk &args, ExpressionState &state, Vector &result)
    {
        // Get the references to the incoming vectors.
        D_ASSERT(args.ColumnCount() == 1);

        auto &sketch_vector = args.data[0];

        UnaryExecutor::Execute<string_t, bool>(
            sketch_vector, result, args.size(),
            [&](string_t sketch_data)
            {
                auto sketch = datasketches::cpc_sketch::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
                return sketch.is_empty();
            });
    }

    static inline void DSCPCdescribe(DataChunk &args, ExpressionState &state, Vector &result)
    {
        // Get the references to the incoming vectors.
        D_ASSERT(args.ColumnCount() == 1);

        auto &sketch_vector = args.data[0];

        UnaryExecutor::Execute<string_t, string_t>(
            sketch_vector, result, args.size(),
            [&](string_t sketch_data)
            {
                auto sketch = datasketches::cpc_sketch::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
                return StringVector::AddString(result, sketch.to_string());
            });
    }

    static inline void DSCPCestimate(DataChunk &args, ExpressionState &state, Vector &result)
    {
        // Get the references to the incoming vectors.
        D_ASSERT(args.ColumnCount() == 1);

        auto &sketch_vector = args.data[0];

        UnaryExecutor::Execute<string_t, double>(
            sketch_vector, result, args.size(),
            [&](string_t sketch_data)
            {
                auto sketch = datasketches::cpc_sketch::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
                return sketch.get_estimate();
            });
    }

    static inline void DSCPClower_bound(DataChunk &args, ExpressionState &state, Vector &result)
    {
        // Get the references to the incoming vectors.
        D_ASSERT(args.ColumnCount() == 2);

        auto &sketch_vector = args.data[0];
        auto &std_dev_vector = args.data[1];

        BinaryExecutor::Execute<string_t, uint8_t, double>(
            sketch_vector, std_dev_vector, result, args.size(),
            [&](string_t sketch_data, uint8_t std_dev_data)
            {
                auto sketch = datasketches::cpc_sketch::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
                return sketch.get_lower_bound(std_dev_data);
            });
    }

    static inline void DSCPCupper_bound(DataChunk &args, ExpressionState &state, Vector &result)
    {
        // Get the references to the incoming vectors.
        D_ASSERT(args.ColumnCount() == 2);

        auto &sketch_vector = args.data[0];
        auto &std_dev_vector = args.data[1];

        BinaryExecutor::Execute<string_t, uint8_t, double>(
            sketch_vector, std_dev_vector, result, args.size(),
            [&](string_t sketch_data, uint8_t std_dev_data)
            {
                auto sketch = datasketches::cpc_sketch::deserialize(sketch_data.GetDataUnsafe(), sketch_data.GetSize());
                return sketch.get_upper_bound(std_dev_data);
            });
    }

    auto static DSCPCMergeAggregate(const LogicalType &result_type) -> AggregateFunction

    {

        return AggregateFunction::UnaryAggregateDestructor<DSCPCState, string_t, string_t, DSCPCMergeOperation<DSCPCBindData>, AggregateDestructorType::LEGACY>(
            result_type, result_type);
    }

    template <typename T>
    auto static DSCPCCreateAggregate(const LogicalType &type, const LogicalType &result_type) -> AggregateFunction
    {

        return AggregateFunction::UnaryAggregateDestructor<DSCPCState, T, string_t, DSCPCCreateOperation<DSCPCBindData>, AggregateDestructorType::LEGACY>(
            type, result_type);
    }

    void LoadCPCSketch(DatabaseInstance &instance)
    {
        auto &system_catalog = Catalog::GetSystemCatalog(instance);
        auto data = CatalogTransaction::GetSystemTransaction(instance);

        auto sketch_type = CreateCPCCountingSketchType(instance);

        {
            ScalarFunctionSet fs("datasketch_cpc_is_empty");

            fs.AddFunction(ScalarFunction(
                {sketch_type}, LogicalType::BOOLEAN, DSCPCis_empty));

            CreateScalarFunctionInfo info(std::move(fs));

            {
                FunctionDescription desc;
                desc.description = "Return a boolean indicating if the sketch is empty";
                desc.examples.push_back("datasketch_cpc_is_empty(sketch)");
                info.descriptions.push_back(desc);
            }

            system_catalog.CreateFunction(data, info);
        }
        {
            ScalarFunctionSet fs("datasketch_cpc_describe");

            fs.AddFunction(ScalarFunction(
                {sketch_type}, LogicalType::VARCHAR, DSCPCdescribe));

            CreateScalarFunctionInfo info(std::move(fs));

            {
                FunctionDescription desc;
                desc.description = "Return a string representation of the sketch";
                desc.examples.push_back("datasketch_cpc_describe(sketch)");
                info.descriptions.push_back(desc);
            }

            system_catalog.CreateFunction(data, info);
        }
        {
            ScalarFunctionSet fs("datasketch_cpc_estimate");

            fs.AddFunction(ScalarFunction(
                {sketch_type}, LogicalType::DOUBLE, DSCPCestimate));

            CreateScalarFunctionInfo info(std::move(fs));

            {
                FunctionDescription desc;
                desc.description = "Return the estimate of the number of distinct items seen by the sketch";
                desc.examples.push_back("datasketch_cpc_estimate(sketch)");
                info.descriptions.push_back(desc);
            }

            system_catalog.CreateFunction(data, info);
        }
        {
            ScalarFunctionSet fs("datasketch_cpc_lower_bound");

            fs.AddFunction(ScalarFunction(
                {sketch_type, LogicalType::UTINYINT}, LogicalType::DOUBLE, DSCPClower_bound));

            CreateScalarFunctionInfo info(std::move(fs));

            {
                FunctionDescription desc;
                desc.description = "Return the lower bound of the number of distinct items seen by the sketch";
                desc.examples.push_back("datasketch_cpc_lower_bound(sketch, std_dev)");
                info.descriptions.push_back(desc);
            }

            system_catalog.CreateFunction(data, info);
        }
        {
            ScalarFunctionSet fs("datasketch_cpc_upper_bound");

            fs.AddFunction(ScalarFunction(
                {sketch_type, LogicalType::UTINYINT}, LogicalType::DOUBLE, DSCPCupper_bound));

            CreateScalarFunctionInfo info(std::move(fs));

            {
                FunctionDescription desc;
                desc.description = "Return the upper bound of the number of distinct items seen by the sketch";
                desc.examples.push_back("datasketch_cpc_upper_bound(sketch, std_dev)");
                info.descriptions.push_back(desc);
            }

            system_catalog.CreateFunction(data, info);
        }

        // This funciton creates the sketches.
        {
            AggregateFunctionSet sketch("datasketch_cpc");

            {
                auto fun = DSCPCCreateAggregate<int8_t>(LogicalType::TINYINT, sketch_type);
                fun.bind = DSCPCBind;
                fun.arguments.insert(fun.arguments.begin(), LogicalType::INTEGER);
                sketch.AddFunction(fun);
            }

            {
                auto fun = DSCPCCreateAggregate<int16_t>(LogicalType::SMALLINT, sketch_type);
                fun.bind = DSCPCBind;
                fun.arguments.insert(fun.arguments.begin(), LogicalType::INTEGER);
                sketch.AddFunction(fun);
            }

            {
                auto fun = DSCPCCreateAggregate<int32_t>(LogicalType::INTEGER, sketch_type);
                fun.bind = DSCPCBind;
                fun.arguments.insert(fun.arguments.begin(), LogicalType::INTEGER);
                sketch.AddFunction(fun);
            }

            {
                auto fun = DSCPCCreateAggregate<int64_t>(LogicalType::BIGINT, sketch_type);
                fun.bind = DSCPCBind;
                fun.arguments.insert(fun.arguments.begin(), LogicalType::INTEGER);
                sketch.AddFunction(fun);
            }

            {
                auto fun = DSCPCCreateAggregate<float>(LogicalType::FLOAT, sketch_type);
                fun.bind = DSCPCBind;
                fun.arguments.insert(fun.arguments.begin(), LogicalType::INTEGER);
                sketch.AddFunction(fun);
            }

            {
                auto fun = DSCPCCreateAggregate<double>(LogicalType::DOUBLE, sketch_type);
                fun.bind = DSCPCBind;
                fun.arguments.insert(fun.arguments.begin(), LogicalType::INTEGER);
                sketch.AddFunction(fun);
            }

            {
                auto fun = DSCPCCreateAggregate<uint8_t>(LogicalType::UTINYINT, sketch_type);
                fun.bind = DSCPCBind;
                fun.arguments.insert(fun.arguments.begin(), LogicalType::INTEGER);
                sketch.AddFunction(fun);
            }

            {
                auto fun = DSCPCCreateAggregate<uint16_t>(LogicalType::USMALLINT, sketch_type);
                fun.bind = DSCPCBind;
                fun.arguments.insert(fun.arguments.begin(), LogicalType::INTEGER);
                sketch.AddFunction(fun);
            }

            {
                auto fun = DSCPCCreateAggregate<uint32_t>(LogicalType::UINTEGER, sketch_type);
                fun.bind = DSCPCBind;
                fun.arguments.insert(fun.arguments.begin(), LogicalType::INTEGER);
                sketch.AddFunction(fun);
            }

            {
                auto fun = DSCPCCreateAggregate<uint64_t>(LogicalType::UBIGINT, sketch_type);
                fun.bind = DSCPCBind;
                fun.arguments.insert(fun.arguments.begin(), LogicalType::INTEGER);
                sketch.AddFunction(fun);
            }

            {
                auto fun = DSCPCCreateAggregate<string_t>(LogicalType::VARCHAR, sketch_type);
                fun.bind = DSCPCBind;
                fun.arguments.insert(fun.arguments.begin(), LogicalType::INTEGER);
                sketch.AddFunction(fun);
            }

            {
                auto fun = DSCPCCreateAggregate<string_t>(LogicalType::BLOB, sketch_type);
                fun.bind = DSCPCBind;
                fun.arguments.insert(fun.arguments.begin(), LogicalType::INTEGER);
                sketch.AddFunction(fun);
            }

            CreateAggregateFunctionInfo sketch_info(sketch);

            {
                FunctionDescription desc;
                desc.description = "Creates a sketch_cpc data sketch by aggregating values or by aggregating other CPC data sketches";
                desc.examples.push_back("datasketch_cpc(k, data)");
                sketch_info.descriptions.push_back(desc);
            }

            system_catalog.CreateFunction(data, sketch_info);
        }

        {
            AggregateFunctionSet sketch("datasketch_cpc_union");
            auto fun = DSCPCMergeAggregate(sketch_type);
            fun.bind = DSCPCBind;
            fun.arguments.insert(fun.arguments.begin(), LogicalType::INTEGER);
            sketch.AddFunction(fun);
            CreateAggregateFunctionInfo sketch_info(sketch);

            {
                FunctionDescription desc;
                desc.description = "Creates a sketch_CPC data sketch by aggregating other CPC data sketches";
                desc.examples.push_back("datasketch_cpc_union(k, data)");
                sketch_info.descriptions.push_back(desc);
            }

            system_catalog.CreateFunction(data, sketch_info);
        }
    }

}