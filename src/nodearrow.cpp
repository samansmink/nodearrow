#define NODE_ADDON_API_DISABLE_DEPRECATED

#include <napi.h>
#include <iostream>
#include <arrow/c/abi.h>
#include <arrow/c/bridge.h>
#include <arrow/io/api.h>
#include <arrow/buffer.h>
#include <arrow/ipc/writer.h>
#include <arrow/api.h>

#include "include/arrow_parquet_read.hpp"

namespace nodearrow {
    static Napi::Value ArrayToIpc(const Napi::CallbackInfo &info) {

        auto env = info.Env();

        if (info.Length() < 2 || !info[0].IsNumber() || !info[1].IsNumber()) {
            Napi::TypeError::New(env, "Need carray and schema pointers").ThrowAsJavaScriptException();
            return env.Null();
        }

        auto carray = (ArrowArray *) (ptrdiff_t) info[0].As<Napi::Number>().Int64Value();
        auto cschema = (ArrowSchema *) (ptrdiff_t) info[1].As<Napi::Number>().Int64Value();
        auto schema = arrow::ImportSchema(cschema);
        if (!schema.ok()) {
            std::cout << "Schema not ok\n";
            return env.Null();
        }
        auto batch = arrow::ImportRecordBatch(carray, *schema);

        if (!batch.ok()) {
            std::cout << "Batch not ok\n";
            return env.Null();
        }

        auto output_stream = arrow::io::BufferOutputStream::Create();
        if (!output_stream.ok()) {
            std::cout << "BufferOutputStream not ok\n";
            return env.Null();
        }
        auto batch_writer = arrow::ipc::MakeStreamWriter(*output_stream, *schema);

        if (!batch_writer.ok()) {
            std::cout << "MakeStreamWriter not ok\n";
            return env.Null();
        }

        auto status = (*batch_writer)->WriteRecordBatch(**batch);

        if (!status.ok()) {
            std::cout << "WriteRecordBatch not ok\n";
            return env.Null();
        }

        auto buffer = (*output_stream)->Finish();

        if (!buffer.ok()) {
            std::cout << "Buffer not ok\n";
            return env.Null();
        }

        // TODO take over buffer ownership and dont copy:
//       auto buf = Napi::ArrayBuffer::New(env, (void *) (*buffer)->address(), (*buffer)->size());

        auto buf = Napi::ArrayBuffer::New(env, (*buffer)->size());
        memcpy(buf.Data(), (const void *) (*buffer)->address(), (*buffer)->size());
        return buf;
    }

    static Napi::Value ParquetToIpc(const Napi::CallbackInfo &info) {

        auto env = info.Env();

        if (info.Length() < 1 ) {
            Napi::TypeError::New(env, "Need argument specifying parquet file to read").ThrowAsJavaScriptException();
            return env.Null();
        }

        auto parquet_file_path = info[0].ToString();

        auto arrow_table = nodearrow::GetArrowTableFromPath(parquet_file_path);

        auto output_stream = arrow::io::BufferOutputStream::Create();
        if (!output_stream.ok()) {
            std::cout << "BufferOutputStream not ok\n";
            return env.Null();
        }
        auto batch_writer = arrow::ipc::MakeStreamWriter(*output_stream, arrow_table->schema());

        if (!batch_writer.ok()) {
            std::cout << "MakeStreamWriter not ok\n";
            return env.Null();
        }

        auto status = (*batch_writer)->WriteTable(*arrow_table);

        if (!status.ok()) {
            std::cout << "WriteTable not ok\n";
            return env.Null();
        }

        auto buffer = (*output_stream)->Finish();

        if (!buffer.ok()) {
            std::cout << "Buffer not ok\n";
            return env.Null();
        }

        // TODO take over buffer ownership and dont copy
        auto buf = Napi::ArrayBuffer::New(env, (*buffer)->size());
        memcpy(buf.Data(), (const void *) (*buffer)->address(), (*buffer)->size());
        return buf;
    }
}

namespace {

    Napi::Object RegisterModule(Napi::Env env, Napi::Object exports) {
        Napi::HandleScope scope(env);
        exports.Set("arraytoipc", Napi::Function::New(env, nodearrow::ArrayToIpc));
        exports.Set("parquettoipc", Napi::Function::New(env, nodearrow::ParquetToIpc));
        return exports;
    }

} // namespace

NODE_API_MODULE(nodearrow, RegisterModule);
