#pragma once

#include <arrow/c/abi.h>
#include <arrow/c/bridge.h>
#include <arrow/io/api.h>
#include <arrow/buffer.h>
#include <arrow/ipc/writer.h>

namespace nodearrow {
    std::shared_ptr <arrow::Table> GetArrowTableFromPath(std::string path_p);
}