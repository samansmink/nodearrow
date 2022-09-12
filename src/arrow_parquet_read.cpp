// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

#include <arrow/api.h>
#include <arrow/compute/exec/expression.h>
#include <arrow/dataset/dataset.h>
#include <arrow/dataset/discovery.h>
#include <arrow/dataset/file_base.h>
#include <arrow/dataset/file_parquet.h>
#include <arrow/dataset/scanner.h>
#include <arrow/filesystem/filesystem.h>
#include <arrow/filesystem/path_util.h>

#include "include/arrow_parquet_read.hpp"

#include <cstdlib>
#include <iostream>

using arrow::field;
using arrow::int16;
using arrow::Schema;
using arrow::Table;

namespace fs = arrow::fs;

namespace ds = arrow::dataset;

namespace cp = arrow::compute;

#define ABORT_ON_FAILURE(expr)                     \
  do {                                             \
    arrow::Status status_ = (expr);                \
    if (!status_.ok()) {                           \
      std::cerr << status_.message() << std::endl; \
      abort();                                     \
    }                                              \
  } while (0);

namespace nodearrow {

    static struct Configuration {
        // Increase the ds::DataSet by repeating `repeat` times the ds::Dataset.
        size_t repeat = 1;

        // Indicates if the Scanner::ToTable should consume in parallel.
        bool use_threads = true;

        ds::InspectOptions inspect_options{};
        ds::FinishOptions finish_options{};
    } conf;

    static std::shared_ptr<fs::FileSystem> GetFileSystemFromUri(const std::string& uri,
                                                         std::string* path) {
        return fs::FileSystemFromUri(uri, path).ValueOrDie();
    }

    static std::shared_ptr<ds::Dataset> GetDatasetFromDirectory(
            std::shared_ptr<fs::FileSystem> fs, std::shared_ptr<ds::ParquetFileFormat> format,
            std::string dir) {
        // Find all files under `path`
        fs::FileSelector s;
        s.base_dir = dir;
        s.recursive = true;

        ds::FileSystemFactoryOptions options;
        // The factory will try to build a child dataset.
        auto factory = ds::FileSystemDatasetFactory::Make(fs, s, format, options).ValueOrDie();

        // Try to infer a common schema for all files.
        auto schema = factory->Inspect(conf.inspect_options).ValueOrDie();
        // Caller can optionally decide another schema as long as it is compatible
        // with the previous one, e.g. `factory->Finish(compatible_schema)`.
        auto child = factory->Finish(conf.finish_options).ValueOrDie();

        ds::DatasetVector children{conf.repeat, child};
        auto dataset = ds::UnionDataset::Make(std::move(schema), std::move(children));

        return dataset.ValueOrDie();
    }

    static std::shared_ptr<ds::Dataset> GetParquetDatasetFromMetadata(
            std::shared_ptr<fs::FileSystem> fs, std::shared_ptr<ds::ParquetFileFormat> format,
            std::string metadata_path) {
        ds::ParquetFactoryOptions options;
        auto factory =
                ds::ParquetDatasetFactory::Make(metadata_path, fs, format, options).ValueOrDie();
        return factory->Finish().ValueOrDie();
    }

    static std::shared_ptr<ds::Dataset> GetDatasetFromFile(
            std::shared_ptr<fs::FileSystem> fs, std::shared_ptr<ds::ParquetFileFormat> format,
            std::string file) {
        ds::FileSystemFactoryOptions options;
        // The factory will try to build a child dataset.
        auto factory =
                ds::FileSystemDatasetFactory::Make(fs, {file}, format, options).ValueOrDie();

        // Try to infer a common schema for all files.
        auto schema = factory->Inspect(conf.inspect_options).ValueOrDie();
        // Caller can optionally decide another schema as long as it is compatible
        // with the previous one, e.g. `factory->Finish(compatible_schema)`.
        auto child = factory->Finish(conf.finish_options).ValueOrDie();

        ds::DatasetVector children;
        children.resize(conf.repeat, child);
        auto dataset = ds::UnionDataset::Make(std::move(schema), std::move(children));

        return dataset.ValueOrDie();
    }

    static std::shared_ptr<ds::Dataset> GetDatasetFromPath(
            std::shared_ptr<fs::FileSystem> fs, std::shared_ptr<ds::ParquetFileFormat> format,
            std::string path) {
        auto info = fs->GetFileInfo(path).ValueOrDie();
        if (info.IsDirectory()) {
            return GetDatasetFromDirectory(fs, format, path);
        }

        auto dirname_basename = arrow::fs::internal::GetAbstractPathParent(path);
        auto basename = dirname_basename.second;

        if (basename == "_metadata") {
            return GetParquetDatasetFromMetadata(fs, format, path);
        }

        return GetDatasetFromFile(fs, format, path);
    }

    static std::shared_ptr<ds::Scanner> GetScannerFromDataset(std::shared_ptr<ds::Dataset> dataset, bool use_threads) {
        auto scanner_builder = dataset->NewScan().ValueOrDie();

        ABORT_ON_FAILURE(scanner_builder->UseThreads(use_threads));

        return scanner_builder->Finish().ValueOrDie();
    }

    static std::shared_ptr<Table> GetTableFromScanner(std::shared_ptr<ds::Scanner> scanner) {
        return scanner->ToTable().ValueOrDie();
    }

    std::shared_ptr<Table> GetArrowTableFromPath(std::string path_p) {
        auto format = std::make_shared<ds::ParquetFileFormat>();

        std::string path;
        auto fs = GetFileSystemFromUri(path_p, &path);
        auto dataset = GetDatasetFromPath(fs, format, path);
        auto scanner = GetScannerFromDataset(dataset, conf.use_threads);

        return GetTableFromScanner(scanner);
    }
}