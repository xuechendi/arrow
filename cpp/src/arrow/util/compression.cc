// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "arrow/util/compression.h"

#include <limits>
#include <memory>
#include <string>

#ifdef ARROW_WITH_BROTLI
#include "arrow/util/compression_brotli.h"
#endif

#ifdef ARROW_WITH_SNAPPY
#include "arrow/util/compression_snappy.h"
#endif

#ifdef ARROW_WITH_LZ4
#include "arrow/util/compression_lz4.h"
#endif

#ifdef ARROW_WITH_ZLIB
#include "arrow/util/compression_zlib.h"
#endif

#ifdef ARROW_WITH_ZSTD
#include "arrow/util/compression_zstd.h"
#endif

#ifdef ARROW_WITH_BZ2
#include "arrow/util/compression_bz2.h"
#endif

#include "arrow/status.h"

namespace arrow {
namespace util {

Compressor::~Compressor() {}

Decompressor::~Decompressor() {}

Codec::~Codec() {}

int Codec::UseDefaultCompressionLevel() { return kUseDefaultCompressionLevel; }

Status Codec::Init() { return Status::OK(); }

std::string Codec::GetCodecAsString(Compression::type t) {
  switch (t) {
    case Compression::UNCOMPRESSED:
      return "UNCOMPRESSED";
    case Compression::SNAPPY:
      return "SNAPPY";
    case Compression::GZIP:
      return "GZIP";
    case Compression::LZO:
      return "LZO";
    case Compression::BROTLI:
      return "BROTLI";
    case Compression::LZ4:
      return "LZ4";
    case Compression::ZSTD:
      return "ZSTD";
    case Compression::BZ2:
      return "BZ2";
    default:
      return "UNKNOWN";
  }
}

Status Codec::Create(Compression::type codec_type, std::unique_ptr<Codec>* result) {
  return Codec::Create(codec_type, Codec::UseDefaultCompressionLevel(), result);
}

Status Codec::Create(Compression::type codec_type, int compression_level,
                     std::unique_ptr<Codec>* result) {
  std::unique_ptr<Codec> codec;
  const bool compression_level_set{compression_level !=
                                   Codec::UseDefaultCompressionLevel()};
  switch (codec_type) {
    case Compression::UNCOMPRESSED:
      if (compression_level_set) {
        return Status::Invalid("Compression level cannot be specified for UNCOMPRESSED.");
      }
      break;
    case Compression::SNAPPY:
#ifdef ARROW_WITH_SNAPPY
      if (compression_level_set) {
        return Status::Invalid("Snappy doesn't support setting a compression level.");
      }
      codec.reset(new SnappyCodec());
      break;
#else
      return Status::NotImplemented("Snappy codec support not built");
#endif
    case Compression::GZIP:
#ifdef ARROW_WITH_ZLIB
      codec.reset(new GZipCodec(compression_level));
      break;
#else
      return Status::NotImplemented("Gzip codec support not built");
#endif
    case Compression::LZO:
      if (compression_level_set) {
        return Status::Invalid("LZ0 doesn't support setting a compression level.");
      }
      return Status::NotImplemented("LZO codec not implemented");
    case Compression::BROTLI:
#ifdef ARROW_WITH_BROTLI
      codec.reset(new BrotliCodec(compression_level));
      break;
#else
      return Status::NotImplemented("Brotli codec support not built");
#endif
    case Compression::LZ4:
#ifdef ARROW_WITH_LZ4
      if (compression_level_set) {
        return Status::Invalid("LZ4 doesn't support setting a compression level.");
      }
      codec.reset(new Lz4Codec());
      break;
#else
      return Status::NotImplemented("LZ4 codec support not built");
#endif
    case Compression::ZSTD:
#ifdef ARROW_WITH_ZSTD
      codec.reset(new ZSTDCodec(compression_level));
      break;
#else
      return Status::NotImplemented("ZSTD codec support not built");
#endif
    case Compression::BZ2:
#ifdef ARROW_WITH_BZ2
      codec.reset(new BZ2Codec(compression_level));
      break;
#else
      return Status::NotImplemented("BZ2 codec support not built");
#endif
    default:
      return Status::Invalid("Unrecognized codec");
  }
  if (codec) {
    const auto status = codec->Init();
    if (status.ok()) {
      result->reset(codec.release());
    }
    return status;
  }
  return Status::OK();
}

}  // namespace util
}  // namespace arrow
