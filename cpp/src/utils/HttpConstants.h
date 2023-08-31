/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once
namespace io::trino::bridge::http {

const uint16_t kHttpOk = 200;
const uint16_t kHttpAccepted = 202;
const uint16_t kHttpNoContent = 204;
const uint16_t kHttpNotFound = 404;
const uint16_t kHttpInternalServerError = 500;

const char kMimeTypeApplicationJson[] = "application/json";
const char kMimeTypeApplicationThrift[] = "application/x-thrift+binary";

#define TRINO_RESULT_RESPONSE_HEADER_LEN 16
#define TRINO_RESULT_RESPONSE_HEADER_MAGIC 0xfea4f001
#define TRINO_SERIALIZED_PAGE_HEADER_SIZE 13
#define TRINO_SERIALIZED_PAGE_ROW_NUM_OFFSET 0
#define TRINO_SERIALIZED_PAGE_CODEC_OFFSET (TRINO_SERIALIZED_PAGE_ROW_NUM_OFFSET + 4)
#define TRINO_SERIALIZED_PAGE_UNCOMPRESSED_SIZE_OFFSET \
  (TRINO_SERIALIZED_PAGE_CODEC_OFFSET + 1)
#define TRINO_SERIALIZED_PAGE_COMPRESSED_SIZE_OFFSET \
  (TRINO_SERIALIZED_PAGE_UNCOMPRESSED_SIZE_OFFSET + 4)
#define TRINO_SERIALIZED_PAGE_COL_NUM_OFFSET \
  (TRINO_SERIALIZED_PAGE_COMPRESSED_SIZE_OFFSET + 4)
}  // namespace io::trino::bridge::http
