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

#include <memory>
#include <string>

#include "folly/executors/CPUThreadPoolExecutor.h"
#include "folly/executors/IOThreadPoolExecutor.h"

namespace io::trino::bridge {

std::shared_ptr<folly::CPUThreadPoolExecutor> getDriverCPUExecutor(
    size_t threadNum = 8, const std::string& name = "Driver");

std::shared_ptr<folly::IOThreadPoolExecutor> getExchangeIOCPUExecutor(
    size_t threadNum = 8, const std::string& name = "ExchangeIO");

std::shared_ptr<folly::IOThreadPoolExecutor> getSpillExecutor(
    size_t threadNum = 8, const std::string& name = "SpillIO");

std::shared_ptr<folly::IOThreadPoolExecutor> getConnectorIOExecutor(
    size_t threadNum = 8, const std::string& name = "ConnectorIO");
}  // namespace io::trino::bridge
