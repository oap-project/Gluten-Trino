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
#include <fmt/core.h>
#include <fmt/printf.h>
#include <gtest/gtest.h>

#include "src/protocol/external/json/json.hpp"
#include "src/types/PrestoToVeloxQueryPlan.h"
#include "velox/core/PlanNode.h"

using namespace io::trino;
using namespace facebook::velox;

class PlanNodeConverterTest : public ::testing::Test {
 public:
  void SetUp() override {
    pool_ = memory::addDefaultLeafMemoryPool();
    converter_ = std::make_unique<VeloxInteractiveQueryPlanConverter>(pool_.get());
  }

  template <typename PlanNodeT>
  void assertInvalidPlanNodeConvert(const std::string& jsonStr) {
    bool normal = false;
    try {
      assertPlanNodeConvert<PlanNodeT, core::PlanNode>(jsonStr);
      normal = true;
    } catch (std::exception&) {
    }
    EXPECT_EQ(normal, false);
  }

  template <typename PlanNodeT, typename VeloxPlanNodeT>
  void assertPlanNodeConvert(const std::string& jsonStr) {
    assertPlanNodeConvert<PlanNodeT, VeloxPlanNodeT>(jsonStr,
                                                     [](const VeloxPlanNodeT*) {});
  }

  template <typename PlanNodeT, typename VeloxPlanNodeT, typename F,
            std::enable_if_t<std::is_invocable_v<F, const VeloxPlanNodeT*>, bool> = true>
  void assertPlanNodeConvert(const std::string& jsonStr, F&& f) {
    nlohmann::json json = nlohmann::json::parse(jsonStr);
    std::shared_ptr<protocol::PlanNode> planNodeJson;
    protocol::from_json(json, planNodeJson);
    core::PlanNodePtr planNode =
        converter_->toVeloxQueryPlan(planNodeJson, nullptr, "test_plan");

    auto actual = std::dynamic_pointer_cast<const VeloxPlanNodeT>(planNode);
    EXPECT_NE(actual, nullptr);

    f(actual.get());
  }

 private:
  std::shared_ptr<memory::MemoryPool> pool_;
  std::unique_ptr<VeloxInteractiveQueryPlanConverter> converter_;
};

TEST_F(PlanNodeConverterTest, Project) {
  std::string str =
      R"##({ "@type": "project", "id": "22", "source": { "@type": "tablescan", "id": "0", "table": { "connectorId": { "catalogName": "tpch" }, "connectorHandle": { "@type": "system:io.trino.plugin.tpch.protocol.TpchTableHandle", "tableName": "lineitem", "scaleFactor": 0.01 }, "transaction": [ "system:io.trino.plugin.tpch.protocol.TpchTransactionHandle", "INSTANCE" ] }, "assignments": { "extendedprice<double>": { "@type": "system:io.trino.plugin.tpch.protocol.TpchColumnHandle", "columnName": "l_extendedprice", "type": "double" } }, "outputVariables": [ { "@type": "variable", "name": "quantity", "type": "double" } ] }, "assignments": { "assignments": { "expr<double>": { "@type": "call", "displayName": "$operator$multiply(double,double):double", "functionHandle": { "@type": "static", "signature": { "name": "presto.default.$operator$multiply", "kind": "SCALAR", "returnType": "double", "argumentTypes": [ "double", "double" ], "variableArity": false } }, "returnType": "double", "arguments": [ { "@type": "variable", "name": "extendedprice", "type": "double" }, { "@type": "call", "displayName": "$operator$subtract(double,double):double", "functionHandle": { "@type": "static", "signature": { "name": "presto.default.$operator$subtract", "kind": "SCALAR", "returnType": "double", "argumentTypes": [ "double", "double" ], "variableArity": false } }, "returnType": "double", "arguments": [ { "@type": "constant", "valueBlock": "CgAAAExPTkdfQVJSQVkBAAAAAAAAAAAAAPA/", "type": "double" }, { "@type": "variable", "name": "discount", "type": "double" } ] } ] }, "extendedprice<double>": { "@type": "variable", "name": "extendedprice", "type": "double" }, "quantity<double>": { "@type": "variable", "name": "quantity", "type": "double" }, "discount<double>": { "@type": "variable", "name": "discount", "type": "double" }, "expr_0<double>": { "@type": "call", "displayName": "$operator$multiply(double,double):double", "functionHandle": { "@type": "static", "signature": { "name": "presto.default.$operator$multiply", "kind": "SCALAR", "returnType": "double", "argumentTypes": [ "double", "double" ], "variableArity": false } }, "returnType": "double", "arguments": [ { "@type": "call", "displayName": "$operator$multiply(double,double):double", "functionHandle": { "@type": "static", "signature": { "name": "presto.default.$operator$multiply", "kind": "SCALAR", "returnType": "double", "argumentTypes": [ "double", "double" ], "variableArity": false } }, "returnType": "double", "arguments": [ { "@type": "variable", "name": "extendedprice", "type": "double" }, { "@type": "call", "displayName": "$operator$subtract(double,double):double", "functionHandle": { "@type": "static", "signature": { "name": "presto.default.$operator$subtract", "kind": "SCALAR", "returnType": "double", "argumentTypes": [ "double", "double" ], "variableArity": false } }, "returnType": "double", "arguments": [ { "@type": "constant", "valueBlock": "CgAAAExPTkdfQVJSQVkBAAAAAAAAAAAAAPA/", "type": "double" }, { "@type": "variable", "name": "discount", "type": "double" } ] } ] }, { "@type": "call", "displayName": "$operator$add(double,double):double", "functionHandle": { "@type": "static", "signature": { "name": "presto.default.$operator$add", "kind": "SCALAR", "returnType": "double", "argumentTypes": [ "double", "double" ], "variableArity": false } }, "returnType": "double", "arguments": [ { "@type": "variable", "name": "tax", "type": "double" }, { "@type": "constant", "valueBlock": "CgAAAExPTkdfQVJSQVkBAAAAAAAAAAAAAPA/", "type": "double" } ] } ] }, "returnflag<varchar(1)>": { "@type": "variable", "name": "returnflag", "type": "varchar(1)" }, "linestatus<varchar(1)>": { "@type": "variable", "name": "linestatus", "type": "varchar(1)" } } }, "locality": "LOCAL" })##";

  assertPlanNodeConvert<protocol::ProjectNode, core::ProjectNode>(str);
}

TEST_F(PlanNodeConverterTest, Filter) {
  std::string str =
      R"##({ "@type": "filter", "id": "1", "source": { "@type": "tablescan", "id": "0", "table": { "connectorId": { "catalogName": "tpch" }, "connectorHandle": { "@type": "system:io.trino.plugin.tpch.protocol.TpchTableHandle", "tableName": "lineitem", "scaleFactor": 0.01 }, "transaction": [ "system:io.trino.plugin.tpch.protocol.TpchTransactionHandle", "INSTANCE" ] }, "assignments": { "shipdate<date>": { "@type": "system:io.trino.plugin.tpch.protocol.TpchColumnHandle", "columnName": "l_shipdate", "type": "date" } }, "outputVariables": [ { "@type": "variable", "name": "quantity", "type": "double" } ] }, "predicate": { "@type": "special", "form": "AND", "returnType": "boolean", "arguments": [ { "@type": "call", "displayName": "$operator$less_than_or_equal<t:orderable>(t,t):boolean", "functionHandle": { "@type": "static", "signature": { "name": "presto.default.$operator$less_than_or_equal", "kind": "SCALAR", "returnType": "boolean", "argumentTypes": [ "date", "date" ], "variableArity": false } }, "returnType": "boolean", "arguments": [ { "@type": "constant", "valueBlock": "CQAAAElOVF9BUlJBWQEAAAAAPiIAAA==", "type": "date" }, { "@type": "variable", "name": "shipdate", "type": "date" } ] }, { "@type": "call", "displayName": "$operator$less_than<t:orderable>(t,t):boolean", "functionHandle": { "@type": "static", "signature": { "name": "presto.default.$operator$less_than", "kind": "SCALAR", "returnType": "boolean", "argumentTypes": [ "date", "date" ], "variableArity": false } }, "returnType": "boolean", "arguments": [ { "@type": "variable", "name": "shipdate", "type": "date" }, { "@type": "constant", "valueBlock": "CQAAAElOVF9BUlJBWQEAAAAAqyMAAA==", "type": "date" } ] }, { "@type": "call", "displayName": "$operator$between", "functionHandle": { "@type": "static", "signature": { "name": "presto.default.$operator$between", "kind": "SCALAR", "returnType": "boolean", "argumentTypes": [ "double", "double", "double" ], "variableArity": false } }, "returnType": "boolean", "arguments": [ { "@type": "variable", "name": "discount", "type": "double" }, { "@type": "constant", "valueBlock": "CgAAAExPTkdfQVJSQVkBAAAAAJqZmZmZmak/", "type": "double" }, { "@type": "constant", "valueBlock": "CgAAAExPTkdfQVJSQVkBAAAAAOxRuB6F67E/", "type": "double" } ] }, { "@type": "call", "displayName": "$operator$less_than<t:orderable>(t,t):boolean", "functionHandle": { "@type": "static", "signature": { "name": "presto.default.$operator$less_than", "kind": "SCALAR", "returnType": "boolean", "argumentTypes": [ "double", "double" ], "variableArity": false } }, "returnType": "boolean", "arguments": [ { "@type": "variable", "name": "quantity", "type": "double" }, { "@type": "constant", "valueBlock": "CgAAAExPTkdfQVJSQVkBAAAAAAAAAAAAADhA", "type": "double" } ] } ] } })##";

  assertPlanNodeConvert<protocol::FilterNode, core::FilterNode>(str);
}

TEST_F(PlanNodeConverterTest, Tablescan) {
  std::string str =
      R"##({ "@type": "tablescan", "id": "0", "table": { "connectorId": { "catalogName": "tpch" }, "connectorHandle": { "@type": "system:io.trino.plugin.tpch.protocol.TpchTableHandle", "tableName": "lineitem", "scaleFactor": 0.01 }, "transaction": [ "system:io.trino.plugin.tpch.protocol.TpchTransactionHandle", "INSTANCE" ] }, "assignments": { "orderkey<bigint>": { "@type": "system:io.trino.plugin.tpch.protocol.TpchColumnHandle", "columnName": "l_orderkey", "type": "bigint" }, "shipdate<date>": { "@type": "system:io.trino.plugin.tpch.protocol.TpchColumnHandle", "columnName": "l_shipdate", "type": "date" }, "quantity<double>": { "@type": "system:io.trino.plugin.tpch.protocol.TpchColumnHandle", "columnName": "l_quantity", "type": "double" } }, "outputVariables": [ { "@type": "variable", "name": "orderkey", "type": "bigint" }, { "@type": "variable", "name": "quantity", "type": "double" }, { "@type": "variable", "name": "shipdate", "type": "date" } ] })##";

  assertPlanNodeConvert<protocol::TableScanNode, core::TableScanNode>(str);
}

TEST_F(PlanNodeConverterTest, OrderBy) {
  std::string str =
      R"##({ "@type": "sort", "id": "146", "source": { "@type": "remotesource", "id": "144", "sourceFragmentIds": [ "2" ], "outputVariables": [ { "@type": "variable", "name": "orderkey", "type": "bigint" }, { "@type": "variable", "name": "quantity", "type": "double" }, { "@type": "variable", "name": "shipdate", "type": "date" } ], "ensureSourceOrdering": false, "exchangeType": "REPARTITION" }, "orderingScheme": { "orderBy": [ { "variable": { "@type": "variable", "name": "shipdate1", "type": "date" }, "sortOrder": "ASC_NULLS_FIRST" }, { "variable": { "@type": "variable", "name": "shipdate2", "type": "date" }, "sortOrder": "ASC_NULLS_LAST" }, { "variable": { "@type": "variable", "name": "shipdate3", "type": "date" }, "sortOrder": "DESC_NULLS_FIRST" }, { "variable": { "@type": "variable", "name": "shipdate4", "type": "date" }, "sortOrder": "DESC_NULLS_LAST" } ] }, "isPartial": true })##";

  assertPlanNodeConvert<protocol::SortNode, core::OrderByNode>(
      str, [](const core::OrderByNode* node) {
        auto&& keys = node->sortingKeys();
        auto&& orders = node->sortingOrders();

        EXPECT_EQ(keys.size(), 4);
        EXPECT_EQ(orders.size(), 4);

        EXPECT_EQ(keys[0]->name(), "shipdate1");
        EXPECT_EQ(keys[1]->name(), "shipdate2");
        EXPECT_EQ(keys[2]->name(), "shipdate3");
        EXPECT_EQ(keys[3]->name(), "shipdate4");

        EXPECT_EQ(orders[0].toString(), "ASC NULLS FIRST");
        EXPECT_EQ(orders[1].toString(), "ASC NULLS LAST");
        EXPECT_EQ(orders[2].toString(), "DESC NULLS FIRST");
        EXPECT_EQ(orders[3].toString(), "DESC NULLS LAST");
      });
}

TEST_F(PlanNodeConverterTest, Limit) {
  std::string str =
      R"##({ "@type": "limit", "id": "98", "source": { "@type": "remotesource", "id": "140", "sourceFragmentIds": [ "1" ], "outputVariables": [ { "@type": "variable", "name": "orderkey", "type": "bigint" }, { "@type": "variable", "name": "quantity", "type": "double" }, { "@type": "variable", "name": "shipdate", "type": "date" } ], "ensureSourceOrdering": false, "exchangeType": "GATHER" }, "count": 100, "step": "%s" })##";

  assertPlanNodeConvert<protocol::LimitNode, core::LimitNode>(
      fmt::sprintf(str, "PARTIAL"), [](const core::LimitNode* node) {
        EXPECT_EQ(node->count(), 100);
        EXPECT_EQ(node->isPartial(), true);
      });
  assertPlanNodeConvert<protocol::LimitNode, core::LimitNode>(
      fmt::sprintf(str, "FINAL"), [](const core::LimitNode* node) {
        EXPECT_EQ(node->count(), 100);
        EXPECT_EQ(node->isPartial(), false);
      });
}

TEST_F(PlanNodeConverterTest, TopN) {
  std::string str =
      R"##({ "@type": "topn", "id": "143", "source": { "@type": "remotesource", "id": "164", "sourceFragmentIds": [ "1" ], "outputVariables": [], "ensureSourceOrdering": false, "exchangeType": "GATHER" }, "count": 100, "orderingScheme": { "orderBy": [ { "variable": { "@type": "variable", "name": "shipdate1", "type": "date" }, "sortOrder": "ASC_NULLS_FIRST" }, { "variable": { "@type": "variable", "name": "shipdate2", "type": "date" }, "sortOrder": "ASC_NULLS_LAST" }, { "variable": { "@type": "variable", "name": "shipdate3", "type": "date" }, "sortOrder": "DESC_NULLS_FIRST" }, { "variable": { "@type": "variable", "name": "shipdate4", "type": "date" }, "sortOrder": "DESC_NULLS_LAST" } ] }, "step": "FINAL" })##";

  assertPlanNodeConvert<protocol::TopNNode, core::TopNNode>(
      str, [](const core::TopNNode* node) {
        auto&& keys = node->sortingKeys();
        auto&& orders = node->sortingOrders();

        EXPECT_EQ(node->count(), 100);

        EXPECT_EQ(keys.size(), 4);
        EXPECT_EQ(orders.size(), 4);

        EXPECT_EQ(keys[0]->name(), "shipdate1");
        EXPECT_EQ(keys[1]->name(), "shipdate2");
        EXPECT_EQ(keys[2]->name(), "shipdate3");
        EXPECT_EQ(keys[3]->name(), "shipdate4");

        EXPECT_EQ(orders[0].toString(), "ASC NULLS FIRST");
        EXPECT_EQ(orders[1].toString(), "ASC NULLS LAST");
        EXPECT_EQ(orders[2].toString(), "DESC NULLS FIRST");
        EXPECT_EQ(orders[3].toString(), "DESC NULLS LAST");
      });
}

TEST_F(PlanNodeConverterTest, Exchange) {
  std::string str =
      R"##({ "@type": "exchange", "id": "197", "type": "%s", "scope": "%s", "partitioningScheme": { "partitioning": { "handle": { "connectorHandle": { "@type": "system:io.trino.sql.planner.SystemPartitioningHandle", "partitioning": "FIXED", "function": "HASH" } }, "arguments": [] }, "outputLayout": [], "replicateNullsAndAny": false }, "sources": [ { "@type": "remotesource", "id": "164", "sourceFragmentIds": [ "1" ], "outputVariables": [ { "@type": "variable", "name": "orderkey", "type": "bigint" }, { "@type": "variable", "name": "quantity", "type": "double" }, { "@type": "variable", "name": "shipdate", "type": "date" } ], "ensureSourceOrdering": false, "exchangeType": "GATHER" } ], "inputs": [ [ { "@type": "variable", "name": "orderkey", "type": "bigint" }, { "@type": "variable", "name": "quantity", "type": "double" }, { "@type": "variable", "name": "shipdate", "type": "date" } ] ], "ensureSourceOrdering": false })##";

  assertInvalidPlanNodeConvert<protocol::ExchangeNode>(
      fmt::sprintf(str, "GATHER", "REMOTE"));
  assertPlanNodeConvert<protocol::ExchangeNode, core::LocalPartitionNode>(
      fmt::sprintf(str, "GATHER", "LOCAL"));
  assertPlanNodeConvert<protocol::ExchangeNode, core::LocalPartitionNode>(
      fmt::sprintf(str, "REPARTITION", "LOCAL"));
  assertInvalidPlanNodeConvert<protocol::ExchangeNode>(
      fmt::sprintf(str, "REPLICATE", "LOCAL"));

  assertPlanNodeConvert<protocol::ExchangeNode, core::LocalMergeNode>(fmt::sprintf(
      R"##({ "@type": "exchange", "id": "197", "type": "%s", "scope": "%s", "partitioningScheme": { "partitioning": { "handle": { "connectorHandle": { "@type": "system:io.trino.sql.planner.SystemPartitioningHandle", "partitioning": "FIXED", "function": "HASH" } }, "arguments": [] }, "outputLayout": [], "replicateNullsAndAny": false }, "sources": [ { "@type": "remotesource", "id": "164", "sourceFragmentIds": [ "1" ], "outputVariables": [ { "@type": "variable", "name": "orderkey", "type": "bigint" }, { "@type": "variable", "name": "quantity", "type": "double" }, { "@type": "variable", "name": "shipdate", "type": "date" } ], "ensureSourceOrdering": false, "exchangeType": "GATHER" } ], "inputs": [ [ { "@type": "variable", "name": "orderkey", "type": "bigint" }, { "@type": "variable", "name": "quantity", "type": "double" }, { "@type": "variable", "name": "shipdate", "type": "date" } ] ], "ensureSourceOrdering": false, "orderingScheme": { "orderBy": [ { "variable": { "@type": "variable", "name": "shipdate", "type": "date" }, "sortOrder": "ASC_NULLS_LAST" } ] } })##",
      "REPARTITION", "LOCAL"));
}

TEST_F(PlanNodeConverterTest, RemoteSource) {
  assertPlanNodeConvert<protocol::RemoteSourceNode, core::ExchangeNode>(
      R"##({ "@type": "remotesource", "id": "145", "sourceFragmentIds": [ "1" ], "outputVariables": [ { "@type": "variable", "name": "orderkey", "type": "bigint" }, { "@type": "variable", "name": "quantity", "type": "double" }, { "@type": "variable", "name": "shipdate", "type": "date" } ], "ensureSourceOrdering": true, "orderingScheme": { "orderBy": [ { "variable": { "@type": "variable", "name": "shipdate", "type": "date" }, "sortOrder": "ASC_NULLS_LAST" } ] }, "exchangeType": "GATHER" })##");

  assertPlanNodeConvert<protocol::RemoteSourceNode, core::MergeExchangeNode>(
      R"##({ "@type": "remotesource", "id": "145", "sourceFragmentIds": [ "1" ], "outputVariables": [ { "@type": "variable", "name": "orderkey", "type": "bigint" }, { "@type": "variable", "name": "quantity", "type": "double" }, { "@type": "variable", "name": "shipdate", "type": "date" } ], "ensureSourceOrdering": true, "orderingScheme": { "orderBy": [ { "variable": { "@type": "variable", "name": "shipdate", "type": "date" }, "sortOrder": "ASC_NULLS_LAST" } ] }, "exchangeType": "GATHER", "orderingScheme": { "orderBy": [ { "variable": { "@type": "variable", "name": "shipdate", "type": "date" }, "sortOrder": "ASC_NULLS_LAST" } ] } })##");
}

TEST_F(PlanNodeConverterTest, Aggregation) {
  assertPlanNodeConvert<
      protocol::AggregationNode, core::
                                     AggregationNode>(R"##( AggregationNode>(R"##({ "@type": "aggregation", "id": "302", "source": { "@type": "remotesource", "id": "225", "sourceFragmentIds": [ "2" ], "outputVariables": [ { "@type": "variable", "name": "returnflag", "type": "varchar(1)" }, { "@type": "variable", "name": "quantity", "type": "double" }, { "@type": "variable", "name": "expr", "type": "boolean" } ], "ensureSourceOrdering": false, "exchangeType": "REPARTITION" }, "aggregations": { "count_2<bigint>": { "call": { "@type": "call", "displayName": "count", "functionHandle": { "@type": "static", "signature": { "name": "presto.default.count", "kind": "AGGREGATE", "returnType": "bigint", "argumentTypes": [], "variableArity": false } }, "returnType": "bigint", "arguments": [] }, "isDistinct": false }, "sum_5<row(double,bigint)>": { "call": { "@type": "call", "displayName": "sum", "functionHandle": { "@type": "static", "signature": { "name": "trino.bridge.sum", "kind": "AGGREGATE", "returnType": "row(double,bigint)", "argumentTypes": [ "double" ], "variableArity": false } }, "returnType": "row(double,bigint)", "arguments": [ { "@type": "variable", "name": "quantity", "type": "double" } ] }, "isDistinct": false }, "sum_3<row(double,bigint)>": { "call": { "@type": "call", "displayName": "sum", "functionHandle": { "@type": "static", "signature": { "name": "trino.bridge.sum", "kind": "AGGREGATE", "returnType": "row(double,bigint)", "argumentTypes": [ "double" ], "variableArity": false } }, "returnType": "row(double,bigint)", "arguments": [ { "@type": "variable", "name": "quantity", "type": "double" } ] }, "isDistinct": false, "arguments":[{"@type": "variable","name": "sum_0","type": "row(double,bigint)"}], "functionHandle" : { "@type": "static" , "signature" : { "name": "trino.bridge.sum","kind": "AGGREGATE","returnType": "double" , "argumentTypes" : [ "row(double,bigint)" ] , "variableArity" : false } }, "mask": { "@type": "variable", "name": "expr", "type": "boolean" } }, "count_4<bigint>": { "call": { "@type": "call", "displayName": "count", "functionHandle": { "@type": "static", "signature": { "name": "presto.default.count", "kind": "AGGREGATE", "returnType": "bigint", "argumentTypes": [ "varchar(1)" ], "variableArity": false } }, "returnType": "bigint", "arguments": [ { "@type": "variable", "name": "returnflag", "type": "varchar(1)" } ] }, "isDistinct": true } }, "groupingSets": { "groupingKeys": [], "groupingSetCount": 1, "globalGroupingSets": [ 0 ] }, "preGroupedVariables": [], "step": "PARTIAL" })##",)##",
                                                      [](const core::AggregationNode*
                                                             node) {
                                                        EXPECT_TRUE(
                                                            node->groupingKeys().empty());
                                                        EXPECT_EQ(node->step(),
                                                                  core::AggregationNode::
                                                                      Step::kPartial);
                                                        EXPECT_EQ(
                                                            node->aggregates().size(), 4);
                                                      });
  assertPlanNodeConvert<protocol::AggregationNode, core::AggregationNode>(
      R"##({ "@type": "aggregation", "id": "42", "source": { "@type": "remotesource", "id": "304", "sourceFragmentIds": [ "1" ], "outputVariables": [ { "@type": "variable", "name": "shipdate", "type": "date" }, { "@type": "variable", "name": "linestatus", "type": "varchar(1)" }, { "@type": "variable", "name": "count_4", "type": "bigint" }, { "@type": "variable", "name": "sum_3", "type": "row(double,bigint)" }, { "@type": "variable", "name": "count_2", "type": "bigint" }, { "@type": "variable", "name": "sum_5", "type": "row(double,bigint)" } ], "ensureSourceOrdering": false, "exchangeType": "REPARTITION" }, "aggregations": { "count<bigint>": { "call": { "@type": "call", "displayName": "count", "functionHandle": { "@type": "static", "signature": { "name": "presto.default.count", "kind": "AGGREGATE", "returnType": "bigint", "argumentTypes": [ "bigint" ], "variableArity": false } }, "returnType": "bigint", "arguments": [ { "@type": "variable", "name": "count_4", "type": "bigint" } ] }, "isDistinct": false }, "count_1<bigint>": { "call": { "@type": "call", "displayName": "count", "functionHandle": { "@type": "static", "signature": { "name": "presto.default.count", "kind": "AGGREGATE", "returnType": "bigint", "argumentTypes": [ "bigint" ], "variableArity": false } }, "returnType": "bigint", "arguments": [ { "@type": "variable", "name": "count_2", "type": "bigint" } ] }, "isDistinct": false }, "sum_0<double>": { "call": { "@type": "call", "displayName": "sum", "functionHandle": { "@type": "static", "signature": { "name": "trino.bridge.sum", "kind": "AGGREGATE", "returnType": "double", "argumentTypes": [ "row(double,bigint)" ], "variableArity": false } }, "returnType": "double", "arguments": [ { "@type": "variable", "name": "sum_3", "type": "row(double,bigint)" } ] }, "isDistinct": false }, "sum<double>": { "call": { "@type": "call", "displayName": "sum", "functionHandle": { "@type": "static", "signature": { "name": "trino.bridge.sum", "kind": "AGGREGATE", "returnType": "double", "argumentTypes": [ "row(double,bigint)" ], "variableArity": false } }, "returnType": "double", "arguments": [ { "@type": "variable", "name": "sum_5", "type": "row(double,bigint)" } ] }, "isDistinct": false } }, "groupingSets": { "groupingKeys": [ { "@type": "variable", "name": "shipdate", "type": "date" }, { "@type": "variable", "name": "linestatus", "type": "varchar(1)" } ], "groupingSetCount": 1, "globalGroupingSets": [] }, "preGroupedVariables": [], "step": "FINAL" })##",
      [](const core::AggregationNode* node) {
        EXPECT_EQ(node->groupingKeys().size(), 2);
        EXPECT_EQ(node->step(), core::AggregationNode::Step::kFinal);
        EXPECT_EQ(node->aggregates().size(), 4);
      });
}

TEST_F(PlanNodeConverterTest, MarkDistinct) {
  assertPlanNodeConvert<protocol::MarkDistinctNode, core::MarkDistinctNode>(
      R"##({ "@type": "markdistinct", "id": "122", "source": { "@type": "remotesource", "id": "225", "sourceFragmentIds": [ "2" ], "outputVariables": [ { "@type": "variable", "name": "returnflag", "type": "varchar(1)" }, { "@type": "variable", "name": "quantity", "type": "double" }, { "@type": "variable", "name": "expr", "type": "boolean" } ], "ensureSourceOrdering": false, "exchangeType": "REPARTITION" }, "markerVariable": { "@type": "variable", "name": "returnflag$distinct", "type": "boolean" }, "distinctVariables": [ { "@type": "variable", "name": "returnflag", "type": "varchar(1)" } ] })##");
}

TEST_F(PlanNodeConverterTest, Join) {
  assertPlanNodeConvert<protocol::JoinNode, core::HashJoinNode>(
      R"##({ "@type": "join", "id": "231", "type": "INNER", "left": { "@type": "tablescan", "id": "1", "table": { "connectorId": { "catalogName": "tpch" }, "connectorHandle": { "@type": "system:io.trino.plugin.tpch.protocol.TpchTableHandle", "tableName": "lineitem", "scaleFactor": 0.01 }, "transaction": [ "system:io.trino.plugin.tpch.protocol.TpchTransactionHandle", "INSTANCE" ] }, "assignments": { "shipdate<date>": { "@type": "system:io.trino.plugin.tpch.protocol.TpchColumnHandle", "columnName": "l_shipdate", "type": "date" }, "orderkey_1<bigint>": { "@type": "system:io.trino.plugin.tpch.protocol.TpchColumnHandle", "columnName": "l_orderkey", "type": "bigint" } }, "outputVariables": [ { "@type": "variable", "name": "orderkey_1", "type": "bigint" }, { "@type": "variable", "name": "shipdate", "type": "date" } ] }, "right": { "@type": "exchange", "id": "279", "type": "GATHER", "scope": "LOCAL", "partitioningScheme": { "partitioning": { "handle": { "connectorHandle": { "@type": "system:io.trino.sql.planner.SystemPartitioningHandle", "partitioning": "SINGLE", "function": "SINGLE" } }, "arguments": [] }, "outputLayout": [ { "@type": "variable", "name": "orderkey", "type": "bigint" }, { "@type": "variable", "name": "orderdate", "type": "date" }, { "@type": "variable", "name": "shippriority", "type": "integer" } ], "replicateNullsAndAny": false }, "sources": [ { "@type": "tablescan", "id": "0", "table": { "connectorId": { "catalogName": "tpch" }, "connectorHandle": { "@type": "system:io.trino.plugin.tpch.protocol.TpchTableHandle", "tableName": "orders", "scaleFactor": 0.01 }, "transaction": [ "system:io.trino.plugin.tpch.protocol.TpchTransactionHandle", "INSTANCE" ] }, "assignments": { "orderkey<bigint>": { "@type": "system:io.trino.plugin.tpch.protocol.TpchColumnHandle", "columnName": "o_orderkey", "type": "bigint" }, "shippriority<integer>": { "@type": "system:io.trino.plugin.tpch.protocol.TpchColumnHandle", "columnName": "o_shippriority", "type": "integer" }, "orderdate<date>": { "@type": "system:io.trino.plugin.tpch.protocol.TpchColumnHandle", "columnName": "o_orderdate", "type": "date" } }, "outputVariables": [ { "@type": "variable", "name": "orderkey", "type": "bigint" }, { "@type": "variable", "name": "orderdate", "type": "date" }, { "@type": "variable", "name": "shippriority", "type": "integer" } ] } ], "inputs": [ [ { "@type": "variable", "name": "orderkey", "type": "bigint" }, { "@type": "variable", "name": "orderdate", "type": "date" }, { "@type": "variable", "name": "shippriority", "type": "integer" } ] ], "ensureSourceOrdering": false }, "criteria": [ { "left": { "@type": "variable", "name": "orderkey_1", "type": "bigint" }, "right": { "@type": "variable", "name": "orderkey", "type": "bigint" } } ], "outputVariables": [ { "@type": "variable", "name": "orderkey_1", "type": "bigint" }, { "@type": "variable", "name": "orderdate", "type": "date" }, { "@type": "variable", "name": "shippriority", "type": "integer" } ], "filter": { "@type": "call", "displayName": "$operator$less_than<t:orderable>(t,t):boolean", "functionHandle": { "@type": "static", "signature": { "name": "presto.default.$operator$less_than", "kind": "SCALAR", "returnType": "boolean", "argumentTypes": [ "date", "date" ], "variableArity": false } }, "returnType": "boolean", "arguments": [ { "@type": "variable", "name": "orderdate", "type": "date" }, { "@type": "variable", "name": "shipdate", "type": "date" } ] }, "distributionType": "PARTITIONED", "dynamicFilters": {} })##");
  assertPlanNodeConvert<protocol::JoinNode, core::NestedLoopJoinNode>(
      R"##({ "@type": "join", "id": "231", "type": "INNER", "left": { "@type": "tablescan", "id": "1", "table": { "connectorId": { "catalogName": "tpch" }, "connectorHandle": { "@type": "system:io.trino.plugin.tpch.protocol.TpchTableHandle", "tableName": "lineitem", "scaleFactor": 0.01 }, "transaction": [ "system:io.trino.plugin.tpch.protocol.TpchTransactionHandle", "INSTANCE" ] }, "assignments": { "shipdate<date>": { "@type": "system:io.trino.plugin.tpch.protocol.TpchColumnHandle", "columnName": "l_shipdate", "type": "date" }, "orderkey_1<bigint>": { "@type": "system:io.trino.plugin.tpch.protocol.TpchColumnHandle", "columnName": "l_orderkey", "type": "bigint" } }, "outputVariables": [ { "@type": "variable", "name": "orderkey_1", "type": "bigint" }, { "@type": "variable", "name": "shipdate", "type": "date" } ] }, "right": { "@type": "exchange", "id": "279", "type": "GATHER", "scope": "LOCAL", "partitioningScheme": { "partitioning": { "handle": { "connectorHandle": { "@type": "system:io.trino.sql.planner.SystemPartitioningHandle", "partitioning": "SINGLE", "function": "SINGLE" } }, "arguments": [] }, "outputLayout": [ { "@type": "variable", "name": "orderkey", "type": "bigint" }, { "@type": "variable", "name": "orderdate", "type": "date" }, { "@type": "variable", "name": "shippriority", "type": "integer" } ], "replicateNullsAndAny": false }, "sources": [ { "@type": "tablescan", "id": "0", "table": { "connectorId": { "catalogName": "tpch" }, "connectorHandle": { "@type": "system:io.trino.plugin.tpch.protocol.TpchTableHandle", "tableName": "orders", "scaleFactor": 0.01 }, "transaction": [ "system:io.trino.plugin.tpch.protocol.TpchTransactionHandle", "INSTANCE" ] }, "assignments": { "orderkey<bigint>": { "@type": "system:io.trino.plugin.tpch.protocol.TpchColumnHandle", "columnName": "o_orderkey", "type": "bigint" }, "shippriority<integer>": { "@type": "system:io.trino.plugin.tpch.protocol.TpchColumnHandle", "columnName": "o_shippriority", "type": "integer" }, "orderdate<date>": { "@type": "system:io.trino.plugin.tpch.protocol.TpchColumnHandle", "columnName": "o_orderdate", "type": "date" } }, "outputVariables": [ { "@type": "variable", "name": "orderkey", "type": "bigint" }, { "@type": "variable", "name": "orderdate", "type": "date" }, { "@type": "variable", "name": "shippriority", "type": "integer" } ] } ], "inputs": [ [ { "@type": "variable", "name": "orderkey", "type": "bigint" }, { "@type": "variable", "name": "orderdate", "type": "date" }, { "@type": "variable", "name": "shippriority", "type": "integer" } ] ], "ensureSourceOrdering": false }, "criteria": [], "outputVariables": [ { "@type": "variable", "name": "orderkey_1", "type": "bigint" }, { "@type": "variable", "name": "orderdate", "type": "date" }, { "@type": "variable", "name": "shippriority", "type": "integer" } ], "distributionType": "PARTITIONED", "dynamicFilters": {} })##");
}