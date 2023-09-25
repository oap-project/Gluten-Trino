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
#include <gtest/gtest.h>

#include "src/protocol/trino_protocol.h"
#include "src/types/PrestoToVeloxQueryPlan.h"
#include "velox/exec/Operator.h"
#include "velox/type/Type.h"
#include "velox/vector/FlatVector.h"

using namespace io::trino;
using namespace facebook::velox;

class TestValues : public ::testing::Test {};

TEST_F(TestValues, valuesRowVector) {
  std::string str =
      R"##({ "@type": "values", "id": "0", "outputVariables": [ { "@type": "variable", "name": "field", "type": "integer" }, { "@type": "variable", "name": "field_0", "type": "varchar(1)" } ], "rows": [ [ { "@type": "constant", "valueBlock": "CQAAAElOVF9BUlJBWQEAAAAAAQAAAA==", "type": "integer" }, { "@type": "constant", "valueBlock": "DgAAAFZBUklBQkxFX1dJRFRIAQAAAAEAAAABAAAAAAEAAABh", "type": "varchar(1)" } ], [ { "@type": "constant", "valueBlock": "CQAAAElOVF9BUlJBWQEAAAAAAgAAAA==", "type": "integer" }, { "@type": "constant", "valueBlock": "DgAAAFZBUklBQkxFX1dJRFRIAQAAAAEAAAABAAAAAAEAAABi", "type": "varchar(1)" } ], [ { "@type": "constant", "valueBlock": "CQAAAElOVF9BUlJBWQEAAAAAAwAAAA==", "type": "integer" }, { "@type": "constant", "valueBlock": "DgAAAFZBUklBQkxFX1dJRFRIAQAAAAEAAAABAAAAAAEAAABj", "type": "varchar(1)" } ] ] })##";

  json j = json::parse(str);
  std::shared_ptr<protocol::ValuesNode> p = j;

  auto pool = memory::addDefaultLeafMemoryPool();
  VeloxInteractiveQueryPlanConverter converter(pool.get());
  auto values = std::dynamic_pointer_cast<const core::ValuesNode>(
      converter.toVeloxQueryPlan(std::dynamic_pointer_cast<protocol::PlanNode>(p),
                                 nullptr, "20201107_130540_00011_wrpkw.1.2.3"));

  ASSERT_NE(values, nullptr);
  ASSERT_EQ(values->values().size(), 1);
  ASSERT_EQ(values->values()[0]->children().size(), 2);
  ASSERT_EQ(values->values()[0]->size(), 3);

  {
    auto v = values->values()[0]->childAt(0)->asFlatVector<int32_t>();
    ASSERT_EQ(v->valueAt(0), 1);
    ASSERT_EQ(v->valueAt(1), 2);
    ASSERT_EQ(v->valueAt(2), 3);
  }

  {
    auto v = values->values()[0]->childAt(1)->asFlatVector<StringView>();
    ASSERT_EQ(v->valueAt(0), StringView("a"));
    ASSERT_EQ(v->valueAt(1), StringView("b"));
    ASSERT_EQ(v->valueAt(2), StringView("c"));
  }
}

TEST_F(TestValues, valuesPlan) {
  // select a, b from (VALUES (1, 'a'), (2, 'b'), (3, 'c')) as t (a, b) where a
  // = 1;

  std::string str =
      R"##({ "id": "0", "root": { "@type": "output", "id": "8", "source": { "@type": "filter", "id": "3", "source": { "@type": "exchange", "id": "150", "type": "REPARTITION", "scope": "LOCAL", "partitioningScheme": { "partitioning": { "handle": { "connectorHandle": { "@type": "system:io.trino.sql.planner.SystemPartitioningHandle", "partitioning": "FIXED", "function": "ROUND_ROBIN" } }, "arguments": [] }, "outputLayout": [ { "@type": "variable", "name": "field", "type": "integer" }, { "@type": "variable", "name": "field_0", "type": "varchar(1)" } ], "replicateNullsAndAny": false }, "sources": [ { "@type": "values", "id": "0", "outputVariables": [ { "@type": "variable", "name": "field", "type": "integer" }, { "@type": "variable", "name": "field_0", "type": "varchar(1)" } ], "rows": [ [ { "@type": "constant", "valueBlock": "CQAAAElOVF9BUlJBWQEAAAAAAQAAAA==", "type": "integer" }, { "@type": "constant", "valueBlock": "DgAAAFZBUklBQkxFX1dJRFRIAQAAAAEAAAABAAAAAAEAAABh", "type": "varchar(1)" } ], [ { "@type": "constant", "valueBlock": "CQAAAElOVF9BUlJBWQEAAAAAAgAAAA==", "type": "integer" }, { "@type": "constant", "valueBlock": "DgAAAFZBUklBQkxFX1dJRFRIAQAAAAEAAAABAAAAAAEAAABi", "type": "varchar(1)" } ], [ { "@type": "constant", "valueBlock": "CQAAAElOVF9BUlJBWQEAAAAAAwAAAA==", "type": "integer" }, { "@type": "constant", "valueBlock": "DgAAAFZBUklBQkxFX1dJRFRIAQAAAAEAAAABAAAAAAEAAABj", "type": "varchar(1)" } ] ] } ], "inputs": [ [ { "@type": "variable", "name": "field", "type": "integer" }, { "@type": "variable", "name": "field_0", "type": "varchar(1)" } ] ], "ensureSourceOrdering": false }, "predicate": { "@type": "call", "displayName": "$operator$equal<t:comparable>(t,t):boolean", "functionHandle": { "@type": "static", "signature": { "name": "presto.default.$operator$equal", "kind": "SCALAR", "returnType": "boolean", "argumentTypes": [ "integer", "integer" ], "variableArity": false } }, "returnType": "boolean", "arguments": [ { "@type": "variable", "name": "field", "type": "integer" }, { "@type": "constant", "valueBlock": "CQAAAElOVF9BUlJBWQEAAAAAAQAAAA==", "type": "integer" } ] } }, "columnNames": [ "a", "b" ], "outputVariables": [ { "@type": "variable", "name": "field", "type": "integer" }, { "@type": "variable", "name": "field_0", "type": "varchar(1)" } ] }, "partitioningScheme": { "partitioning": { "handle": { "connectorHandle": { "@type": "system:io.trino.sql.planner.SystemPartitioningHandle", "partitioning": "SINGLE", "function": "SINGLE" } }, "arguments": [] }, "outputLayout": [ { "@type": "variable", "name": "field", "type": "integer" }, { "@type": "variable", "name": "field_0", "type": "varchar(1)" } ], "replicateNullsAndAny": false, "bucketToPartition": [ 0 ] } })##";

  json j = json::parse(str);
  std::shared_ptr<protocol::PlanFragment> p = j;

  auto pool = memory::addDefaultLeafMemoryPool();
  VeloxInteractiveQueryPlanConverter converter(pool.get());
  auto values = converter.toVeloxQueryPlan(
      std::dynamic_pointer_cast<protocol::OutputNode>(p->root)->source, nullptr,
      "20201107_130540_00011_wrpkw.1.2.3");

  ASSERT_EQ(values->name(), "Filter");
  ASSERT_EQ(values->sources()[0]->name(), "LocalPartition");
  ASSERT_EQ(values->sources()[0]->sources()[0]->name(), "Project");
  ASSERT_EQ(values->sources()[0]->sources()[0]->sources()[0]->name(), "Values");

  ASSERT_EQ(values->id(), "3");
  ASSERT_EQ(values->sources()[0]->sources()[0]->sources()[0]->id(), "0");
}
