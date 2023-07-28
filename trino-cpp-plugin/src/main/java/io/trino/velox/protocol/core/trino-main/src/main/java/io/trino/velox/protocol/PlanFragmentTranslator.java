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
package io.trino.velox.protocol;

import io.trino.Session;
import io.trino.metadata.Metadata;
import io.trino.spi.block.BlockEncodingSerde;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.sql.planner.PartitioningScheme;
import io.trino.sql.planner.PlanFragment;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.plan.PlanNode;

import java.util.List;
import java.util.Map;

import static com.google.common.collect.ImmutableList.toImmutableList;

public class PlanFragmentTranslator
{
    private final Metadata metadata;
    private final TypeManager typeManager;
    private final BlockEncodingSerde blockEncodingSerde;
    private final Session session;
    private final Map<Symbol, Type> symbolTypeHashMap;

    public PlanFragmentTranslator(Metadata metadata, TypeManager typeManager, BlockEncodingSerde blockEncodingSerde, Session session, Map<Symbol, Type> symbolTypeHashMap)
    {
        this.metadata = metadata;
        this.typeManager = typeManager;
        this.blockEncodingSerde = blockEncodingSerde;
        this.session = session;
        this.symbolTypeHashMap = symbolTypeHashMap;
    }

    public MockPlanFragment translatePlanFragment(PlanFragment planFragment)
    {
        return new MockPlanFragment(
                planFragment.getId(),
                translatePlanNodeTree(planFragment.getRoot()),
                translatePartitionScheme(planFragment.getOutputPartitioningScheme()));
    }

    private MockPlanNode translatePlanNodeTree(PlanNode planNode)
    {
        PlanNodeTranslator translator = new PlanNodeTranslator(metadata, typeManager, blockEncodingSerde, session, symbolTypeHashMap);

        return translator.translatePlanNodeTree(planNode);
    }

    private MockPartitioningScheme translatePartitionScheme(PartitioningScheme partitioningScheme)
    {
        List<MockRowExpression> partitionKeys = partitioningScheme.getPartitioning().getArguments()
                .stream()
                .map(argumentBinding -> ExpressionTranslator.translateExpressionTree(
                        argumentBinding.getExpression(), metadata, typeManager, blockEncodingSerde, session, symbolTypeHashMap))
                .collect(toImmutableList());
        MockPartitioning mockPartitioning = new MockPartitioning(
                partitioningScheme.getPartitioning().getHandle().getProtocol(),
                partitionKeys);

        List<MockVariableReferenceExpression> outputLayout = partitioningScheme.getOutputLayout().stream()
                .map(symbol -> new MockVariableReferenceExpression(symbol.getName(), symbolTypeHashMap.get(symbol)))
                .collect(toImmutableList());

        return new MockPartitioningScheme(mockPartitioning, outputLayout, partitioningScheme.isReplicateNullsAndAny(), partitioningScheme.getBucketToPartition());
    }
}
