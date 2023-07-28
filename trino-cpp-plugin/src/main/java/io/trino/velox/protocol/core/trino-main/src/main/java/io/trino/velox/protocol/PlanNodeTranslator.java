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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.metadata.Metadata;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TableHandle;
import io.trino.spi.block.BlockEncodingSerde;
import io.trino.spi.protocol.MockColumnHandle;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.sql.DynamicFilters;
import io.trino.sql.analyzer.TypeSignatureProvider;
import io.trino.sql.planner.OrderingScheme;
import io.trino.sql.planner.PartitioningScheme;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.AssignUniqueId;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.LimitNode;
import io.trino.sql.planner.plan.OutputNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanVisitor;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.RemoteSourceNode;
import io.trino.sql.planner.plan.SemiJoinNode;
import io.trino.sql.planner.plan.SortNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.TopNNode;
import io.trino.sql.planner.plan.ValuesNode;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.Row;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Locale.ENGLISH;

public class PlanNodeTranslator
{
    private final Metadata metadata;
    private final TypeManager typeManager;
    private final BlockEncodingSerde blockEncodingSerde;
    private final Session session;
    private final Map<Symbol, Type> symbolTypeMap;

    public PlanNodeTranslator(Metadata metadata, TypeManager typeManager, BlockEncodingSerde blockEncodingSerde, Session session, Map<Symbol, Type> symbolTypeMap)
    {
        this.metadata = metadata;
        this.typeManager = typeManager;
        this.blockEncodingSerde = blockEncodingSerde;
        this.session = session;
        this.symbolTypeMap = symbolTypeMap;
    }

    public MockPlanNode translatePlanNodeTree(PlanNode root)
    {
        Visitor visitor = new Visitor(metadata, typeManager, blockEncodingSerde, session, symbolTypeMap);
        return root.accept(visitor, null);
    }

    protected static class Visitor
            extends PlanVisitor<MockPlanNode, Void>
    {
        private final Metadata metadata;
        private final TypeManager typeManager;
        private final BlockEncodingSerde blockEncodingSerde;
        private final Session session;
        private final Map<Symbol, Type> symbolType;

        protected Visitor(Metadata metadata, TypeManager typeManager, BlockEncodingSerde blockEncodingSerde, Session session, Map<Symbol, Type> symbolType)
        {
            this.metadata = metadata;
            this.typeManager = typeManager;
            this.blockEncodingSerde = blockEncodingSerde;
            this.session = session;
            this.symbolType = symbolType;
        }

        private MockVariableReferenceExpression buildVariableRefExpression(Symbol symbol)
        {
            Type type = symbolType.get(symbol);
            if (type == null) {
                throw new IllegalStateException("Unknown symbol reference: " + symbol.getName());
            }
            return new MockVariableReferenceExpression(symbol.getName(), type);
        }

        @Override
        protected MockPlanNode visitPlan(PlanNode node, Void context)
        {
            throw new UnsupportedOperationException("Unsupported PlanNode type to translate: " + node.getClass().getName());
        }

        @Override
        public MockPlanNode visitTableScan(TableScanNode node, Void context)
        {
            TableHandle tableHandle = node.getTable();
            String catalogName = tableHandle.getCatalogHandle().getCatalogName();

            MockConnectorId mockConnectorId = new MockConnectorId(catalogName);

            switch (catalogName) {
                case "tpch":
                case "hive":
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported catalog type to translate: " + catalogName);
            }
            MockTableHandle mockTableHandle = new MockTableHandle(
                    mockConnectorId,
                    tableHandle.getConnectorHandle().getProtocol(),
                    tableHandle.getTransaction().getProtocol());

            ImmutableList<MockVariableReferenceExpression> outputSymbols = node.getOutputSymbols().stream()
                    .map(this::buildVariableRefExpression)
                    .collect(toImmutableList());

            Map<MockVariableReferenceExpression, MockColumnHandle> assignments = node.getAssignments().entrySet().stream()
                    .collect(Collectors.toMap(
                            entry -> buildVariableRefExpression(entry.getKey()),
                            entry -> entry.getValue().getProtocol().refine(mockTableHandle.getConnectorHandle())));

            return new MockTableScanNode(node.getId(), mockTableHandle, assignments, outputSymbols);
        }

        private ResolvedFunction mapAggregateFunction(ResolvedFunction origin, AggregationNode.Step step)
        {
            if (step.equals(AggregationNode.Step.PARTIAL) && origin.getFunctionId().toString().startsWith("sum")) {
                return metadata.resolveFunction(session, QualifiedName.of("avg"), TypeSignatureProvider.fromTypes(origin.getSignature().getArgumentTypes()));
            }
            return origin;
        }

        private Type convertAggregateResultType(Type origin, ResolvedFunction function, AggregationNode.Step step)
        {
            if (step.equals(AggregationNode.Step.PARTIAL)) {
                if (function.getFunctionId().toString().startsWith("avg")) {
                    return RowType.anonymousRow(origin, BigintType.BIGINT);
                }
                if (function.getFunctionId().toString().toLowerCase(ENGLISH).startsWith("sum")) {
                    return RowType.anonymousRow(origin, BigintType.BIGINT);
                }
            }
            return origin;
        }

        @Override
        public MockPlanNode visitAggregation(AggregationNode node, Void context)
        {
            MockPlanNode source = node.getSource().accept(this, context);

            // Translate Aggregations
            HashMap<MockVariableReferenceExpression, MockAggregationNode.Aggregation> aggregations = new HashMap<>();
            node.getAggregations().forEach(((symbol, aggregation) -> {
//                ResolvedFunction function = mapAggregateFunction(aggregation.getResolvedFunction(), node.getStep());
                ResolvedFunction function = aggregation.getResolvedFunction();
                Type aggValueType = convertAggregateResultType(function.getSignature().getReturnType(), function, node.getStep());
                MockVariableReferenceExpression aggValue = new MockVariableReferenceExpression(symbol.getName(), aggValueType);

                //Translate arguments
                List<MockRowExpression> arguments = aggregation.getArguments().stream()
                        .map(expression -> ExpressionTranslator.translateExpressionTree(expression, metadata, typeManager, blockEncodingSerde, session, symbolType))
                        .collect(toImmutableList());

                MockCallExpression mockFunction = ExpressionTranslator.buildCallExpression(function.getSignature().getName(), function.getFunctionKind(), aggValueType, arguments);

                Optional<MockVariableReferenceExpression> mask = Optional.empty();
                if (aggregation.getMask().isPresent()) {
                    mask = Optional.of(buildVariableRefExpression(aggregation.getMask().get()));
                }

                Optional<MockOrderingScheme> ordering = Optional.empty();
                if (aggregation.getOrderingScheme().isPresent()) {
                    Optional<OrderingScheme> trinoOrderingScheme = aggregation.getOrderingScheme();
                    Optional<MockOrderingScheme> orderingScheme = Optional.empty();
                    if (trinoOrderingScheme.isPresent()) {
                        OrderingScheme os = trinoOrderingScheme.get();
                        ordering = Optional.of(
                                // Symbol -> MockVariableReferenceExpression + SortOrder
                                new MockOrderingScheme(os.getOrderBy().stream().map(_symbol ->
                                        new MockOrderingScheme.Ordering(buildVariableRefExpression(_symbol), os.getOrdering(_symbol))
                                ).collect(toImmutableList())));
                    }
                }

                Optional<MockRowExpression> filter = Optional.empty();
                if (aggregation.getFilter().isPresent()) {
                    filter = Optional.of(aggregation.getFilter().get().toSymbolReference())
                            .map(filter_ -> ExpressionTranslator.translateExpressionTree(filter_, metadata, typeManager, blockEncodingSerde, session, symbolType));
                }
                MockAggregationNode.Aggregation mockAggregation = new MockAggregationNode.Aggregation(mockFunction,
                        filter, ordering, aggregation.isDistinct(), mask);

                aggregations.put(aggValue, mockAggregation);
            }));

            // Translate GroupingSetDescriptor
            List<MockVariableReferenceExpression> keys = node.getGroupingKeys().stream()
                    .map(this::buildVariableRefExpression)
                    .collect(toImmutableList());
            MockAggregationNode.GroupingSetDescriptor groupingSet = new MockAggregationNode.GroupingSetDescriptor(
                    keys, node.getGroupingSetCount(), node.getGlobalGroupingSets());

            List<MockVariableReferenceExpression> preGroupedSymbols = ImmutableList.of();
            if (!node.getPreGroupedSymbols().isEmpty()) {
                preGroupedSymbols = node.getPreGroupedSymbols().stream()
                        .map(this::buildVariableRefExpression)
                        .collect(toImmutableList());
            }

            if (node.getHashSymbol().isPresent() || node.getGroupIdSymbol().isPresent()) {
                throw new UnsupportedOperationException("Unsupported AggregationNode to translate: contains hashSymbol or groupIdVariable.");
            }

            return new MockAggregationNode(node.getId(), source, aggregations, groupingSet, preGroupedSymbols, node.getStep(), Optional.empty(), Optional.empty());
        }

        @Override
        public MockPlanNode visitFilter(FilterNode node, Void context)
        {
            MockPlanNode source = node.getSource().accept(this, context);

            Expression predicate = node.getPredicate();
            if (DynamicFilters.isDynamicFilter(predicate)) {
                // skip dynamic filter because we can't support yet.
                return source;
            }
            else {
                return new MockFilterNode(node.getId(), source,
                        ExpressionTranslator.translateExpressionTree(predicate, metadata, typeManager, blockEncodingSerde, session, symbolType));
            }
        }

        @Override
        public MockPlanNode visitProject(ProjectNode node, Void context)
        {
            MockPlanNode source = node.getSource().accept(this, context);

            HashMap<MockVariableReferenceExpression, MockRowExpression> assignments = new HashMap<>();
            node.getAssignments().forEach((symbol, expression) -> {
                MockRowExpression rowExpression = ExpressionTranslator.translateExpressionTree(expression, metadata, typeManager, blockEncodingSerde, session, symbolType);
                MockVariableReferenceExpression key = new MockVariableReferenceExpression(symbol.getName(), rowExpression.getType());
                assignments.put(key, rowExpression);
            });

            return new MockProjectNode(node.getId(), source, new MockAssignments(assignments));
        }

        @Override
        public MockPlanNode visitOutput(OutputNode node, Void context)
        {
            MockPlanNode source = node.getSource().accept(this, context);
            List<MockVariableReferenceExpression> outputVariables = node.getOutputSymbols().stream()
                    .map(this::buildVariableRefExpression)
                    .collect(toImmutableList());
            return new MockOutputNode(node.getId(), source, node.getColumnNames(), outputVariables);
        }

        @Override
        public MockPlanNode visitExchange(ExchangeNode node, Void context)
        {
            List<MockPlanNode> sources = node.getSources().stream().map(source -> source.accept(this, context)).collect(Collectors.toList());

            MockExchangeNode.Type type = switch (node.getType()) {
                case GATHER -> MockExchangeNode.Type.GATHER;
                case REPARTITION -> MockExchangeNode.Type.REPARTITION;
                case REPLICATE -> MockExchangeNode.Type.REPLICATE;
            };
            MockExchangeNode.Scope scope = switch (node.getScope()) {
                case LOCAL -> MockExchangeNode.Scope.LOCAL;
                case REMOTE -> MockExchangeNode.Scope.REMOTE;
            };

            PartitioningScheme trinoPartitioningScheme = node.getPartitioningScheme();
            List<MockRowExpression> arguments = trinoPartitioningScheme.getPartitioning().getArguments()
                    .stream()
                    .map(argumentBinding -> ExpressionTranslator.translateExpressionTree(
                            argumentBinding.getExpression(), metadata, typeManager, blockEncodingSerde, session, symbolType))
                    .collect(toImmutableList());
            MockPartitioning partitioning = new MockPartitioning(trinoPartitioningScheme.getPartitioning().getHandle().getProtocol(), arguments);
            List<MockVariableReferenceExpression> outputLayout = node.getOutputSymbols().stream()
                    .map(this::buildVariableRefExpression)
                    .collect(toImmutableList());
            MockPartitioningScheme partitioningScheme = new MockPartitioningScheme(partitioning, outputLayout, trinoPartitioningScheme.isReplicateNullsAndAny(), trinoPartitioningScheme.getBucketToPartition());

            List<List<MockVariableReferenceExpression>> inputs = node.getInputs().stream()
                    .map(listV -> listV.stream().map(this::buildVariableRefExpression).collect(toImmutableList()))
                    .collect(toImmutableList());

            boolean ensureSourceOrdering = false;
            Optional<OrderingScheme> trinoOrderingScheme = node.getOrderingScheme();
            Optional<MockOrderingScheme> orderingScheme = Optional.empty();
            if (trinoOrderingScheme.isPresent()) {
                ensureSourceOrdering = true;
                OrderingScheme os = trinoOrderingScheme.get();
                orderingScheme = Optional.of(
                        // Symbol -> MockVariableReferenceExpression + SortOrder
                        new MockOrderingScheme(os.getOrderBy().stream().map(symbol ->
                                new MockOrderingScheme.Ordering(buildVariableRefExpression(symbol), os.getOrdering(symbol))
                        ).collect(toImmutableList())));
            }

            return new MockExchangeNode(node.getId(), type, scope, partitioningScheme, sources, inputs, ensureSourceOrdering, orderingScheme);
        }

        @Override
        public MockPlanNode visitRemoteSource(RemoteSourceNode node, Void context)
        {
            List<MockVariableReferenceExpression> outputVariables = node.getOutputSymbols().stream()
                    .map(this::buildVariableRefExpression)
                    .collect(toImmutableList());

            boolean ensureSourceOrdering = false;
            Optional<OrderingScheme> trinoOrderingScheme = node.getOrderingScheme();
            Optional<MockOrderingScheme> orderingScheme = Optional.empty();
            if (trinoOrderingScheme.isPresent()) {
                ensureSourceOrdering = true;
                OrderingScheme os = trinoOrderingScheme.get();
                orderingScheme = Optional.of(
                        // Symbol -> MockVariableReferenceExpression + SortOrder
                        new MockOrderingScheme(os.getOrderBy().stream().map(symbol ->
                                new MockOrderingScheme.Ordering(buildVariableRefExpression(symbol), os.getOrdering(symbol))
                        ).collect(toImmutableList())));
            }

            MockExchangeNode.Type exchangeType = switch (node.getExchangeType()) {
                case GATHER -> MockExchangeNode.Type.GATHER;
                case REPARTITION -> MockExchangeNode.Type.REPARTITION;
                case REPLICATE -> MockExchangeNode.Type.REPLICATE;
            };

            return new MockRemoteSourceNode(node.getId(), node.getSourceFragmentIds(), outputVariables, ensureSourceOrdering, orderingScheme, exchangeType);
        }

        @Override
        public MockTopNNode visitTopN(TopNNode node, Void context)
        {
            MockPlanNode source = node.getSource().accept(this, context);

            OrderingScheme trinoOrderingScheme = node.getOrderingScheme();
            MockOrderingScheme orderingScheme = new MockOrderingScheme(trinoOrderingScheme.getOrderBy().stream().map(symbol ->
                    new MockOrderingScheme.Ordering(buildVariableRefExpression(symbol), trinoOrderingScheme.getOrdering(symbol))
            ).collect(toImmutableList()));

            MockTopNNode.Step step = switch (node.getStep()) {
                case PARTIAL -> MockTopNNode.Step.PARTIAL;
                case FINAL -> MockTopNNode.Step.FINAL;
                case SINGLE -> MockTopNNode.Step.SINGLE;
            };

            return new MockTopNNode(node.getId(), source, node.getCount(), orderingScheme, step);
        }

        @Override
        public MockLimitNode visitLimit(LimitNode node, Void context)
        {
            MockPlanNode source = node.getSource().accept(this, context);
            MockLimitNode.Step step = node.isPartial() ? MockLimitNode.Step.PARTIAL : MockLimitNode.Step.FINAL;
            return new MockLimitNode(node.getId(), source, node.getCount(), step);
        }

        @Override
        public MockSortNode visitSort(SortNode node, Void context)
        {
            MockPlanNode source = node.getSource().accept(this, context);

            OrderingScheme trinoOrderingScheme = node.getOrderingScheme();
            MockOrderingScheme orderingScheme = new MockOrderingScheme(trinoOrderingScheme.getOrderBy().stream().map(symbol ->
                    new MockOrderingScheme.Ordering(buildVariableRefExpression(symbol), trinoOrderingScheme.getOrdering(symbol))
            ).collect(toImmutableList()));
            return new MockSortNode(node.getId(), source, orderingScheme, node.isPartial());
        }

        @Override
        public MockValuesNode visitValues(ValuesNode node, Void context)
        {
            List<MockVariableReferenceExpression> outputVariables = node.getOutputSymbols().stream()
                    .map(this::buildVariableRefExpression)
                    .collect(toImmutableList());

            List<List<MockRowExpression>> rows = new ArrayList<>();
            if (outputVariables.size() > 0 && node.getRows().isPresent()) {
                node.getRows().get().stream()
                        .filter(expression -> expression instanceof Row)
                        .forEach(expression -> {
                            List<MockRowExpression> rowExpressions = ((Row) expression).getItems().stream()
                                    .map(rowExpression -> ExpressionTranslator.translateExpressionTree(rowExpression, metadata, typeManager, blockEncodingSerde, session, symbolType))
                                    .collect(toImmutableList());
                            checkArgument(outputVariables.size() == rowExpressions.size(),
                                    "declared and actual row counts don't match: %s vs %s", outputVariables.size(), rowExpressions.size());
                            rows.add(rowExpressions);
                        });
            }

            return new MockValuesNode(node.getId(), outputVariables, rows, Optional.empty());
        }

        @Override
        public MockJoinNode visitJoin(JoinNode node, Void context)
        {
            MockPlanNode leftNode = node.getLeft().accept(this, context);
            MockPlanNode rightNode = node.getRight().accept(this, context);

            MockJoinNode.Type type = switch (node.getType()) {
                case FULL -> MockJoinNode.Type.FULL;
                case INNER -> MockJoinNode.Type.INNER;
                case LEFT -> MockJoinNode.Type.LEFT;
                case RIGHT -> MockJoinNode.Type.RIGHT;
            };

            List<MockJoinNode.EquiJoinClause> criteria = node.getCriteria().stream().map(equiJoinClause -> {
                MockVariableReferenceExpression left = buildVariableRefExpression(equiJoinClause.getLeft());
                MockVariableReferenceExpression right = buildVariableRefExpression(equiJoinClause.getRight());
                return new MockJoinNode.EquiJoinClause(left, right);
            }).toList();

            Optional<MockRowExpression> filter = node.getFilter()
                    .map(filter_ -> ExpressionTranslator.translateExpressionTree(filter_, metadata, typeManager, blockEncodingSerde, session, symbolType));

            Optional<MockJoinNode.DistributionType> distributionType = node.getDistributionType().map(
                    distributionType_ -> switch (distributionType_) {
                        case PARTITIONED -> MockJoinNode.DistributionType.PARTITIONED;
                        case REPLICATED -> MockJoinNode.DistributionType.REPLICATED;
                    });

            Map<String, MockVariableReferenceExpression> dynamicFilters = node.getDynamicFilters().entrySet().stream()
                    .collect(Collectors.toMap(
                            entry -> entry.getKey().toString(),
                            entry -> buildVariableRefExpression(entry.getValue())));

            return new MockJoinNode(node.getId(),
                    type,
                    leftNode,
                    rightNode,
                    criteria,
                    node.getOutputSymbols().stream().map(this::buildVariableRefExpression).toList(),
                    filter,
                    node.getLeftHashSymbol().map(this::buildVariableRefExpression),
                    node.getRightHashSymbol().map(this::buildVariableRefExpression),
                    distributionType,
                    dynamicFilters);
        }

        @Override
        public MockPlanNode visitSemiJoin(SemiJoinNode node, Void context)
        {
            return new MockSemiJoinNode(node.getId(),
                    node.getSource().accept(this, context),
                    node.getFilteringSource().accept(this, context),
                    buildVariableRefExpression(node.getSourceJoinSymbol()),
                    buildVariableRefExpression(node.getFilteringSourceJoinSymbol()),
                    buildVariableRefExpression(node.getSemiJoinOutput()),
                    node.getSourceHashSymbol().map(this::buildVariableRefExpression),
                    node.getFilteringSourceHashSymbol().map(this::buildVariableRefExpression),
                    node.getDistributionType().map(type -> switch (type) {
                        case PARTITIONED -> MockSemiJoinNode.DistributionType.PARTITIONED;
                        case REPLICATED -> MockSemiJoinNode.DistributionType.REPLICATED;
                    }),
                    ImmutableMap.of());
        }

        @Override
        public MockAssignUniqueId visitAssignUniqueId(AssignUniqueId node, Void context)
        {
            MockPlanNode source = node.getSource().accept(this, context);
            return new MockAssignUniqueId(node.getId(), source, buildVariableRefExpression(node.getIdColumn()));
        }
    }
}
