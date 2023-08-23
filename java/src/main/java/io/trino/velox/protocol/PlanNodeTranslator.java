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
import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.metadata.Metadata;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TableHandle;
import io.trino.spi.block.BlockEncodingSerde;
import io.trino.spi.protocol.GlutenColumnHandle;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.sql.DynamicFilters;
import io.trino.sql.planner.OrderingScheme;
import io.trino.sql.planner.PartitioningScheme;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.AssignUniqueId;
import io.trino.sql.planner.plan.EnforceSingleRowNode;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.GroupIdNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.LimitNode;
import io.trino.sql.planner.plan.MarkDistinctNode;
import io.trino.sql.planner.plan.OutputNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanVisitor;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.RemoteSourceNode;
import io.trino.sql.planner.plan.SemiJoinNode;
import io.trino.sql.planner.plan.SortNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.TopNNode;
import io.trino.sql.planner.plan.TopNRankingNode;
import io.trino.sql.planner.plan.ValuesNode;
import io.trino.sql.planner.plan.WindowNode;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.Row;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Locale.ENGLISH;

public class PlanNodeTranslator
{
    private static final Logger log = Logger.get(PlanNodeTranslator.class);
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

    public GlutenPlanNode translatePlanNodeTree(PlanNode root)
    {
        Visitor visitor = new Visitor(metadata, typeManager, blockEncodingSerde, session, symbolTypeMap);
        return root.accept(visitor, null);
    }

    protected static class Visitor
            extends PlanVisitor<GlutenPlanNode, Void>
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

        private GlutenVariableReferenceExpression buildVariableRefExpression(Symbol symbol)
        {
            Type type = symbolType.get(symbol);
            if (type == null) {
                throw new IllegalStateException("Unknown variable reference: " + symbol.getName());
            }
            return new GlutenVariableReferenceExpression(symbol.getName(), type);
        }

        private GlutenWindowNode.Function buildWindowFunction(WindowNode.Function function)
        {
            ImmutableList<GlutenRowExpression> arguments = function.getArguments().stream()
                    .map(expression -> ExpressionTranslator.translateExpressionTree(expression, metadata, typeManager, blockEncodingSerde, session, symbolType))
                    .collect(toImmutableList());
            GlutenCallExpression functionCall = ExpressionTranslator.buildCallExpression(function.getResolvedFunction(), arguments, false);

            GlutenWindowNode.Frame.WindowType type = switch (function.getFrame().getType()) {
                case ROWS -> GlutenWindowNode.Frame.WindowType.ROWS;
                case RANGE -> GlutenWindowNode.Frame.WindowType.RANGE;
                case GROUPS -> GlutenWindowNode.Frame.WindowType.GROUPS;
            };
            GlutenWindowNode.Frame.BoundType startType = switch (function.getFrame().getStartType()) {
                case FOLLOWING -> GlutenWindowNode.Frame.BoundType.FOLLOWING;
                case PRECEDING -> GlutenWindowNode.Frame.BoundType.PRECEDING;
                case CURRENT_ROW -> GlutenWindowNode.Frame.BoundType.CURRENT_ROW;
                case UNBOUNDED_FOLLOWING -> GlutenWindowNode.Frame.BoundType.UNBOUNDED_FOLLOWING;
                case UNBOUNDED_PRECEDING -> GlutenWindowNode.Frame.BoundType.UNBOUNDED_PRECEDING;
            };
            Optional<GlutenVariableReferenceExpression> startValue = Optional.empty();
            Optional<String> originalStartValue = Optional.empty();
            if (function.getFrame().getStartValue().isPresent()) {
                startValue = Optional.of(buildVariableRefExpression(function.getFrame().getStartValue().get()));
                originalStartValue = startValue.map(GlutenVariableReferenceExpression::toString);
            }
            Optional<GlutenVariableReferenceExpression> sortKeyCoercedForFrameStartComparison = Optional.empty();
            if (function.getFrame().getSortKeyCoercedForFrameStartComparison().isPresent()) {
                sortKeyCoercedForFrameStartComparison = Optional.of(buildVariableRefExpression(function.getFrame().getSortKeyCoercedForFrameStartComparison().get()));
            }
            GlutenWindowNode.Frame.BoundType endType = switch (function.getFrame().getEndType()) {
                case FOLLOWING -> GlutenWindowNode.Frame.BoundType.FOLLOWING;
                case PRECEDING -> GlutenWindowNode.Frame.BoundType.PRECEDING;
                case CURRENT_ROW -> GlutenWindowNode.Frame.BoundType.CURRENT_ROW;
                case UNBOUNDED_FOLLOWING -> GlutenWindowNode.Frame.BoundType.UNBOUNDED_FOLLOWING;
                case UNBOUNDED_PRECEDING -> GlutenWindowNode.Frame.BoundType.UNBOUNDED_PRECEDING;
            };
            Optional<GlutenVariableReferenceExpression> endValue = Optional.empty();
            Optional<String> originalEndValue = Optional.empty();
            if (function.getFrame().getEndValue().isPresent()) {
                endValue = Optional.of(buildVariableRefExpression(function.getFrame().getEndValue().get()));
                originalEndValue = endValue.map(GlutenVariableReferenceExpression::toString);
            }
            Optional<GlutenVariableReferenceExpression> sortKeyCoercedForFrameEndComparison = Optional.empty();
            if (function.getFrame().getSortKeyCoercedForFrameEndComparison().isPresent()) {
                sortKeyCoercedForFrameEndComparison = Optional.of(buildVariableRefExpression(function.getFrame().getSortKeyCoercedForFrameEndComparison().get()));
            }

            GlutenWindowNode.Frame frame = new GlutenWindowNode.Frame(type, startType, startValue, sortKeyCoercedForFrameStartComparison, endType, endValue,
                    sortKeyCoercedForFrameEndComparison, originalStartValue, originalEndValue);
            boolean ignoreNulls = function.isIgnoreNulls();
            return new GlutenWindowNode.Function(functionCall, frame, ignoreNulls);
        }

        @Override
        protected GlutenPlanNode visitPlan(PlanNode node, Void context)
        {
            throw new UnsupportedOperationException("Unsupported PlanNode type to translate: " + node.getClass().getName());
        }

        @Override
        public GlutenPlanNode visitTableScan(TableScanNode node, Void context)
        {
            TableHandle tableHandle = node.getTable();
            String catalogName = tableHandle.getCatalogHandle().getCatalogName();

            GlutenConnectorId glutenConnectorId = new GlutenConnectorId(catalogName);

            switch (catalogName) {
                case "tpch":
                case "hive":
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported catalog type to translate: " + catalogName);
            }
            GlutenTableHandle glutenTableHandle = new GlutenTableHandle(
                    glutenConnectorId,
                    tableHandle.getConnectorHandle().getProtocol(),
                    tableHandle.getTransaction().getProtocol());

            ImmutableList<GlutenVariableReferenceExpression> outputSymbols = node.getOutputSymbols().stream()
                    .map(this::buildVariableRefExpression)
                    .collect(toImmutableList());

            Map<GlutenVariableReferenceExpression, GlutenColumnHandle> assignments = node.getAssignments().entrySet().stream()
                    .collect(Collectors.toMap(
                            entry -> buildVariableRefExpression(entry.getKey()),
                            entry -> entry.getValue().getProtocol().refine(glutenTableHandle.getConnectorHandle())));

            return new GlutenTableScanNode(node.getId(), glutenTableHandle, assignments, outputSymbols);
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
                if (function.getFunctionId().toString().toLowerCase(ENGLISH).startsWith("stddev")) {
                    return RowType.anonymousRow(BigintType.BIGINT, DoubleType.DOUBLE, DoubleType.DOUBLE);
                }
            }
            return origin;
        }

        @Override
        public GlutenPlanNode visitAggregation(AggregationNode node, Void context)
        {
            GlutenPlanNode source = node.getSource().accept(this, context);

            // Translate Aggregations
            HashMap<GlutenVariableReferenceExpression, GlutenAggregationNode.Aggregation> aggregations = new HashMap<>();
            node.getAggregations().forEach(((symbol, aggregation) -> {
                ResolvedFunction function = aggregation.getResolvedFunction();
                Type aggValueType = convertAggregateResultType(function.getSignature().getReturnType(), function, node.getStep());
                GlutenVariableReferenceExpression aggValue = new GlutenVariableReferenceExpression(symbol.getName(), aggValueType);

                //Translate arguments
                List<GlutenRowExpression> arguments = aggregation.getArguments().stream()
                        .map(expression -> ExpressionTranslator.translateExpressionTree(expression, metadata, typeManager, blockEncodingSerde, session, symbolType))
                        .collect(toImmutableList());

                GlutenCallExpression glutenFunction = ExpressionTranslator.buildCallExpression(function.getSignature().getName(), function.getFunctionKind(), aggValueType, arguments);

                Optional<GlutenVariableReferenceExpression> mask = Optional.empty();
                if (aggregation.getMask().isPresent()) {
                    mask = Optional.of(buildVariableRefExpression(aggregation.getMask().get()));
                }

                Optional<GlutenOrderingScheme> ordering = Optional.empty();
                if (aggregation.getOrderingScheme().isPresent()) {
                    Optional<OrderingScheme> trinoOrderingScheme = aggregation.getOrderingScheme();
                    if (trinoOrderingScheme.isPresent()) {
                        OrderingScheme os = trinoOrderingScheme.get();
                        ordering = Optional.of(
                                // Symbol -> GlutenVariableReferenceExpression + SortOrder
                                new GlutenOrderingScheme(os.getOrderBy().stream().map(_symbol ->
                                        new GlutenOrderingScheme.Ordering(buildVariableRefExpression(_symbol), os.getOrdering(_symbol))
                                ).collect(toImmutableList())));
                    }
                }

                Optional<GlutenRowExpression> filter = Optional.empty();
                if (aggregation.getFilter().isPresent()) {
                    filter = Optional.of(aggregation.getFilter().get().toSymbolReference())
                            .map(filter_ -> ExpressionTranslator.translateExpressionTree(filter_, metadata, typeManager, blockEncodingSerde, session, symbolType));
                }
                GlutenAggregationNode.Aggregation glutenAggregation = new GlutenAggregationNode.Aggregation(glutenFunction,
                        filter, ordering, aggregation.isDistinct(), mask);

                aggregations.put(aggValue, glutenAggregation);
            }));

            // Translate GroupingSetDescriptor
            List<GlutenVariableReferenceExpression> keys = node.getGroupingKeys().stream()
                    .map(this::buildVariableRefExpression)
                    .collect(toImmutableList());
            GlutenAggregationNode.GroupingSetDescriptor groupingSet = new GlutenAggregationNode.GroupingSetDescriptor(
                    keys, node.getGroupingSetCount(), node.getGlobalGroupingSets());

            List<GlutenVariableReferenceExpression> preGroupedSymbols = ImmutableList.of();
            if (!node.getPreGroupedSymbols().isEmpty()) {
                preGroupedSymbols = node.getPreGroupedSymbols().stream()
                        .map(this::buildVariableRefExpression)
                        .collect(toImmutableList());
            }

            Optional<GlutenVariableReferenceExpression> hashVariable = Optional.empty();
            if (node.getHashSymbol().isPresent()) {
                hashVariable = Optional.of(buildVariableRefExpression(node.getHashSymbol().get()));
            }
            Optional<GlutenVariableReferenceExpression> groupIdVariable = Optional.empty();
            if (node.getGroupIdSymbol().isPresent()) {
                groupIdVariable = Optional.of(buildVariableRefExpression(node.getGroupIdSymbol().get()));
            }

            return new GlutenAggregationNode(node.getId(), source, aggregations, groupingSet, preGroupedSymbols, node.getStep(), hashVariable, groupIdVariable);
        }

        @Override
        public GlutenPlanNode visitFilter(FilterNode node, Void context)
        {
            GlutenPlanNode source = node.getSource().accept(this, context);

            Expression predicate = node.getPredicate();
            if (DynamicFilters.isDynamicFilter(predicate)) {
                // skip dynamic filter because we can't support yet.
                return source;
            }
            else {
                return new GlutenFilterNode(node.getId(), source,
                        ExpressionTranslator.translateExpressionTree(predicate, metadata, typeManager, blockEncodingSerde, session, symbolType));
            }
        }

        @Override
        public GlutenPlanNode visitProject(ProjectNode node, Void context)
        {
            GlutenPlanNode source = node.getSource().accept(this, context);

            HashMap<GlutenVariableReferenceExpression, GlutenRowExpression> assignments = new HashMap<>();
            node.getAssignments().forEach((symbol, expression) -> {
                GlutenRowExpression rowExpression = ExpressionTranslator.translateExpressionTree(expression, metadata, typeManager, blockEncodingSerde, session, symbolType);
                GlutenVariableReferenceExpression key = new GlutenVariableReferenceExpression(symbol.getName(), rowExpression.getType());
                assignments.put(key, rowExpression);
            });

            return new GlutenProjectNode(node.getId(), source, new GlutenAssignments(assignments));
        }

        @Override
        public GlutenPlanNode visitOutput(OutputNode node, Void context)
        {
            GlutenPlanNode source = node.getSource().accept(this, context);
            List<GlutenVariableReferenceExpression> outputVariables = node.getOutputSymbols().stream()
                    .map(this::buildVariableRefExpression)
                    .collect(toImmutableList());
            return new GlutenOutputNode(node.getId(), source, node.getColumnNames(), outputVariables);
        }

        @Override
        public GlutenPlanNode visitExchange(ExchangeNode node, Void context)
        {
            List<GlutenPlanNode> sources = node.getSources().stream().map(source -> source.accept(this, context)).collect(Collectors.toList());

            GlutenExchangeNode.Type type = switch (node.getType()) {
                case GATHER -> GlutenExchangeNode.Type.GATHER;
                case REPARTITION -> GlutenExchangeNode.Type.REPARTITION;
                case REPLICATE -> GlutenExchangeNode.Type.REPLICATE;
            };
            GlutenExchangeNode.Scope scope = switch (node.getScope()) {
                case LOCAL -> GlutenExchangeNode.Scope.LOCAL;
                case REMOTE -> GlutenExchangeNode.Scope.REMOTE;
            };

            PartitioningScheme trinoPartitioningScheme = node.getPartitioningScheme();
            List<GlutenRowExpression> arguments = trinoPartitioningScheme.getPartitioning().getArguments()
                    .stream()
                    .map(argumentBinding -> ExpressionTranslator.translateExpressionTree(
                            argumentBinding.getExpression(), metadata, typeManager, blockEncodingSerde, session, symbolType))
                    .collect(toImmutableList());
            GlutenPartitioning partitioning = new GlutenPartitioning(trinoPartitioningScheme.getPartitioning().getHandle().getProtocol(), arguments);
            List<GlutenVariableReferenceExpression> outputLayout = node.getOutputSymbols().stream()
                    .map(this::buildVariableRefExpression)
                    .collect(toImmutableList());
            GlutenPartitioningScheme partitioningScheme = new GlutenPartitioningScheme(partitioning, outputLayout, trinoPartitioningScheme.isReplicateNullsAndAny(), trinoPartitioningScheme.getBucketToPartition());

            List<List<GlutenVariableReferenceExpression>> inputs = node.getInputs().stream()
                    .map(listV -> listV.stream().map(this::buildVariableRefExpression).collect(toImmutableList()))
                    .collect(toImmutableList());

            boolean ensureSourceOrdering = false;
            Optional<OrderingScheme> trinoOrderingScheme = node.getOrderingScheme();
            Optional<GlutenOrderingScheme> orderingScheme = Optional.empty();
            if (trinoOrderingScheme.isPresent()) {
                ensureSourceOrdering = true;
                OrderingScheme os = trinoOrderingScheme.get();
                orderingScheme = Optional.of(
                        // Symbol -> GlutenVariableReferenceExpression + SortOrder
                        new GlutenOrderingScheme(os.getOrderBy().stream().map(symbol ->
                                new GlutenOrderingScheme.Ordering(buildVariableRefExpression(symbol), os.getOrdering(symbol))
                        ).collect(toImmutableList())));
            }

            return new GlutenExchangeNode(node.getId(), type, scope, partitioningScheme, sources, inputs, ensureSourceOrdering, orderingScheme);
        }

        @Override
        public GlutenPlanNode visitRemoteSource(RemoteSourceNode node, Void context)
        {
            List<GlutenVariableReferenceExpression> outputVariables = node.getOutputSymbols().stream()
                    .map(this::buildVariableRefExpression)
                    .collect(toImmutableList());

            boolean ensureSourceOrdering = false;
            Optional<OrderingScheme> trinoOrderingScheme = node.getOrderingScheme();
            Optional<GlutenOrderingScheme> orderingScheme = Optional.empty();
            if (trinoOrderingScheme.isPresent()) {
                ensureSourceOrdering = true;
                OrderingScheme os = trinoOrderingScheme.get();
                orderingScheme = Optional.of(
                        // Symbol -> GlutenVariableReferenceExpression + SortOrder
                        new GlutenOrderingScheme(os.getOrderBy().stream().map(symbol ->
                                new GlutenOrderingScheme.Ordering(buildVariableRefExpression(symbol), os.getOrdering(symbol))
                        ).collect(toImmutableList())));
            }

            GlutenExchangeNode.Type exchangeType = switch (node.getExchangeType()) {
                case GATHER -> GlutenExchangeNode.Type.GATHER;
                case REPARTITION -> GlutenExchangeNode.Type.REPARTITION;
                case REPLICATE -> GlutenExchangeNode.Type.REPLICATE;
            };

            return new GlutenRemoteSourceNode(node.getId(), node.getSourceFragmentIds(), outputVariables, ensureSourceOrdering, orderingScheme, exchangeType);
        }

        @Override
        public GlutenTopNNode visitTopN(TopNNode node, Void context)
        {
            GlutenPlanNode source = node.getSource().accept(this, context);

            OrderingScheme trinoOrderingScheme = node.getOrderingScheme();
            GlutenOrderingScheme orderingScheme = new GlutenOrderingScheme(trinoOrderingScheme.getOrderBy().stream().map(symbol ->
                    new GlutenOrderingScheme.Ordering(buildVariableRefExpression(symbol), trinoOrderingScheme.getOrdering(symbol))
            ).collect(toImmutableList()));

            GlutenTopNNode.Step step = switch (node.getStep()) {
                case PARTIAL -> GlutenTopNNode.Step.PARTIAL;
                case FINAL -> GlutenTopNNode.Step.FINAL;
                case SINGLE -> GlutenTopNNode.Step.SINGLE;
            };

            return new GlutenTopNNode(node.getId(), source, node.getCount(), orderingScheme, step);
        }

        @Override
        public GlutenLimitNode visitLimit(LimitNode node, Void context)
        {
            GlutenPlanNode source = node.getSource().accept(this, context);
            GlutenLimitNode.Step step = node.isPartial() ? GlutenLimitNode.Step.PARTIAL : GlutenLimitNode.Step.FINAL;
            return new GlutenLimitNode(node.getId(), source, node.getCount(), step);
        }

        @Override
        public GlutenSortNode visitSort(SortNode node, Void context)
        {
            GlutenPlanNode source = node.getSource().accept(this, context);

            OrderingScheme trinoOrderingScheme = node.getOrderingScheme();
            GlutenOrderingScheme orderingScheme = new GlutenOrderingScheme(trinoOrderingScheme.getOrderBy().stream().map(symbol ->
                    new GlutenOrderingScheme.Ordering(buildVariableRefExpression(symbol), trinoOrderingScheme.getOrdering(symbol))
            ).collect(toImmutableList()));
            return new GlutenSortNode(node.getId(), source, orderingScheme, node.isPartial());
        }

        @Override
        public GlutenValuesNode visitValues(ValuesNode node, Void context)
        {
            List<GlutenVariableReferenceExpression> outputVariables = node.getOutputSymbols().stream()
                    .map(this::buildVariableRefExpression)
                    .collect(toImmutableList());

            List<List<GlutenRowExpression>> rows = new ArrayList<>();
            if (outputVariables.size() > 0 && node.getRows().isPresent()) {
                node.getRows().get().stream()
                        .filter(expression -> expression instanceof Row)
                        .forEach(expression -> {
                            List<GlutenRowExpression> rowExpressions = ((Row) expression).getItems().stream()
                                    .map(rowExpression -> ExpressionTranslator.translateExpressionTree(rowExpression, metadata, typeManager, blockEncodingSerde, session, symbolType))
                                    .collect(toImmutableList());
                            checkArgument(outputVariables.size() == rowExpressions.size(),
                                    "declared and actual row counts don't match: %s vs %s", outputVariables.size(), rowExpressions.size());
                            rows.add(rowExpressions);
                        });
            }

            return new GlutenValuesNode(node.getId(), outputVariables, rows, Optional.empty());
        }

        @Override
        public GlutenJoinNode visitJoin(JoinNode node, Void context)
        {
            GlutenPlanNode leftNode = node.getLeft().accept(this, context);
            GlutenPlanNode rightNode = node.getRight().accept(this, context);

            GlutenJoinNode.Type type = switch (node.getType()) {
                case FULL -> GlutenJoinNode.Type.FULL;
                case INNER -> GlutenJoinNode.Type.INNER;
                case LEFT -> GlutenJoinNode.Type.LEFT;
                case RIGHT -> GlutenJoinNode.Type.RIGHT;
            };

            List<GlutenJoinNode.EquiJoinClause> criteria = node.getCriteria().stream().map(equiJoinClause -> {
                GlutenVariableReferenceExpression left = buildVariableRefExpression(equiJoinClause.getLeft());
                GlutenVariableReferenceExpression right = buildVariableRefExpression(equiJoinClause.getRight());
                return new GlutenJoinNode.EquiJoinClause(left, right);
            }).collect(toImmutableList());

            Optional<GlutenRowExpression> filter = node.getFilter()
                    .map(filter_ -> ExpressionTranslator.translateExpressionTree(filter_, metadata, typeManager, blockEncodingSerde, session, symbolType));

            Optional<GlutenJoinNode.DistributionType> distributionType = node.getDistributionType().map(
                    distributionType_ -> switch (distributionType_) {
                        case PARTITIONED -> GlutenJoinNode.DistributionType.PARTITIONED;
                        case REPLICATED -> GlutenJoinNode.DistributionType.REPLICATED;
                    });

            Map<String, GlutenVariableReferenceExpression> dynamicFilters = node.getDynamicFilters().entrySet().stream()
                    .collect(Collectors.toMap(
                            entry -> entry.getKey().toString(),
                            entry -> buildVariableRefExpression(entry.getValue())));

            return new GlutenJoinNode(node.getId(),
                    type,
                    leftNode,
                    rightNode,
                    criteria,
                    node.getOutputSymbols().stream().map(this::buildVariableRefExpression).collect(toImmutableList()),
                    filter,
                    node.getLeftHashSymbol().map(this::buildVariableRefExpression),
                    node.getRightHashSymbol().map(this::buildVariableRefExpression),
                    distributionType,
                    dynamicFilters);
        }

        @Override
        public GlutenPlanNode visitSemiJoin(SemiJoinNode node, Void context)
        {
            return new GlutenSemiJoinNode(node.getId(),
                    node.getSource().accept(this, context),
                    node.getFilteringSource().accept(this, context),
                    buildVariableRefExpression(node.getSourceJoinSymbol()),
                    buildVariableRefExpression(node.getFilteringSourceJoinSymbol()),
                    buildVariableRefExpression(node.getSemiJoinOutput()),
                    node.getSourceHashSymbol().map(this::buildVariableRefExpression),
                    node.getFilteringSourceHashSymbol().map(this::buildVariableRefExpression),
                    node.getDistributionType().map(type -> switch (type) {
                        case PARTITIONED -> GlutenSemiJoinNode.DistributionType.PARTITIONED;
                        case REPLICATED -> GlutenSemiJoinNode.DistributionType.REPLICATED;
                    }),
                    ImmutableMap.of());
        }

        @Override
        public GlutenAssignUniqueId visitAssignUniqueId(AssignUniqueId node, Void context)
        {
            GlutenPlanNode source = node.getSource().accept(this, context);
            return new GlutenAssignUniqueId(node.getId(), source, buildVariableRefExpression(node.getIdColumn()));
        }

        @Override
        public GlutenGroupIdNode visitGroupId(GroupIdNode node, Void context)
        {
            GlutenPlanNode source = node.getSource().accept(this, context);

            List<List<GlutenVariableReferenceExpression>> groupingSets = node.getGroupingSets().stream()
                    .map(list -> list.stream().map(this::buildVariableRefExpression).collect(toImmutableList()))
                    .collect(toImmutableList());
            Map<GlutenVariableReferenceExpression, GlutenVariableReferenceExpression> groupingColumns = node.getGroupingColumns().entrySet().stream()
                    .collect(Collectors.toMap(
                            e -> buildVariableRefExpression(e.getKey()),
                            e -> buildVariableRefExpression(e.getValue())));
            List<GlutenVariableReferenceExpression> aggregationArguments = node.getAggregationArguments().stream()
                    .map(this::buildVariableRefExpression)
                    .collect(toImmutableList());
            GlutenVariableReferenceExpression groupIdVariable = buildVariableRefExpression(node.getGroupIdSymbol());

            return new GlutenGroupIdNode(node.getId(),
                    source,
                    groupingSets,
                    groupingColumns,
                    aggregationArguments,
                    groupIdVariable);
        }

        @Override
        public GlutenMarkDistinctNode visitMarkDistinct(MarkDistinctNode node, Void context)
        {
            GlutenPlanNode source = node.getSource().accept(this, context);
            GlutenVariableReferenceExpression markerVariable = buildVariableRefExpression(node.getMarkerSymbol());
            List<GlutenVariableReferenceExpression> distinctVariables = node.getDistinctSymbols().stream()
                    .map(this::buildVariableRefExpression)
                    .collect(toImmutableList());
            Optional<GlutenVariableReferenceExpression> hashVariable = Optional.empty();
            if (node.getHashSymbol().isPresent()) {
                hashVariable = Optional.of(buildVariableRefExpression(node.getHashSymbol().get()));
            }
            return new GlutenMarkDistinctNode(node.getId(),
                    source,
                    markerVariable,
                    distinctVariables,
                    hashVariable);
        }

        @Override
        public GlutenEnforceSingleRowNode visitEnforceSingleRow(EnforceSingleRowNode node, Void context)
        {
            return new GlutenEnforceSingleRowNode(node.getId(), node.getSource().accept(this, context));
        }

        @Override
        public GlutenWindowNode visitWindow(WindowNode node, Void context)
        {
            GlutenPlanNode source = node.getSource().accept(this, context);

            List<GlutenVariableReferenceExpression> partitionBy = node.getSpecification().getPartitionBy().stream()
                    .map(this::buildVariableRefExpression)
                    .collect(toImmutableList());
            Optional<GlutenOrderingScheme> orderingScheme = Optional.empty();
            if (node.getSpecification().getOrderingScheme().isPresent()) {
                OrderingScheme os = node.getSpecification().getOrderingScheme().get();
                orderingScheme = Optional.of(new GlutenOrderingScheme(os.getOrderBy().stream().map(symbol ->
                        new GlutenOrderingScheme.Ordering(buildVariableRefExpression(symbol), os.getOrdering(symbol))
                ).collect(toImmutableList())));
            }
            GlutenWindowNode.Specification specification = new GlutenWindowNode.Specification(partitionBy, orderingScheme);

            Map<GlutenVariableReferenceExpression, GlutenWindowNode.Function> windowFunctions = node.getWindowFunctions().entrySet().stream()
                    .collect(toImmutableMap(
                            entry -> buildVariableRefExpression(entry.getKey()),
                            entry -> buildWindowFunction(entry.getValue())));

            Optional<GlutenVariableReferenceExpression> hashVariable = Optional.empty();
            if (node.getHashSymbol().isPresent()) {
                hashVariable = Optional.of(buildVariableRefExpression(node.getHashSymbol().get()));
            }
            Set<GlutenVariableReferenceExpression> prePartitionedInputs = node.getPrePartitionedInputs().stream()
                    .map(this::buildVariableRefExpression)
                    .collect(toImmutableSet());
            int preSortedOrderPrefix = node.getPreSortedOrderPrefix();
            return new GlutenWindowNode(node.getId(),
                    source,
                    specification,
                    windowFunctions,
                    hashVariable,
                    prePartitionedInputs,
                    preSortedOrderPrefix);
        }

        @Override
        public GlutenTopNRowNumberNode visitTopNRanking(TopNRankingNode node, Void context)
        {
            GlutenPlanNode source = node.getSource().accept(this, context);

            List<GlutenVariableReferenceExpression> partitionBy = node.getSpecification().getPartitionBy().stream()
                    .map(this::buildVariableRefExpression)
                    .collect(toImmutableList());
            Optional<GlutenOrderingScheme> orderingScheme = Optional.empty();
            if (node.getSpecification().getOrderingScheme().isPresent()) {
                OrderingScheme os = node.getSpecification().getOrderingScheme().get();
                orderingScheme = Optional.of(new GlutenOrderingScheme(os.getOrderBy().stream().map(symbol ->
                        new GlutenOrderingScheme.Ordering(buildVariableRefExpression(symbol), os.getOrdering(symbol))
                ).collect(toImmutableList())));
            }
            GlutenWindowNode.Specification specification = new GlutenWindowNode.Specification(partitionBy, orderingScheme);

            // The ranking symbol of rank functions is not included in fragment's symbolType map, so we have to translate rowNumberVariable manually
            String rankName = node.getRankingSymbol().getName();
            GlutenVariableReferenceExpression rowNumberVariable;
            if ("percent_rank".equals(rankName)) {
                rowNumberVariable = new GlutenVariableReferenceExpression(rankName, DoubleType.DOUBLE);
            }
            else {
                rowNumberVariable = new GlutenVariableReferenceExpression(rankName, BigintType.BIGINT);
            }

            int maxRowCountPerPartition = node.getMaxRankingPerPartition();

            boolean partial = node.isPartial();

            Optional<GlutenVariableReferenceExpression> hashVariable = Optional.empty();
            if (node.getHashSymbol().isPresent()) {
                hashVariable = Optional.of(buildVariableRefExpression(node.getHashSymbol().get()));
            }

            return new GlutenTopNRowNumberNode(node.getId(),
                    source,
                    specification,
                    rowNumberVariable,
                    maxRowCountPerPartition,
                    partial,
                    hashVariable);
        }
    }
}
