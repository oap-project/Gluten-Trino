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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.sql.planner.plan.PlanNodeId;

import javax.annotation.concurrent.Immutable;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.velox.protocol.GlutenWindowNode.Frame.WindowType.RANGE;
import static java.util.Objects.requireNonNull;

public class GlutenWindowNode
        extends GlutenPlanNode
{
    private final GlutenPlanNode source;
    private final Set<GlutenVariableReferenceExpression> prePartitionedInputs;
    private final Specification specification;
    private final int preSortedOrderPrefix;
    private final Map<GlutenVariableReferenceExpression, Function> windowFunctions;
    private final Optional<GlutenVariableReferenceExpression> hashVariable;

    @JsonCreator
    public GlutenWindowNode(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") GlutenPlanNode source,
            @JsonProperty("specification") Specification specification,
            @JsonProperty("windowFunctions") Map<GlutenVariableReferenceExpression, Function> windowFunctions,
            @JsonProperty("hashVariable") Optional<GlutenVariableReferenceExpression> hashVariable,
            @JsonProperty("prePartitionedInputs") Set<GlutenVariableReferenceExpression> prePartitionedInputs,
            @JsonProperty("preSortedOrderPrefix") int preSortedOrderPrefix)
    {
        super(id);

        requireNonNull(source, "source is null");
        requireNonNull(specification, "specification is null");
        requireNonNull(windowFunctions, "windowFunctions is null");
        requireNonNull(hashVariable, "hashVariable is null");
        checkArgument(specification.getPartitionBy().containsAll(prePartitionedInputs), "prePartitionedInputs must be contained in partitionBy");
        Optional<GlutenOrderingScheme> orderingScheme = specification.getOrderingScheme();
        checkArgument(preSortedOrderPrefix == 0 || (orderingScheme.isPresent() && preSortedOrderPrefix <= orderingScheme.get().getOrderByVariables().size()), "Cannot have sorted more symbols than those requested");
        checkArgument(preSortedOrderPrefix == 0 || ImmutableSet.copyOf(prePartitionedInputs).equals(ImmutableSet.copyOf(specification.getPartitionBy())), "preSortedOrderPrefix can only be greater than zero if all partition symbols are pre-partitioned");

        this.source = source;
        this.prePartitionedInputs = ImmutableSet.copyOf(prePartitionedInputs);
        this.specification = specification;
        this.windowFunctions = ImmutableMap.copyOf(windowFunctions);
        this.hashVariable = hashVariable;
        this.preSortedOrderPrefix = preSortedOrderPrefix;
    }

    @JsonProperty
    public GlutenPlanNode getSource()
    {
        return source;
    }

    @JsonProperty
    public Specification getSpecification()
    {
        return specification;
    }

    @JsonProperty
    public Map<GlutenVariableReferenceExpression, Function> getWindowFunctions()
    {
        return windowFunctions;
    }

    @JsonProperty
    public Optional<GlutenVariableReferenceExpression> getHashVariable()
    {
        return hashVariable;
    }

    @JsonProperty
    public Set<GlutenVariableReferenceExpression> getPrePartitionedInputs()
    {
        return prePartitionedInputs;
    }

    @JsonProperty
    public int getPreSortedOrderPrefix()
    {
        return preSortedOrderPrefix;
    }

    @Immutable
    public static class Specification
    {
        private final List<GlutenVariableReferenceExpression> partitionBy;
        private final Optional<GlutenOrderingScheme> orderingScheme;

        @JsonCreator
        public Specification(
                @JsonProperty("partitionBy") List<GlutenVariableReferenceExpression> partitionBy,
                @JsonProperty("orderingScheme") Optional<GlutenOrderingScheme> orderingScheme)
        {
            requireNonNull(partitionBy, "partitionBy is null");
            requireNonNull(orderingScheme, "orderingScheme is null");

            this.partitionBy = ImmutableList.copyOf(partitionBy);
            this.orderingScheme = requireNonNull(orderingScheme, "orderingScheme is null");
        }

        @JsonProperty
        public List<GlutenVariableReferenceExpression> getPartitionBy()
        {
            return partitionBy;
        }

        @JsonProperty
        public Optional<GlutenOrderingScheme> getOrderingScheme()
        {
            return orderingScheme;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(partitionBy, orderingScheme);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }

            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }

            Specification other = (Specification) obj;

            return Objects.equals(this.partitionBy, other.partitionBy) &&
                    Objects.equals(this.orderingScheme, other.orderingScheme);
        }
    }

    @Immutable
    public static class Frame
    {
        private final WindowType type;
        private final BoundType startType;
        private final Optional<GlutenVariableReferenceExpression> startValue;
        // Sort key coerced to the same type of range start expression, for comparing and deciding frame start for range expression
        private final Optional<GlutenVariableReferenceExpression> sortKeyCoercedForFrameStartComparison;
        private final BoundType endType;
        private final Optional<GlutenVariableReferenceExpression> endValue;
        // Sort key coerced to the same type of range end expression, for comparing and deciding frame end for range expression
        private final Optional<GlutenVariableReferenceExpression> sortKeyCoercedForFrameEndComparison;

        // This information is only used for printing the plan.
        private final Optional<String> originalStartValue;
        private final Optional<String> originalEndValue;

        @JsonCreator
        public Frame(
                @JsonProperty("type") WindowType type,
                @JsonProperty("startType") BoundType startType,
                @JsonProperty("startValue") Optional<GlutenVariableReferenceExpression> startValue,
                @JsonProperty("sortKeyCoercedForFrameStartComparison") Optional<GlutenVariableReferenceExpression> sortKeyCoercedForFrameStartComparison,
                @JsonProperty("endType") BoundType endType,
                @JsonProperty("endValue") Optional<GlutenVariableReferenceExpression> endValue,
                @JsonProperty("sortKeyCoercedForFrameEndComparison") Optional<GlutenVariableReferenceExpression> sortKeyCoercedForFrameEndComparison,
                @JsonProperty("originalStartValue") Optional<String> originalStartValue,
                @JsonProperty("originalEndValue") Optional<String> originalEndValue)
        {
            this.startType = requireNonNull(startType, "startType is null");
            this.startValue = requireNonNull(startValue, "startValue is null");
            this.sortKeyCoercedForFrameStartComparison = requireNonNull(sortKeyCoercedForFrameStartComparison, "sortKeyCoercedForFrameStartComparison is null");
            this.endType = requireNonNull(endType, "endType is null");
            this.endValue = requireNonNull(endValue, "endValue is null");
            this.sortKeyCoercedForFrameEndComparison = requireNonNull(sortKeyCoercedForFrameEndComparison, "sortKeyCoercedForFrameEndComparison is null");
            this.type = requireNonNull(type, "type is null");
            this.originalStartValue = requireNonNull(originalStartValue, "originalStartValue is null");
            this.originalEndValue = requireNonNull(originalEndValue, "originalEndValue is null");

            if (startValue.isPresent()) {
                checkArgument(originalStartValue.isPresent(), "originalStartValue must be present if startValue is present");
                if (type == RANGE) {
                    checkArgument(sortKeyCoercedForFrameStartComparison.isPresent(), "for frame of type RANGE, sortKeyCoercedForFrameStartComparison must be present if startValue is present");
                }
            }

            if (endValue.isPresent()) {
                checkArgument(originalEndValue.isPresent(), "originalEndValue must be present if endValue is present");
                if (type == RANGE) {
                    checkArgument(sortKeyCoercedForFrameEndComparison.isPresent(), "for frame of type RANGE, sortKeyCoercedForFrameEndComparison must be present if endValue is present");
                }
            }
        }

        @JsonProperty
        public WindowType getType()
        {
            return type;
        }

        @JsonProperty
        public BoundType getStartType()
        {
            return startType;
        }

        @JsonProperty
        public Optional<GlutenVariableReferenceExpression> getStartValue()
        {
            return startValue;
        }

        @JsonProperty
        public Optional<GlutenVariableReferenceExpression> getSortKeyCoercedForFrameStartComparison()
        {
            return sortKeyCoercedForFrameStartComparison;
        }

        @JsonProperty
        public BoundType getEndType()
        {
            return endType;
        }

        @JsonProperty
        public Optional<GlutenVariableReferenceExpression> getEndValue()
        {
            return endValue;
        }

        @JsonProperty
        public Optional<GlutenVariableReferenceExpression> getSortKeyCoercedForFrameEndComparison()
        {
            return sortKeyCoercedForFrameEndComparison;
        }

        @JsonProperty
        public Optional<String> getOriginalStartValue()
        {
            return originalStartValue;
        }

        @JsonProperty
        public Optional<String> getOriginalEndValue()
        {
            return originalEndValue;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Frame frame = (Frame) o;
            return type == frame.type &&
                    startType == frame.startType &&
                    Objects.equals(startValue, frame.startValue) &&
                    Objects.equals(sortKeyCoercedForFrameStartComparison, frame.sortKeyCoercedForFrameStartComparison) &&
                    endType == frame.endType &&
                    Objects.equals(endValue, frame.endValue) &&
                    Objects.equals(sortKeyCoercedForFrameEndComparison, frame.sortKeyCoercedForFrameEndComparison);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(type, startType, startValue, sortKeyCoercedForFrameStartComparison, endType, endValue, originalStartValue, originalEndValue, sortKeyCoercedForFrameEndComparison);
        }

        public enum WindowType
        {
            RANGE, ROWS, GROUPS
        }

        public enum BoundType
        {
            UNBOUNDED_PRECEDING,
            PRECEDING,
            CURRENT_ROW,
            FOLLOWING,
            UNBOUNDED_FOLLOWING
        }
    }

    @Immutable
    public static final class Function
    {
        private final GlutenCallExpression functionCall;
        private final Frame frame;
        private final boolean ignoreNulls;

        @JsonCreator
        public Function(
                @JsonProperty("functionCall") GlutenCallExpression functionCall,
                @JsonProperty("frame") Frame frame,
                @JsonProperty("ignoreNulls") boolean ignoreNulls)
        {
            this.functionCall = requireNonNull(functionCall, "functionCall is null");
            this.frame = requireNonNull(frame, "Frame is null");
            this.ignoreNulls = ignoreNulls;
        }

        @JsonProperty
        public GlutenCallExpression getFunctionCall()
        {
            return functionCall;
        }

        @JsonProperty
        public GlutenFunctionHandle getFunctionHandle()
        {
            return functionCall.getFunctionHandle();
        }

        @JsonProperty
        public Frame getFrame()
        {
            return frame;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(functionCall, frame, ignoreNulls);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            Function other = (Function) obj;
            return Objects.equals(this.functionCall, other.functionCall) &&
                    Objects.equals(this.frame, other.frame) &&
                    Objects.equals(this.ignoreNulls, other.ignoreNulls);
        }

        @JsonProperty
        public boolean isIgnoreNulls()
        {
            return ignoreNulls;
        }
    }
}
