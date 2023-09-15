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
import io.airlift.log.Logger;
import io.airlift.slice.BasicSliceInput;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.Session;
import io.trino.metadata.Metadata;
import io.trino.metadata.ResolvedFunction;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockEncodingSerde;
import io.trino.spi.block.VariableWidthBlock;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionKind;
import io.trino.spi.function.OperatorType;
import io.trino.spi.function.Signature;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.type.DecimalParseResult;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.FixedWidthType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.VarcharType;
import io.trino.sql.planner.Symbol;
import io.trino.sql.tree.ArithmeticBinaryExpression;
import io.trino.sql.tree.AstVisitor;
import io.trino.sql.tree.BetweenPredicate;
import io.trino.sql.tree.BooleanLiteral;
import io.trino.sql.tree.Cast;
import io.trino.sql.tree.CoalesceExpression;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.DecimalLiteral;
import io.trino.sql.tree.DoubleLiteral;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.GenericLiteral;
import io.trino.sql.tree.InListExpression;
import io.trino.sql.tree.InPredicate;
import io.trino.sql.tree.IsNotNullPredicate;
import io.trino.sql.tree.IsNullPredicate;
import io.trino.sql.tree.Literal;
import io.trino.sql.tree.LogicalExpression;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.NotExpression;
import io.trino.sql.tree.NullLiteral;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.Row;
import io.trino.sql.tree.SearchedCaseExpression;
import io.trino.sql.tree.SimpleCaseExpression;
import io.trino.sql.tree.StringLiteral;
import io.trino.sql.tree.SubscriptExpression;
import io.trino.sql.tree.SymbolReference;
import io.trino.sql.tree.WhenClause;
import io.trino.type.TypeCoercion;
import io.trino.type.UnknownType;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.function.FunctionKind.SCALAR;
import static io.trino.spi.function.OperatorType.ADD;
import static io.trino.spi.function.OperatorType.DIVIDE;
import static io.trino.spi.function.OperatorType.MODULUS;
import static io.trino.spi.function.OperatorType.MULTIPLY;
import static io.trino.spi.function.OperatorType.SUBTRACT;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.analyzer.TypeSignatureTranslator.toTypeSignature;
import static io.trino.type.IntervalDayTimeType.INTERVAL_DAY_TIME;
import static io.trino.type.LikePatternType.LIKE_PATTERN;
import static io.trino.velox.protocol.GlutenSpecialFormExpression.Form.IS_NULL;
import static io.trino.velox.protocol.GlutenSpecialFormExpression.Form.ROW_CONSTRUCTOR;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public final class ExpressionTranslator
{
    private static final Logger log = Logger.get(ExpressionTranslator.class);
    private static final String OPERATOR_PREFIX = "$operator$";

    private static final List<String> GLUTEN_TRINO_ADDED_FUNCTION_NAMES = ImmutableList.of("sum");

    private ExpressionTranslator()
    {
    }

    public static GlutenRowExpression translateExpressionTree(
            Expression root,
            Metadata metadata,
            TypeManager typeManager,
            BlockEncodingSerde blockEncodingSerde,
            Session session,
            Map<Symbol, Type> symbolTypes)
    {
        Visitor visitor = new Visitor(metadata, typeManager, blockEncodingSerde, session, symbolTypes);
        return visitor.process(root, null);
    }

    public static GlutenRowExpression translateNullableValue(NullableValue constant)
    {
        return new GlutenConstantExpression(constant.getValue(), constant.getType());
    }

    private static GlutenSignature.FunctionQualifiedName renameTrinoBridgeFunction(String originName)
    {
        for (String trinoBridgeAddedFunctionName : GLUTEN_TRINO_ADDED_FUNCTION_NAMES) {
            if (originName.toLowerCase(ENGLISH).startsWith(trinoBridgeAddedFunctionName)) {
                return new GlutenSignature.FunctionQualifiedName("trino", "bridge", trinoBridgeAddedFunctionName);
            }
        }
        return new GlutenSignature.FunctionQualifiedName(originName);
    }

    // TODO: Refactor function.
    protected static GlutenCallExpression buildCallExpression(ResolvedFunction function, List<GlutenRowExpression> arguments, boolean isOperator)
    {
        String displayName = function.getFunctionId().toString();
        BoundSignature signature = function.getSignature();

        // TBD: Mark variableArity as default false
        GlutenSignature functionSignature = new GlutenSignature(
                renameTrinoBridgeFunction(signature.getName().toLowerCase(ENGLISH)),
                function.getFunctionKind(),
                signature.getReturnType().getTypeSignature(),
                signature.getArgumentTypes().stream().map(Type::getTypeSignature).collect(toImmutableList()),
                false);
        // TBD: Only support buildin functions currently
        GlutenFunctionHandle functionHandle = new GlutenBuiltInFunctionHandle(functionSignature);

        return new GlutenCallExpression(displayName, functionHandle, signature.getReturnType(), arguments);
    }

    protected static GlutenCallExpression buildCallExpression(String functionName, FunctionKind kind, Type returnType, List<GlutenRowExpression> arguments)
    {
        // TBD: Mark variableArity as default false
        GlutenSignature functionSignature = new GlutenSignature(
                renameTrinoBridgeFunction(functionName.toLowerCase(ENGLISH)),
                kind,
                returnType.getTypeSignature(),
                arguments.stream().map(glutenRowExpression -> glutenRowExpression.getType().getTypeSignature()).collect(toImmutableList()),
                false);
        // TBD: Only support buildin functions currently
        GlutenFunctionHandle functionHandle = new GlutenBuiltInFunctionHandle(functionSignature);

        return new GlutenCallExpression(functionName, functionHandle, returnType, arguments);
    }

    private static class Visitor
            extends AstVisitor<GlutenRowExpression, Void>
    {
        private final Metadata metadata;
        private final TypeManager typeManager;
        private final BlockEncodingSerde blockEncodingSerde;
        private final Session session;
        private final Map<Symbol, Type> types;

        private Visitor(
                Metadata metadata,
                TypeManager typeManager,
                BlockEncodingSerde blockEncodingSerde,
                Session session,
                Map<Symbol, Type> types)
        {
            this.metadata = metadata;
            this.typeManager = typeManager;
            this.blockEncodingSerde = blockEncodingSerde;
            this.session = session;
            this.types = types;
        }

        @Override
        protected GlutenRowExpression visitExpression(Expression node, Void context)
        {
            throw new UnsupportedOperationException("Unsupported Expression type to translate: " + node.getClass().getName());
        }

        private GlutenRowExpression castIfNeeded(GlutenRowExpression node, Type toType)
        {
            requireNonNull(toType, "toType is null.");
            requireNonNull(node, "node is null.");
            if (node instanceof GlutenConstantExpression) {
                return node;
            }
            if (!toType.equals(node.getType())) {
                String displayName = "CAST";
                GlutenSignature newSignature = new GlutenSignature(
                        OPERATOR_PREFIX + displayName.toLowerCase(ENGLISH),
                        SCALAR,
                        toType.getTypeSignature(),
                        ImmutableList.of(node.getType().getTypeSignature()),
                        false);
                GlutenFunctionHandle functionHandle = new GlutenBuiltInFunctionHandle(newSignature);

                List<GlutenRowExpression> newArguments = ImmutableList.of(node);
                return new GlutenCallExpression(displayName, functionHandle, toType, newArguments);
            }
            return node;
        }

        private boolean isConvertible(GlutenRowExpression constant, Type toType)
        {
            requireNonNull(constant, "constant is null.");
            requireNonNull(toType, "toType is null.");
            TypeCoercion typeCoercion = new TypeCoercion(typeManager::getType);
            Type fromType = constant.getType();
            // For date type
            if (fromType instanceof FixedWidthType && toType instanceof FixedWidthType) {
                if (((FixedWidthType) fromType).getFixedSize() < ((FixedWidthType) toType).getFixedSize()) {
                    return true;
                }
            }
            return typeCoercion.canCoerce(constant.getType(), toType);
        }

        private GlutenRowExpression castConstantExpressionIfNeeded(GlutenRowExpression node, Type toType)
        {
            requireNonNull(toType, "toType is null.");
            requireNonNull(node, "node is null.");

            if (!(node instanceof GlutenConstantExpression constant)) {
                // Maybe should throw unsupported exception here.
                return node;
            }
            if (!toType.equals(constant.getType()) && isConvertible(constant, toType)) {
                if (toType instanceof DecimalType decimalType) {
                    // LongDecimal use int128, must cast here
                    if (!decimalType.isShort() && constant.getValue() instanceof Long value) {
                        return new GlutenConstantExpression(Int128.valueOf(0L, value), toType);
                    }
                }
                return new GlutenConstantExpression(constant.getValue(), toType);
            }
            log.debug("Constant Expression %s is not converted to %s!", constant.getValue(), toType.getBaseName());
            return constant;
        }

        @Override
        protected GlutenRowExpression visitInPredicate(InPredicate node, Void context)
        {
            List<GlutenRowExpression> expressionList = new ArrayList<>();
            GlutenRowExpression glutenValue = process(node.getValue(), context);
            expressionList.add(glutenValue);
            // ValueList is a InListExpression, we will not do process here since it should return a const value array
            if (!(node.getValueList() instanceof InListExpression inListNode)) {
                throw new UnsupportedOperationException("Unsupported InPredicate value list type: " + node.getValueList().getClass().getSimpleName());
            }

            Type compareType = glutenValue.getType();
            inListNode.getValues().stream().map(expression
                    -> process(expression, context)).map(value -> castConstantExpressionIfNeeded(value, compareType)).forEach(expressionList::add);

            GlutenSpecialFormExpression.Form form = GlutenSpecialFormExpression.Form.IN;
            return new GlutenSpecialFormExpression(form, BOOLEAN, expressionList);
        }

        @Override
        protected GlutenRowExpression visitBooleanLiteral(BooleanLiteral node, Void context)
        {
            return new GlutenConstantExpression(node.getValue(), BOOLEAN);
        }

        @Override
        protected GlutenRowExpression visitDoubleLiteral(DoubleLiteral node, Void context)
        {
            return new GlutenConstantExpression(node.getValue(), DOUBLE);
        }

        @Override
        protected GlutenRowExpression visitDecimalLiteral(DecimalLiteral node, Void context)
        {
            DecimalParseResult parseResult = Decimals.parse(node.getValue());
            return new GlutenConstantExpression(parseResult.getObject(), createDecimalType(parseResult.getType().getPrecision(), parseResult.getType().getScale()));
        }

        @Override
        protected GlutenRowExpression visitSubscriptExpression(SubscriptExpression node, Void context)
        {
            GlutenRowExpression base = process(node.getBase(), context);
            GlutenRowExpression index = process(node.getIndex(), context);

            if (base.getType() instanceof RowType) {
                return new GlutenSpecialFormExpression(GlutenSpecialFormExpression.Form.DEREFERENCE, process(node, context).getType(), ImmutableList.of(base, index));
            }

            return buildCallExpression(
                    metadata.resolveOperator(session, OperatorType.SUBSCRIPT, ImmutableList.of(base.getType(), index.getType())),
                    ImmutableList.of(base, index),
                    true);
        }

        @Override
        protected GlutenRowExpression visitLongLiteral(LongLiteral node, Void context)
        {
            if (node.getValue() >= Integer.MIN_VALUE && node.getValue() <= Integer.MAX_VALUE) {
                return new GlutenConstantExpression(node.getValue(), IntegerType.INTEGER);
            }
            return new GlutenConstantExpression(node.getValue(), BIGINT);
        }

        @Override
        protected GlutenRowExpression visitStringLiteral(StringLiteral node, Void context)
        {
            return new GlutenConstantExpression(utf8Slice(node.getValue()), VarcharType.createVarcharType(node.length()));
        }

        @Override
        protected GlutenRowExpression visitWhenClause(WhenClause node, Void context)
        {
            GlutenRowExpression glutenOperand = process(node.getOperand(), context);
            GlutenRowExpression glutenResult = process(node.getResult(), context);
            GlutenSpecialFormExpression.Form form = GlutenSpecialFormExpression.Form.WHEN;
            return new GlutenSpecialFormExpression(form, glutenResult.getType(), ImmutableList.of(glutenOperand, glutenResult));
        }

        @Override
        protected GlutenRowExpression visitCoalesceExpression(CoalesceExpression node, Void context)
        {
            GlutenSpecialFormExpression.Form form = GlutenSpecialFormExpression.Form.COALESCE;
            List<GlutenRowExpression> glutenOperands = node.getOperands().stream()
                    .map(operand -> process(operand, context))
                    .collect(toImmutableList());
            // Test: only compare the first one and the last one
            GlutenRowExpression left = glutenOperands.get(0);
            GlutenRowExpression right = glutenOperands.get(glutenOperands.size() - 1);
            List<GlutenRowExpression> arguments = new ArrayList<>();
            if (!left.getType().equals(right.getType())) {
                if (isConvertible(left, right.getType())) {
                    for (GlutenRowExpression argument : glutenOperands) {
                        argument = castIfNeeded(argument, right.getType());
                        arguments.add(argument);
                    }
                }
                else if (isConvertible(right, left.getType())) {
                    for (GlutenRowExpression argument : glutenOperands) {
                        argument = castIfNeeded(argument, left.getType());
                        arguments.add(argument);
                    }
                }
                else {
                    throw new IllegalStateException(format("Type of the expressions '%s' and '%s' may not be comparable.",
                            left.getType().getDisplayName(), right.getType().getDisplayName()));
                }
            }
            if (arguments.isEmpty()) {
                arguments = glutenOperands;
            }

            return new GlutenSpecialFormExpression(form, arguments.get(0).getType(), arguments);
        }

        @Override
        protected GlutenRowExpression visitSimpleCaseExpression(SimpleCaseExpression node, Void context)
        {
            return buildSwitch(process(node.getOperand(), context), node.getWhenClauses(), node.getDefaultValue(), context);
        }

        @Override
        protected GlutenRowExpression visitSearchedCaseExpression(SearchedCaseExpression node, Void context)
        {
            return buildSwitch(new GlutenConstantExpression(true, BOOLEAN), node.getWhenClauses(), node.getDefaultValue(), context);
        }

        private GlutenRowExpression constantNull(Type type)
        {
            return new GlutenConstantExpression((Object) null, type);
        }

        private GlutenRowExpression buildSwitch(GlutenRowExpression operand, List<WhenClause> whenClauses, Optional<Expression> defaultValue, Void context)
        {
            GlutenSpecialFormExpression.Form form = GlutenSpecialFormExpression.Form.SWITCH;

            ImmutableList.Builder<GlutenRowExpression> arguments = ImmutableList.builder();
            arguments.add(operand);

            Type returnType = null;
            for (WhenClause clause : whenClauses) {
                GlutenRowExpression glutenClause = process(clause, context);
                if (returnType == null) {
                    returnType = glutenClause.getType();
                }
                arguments.add(glutenClause);
            }

            if (returnType == null) {
                throw new IllegalStateException("Switch Expression must contain at least one When clauses.");
            }

            arguments.add(defaultValue
                    .map((value) -> process(value, context))
                    .orElse(constantNull(returnType)));

            return new GlutenSpecialFormExpression(form, returnType, arguments.build());
        }

        @Override
        protected GlutenRowExpression visitLogicalExpression(LogicalExpression node, Void context)
        {
            GlutenSpecialFormExpression.Form form = switch (node.getOperator()) {
                case AND -> GlutenSpecialFormExpression.Form.AND;
                case OR -> GlutenSpecialFormExpression.Form.OR;
            };
            return new GlutenSpecialFormExpression(form, BOOLEAN, node.getTerms().stream().map(term -> process(term, context)).collect(toImmutableList()));
        }

        private GlutenRowExpression notExpression(GlutenRowExpression node)
        {
            return buildCallExpression(
                    metadata.resolveFunction(session, QualifiedName.of("not"), fromTypes(BOOLEAN)),
                    ImmutableList.of(node),
                    false);
        }

        @Override
        protected GlutenRowExpression visitNotExpression(NotExpression node, Void context)
        {
            GlutenRowExpression value = process(node.getValue());
            return notExpression(value);
        }

        @Override
        protected GlutenRowExpression visitIsNotNullPredicate(IsNotNullPredicate node, Void context)
        {
            GlutenRowExpression expression = process(node.getValue(), context);
            return notExpression(new GlutenSpecialFormExpression(IS_NULL, BOOLEAN, ImmutableList.of(expression)));
        }

        @Override
        protected GlutenRowExpression visitIsNullPredicate(IsNullPredicate node, Void context)
        {
            GlutenRowExpression expression = process(node.getValue(), context);
            return new GlutenSpecialFormExpression(IS_NULL, BOOLEAN, ImmutableList.of(expression));
        }

        @Override
        protected GlutenRowExpression visitComparisonExpression(ComparisonExpression node, Void context)
        {
            GlutenRowExpression left = process(node.getLeft(), context);
            GlutenRowExpression right = process(node.getRight(), context);

            if (!left.getType().equals(right.getType())) {
                if (isConvertible(left, right.getType())) {
                    left = castIfNeeded(left, right.getType());
                }
                else if (isConvertible(right, left.getType())) {
                    right = castIfNeeded(right, left.getType());
                }
                else {
                    throw new IllegalStateException(format("Type of the expressions '%s' and '%s' may not be comparable.",
                            left.getType().getDisplayName(), right.getType().getDisplayName()));
                }
            }
            ComparisonExpression.Operator op = node.getOperator();

            return resolveComparisonExpression(op, left, right);
        }

        private GlutenRowExpression resolveComparisonExpression(ComparisonExpression.Operator op, GlutenRowExpression left, GlutenRowExpression right)
        {
            ResolvedFunction resolvedFunction;
            ImmutableList<GlutenRowExpression> argumentList;

            switch (op) {
                case GREATER_THAN_OR_EQUAL -> {
                    resolvedFunction = metadata.resolveOperator(session, OperatorType.LESS_THAN_OR_EQUAL, ImmutableList.of(right.getType(), left.getType()));
                    argumentList = ImmutableList.of(right, left);
                }
                case LESS_THAN_OR_EQUAL -> {
                    resolvedFunction = metadata.resolveOperator(session, OperatorType.LESS_THAN_OR_EQUAL, ImmutableList.of(right.getType(), left.getType()));
                    argumentList = ImmutableList.of(left, right);
                }
                case LESS_THAN -> {
                    resolvedFunction = metadata.resolveOperator(session, OperatorType.LESS_THAN, ImmutableList.of(left.getType(), right.getType()));
                    argumentList = ImmutableList.of(left, right);
                }
                case GREATER_THAN -> {
                    resolvedFunction = metadata.resolveOperator(session, OperatorType.LESS_THAN, ImmutableList.of(left.getType(), right.getType()));
                    argumentList = ImmutableList.of(right, left);
                }
                case EQUAL -> {
                    resolvedFunction = metadata.resolveOperator(session, OperatorType.EQUAL, ImmutableList.of(left.getType(), right.getType()));
                    argumentList = ImmutableList.of(left, right);
                }
                case NOT_EQUAL -> {
                    resolvedFunction = metadata.resolveFunction(session, QualifiedName.of("not"), fromTypes(BOOLEAN));
                    argumentList = ImmutableList.of(resolveComparisonExpression(ComparisonExpression.Operator.EQUAL, left, right));
                }
                case IS_DISTINCT_FROM -> {
                    resolvedFunction = metadata.resolveOperator(session, OperatorType.IS_DISTINCT_FROM, ImmutableList.of(left.getType(), right.getType()));
                    argumentList = ImmutableList.of(left, right);
                }
                default -> throw new UnsupportedOperationException("Unsupported Operator type to translate: " + op.getValue());
            }
            return buildCallExpression(resolvedFunction, argumentList, true);
        }

        @Override
        protected GlutenRowExpression visitBetweenPredicate(BetweenPredicate node, Void context)
        {
            GlutenRowExpression value = process(node.getValue(), context);
            GlutenRowExpression min = process(node.getMin(), context);
            GlutenRowExpression max = process(node.getMax(), context);

            if (max.getType().equals(min.getType()) && !value.getType().equals(min.getType())) {
                if (isConvertible(value, min.getType())) {
                    value = castIfNeeded(value, min.getType());
                }
                else if (isConvertible(min, value.getType())) {
                    min = castIfNeeded(min, value.getType());
                }
                else {
                    throw new IllegalStateException(format("Type of the expressions '%s' and '%s' may not be comparable.",
                            value.getType().getDisplayName(), min.getType().getDisplayName()));
                }
            }

            return buildCallExpression(OPERATOR_PREFIX + "between", SCALAR, BOOLEAN, ImmutableList.of(value, min, max));
        }

        @Override
        protected GlutenRowExpression visitArithmeticBinary(ArithmeticBinaryExpression node, Void context)
        {
            GlutenRowExpression left = process(node.getLeft(), context);
            GlutenRowExpression right = process(node.getRight(), context);

            // Do not convert interval time, do it native side
            if (!left.getType().equals(right.getType()) && !left.getType().equals(INTERVAL_DAY_TIME) && !right.getType().equals(INTERVAL_DAY_TIME)) {
                if (isConvertible(left, right.getType())) {
                    left = castIfNeeded(left, right.getType());
                }
                else if (isConvertible(right, left.getType())) {
                    right = castIfNeeded(right, left.getType());
                }
                else {
                    throw new IllegalStateException(format("Type of the expressions '%s' and '%s' may not be comparable.",
                            left.getType().getDisplayName(), right.getType().getDisplayName()));
                }
            }

            OperatorType operatorType = switch (node.getOperator()) {
                case ADD -> ADD;
                case SUBTRACT -> SUBTRACT;
                case MULTIPLY -> MULTIPLY;
                case DIVIDE -> DIVIDE;
                case MODULUS -> MODULUS;
            };
            ResolvedFunction function = metadata.resolveOperator(session, operatorType, ImmutableList.of(left.getType(), right.getType()));

            return buildCallExpression(function, ImmutableList.of(left, right), true);
        }

        @Override
        protected GlutenRowExpression visitSymbolReference(SymbolReference node, Void context)
        {
            Type type = types.get(new Symbol(node.getName()));
            if (type == null) {
                throw new IllegalStateException("Unknown symbol reference: " + node.getName());
            }
            return new GlutenVariableReferenceExpression(node.getName(), type);
        }

        @Override
        protected GlutenRowExpression visitFunctionCall(FunctionCall node, Void context)
        {
            ResolvedFunction function = metadata.decodeFunction(node.getName());

            GlutenRowExpression like = buildLike(function, node);
            if (like != null) {
                return like;
            }

            ImmutableList<GlutenRowExpression> arguments = node.getArguments().stream()
                    .map(value -> process(value, context)).collect(toImmutableList());
            return buildCallExpression(function, arguments, false);
        }

        private boolean invalidCallExpression(ResolvedFunction function, Type returnType, List<Type> parameterType, int numArguments)
        {
            if (function == null) {
                return true;
            }

            BoundSignature signature = function.getSignature();
            if (!returnType.equals(signature.getReturnType())) {
                return true;
            }
            List<Type> typeSignatures = signature.getArgumentTypes();
            for (int index = 0; index < parameterType.size(); ++index) {
                if (index >= typeSignatures.size() || !parameterType.get(index).equals(typeSignatures.get(index))) {
                    return true;
                }
            }
            return typeSignatures.size() != numArguments;
        }

        private GlutenCallExpression convertLiteralCallFunctionForLike(FunctionCall literalCall)
        {
            ResolvedFunction literalFunction = metadata.decodeFunction(literalCall.getName());
            if (invalidCallExpression(literalFunction, LIKE_PATTERN, ImmutableList.of(VARBINARY), 1)) {
                return null;
            }

            Expression literalCallArgument = literalCall.getArguments().get(0);
            if (!(literalCallArgument instanceof FunctionCall fromBase64)) {
                return null;
            }

            ResolvedFunction fromBase64Function = metadata.decodeFunction(fromBase64.getName());
            if (invalidCallExpression(fromBase64Function, VARBINARY, ImmutableList.of(VARCHAR), 1)) {
                return null;
            }

            Expression fromBase64Argument = fromBase64.getArguments().get(0);
            if (!(fromBase64Argument instanceof StringLiteral base64Pattern)) {
                return null;
            }

            BoundSignature signature = literalFunction.getSignature();

            String rawBase64Pattern = base64Pattern.getValue();
            byte[] decodedPattern = Base64.getDecoder().decode(rawBase64Pattern);

            Slice serializedBlock = Slices.wrappedBuffer(decodedPattern);
            Block deserializedBlock = blockEncodingSerde.readBlock(serializedBlock.getInput());
            if (!(deserializedBlock instanceof VariableWidthBlock variableWidthBlock)) {
                return null;
            }
            BasicSliceInput input = variableWidthBlock.getRawSlice().getInput();
            int varcharTypeBoundedLength = input.readInt();
            Slice rawPattern = input.readSlice(varcharTypeBoundedLength);

            Type returnType = VarcharType.createVarcharType(varcharTypeBoundedLength);

            String displayName = "CAST";

            Signature oldSignature = signature.toSignature();
            GlutenSignature newSignature = new GlutenSignature(
                    OPERATOR_PREFIX + displayName.toLowerCase(ENGLISH),
                    SCALAR,
                    LIKE_PATTERN.getTypeSignature(),
                    ImmutableList.of(VARCHAR.getTypeSignature()),
                    oldSignature.isVariableArity());
            GlutenFunctionHandle newLiteralCallFunctionHandle = new GlutenBuiltInFunctionHandle(newSignature);

            // get new literal arguments
            List<GlutenRowExpression> newArguments = ImmutableList.of(new GlutenConstantExpression(rawPattern, returnType));

            return new GlutenCallExpression(
                    displayName,
                    newLiteralCallFunctionHandle,
                    signature.getReturnType(),
                    newArguments);
        }

        private GlutenCallExpression buildLike(ResolvedFunction function, FunctionCall call)
        {
            BoundSignature signature = function.getSignature();
            String signatureName = signature.getName();
            List<Expression> arguments = call.getArguments();
            if (!"$like".equals(signatureName) || arguments.size() != 2) {
                return null;
            }

            // Make signature name compatible with velox.
            signatureName = "like";

            // Create glutened functionHandle.
            GlutenSignature functionSignature = new GlutenSignature(
                    signatureName,
                    function.getFunctionKind(),
                    signature.getReturnType().getTypeSignature(),
                    signature.getArgumentTypes().stream().map(Type::getTypeSignature).collect(toImmutableList()),
                    false);
            GlutenFunctionHandle functionHandle = new GlutenBuiltInFunctionHandle(functionSignature);

            // Create new arguments.
            List<GlutenRowExpression> newArguments = new ArrayList<>(arguments.size());
            newArguments.add(process(arguments.get(0)));

            Expression second = arguments.get(1);
            if (!(second instanceof FunctionCall literalCall)) {
                return null;
            }

            GlutenRowExpression newLiteralCall = convertLiteralCallFunctionForLike(literalCall);
            if (newLiteralCall == null) {
                return null;
            }
            newArguments.add(newLiteralCall);

            String displayName = function.getFunctionId().toString();
            return new GlutenCallExpression(displayName, functionHandle, signature.getReturnType(), newArguments);
        }

        @Override
        protected GlutenRowExpression visitNullLiteral(NullLiteral node, Void context)
        {
            return constantNull(UnknownType.UNKNOWN);
        }

        @Override
        protected GlutenRowExpression visitGenericLiteral(GenericLiteral node, Void context)
        {
            Type type = stringToType(node.getType());
            if (DATE.equals(type)) {
                return new GlutenConstantExpression(LocalDate.parse(node.getValue()).toEpochDay(), DATE);
            }
            else if (BIGINT.equals(type)) {
                return new GlutenConstantExpression(Long.parseLong(node.getValue()), BIGINT);
            }
            else if (VARCHAR.equals(type)) {
                return new GlutenConstantExpression(Slices.utf8Slice(node.getValue()), VARCHAR);
            }
            else {
                throw new UnsupportedOperationException("Unsupported type in visitGenericLiteral: " + node.getType());
            }
        }

        private Type stringToType(String type)
        {
            if ("DATE".equals(type)) {
                return DATE;
            }
            if ("bigint".equalsIgnoreCase(type)) {
                return BIGINT;
            }
            if ("varchar".equalsIgnoreCase(type)) {
                return VARCHAR;
            }
            throw new UnsupportedOperationException("Unsupported type in stringToType: " + type);
        }

        @Override
        protected GlutenRowExpression visitCast(Cast node, Void context)
        {
            Type returnType = typeManager.getType(toTypeSignature(node.getType()));

            if (node.getExpression() instanceof Literal) {
                GlutenConstantExpression temp = (GlutenConstantExpression) process(node.getExpression());
                return castConstantExpressionIfNeeded(temp, returnType);
            }
            GlutenRowExpression value = process(node.getExpression(), context);

            ResolvedFunction resolvedFunction;
            if (node.isSafe()) {
                resolvedFunction = metadata.getCoercion(session, QualifiedName.of("TRY_CAST"), value.getType(), returnType);
            }
            else {
                resolvedFunction = metadata.getCoercion(session, value.getType(), returnType);
            }

            return buildCallExpression(resolvedFunction, ImmutableList.of(value), true);
        }

        @Override
        protected GlutenRowExpression visitRow(Row node, Void context)
        {
            List<GlutenRowExpression> arguments = node.getItems().stream()
                    .map(value -> process(value, context))
                    .collect(toImmutableList());
            Type returnType = arguments.get(0).getType();
            return new GlutenSpecialFormExpression(ROW_CONSTRUCTOR, returnType, arguments);
        }
    }
}
