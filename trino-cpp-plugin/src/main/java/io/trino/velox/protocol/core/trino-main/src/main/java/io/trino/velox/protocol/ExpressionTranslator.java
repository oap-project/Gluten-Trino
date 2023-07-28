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
import io.trino.spi.type.FixedWidthType;
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
import io.trino.sql.tree.SymbolReference;
import io.trino.sql.tree.WhenClause;
import io.trino.type.TypeCoercion;
import io.trino.type.UnknownType;

import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkNotNull;
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
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.analyzer.TypeSignatureTranslator.toTypeSignature;
import static io.trino.type.LikePatternType.LIKE_PATTERN;
import static io.trino.velox.protocol.MockSpecialFormExpression.Form.IS_NULL;
import static io.trino.velox.protocol.MockSpecialFormExpression.Form.ROW_CONSTRUCTOR;
import static java.util.Locale.ENGLISH;

public final class ExpressionTranslator
{
    private static final String OPERATOR_PREFIX = "$operator$";

    private static final List<String> TRINO_BRIDGE_ADDED_FUNCTION_NAMES = ImmutableList.of("sum");

    private ExpressionTranslator()
    {
    }

    public static MockRowExpression translateExpressionTree(
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

    private static MockSignature.FunctionQualifiedName renameTrinoBridgeFunction(String originName)
    {
        for (String trinoBridgeAddedFunctionName : TRINO_BRIDGE_ADDED_FUNCTION_NAMES) {
            if (originName.toLowerCase(ENGLISH).startsWith(trinoBridgeAddedFunctionName)) {
                return new MockSignature.FunctionQualifiedName("trino", "bridge", trinoBridgeAddedFunctionName);
            }
        }
        return new MockSignature.FunctionQualifiedName(originName);
    }

    // TODO: Refactor function.
    protected static MockCallExpression buildCallExpression(ResolvedFunction function, List<MockRowExpression> arguments, boolean isOperator)
    {
        String displayName = function.getFunctionId().toString();
        BoundSignature signature = function.getSignature();

        // TBD: Mark variableArity as default false
        MockSignature functionSignature = new MockSignature(
                renameTrinoBridgeFunction(signature.getName().toLowerCase(ENGLISH)),
                function.getFunctionKind(),
                signature.getReturnType().getTypeSignature(),
                signature.getArgumentTypes().stream().map(Type::getTypeSignature).collect(toImmutableList()),
                false);
        // TBD: Only support buildin functions currently
        MockFunctionHandle functionHandle = new MockBuiltInFunctionHandle(functionSignature);

        return new MockCallExpression(displayName, functionHandle, signature.getReturnType(), arguments);
    }

    protected static MockCallExpression buildCallExpression(String functionName, FunctionKind kind, Type returnType, List<MockRowExpression> arguments)
    {
        // TBD: Mark variableArity as default false
        MockSignature functionSignature = new MockSignature(
                renameTrinoBridgeFunction(functionName.toLowerCase(ENGLISH)),
                kind,
                returnType.getTypeSignature(),
                arguments.stream().map(mockRowExpression -> mockRowExpression.getType().getTypeSignature()).collect(toImmutableList()),
                false);
        // TBD: Only support buildin functions currently
        MockFunctionHandle functionHandle = new MockBuiltInFunctionHandle(functionSignature);

        return new MockCallExpression(functionName, functionHandle, returnType, arguments);
    }

    private static class Visitor
            extends AstVisitor<MockRowExpression, Void>
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
        protected MockRowExpression visitExpression(Expression node, Void context)
        {
            throw new UnsupportedOperationException("Unsupported Expression type to translate: " + node.getClass().getName());
        }

        private MockRowExpression castIfNeeded(MockRowExpression node, Type toType)
        {
            checkNotNull(toType, "toType is null.");
            checkNotNull(node, "node is null.");
            if (!toType.equals(node.getType())) {
                String displayName = "CAST";

                MockSignature newSignature = new MockSignature(
                        OPERATOR_PREFIX + displayName.toLowerCase(ENGLISH),
                        SCALAR,
                        toType.getTypeSignature(),
                        ImmutableList.of(node.getType().getTypeSignature()),
                        false);
                MockFunctionHandle functionHandle = new MockBuiltInFunctionHandle(newSignature);

                List<MockRowExpression> newArguments = ImmutableList.of(node);
                return new MockCallExpression(displayName, functionHandle, toType, newArguments);
            }
            return node;
        }

        private boolean isConvertible(MockConstantExpression constant, Type toType)
        {
            checkNotNull(constant, "constant is null.");
            checkNotNull(toType, "toType is null.");

            Type fromType = constant.getType();
            // todo: add more types to convert
            if (fromType instanceof FixedWidthType && toType instanceof FixedWidthType) {
                if (fromType.equals(BIGINT) && toType.equals(INTEGER)) {
                    long value = (long) constant.getValue();
                    return ((int) value) == value;
                }
                if (((FixedWidthType) fromType).getFixedSize() < ((FixedWidthType) toType).getFixedSize()) {
                    return true;
                }
            }
            return false;
        }

        private MockRowExpression castConstantExpressionIfNeeded(Type toType, MockRowExpression node)
        {
            checkNotNull(toType, "toType is null.");
            checkNotNull(node, "node is null.");

            if (!(node instanceof MockConstantExpression constant)) {
                // Maybe should throw unsupported exception here.
                return node;
            }

            if (!toType.equals(constant.getType()) && isConvertible(constant, toType)) {
                return new MockConstantExpression(constant.getValue(), toType);
            }
            return constant;
        }

        @Override
        protected MockRowExpression visitInPredicate(InPredicate node, Void context)
        {
            List<MockRowExpression> expressionList = new ArrayList<>();
            MockRowExpression mockValue = process(node.getValue(), context);
            expressionList.add(mockValue);
            // ValueList is a InListExpression, we will not do process here since it should return a const value array
            if (!(node.getValueList() instanceof InListExpression inListNode)) {
                throw new UnsupportedOperationException("Unsupported InPredicate value list type: " + node.getValueList().getClass().getSimpleName());
            }

            Type compareType = mockValue.getType();
            inListNode.getValues().forEach(value -> expressionList.add(castConstantExpressionIfNeeded(compareType, process(value, context))));

            MockSpecialFormExpression.Form form = MockSpecialFormExpression.Form.IN;
            return new MockSpecialFormExpression(form, BOOLEAN, expressionList);
        }

        @Override
        protected MockRowExpression visitBooleanLiteral(BooleanLiteral node, Void context)
        {
            return new MockConstantExpression(node.getValue(), BOOLEAN);
        }

        @Override
        protected MockRowExpression visitDoubleLiteral(DoubleLiteral node, Void context)
        {
            return new MockConstantExpression(node.getValue(), DOUBLE);
        }

        @Override
        protected MockRowExpression visitDecimalLiteral(DecimalLiteral node, Void context)
        {
            return new MockConstantExpression(node.getValue(), DOUBLE);
        }

        @Override
        protected MockRowExpression visitLongLiteral(LongLiteral node, Void context)
        {
            return new MockConstantExpression(node.getValue(), BIGINT);
        }

        @Override
        protected MockRowExpression visitStringLiteral(StringLiteral node, Void context)
        {
            return new MockConstantExpression(utf8Slice(node.getValue()), VarcharType.createVarcharType(node.length()));
        }

        @Override
        protected MockRowExpression visitWhenClause(WhenClause node, Void context)
        {
            MockRowExpression mockOperand = process(node.getOperand(), context);
            MockRowExpression mockResult = process(node.getResult(), context);
            MockSpecialFormExpression.Form form = MockSpecialFormExpression.Form.WHEN;
            return new MockSpecialFormExpression(form, mockResult.getType(), ImmutableList.of(mockOperand, mockResult));
        }

        @Override
        protected MockRowExpression visitCoalesceExpression(CoalesceExpression node, Void context)
        {
            MockSpecialFormExpression.Form form = MockSpecialFormExpression.Form.COALESCE;
            List<MockRowExpression> mockOperands = node.getOperands().stream()
                    .map(operand -> process(operand, context))
                    .collect(toImmutableList());
            return new MockSpecialFormExpression(form, mockOperands.get(0).getType(), mockOperands);
        }

        @Override
        protected MockRowExpression visitSimpleCaseExpression(SimpleCaseExpression node, Void context)
        {
            return buildSwitch(process(node.getOperand(), context), node.getWhenClauses(), node.getDefaultValue(), context);
        }

        @Override
        protected MockRowExpression visitSearchedCaseExpression(SearchedCaseExpression node, Void context)
        {
            return buildSwitch(new MockConstantExpression(true, BOOLEAN), node.getWhenClauses(), node.getDefaultValue(), context);
        }

        private MockRowExpression constantNull(Type type)
        {
            return new MockConstantExpression((Object) null, type);
        }

        private MockRowExpression buildSwitch(MockRowExpression operand, List<WhenClause> whenClauses, Optional<Expression> defaultValue, Void context)
        {
            MockSpecialFormExpression.Form form = MockSpecialFormExpression.Form.SWITCH;

            ImmutableList.Builder<MockRowExpression> arguments = ImmutableList.builder();
            arguments.add(operand);

            Type returnType = null;
            for (WhenClause clause : whenClauses) {
                MockRowExpression mockClause = process(clause, context);
                if (returnType == null) {
                    returnType = mockClause.getType();
                }
                arguments.add(mockClause);
            }

            if (returnType == null) {
                throw new IllegalStateException("Switch Expression must contain at least one When clauses.");
            }

            arguments.add(defaultValue
                    .map((value) -> process(value, context))
                    .orElse(constantNull(returnType)));

            return new MockSpecialFormExpression(form, returnType, arguments.build());
        }

        @Override
        protected MockRowExpression visitLogicalExpression(LogicalExpression node, Void context)
        {
            MockSpecialFormExpression.Form form = switch (node.getOperator()) {
                case AND -> MockSpecialFormExpression.Form.AND;
                case OR -> MockSpecialFormExpression.Form.OR;
            };
            return new MockSpecialFormExpression(form, BOOLEAN, node.getTerms().stream().map(term -> process(term, context)).collect(toImmutableList()));
        }

        private MockRowExpression notExpression(MockRowExpression node)
        {
            return buildCallExpression(
                    metadata.resolveFunction(session, QualifiedName.of("not"), fromTypes(BOOLEAN)),
                    ImmutableList.of(node),
                    false);
        }

        @Override
        protected MockRowExpression visitNotExpression(NotExpression node, Void context)
        {
            MockRowExpression value = process(node.getValue());
            return notExpression(value);
        }

        @Override
        protected MockRowExpression visitIsNotNullPredicate(IsNotNullPredicate node, Void context)
        {
            MockRowExpression expression = process(node.getValue(), context);
            return notExpression(new MockSpecialFormExpression(IS_NULL, BOOLEAN, ImmutableList.of(expression)));
        }

        @Override
        protected MockRowExpression visitIsNullPredicate(IsNullPredicate node, Void context)
        {
            MockRowExpression expression = process(node.getValue(), context);
            return new MockSpecialFormExpression(IS_NULL, BOOLEAN, ImmutableList.of(expression));
        }

        @Override
        protected MockRowExpression visitComparisonExpression(ComparisonExpression node, Void context)
        {
            MockRowExpression left = process(node.getLeft(), context);
            MockRowExpression right = process(node.getRight(), context);

            if (!left.getType().equals(right.getType())) {
                TypeCoercion typeCoercion = new TypeCoercion(typeManager::getType);

                if (typeCoercion.canCoerce(left.getType(), right.getType())) {
                    left = castIfNeeded(left, right.getType());
                }
                else if (typeCoercion.canCoerce(right.getType(), left.getType())) {
                    right = castIfNeeded(right, left.getType());
                }
                else {
                    throw new IllegalStateException("Type of the expressions may not be comparable.");
                }
            }
            ComparisonExpression.Operator op = node.getOperator();

            return resolveComparisonExpression(op, left, right);
        }

        private MockRowExpression resolveComparisonExpression(ComparisonExpression.Operator op, MockRowExpression left, MockRowExpression right)
        {
            ResolvedFunction resolvedFunction;
            ImmutableList<MockRowExpression> argumentList;

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
        protected MockRowExpression visitBetweenPredicate(BetweenPredicate node, Void context)
        {
            MockRowExpression value = process(node.getValue(), context);
            MockRowExpression min = process(node.getMin(), context);
            MockRowExpression max = process(node.getMax(), context);

            if (max.getType().equals(min.getType()) && !value.getType().equals(min.getType())) {
                TypeCoercion typeCoercion = new TypeCoercion(typeManager::getType);

                if (typeCoercion.canCoerce(value.getType(), min.getType())) {
                    value = castIfNeeded(value, min.getType());
                }
                else if (typeCoercion.canCoerce(min.getType(), value.getType())) {
                    min = castIfNeeded(min, value.getType());
                }
                else {
                    throw new IllegalStateException("Type of the expressions may not be comparable.");
                }
            }

            return buildCallExpression(OPERATOR_PREFIX + "between", SCALAR, BOOLEAN, ImmutableList.of(value, min, max));
        }

        @Override
        protected MockRowExpression visitArithmeticBinary(ArithmeticBinaryExpression node, Void context)
        {
            MockRowExpression left = process(node.getLeft(), context);
            MockRowExpression right = process(node.getRight(), context);

            if (!left.getType().equals(right.getType())) {
                TypeCoercion typeCoercion = new TypeCoercion(typeManager::getType);

                if (typeCoercion.canCoerce(left.getType(), right.getType())) {
                    left = castIfNeeded(left, right.getType());
                }
                else if (typeCoercion.canCoerce(right.getType(), left.getType())) {
                    right = castIfNeeded(right, left.getType());
                }
                else {
                    throw new IllegalStateException("Type of the expressions may not be comparable.");
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
        protected MockRowExpression visitSymbolReference(SymbolReference node, Void context)
        {
            Type type = types.get(new Symbol(node.getName()));
            if (type == null) {
                throw new IllegalStateException("Unknown symbol reference: " + node.getName());
            }
            return new MockVariableReferenceExpression(node.getName(), type);
        }

        @Override
        protected MockRowExpression visitFunctionCall(FunctionCall node, Void context)
        {
            ResolvedFunction function = metadata.decodeFunction(node.getName());

            MockRowExpression like = buildLike(function, node);
            if (like != null) {
                return like;
            }

            ImmutableList<MockRowExpression> arguments = node.getArguments().stream()
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

        private MockCallExpression convertLiteralCallFunctionForLike(FunctionCall literalCall)
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
            MockSignature newSignature = new MockSignature(
                    OPERATOR_PREFIX + displayName.toLowerCase(ENGLISH),
                    SCALAR,
                    LIKE_PATTERN.getTypeSignature(),
                    ImmutableList.of(VARCHAR.getTypeSignature()),
                    oldSignature.isVariableArity());
            MockFunctionHandle newLiteralCallFunctionHandle = new MockBuiltInFunctionHandle(newSignature);

            // get new literal arguments
            List<MockRowExpression> newArguments = ImmutableList.of(new MockConstantExpression(rawPattern, returnType));

            return new MockCallExpression(
                    displayName,
                    newLiteralCallFunctionHandle,
                    signature.getReturnType(),
                    newArguments);
        }

        private MockCallExpression buildLike(ResolvedFunction function, FunctionCall call)
        {
            BoundSignature signature = function.getSignature();
            String signatureName = signature.getName();
            List<Expression> arguments = call.getArguments();
            if (!"$like".equals(signatureName) || arguments.size() != 2) {
                return null;
            }

            // Make signature name compatible with velox.
            signatureName = "like";

            // Create mocked functionHandle.
            MockSignature functionSignature = new MockSignature(
                    signatureName,
                    function.getFunctionKind(),
                    signature.getReturnType().getTypeSignature(),
                    signature.getArgumentTypes().stream().map(Type::getTypeSignature).collect(toImmutableList()),
                    false);
            MockFunctionHandle functionHandle = new MockBuiltInFunctionHandle(functionSignature);

            // Create new arguments.
            List<MockRowExpression> newArguments = new ArrayList<>(arguments.size());
            newArguments.add(process(arguments.get(0)));

            Expression second = arguments.get(1);
            if (!(second instanceof FunctionCall literalCall)) {
                return null;
            }

            MockRowExpression newLiteralCall = convertLiteralCallFunctionForLike(literalCall);
            if (newLiteralCall == null) {
                return null;
            }
            newArguments.add(newLiteralCall);

            String displayName = function.getFunctionId().toString();
            return new MockCallExpression(displayName, functionHandle, signature.getReturnType(), newArguments);
        }

        @Override
        protected MockRowExpression visitNullLiteral(NullLiteral node, Void context)
        {
            return constantNull(UnknownType.UNKNOWN);
        }

        @Override
        protected MockRowExpression visitGenericLiteral(GenericLiteral node, Void context)
        {
            Type type = stringToType(node.getType());
            if (DATE.equals(type)) {
                MockRowExpression arg = new MockConstantExpression(Slices.utf8Slice(node.getValue()), VARCHAR);
                return buildCallExpression("date", SCALAR, DATE, ImmutableList.of(arg));
            }
            else if (BIGINT.equals(type)) {
                return new MockConstantExpression(Long.parseLong(node.getValue()), BIGINT);
            }
            else if (VARCHAR.equals(type)) {
                return new MockConstantExpression(Slices.utf8Slice(node.getValue()), VARCHAR);
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
        protected MockRowExpression visitCast(Cast node, Void context)
        {
            Type returnType = typeManager.getType(toTypeSignature(node.getType()));

            if (node.getExpression() instanceof Literal) {
                MockConstantExpression temp = (MockConstantExpression) process(node.getExpression());
                return new MockConstantExpression(temp.getValueBlock(), returnType);
            }
            MockRowExpression value = process(node.getExpression(), context);

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
        protected MockRowExpression visitRow(Row node, Void context)
        {
            List<MockRowExpression> arguments = node.getItems().stream()
                    .map(value -> process(value, context))
                    .collect(toImmutableList());
            Type returnType = arguments.get(0).getType();
            return new MockSpecialFormExpression(ROW_CONSTRUCTOR, returnType, arguments);
        }
    }
}
