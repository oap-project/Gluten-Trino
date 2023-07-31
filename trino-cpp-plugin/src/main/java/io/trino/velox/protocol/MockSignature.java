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
import com.fasterxml.jackson.annotation.JsonValue;
import io.trino.spi.function.FunctionKind;
import io.trino.spi.type.TypeSignature;

import java.util.List;

public class MockSignature
{
    private final FunctionQualifiedName name;
    private final FunctionKind kind;
    private final TypeSignature returnType;
    private final List<TypeSignature> argumentTypes;
    private final boolean variableArity;

    @JsonCreator
    public MockSignature(@JsonProperty("name") String name,
            @JsonProperty("kind") FunctionKind kind,
            @JsonProperty("returnType") TypeSignature returnType,
            @JsonProperty("argumentTypes") List<TypeSignature> argumentTypes,
            @JsonProperty("variableArity") boolean variableArity)
    {
        this.name = new FunctionQualifiedName(name);
        this.kind = kind;
        this.returnType = returnType;
        this.argumentTypes = argumentTypes;
        this.variableArity = variableArity;
    }

    @JsonCreator
    public MockSignature(
            @JsonProperty("name") FunctionQualifiedName name,
            @JsonProperty("kind") FunctionKind kind,
            @JsonProperty("returnType") TypeSignature returnType,
            @JsonProperty("argumentTypes") List<TypeSignature> argumentTypes,
            @JsonProperty("variableArity") boolean variableArity)
    {
        this.name = name;
        this.kind = kind;
        this.returnType = returnType;
        this.argumentTypes = argumentTypes;
        this.variableArity = variableArity;
    }

    @JsonProperty
    public FunctionQualifiedName getName()
    {
        return name;
    }

    @JsonProperty
    public FunctionKind getKind()
    {
        return kind;
    }

    @JsonProperty
    public TypeSignature getReturnType()
    {
        return returnType;
    }

    @JsonProperty
    public List<TypeSignature> getArgumentTypes()
    {
        return argumentTypes;
    }

    @JsonProperty
    public boolean isVariableArity()
    {
        return variableArity;
    }

    public static class FunctionQualifiedName
    {
        // TODO: Rename to Trino
        private static final String DEFAULT_CATALOG = "presto";
        private static final String DEFAULT_SCHEMA = "default";
        private final String catalogName;
        private final String schemaName;
        private final String objectName;

        public FunctionQualifiedName(String objectName)
        {
            this.catalogName = DEFAULT_CATALOG;
            this.schemaName = DEFAULT_SCHEMA;
            this.objectName = objectName;
        }

        public FunctionQualifiedName(String catalogName, String schemaName, String objectName)
        {
            this.catalogName = catalogName;
            this.schemaName = schemaName;
            this.objectName = objectName;
        }

        @Override
        @JsonValue
        public String toString()
        {
            return catalogName + "." + schemaName + "." + objectName;
        }
    }
}
