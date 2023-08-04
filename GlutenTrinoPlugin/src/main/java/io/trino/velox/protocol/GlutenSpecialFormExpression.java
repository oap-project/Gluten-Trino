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
import io.trino.spi.type.Type;

import java.util.List;

public final class GlutenSpecialFormExpression
        extends GlutenRowExpression
{
    private final Form form;
    private final Type returnType;
    private final List<GlutenRowExpression> arguments;

    @JsonCreator
    public GlutenSpecialFormExpression(@JsonProperty("form") Form form,
            @JsonProperty("returnType") Type returnType,
            @JsonProperty("arguments") List<GlutenRowExpression> arguments)
    {
        this.form = form;
        this.returnType = returnType;
        this.arguments = arguments;
    }

    @Override
    @JsonProperty("returnType")
    public Type getType()
    {
        return returnType;
    }

    @JsonProperty
    public Form getForm()
    {
        return form;
    }

    @JsonProperty
    public List<GlutenRowExpression> getArguments()
    {
        return arguments;
    }

    public enum Form
    {
        IF,
        NULL_IF,
        SWITCH,
        WHEN,
        IS_NULL,
        COALESCE,
        IN,
        AND,
        OR,
        DEREFERENCE,
        ROW_CONSTRUCTOR,
        BIND,
    }
}
