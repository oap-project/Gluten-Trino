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

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.trino.spi.type.Type;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "@type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = GlutenVariableReferenceExpression.class, name = "variable"),
        @JsonSubTypes.Type(value = GlutenCallExpression.class, name = "call"),
        @JsonSubTypes.Type(value = GlutenConstantExpression.class, name = "constant"),
        @JsonSubTypes.Type(value = GlutenSpecialFormExpression.class, name = "special")
})
public abstract class GlutenRowExpression
{
    public abstract Type getType();
}
