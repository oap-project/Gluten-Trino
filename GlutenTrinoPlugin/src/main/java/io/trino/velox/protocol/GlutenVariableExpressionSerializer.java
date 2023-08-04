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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;

import static java.lang.String.format;

public class GlutenVariableExpressionSerializer
        extends JsonSerializer<GlutenVariableReferenceExpression>
{
    private static final char VARIABLE_TYPE_OPEN_BRACKET = '<';
    private static final char VARIABLE_TYPE_CLOSE_BRACKET = '>';

    @Override
    public void serialize(GlutenVariableReferenceExpression value, JsonGenerator gen, SerializerProvider serializers)
            throws IOException
    {
        gen.writeFieldName(format("%s%s%s%s", value.getName(), VARIABLE_TYPE_OPEN_BRACKET, value.getType().getTypeSignature(), VARIABLE_TYPE_CLOSE_BRACKET));
    }
}
