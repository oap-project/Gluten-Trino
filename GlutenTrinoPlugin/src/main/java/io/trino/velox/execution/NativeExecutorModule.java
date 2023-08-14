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
package io.trino.velox.execution;

import com.google.inject.Binder;
import com.google.inject.Scopes;
import com.google.inject.util.Providers;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.json.JsonBinder;
import io.airlift.json.JsonCodecBinder;
import io.trino.execution.TaskStatus;
import io.trino.operator.TaskStats;
import io.trino.server.ServerConfig;
import io.trino.sql.planner.PlanFragment;
import io.trino.velox.NativeConfigs;
import io.trino.velox.NativeTaskConfig;
import io.trino.velox.protocol.GlutenPlanFragment;
import io.trino.velox.protocol.GlutenVariableExpressionSerializer;
import io.trino.velox.protocol.GlutenVariableReferenceExpression;
import io.trino.velox.protocol.SplitAssignmentsMessage;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;

public class NativeExecutorModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        ServerConfig serverConfig = buildConfigObject(ServerConfig.class);

        // Always bind config in case of 'Configuration was not used.'
        configBinder(binder).bindConfig(NativeTaskConfig.class);
        binder.bind(NativeConfigs.class).in(Scopes.SINGLETON);

        if (serverConfig.getLaunchNativeEngine()) {
            binder.bind(NativeSqlTaskExecutionManager.class).in(Scopes.SINGLETON);
            jsonCodecBinder(binder).bindJsonCodec(GlutenPlanFragment.class);
        }
        else {
            binder.bind(NativeSqlTaskExecutionManager.class).toProvider(Providers.of(null));
        }

        // Specialized JSON class
        JsonBinder.jsonBinder(binder).addKeySerializerBinding(GlutenVariableReferenceExpression.class).to(GlutenVariableExpressionSerializer.class);
        JsonCodecBinder.jsonCodecBinder(binder).bindJsonCodec(PlanFragment.class);
        JsonCodecBinder.jsonCodecBinder(binder).bindJsonCodec(SplitAssignmentsMessage.class);
        JsonCodecBinder.jsonCodecBinder(binder).bindJsonCodec(TaskStatus.class);
        JsonCodecBinder.jsonCodecBinder(binder).bindJsonCodec(NativeConfigs.class);
        JsonCodecBinder.jsonCodecBinder(binder).bindJsonCodec(TaskStats.class);
    }
}
