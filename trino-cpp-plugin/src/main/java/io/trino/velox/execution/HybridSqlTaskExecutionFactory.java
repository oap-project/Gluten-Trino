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

import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.event.SplitMonitor;
import io.trino.execution.SqlTaskExecutionFactory;
import io.trino.execution.TaskExecution;
import io.trino.execution.TaskManagerConfig;
import io.trino.execution.TaskStateMachine;
import io.trino.execution.buffer.OutputBuffer;
import io.trino.execution.executor.TaskExecutor;
import io.trino.memory.QueryContext;
import io.trino.metadata.Metadata;
import io.trino.operator.TaskContext;
import io.trino.spi.block.BlockEncodingSerde;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.sql.planner.LocalExecutionPlanner;
import io.trino.sql.planner.PlanFragment;
import io.trino.sql.planner.Symbol;
import io.trino.velox.protocol.MockPlanFragment;
import io.trino.velox.protocol.PlanFragmentTranslator;

import java.util.Map;
import java.util.concurrent.Executor;

import static java.util.Objects.requireNonNull;

public class HybridSqlTaskExecutionFactory
        extends SqlTaskExecutionFactory
{
    private static final Logger logger = Logger.get(HybridSqlTaskExecutionFactory.class);

    private final NativeSqlTaskExecutionManager nativeSqlTaskExecutionManager;
    private final Metadata metadata;
    private final TypeManager typeManager;
    private final BlockEncodingSerde blockEncodingSerde;

    private final Executor taskNotificationExecutor;
    private final boolean perOperatorCpuTimerEnabled;
    private final boolean cpuTimerEnabled;

    public HybridSqlTaskExecutionFactory(
            Executor taskNotificationExecutor,
            TaskExecutor taskExecutor,
            LocalExecutionPlanner planner,
            SplitMonitor splitMonitor,
            TaskManagerConfig config,
            NativeSqlTaskExecutionManager nativeSqlTaskExecutionManager)
    {
        super(taskNotificationExecutor, taskExecutor, planner, splitMonitor, config);

        this.nativeSqlTaskExecutionManager = requireNonNull(nativeSqlTaskExecutionManager, "nativeSqlTaskExecutionManager is null.");
        this.metadata = requireNonNull(nativeSqlTaskExecutionManager.getMetadata(), "metadata is null.");
        this.typeManager = requireNonNull(nativeSqlTaskExecutionManager.getTypeManager(), "typeManager is null.");
        this.blockEncodingSerde = requireNonNull(nativeSqlTaskExecutionManager.getBlockEncodingSerde(), "blockEncodingSerde is null.");
        this.taskNotificationExecutor = requireNonNull(taskNotificationExecutor, "taskNotificationExecutor is null.");
        this.perOperatorCpuTimerEnabled = config.isPerOperatorCpuTimerEnabled();
        this.cpuTimerEnabled = config.isTaskCpuTimerEnabled();
    }

    @Override
    public TaskExecution create(Session session, QueryContext queryContext, TaskStateMachine taskStateMachine, OutputBuffer outputBuffer, PlanFragment fragment, Runnable notifyStatusChanged)
    {
        MockPlanFragment newFragment = translatePlanFragment(fragment, session);
        if (newFragment != null) {
            try {
                return createNativeSqlTaskExecution(session, queryContext, taskStateMachine, outputBuffer, newFragment, notifyStatusChanged);
            }
            catch (RuntimeException e) {
                logger.error(e, "Create native task failed, fallback. Reason=%s", e.getMessage());
            }
        }
        return super.create(session, queryContext, taskStateMachine, outputBuffer, fragment, notifyStatusChanged);
    }

    private TaskExecution createNativeSqlTaskExecution(Session session, QueryContext queryContext, TaskStateMachine taskStateMachine, OutputBuffer outputBuffer, MockPlanFragment newFragment, Runnable notifyStatusChanged)
    {
        TaskContext taskContext = queryContext.addTaskContext(
                taskStateMachine,
                session,
                notifyStatusChanged,
                perOperatorCpuTimerEnabled,
                cpuTimerEnabled);
        NativeSqlTaskExecution taskExecution = new NativeSqlTaskExecution(taskContext, nativeSqlTaskExecutionManager, taskStateMachine, outputBuffer, newFragment, taskNotificationExecutor);
        try {
            nativeSqlTaskExecutionManager.registerTask(taskExecution, taskStateMachine.getTaskId(), newFragment);
        }
        catch (Exception e) {
            taskContext.failed(e);
            throw e;
        }

        return taskExecution;
    }

    private MockPlanFragment translatePlanFragment(PlanFragment fragment, Session session)
    {
        logger.debug("Translating plan fragment %s.", fragment.getId().toString());
        // Try to convert PlanFragment to MockPlanNodes.
        try {
            Map<Symbol, Type> symbolTypes = fragment.getSymbols();
            PlanFragmentTranslator translator = new PlanFragmentTranslator(metadata, typeManager, blockEncodingSerde, session, symbolTypes);
            return translator.translatePlanFragment(fragment);
        }
        catch (UnsupportedOperationException exception) {
            if (logger.isDebugEnabled()) {
                logger.warn(exception, "Native plan translation failed due to unsupported operations: %s", exception.getMessage());
            }
            else {
                logger.warn("Native plan translation failed due to unsupported operations: %s", exception.getMessage());
            }
        }
        catch (Exception exception) {
            logger.error(exception, "Native plan translation failed due to error: %s", exception.getMessage());
        }
        return null;
    }
}
