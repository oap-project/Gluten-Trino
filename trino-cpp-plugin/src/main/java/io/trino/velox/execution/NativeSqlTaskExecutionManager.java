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

import io.airlift.concurrent.SetThreadName;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.trino.execution.SplitAssignment;
import io.trino.execution.TaskId;
import io.trino.execution.TaskStatus;
import io.trino.jni.TrinoBridge;
import io.trino.metadata.Metadata;
import io.trino.spi.block.BlockEncodingSerde;
import io.trino.spi.type.TypeManager;
import io.trino.velox.NativeConfigs;
import io.trino.velox.protocol.MockPlanFragment;
import io.trino.velox.protocol.SplitAssignmentsMessage;

import javax.inject.Inject;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class NativeSqlTaskExecutionManager
        implements Closeable
{
    private static final Logger logger = Logger.get(NativeSqlTaskExecutionManager.class);
    private static final long maxOutputPageFetchBytes = 32 * 1024 * 1024; // Max bytes of pages fetched from native each time.
    private static final int defaultExecutorThreadNum = 8; // Number of threads to fetch results and update status.

    private final Metadata metadata;
    private final TypeManager typeManager;
    private final BlockEncodingSerde blockEncodingSerde;
    private final JsonCodec<MockPlanFragment> mockPlanFragmentJsonCodec;
    private final JsonCodec<SplitAssignmentsMessage> splitAssignmentsMessageJsonCodec;
    private final JsonCodec<TaskStatus> taskStatusJsonCodec;
    private final TrinoBridge trinoBridge = new TrinoBridge();
    private final long nativeHandler;
    private final ConcurrentHashMap<TaskId, NativeSqlTaskExecution> tasks = new ConcurrentHashMap<>();
    private final Executor executor;

    @Inject
    public NativeSqlTaskExecutionManager(
            Metadata metadata,
            TypeManager typeManager,
            BlockEncodingSerde blockEncodingSerde,
            JsonCodec<MockPlanFragment> mockPlanFragmentJsonCodec,
            JsonCodec<SplitAssignmentsMessage> splitAssignmentsMessageJsonCodec,
            JsonCodec<TaskStatus> taskStatusJsonCodec,
            JsonCodec<NativeConfigs> nativeConfigsJsonCodec,
            NativeConfigs config)
    {
        System.out.println(ProcessHandle.current().pid());
        this.metadata = metadata;
        this.typeManager = typeManager;
        this.blockEncodingSerde = blockEncodingSerde;
        this.mockPlanFragmentJsonCodec = mockPlanFragmentJsonCodec;
        this.splitAssignmentsMessageJsonCodec = splitAssignmentsMessageJsonCodec;
        this.taskStatusJsonCodec = taskStatusJsonCodec;
        this.executor = Executors.newFixedThreadPool(defaultExecutorThreadNum);

        String configJson = nativeConfigsJsonCodec.toJson(config);
        nativeHandler = trinoBridge.init(configJson, this);
        if (nativeHandler == 0) {
            throw new RuntimeException("trinoBridge initialization failed.");
        }
    }

    @Override
    public void close()
            throws IOException
    {
        trinoBridge.close(nativeHandler);
    }

    public void noMoreBroadcastBuffers(String taskId, int numPartitions)
    {
        logger.debug("No more buffers for task %s, partition %d.", taskId, numPartitions);
        trinoBridge.noMoreBuffers(nativeHandler, taskId, numPartitions);
    }

    public void registerTask(NativeSqlTaskExecution taskExecution, TaskId id, MockPlanFragment planFragment)
    {
        String jsonString = mockPlanFragmentJsonCodec.toJson(planFragment);
        if (tasks.containsKey(id)) {
            logger.debug("Task %s has already been registered.", id);
            return;
        }
        try {
            trinoBridge.createTask(nativeHandler, id.toString(), jsonString);
        }
        catch (Exception e) {
            logger.error("Task %s register failed, reason: %s", e.getMessage());
        }
        tasks.put(id, taskExecution);

        // Register output partition listeners.
        for (int partitionId = 0; partitionId < taskExecution.getOutputPartitionNum(); ++partitionId) {
            trinoBridge.registerOutputPartitionListener(nativeHandler, id.toString(), partitionId, taskExecution.getPartitionSequenceId(partitionId), maxOutputPageFetchBytes);
        }
    }

    public void addSplits(TaskId id, List<SplitAssignment> splitAssignments)
    {
        SplitAssignmentsMessage message = SplitAssignmentsMessage.create(id, splitAssignments);
        String jsonString = splitAssignmentsMessageJsonCodec.toJson(message);
        trinoBridge.addSplits(nativeHandler, id.toString(), jsonString);
        splitAssignments.forEach(splitAssignment -> {
            logger.info("Native task %s added %d splits to PlanNode %s.", id, splitAssignment.getSplits().size(), splitAssignment.getPlanNodeId());
            if (splitAssignment.isNoMoreSplits()) {
                logger.info("No more splits in native task %s, PlanNode %s.", id, splitAssignment.getPlanNodeId().toString());
            }
        });
    }

    public Metadata getMetadata()
    {
        return metadata;
    }

    public TypeManager getTypeManager()
    {
        return typeManager;
    }

    public BlockEncodingSerde getBlockEncodingSerde()
    {
        return blockEncodingSerde;
    }

    private void removeNativeTask(TaskId taskId)
    {
        tasks.remove(taskId);
        trinoBridge.removeTask(nativeHandler, taskId.toString());
        logger.info("Native task %s is removed.", taskId);
    }

    public void fetchOutputFromNative(String id, int partitionId)
    {
        executor.execute(() -> {
            TaskId taskId = TaskId.valueOf(id);
            NativeSqlTaskExecution taskExecution = tasks.getOrDefault(taskId, null);
            if (taskExecution != null) {
                try (SetThreadName ignored = new SetThreadName("Native-Task-%s-partition-%d", taskId, partitionId)) {
                    // Fetching output results.
                    logger.debug("Get buffer for task %s, partition %d.", taskId.toString(), partitionId);

                    List<Slice> output = trinoBridge.getBuffer(nativeHandler, taskId.toString(), partitionId);
                    if (!output.isEmpty()) {
                        taskExecution.updateOutput(partitionId, output);
                        // Register next listener.
                        trinoBridge.registerOutputPartitionListener(nativeHandler,
                                taskId.toString(),
                                partitionId,
                                taskExecution.getPartitionSequenceId(partitionId),
                                maxOutputPageFetchBytes);
                    }
                    else {
                        // Only partition-finished cases can enter this branch.
                        if (taskExecution.outputPartitionFinished() && taskExecution.isNativeTaskDone()) {
                            removeNativeTask(taskId);
                        }
                        logger.info("Native Task %s, Partition %d is finished.", taskId, partitionId);
                    }
                }
                catch (Exception e) {
                    trinoBridge.failedTask(nativeHandler, taskId.toString(), e.toString());
                    logger.error(e, "Exception in fetchOutputFromNative: %s", e.toString());
                }
            }
            else {
                logger.warn("Task %s is not existing.", taskId.toString());
            }
        });
    }

    public void updateNativeTaskStatus(String id)
    {
        executor.execute(() -> {
            TaskId taskId = TaskId.valueOf(id);
            NativeSqlTaskExecution taskExecution = tasks.getOrDefault(taskId, null);
            if (taskExecution != null) {
                try (SetThreadName ignored = new SetThreadName("Native-Task-%s-partition-getStatus", taskId)) {
                    logger.debug("Update task status for task %s.", taskId.toString());
                    String taskState = trinoBridge.getTaskStatus(nativeHandler, taskId.toString());
                    TaskStatus status = taskStatusJsonCodec.fromJson(taskState);
                    taskExecution.nativeTaskFinished(status.getState());

                    if (taskExecution.isNativeTaskDone() && taskExecution.allOutputPartitionFinished()) {
                        removeNativeTask(taskId);
                    }
                }
                catch (Exception e) {
                    logger.error("Exception in updateNativeTaskStatus: %s", e.toString());
                }
            }
            else {
                logger.warn("Task %s is not existing.", taskId.toString());
            }
        });
    }
}
