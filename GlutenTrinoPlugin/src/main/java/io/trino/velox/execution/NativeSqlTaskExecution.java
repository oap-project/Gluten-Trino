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

import com.google.common.collect.ImmutableSet;
import io.airlift.concurrent.SetThreadName;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.trino.execution.SplitAssignment;
import io.trino.execution.StateMachine;
import io.trino.execution.TaskExecution;
import io.trino.execution.TaskId;
import io.trino.execution.TaskState;
import io.trino.execution.TaskStateMachine;
import io.trino.execution.buffer.BufferState;
import io.trino.execution.buffer.OutputBuffer;
import io.trino.operator.TaskContext;
import io.trino.operator.TaskStats;
import io.trino.spi.TrinoException;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.velox.protocol.GlutenPlanFragment;

import java.lang.ref.WeakReference;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

import static io.trino.execution.TaskState.FINISHED;
import static io.trino.execution.TaskState.RUNNING;
import static io.trino.execution.TaskState.TERMINAL_TASK_STATES;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Objects.requireNonNull;

public class NativeSqlTaskExecution
        implements TaskExecution
{
    private static final Logger logger = Logger.get(NativeSqlTaskExecution.class);

    private final TaskId taskId;
    private final TaskContext taskContext;
    private final NativeSqlTaskExecutionManager taskExecutionManager;
    private final TaskStateMachine taskStateMachine;
    private final OutputBuffer outputBuffer;
    private final long[] partitionSequenceId;
    private final int outputPartitionNum;
    private final AtomicInteger finishedOutputPartitionNum = new AtomicInteger(0);
    private final HashSet<PlanNodeId> noMoreSplitPlanNodes = new HashSet<>();
    private final StateMachine<TaskState> nativeWorkerState;
    private TaskStats nativeFinalStats;

    public NativeSqlTaskExecution(TaskContext taskContext, NativeSqlTaskExecutionManager taskExecutionManager, TaskStateMachine taskStateMachine, OutputBuffer outputBuffer, GlutenPlanFragment planFragment, Executor taskNotificationExecutor)
    {
        this.taskId = taskContext.getTaskId();
        this.taskContext = requireNonNull(taskContext, "taskContext is null.");
        this.taskExecutionManager = requireNonNull(taskExecutionManager, "taskExecutionManager is null.");
        this.taskStateMachine = requireNonNull(taskStateMachine, "taskStateMachine is null.");
        this.outputBuffer = requireNonNull(outputBuffer, "outputBuffer is null.");
        this.outputPartitionNum = 1 + planFragment.getPartitionScheme().getBucketToPartition()
                .map(bucketToPartitions -> Arrays.stream(bucketToPartitions).reduce(Integer::max).orElse(0))
                .orElse(0);
        this.partitionSequenceId = new long[this.outputPartitionNum];
        Arrays.fill(this.partitionSequenceId, 0);
        this.nativeWorkerState = new StateMachine<>("nativeTask" + taskId, taskNotificationExecutor, RUNNING, TERMINAL_TASK_STATES);
        this.nativeFinalStats = null;
    }

    public synchronized TaskStats getTaskStats()
    {
        try (SetThreadName ignored = new SetThreadName("Task-%s", taskId)) {
            TaskStats javaStats = taskContext.getTaskStats();
            TaskStats nativeStats;
            if (nativeWorkerState.get().isDone()) {
                if (nativeFinalStats == null) {
                    nativeFinalStats = taskExecutionManager.removeNativeTask(taskId);
                }
                nativeStats = nativeFinalStats;
            }
            else {
                nativeStats = taskExecutionManager.getTaskStats(taskId);
            }

            return generateTaskStats(javaStats, nativeStats);
        }
        catch (Exception e) {
            logger.error("Exception in getTaskStats, %s", e.getMessage());
            return null;
        }
    }

    private TaskStats generateTaskStats(TaskStats javaStats, TaskStats nativeStats)
    {
        return new TaskStats(
                javaStats.getCreateTime(),
                nativeStats.getFirstStartTime().isEqual(0) ? null : nativeStats.getFirstStartTime(),
                nativeStats.getLastStartTime().isEqual(0) ? null : nativeStats.getLastStartTime(),
                javaStats.getTerminatingStartTime(),
                isNativeTaskDone() ? nativeStats.getLastEndTime() : null,
                javaStats.getEndTime(),
                javaStats.getElapsedTime(),
                javaStats.getQueuedTime(),
                nativeStats.getTotalDrivers(),
                nativeStats.getQueuedDrivers(),
                javaStats.getQueuedPartitionedDrivers(), // TODO
                javaStats.getQueuedPartitionedSplitsWeight(), // TODO
                nativeStats.getRunningDrivers(),
                javaStats.getRunningPartitionedDrivers(), // TODO
                javaStats.getRunningPartitionedSplitsWeight(), // TODO
                nativeStats.getBlockedDrivers(),
                nativeStats.getCompletedDrivers(),
                javaStats.getCumulativeUserMemory(), // TODO
                javaStats.getUserMemoryReservation(), // TODO
                javaStats.getPeakUserMemoryReservation(), // TODO
                javaStats.getRevocableMemoryReservation(), // TODO
                nativeStats.getTotalScheduledTime(),
                nativeStats.getTotalCpuTime(),
                nativeStats.getTotalBlockedTime(),
                javaStats.isFullyBlocked(), // TODO
                nativeStats.getBlockedReasons(),
                javaStats.getPhysicalInputDataSize(), // TODO
                javaStats.getPhysicalInputPositions(), // TODO
                javaStats.getPhysicalInputReadTime(), // TODO
                javaStats.getInternalNetworkInputDataSize(), // TODO
                javaStats.getInternalNetworkInputPositions(), // TODO
                nativeStats.getRawInputDataSize(),
                nativeStats.getRawInputPositions(),
                nativeStats.getProcessedInputDataSize(), // TODO
                nativeStats.getProcessedInputPositions(), // TODO
                javaStats.getInputBlockedTime(), // TODO
                nativeStats.getOutputDataSize(),
                nativeStats.getOutputPositions(),
                javaStats.getOutputBlockedTime(), // TODO
                javaStats.getPhysicalWrittenDataSize(), // TODO
                javaStats.getMaxWriterCount(), // TODO
                javaStats.getFullGcCount(),
                javaStats.getFullGcTime(),
                nativeStats.getPipelines());
    }

    @Override
    public synchronized void start()
    {
        try (SetThreadName ignored = new SetThreadName("Task-%s", taskId)) {
            // Signal immediate termination complete if task termination has started
            if (taskStateMachine.getState().isTerminating()) {
                taskStateMachine.terminationComplete();
            }
            else {
                // Output buffer state change listener callback must not run in the constructor to avoid leaking a reference to "this" across to another thread
                outputBuffer.addStateChangeListener(new NativeSqlTaskExecution.CheckTaskCompletionOnBufferFinish(NativeSqlTaskExecution.this));
            }
        }
    }

    @Override
    public void addSplitAssignments(List<SplitAssignment> splitAssignments)
    {
        try (SetThreadName ignored = new SetThreadName("Task-%s", taskId)) {
            if (taskStateMachine.getState().isTerminatingOrDone() || splitAssignments.isEmpty()) {
                return;
            }
            // TBD: Whether needs to check splits overlap.
            taskExecutionManager.addSplits(taskId, splitAssignments);
            updateNoMoreSplitPlanNodes(splitAssignments);
        }
    }

    private synchronized void updateNoMoreSplitPlanNodes(List<SplitAssignment> splitAssignments)
    {
        splitAssignments.stream()
                .filter(splitAssignment -> splitAssignment.isNoMoreSplits())
                .forEach(splitAssignment -> noMoreSplitPlanNodes.add(splitAssignment.getPlanNodeId()));
    }

    @Override
    public synchronized Set<PlanNodeId> getNoMoreSplits()
    {
        return ImmutableSet.copyOf(noMoreSplitPlanNodes);
    }

    @Override
    public TaskContext getTaskContext()
    {
        return taskContext;
    }

    public void nativeTaskFinished(TaskState state)
    {
        nativeWorkerState.set(state);
        switch (state) {
            case FINISHED -> taskStateMachine.transitionToFlushing();
            case FAILED -> taskStateMachine.failed(new TrinoException(GENERIC_INTERNAL_ERROR, "Native task is failed"));
            case ABORTED -> taskStateMachine.abort();
            case CANCELED -> taskStateMachine.cancel();
        }
        outputBuffer.setNoMorePages();
        logger.info("Native task %s is transferred to %s.", taskId.toString(), state.toString());
    }

    public void noMoreBroadcastBuffers()
    {
        try {
            taskExecutionManager.noMoreBroadcastBuffers(taskId.toString(), partitionSequenceId.length);
        }
        catch (Exception e) {
            logger.warn("Failed to signal to broadcast buffers, reason: " + e.getMessage());
        }
    }

    public synchronized void checkAllOutputConsumed()
    {
        TaskState taskState = taskStateMachine.getState();
        if (taskState.isTerminatingOrDone()) {
            logger.info("Terminating task " + taskId + " with state " + taskState);
            return;
        }

        BufferState bufferState = outputBuffer.getState();
        if (bufferState == BufferState.FINISHED) {
            taskStateMachine.finished();
            return;
        }

        if (bufferState == BufferState.FAILED) {
            Throwable failureCause = outputBuffer.getFailureCause()
                    .orElseGet(() -> new TrinoException(GENERIC_INTERNAL_ERROR, "Output buffer is failed but the failure cause is missing"));
            taskStateMachine.failed(failureCause);
            return;
        }

        // The only terminal state that remains is ABORTED.
        // Buffer is expected to be aborted only if the task itself is aborted. In this scenario the following statement is expected to be noop.
        taskStateMachine.failed(new TrinoException(GENERIC_INTERNAL_ERROR, "Unexpected buffer state: " + bufferState));
        TaskState nativeState = nativeWorkerState.get();
        if (nativeState == FINISHED) {
            taskStateMachine.terminationComplete();
        }
        else {
            logger.error("Can't handle query abort when native task is running currently.");
        }
    }

    public long getPartitionSequenceId(int partitionId)
    {
        return partitionSequenceId[partitionId];
    }

    public int getOutputPartitionNum()
    {
        return outputPartitionNum;
    }

    // Return a flag to indicate whether all native partitions are finished, thread-safe.
    public boolean outputPartitionFinished()
    {
        return finishedOutputPartitionNum.incrementAndGet() == getOutputPartitionNum();
    }

    public boolean allOutputPartitionFinished()
    {
        return finishedOutputPartitionNum.get() == getOutputPartitionNum();
    }

    public boolean isNativeTaskDone()
    {
        return nativeWorkerState.get().isDone();
    }

    public void updateOutput(int partitionId, List<Slice> outputs)
    {
        if (outputs.isEmpty()) {
            return;
        }
        outputBuffer.enqueue(partitionId, outputs);
        long prev = partitionSequenceId[partitionId];
        partitionSequenceId[partitionId] += outputs.size();
        logger.info("Advanced sequenceId of output partition %d from %d to %d.", partitionId, prev, partitionSequenceId[partitionId]);
    }

    private static final class CheckTaskCompletionOnBufferFinish
            implements StateMachine.StateChangeListener<BufferState>
    {
        private final WeakReference<NativeSqlTaskExecution> sqlTaskExecutionReference;

        public CheckTaskCompletionOnBufferFinish(NativeSqlTaskExecution sqlTaskExecution)
        {
            // we are only checking for completion of the task, so don't hold up GC if the task is dead
            this.sqlTaskExecutionReference = new WeakReference<>(sqlTaskExecution);
        }

        @Override
        public void stateChanged(BufferState newState)
        {
            NativeSqlTaskExecution sqlTaskExecution = sqlTaskExecutionReference.get();
            if (newState == BufferState.NO_MORE_BUFFERS) {
                if (sqlTaskExecution != null) {
                    sqlTaskExecution.noMoreBroadcastBuffers();
                }
            }
            if (newState.isTerminal()) {
                if (sqlTaskExecution != null) {
                    sqlTaskExecution.checkAllOutputConsumed();
                }
            }
        }
    }
}
