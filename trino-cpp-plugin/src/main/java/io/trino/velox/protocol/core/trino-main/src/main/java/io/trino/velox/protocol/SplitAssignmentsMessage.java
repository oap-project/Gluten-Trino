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
import io.trino.execution.SplitAssignment;
import io.trino.execution.TaskId;

import java.util.ArrayList;
import java.util.List;

public class SplitAssignmentsMessage
{
    private final TaskId taskId;
    private final List<MockSplitAssignment> splitAssignments;

    @JsonCreator
    public SplitAssignmentsMessage(
            @JsonProperty("taskId") TaskId taskId,
            @JsonProperty("splitAssignments") List<MockSplitAssignment> splitAssignments)
    {
        this.taskId = taskId;
        this.splitAssignments = splitAssignments;
    }

    @JsonProperty
    public TaskId getTaskId()
    {
        return taskId;
    }

    @JsonProperty
    public List<MockSplitAssignment> getSplitAssignments()
    {
        return splitAssignments;
    }

    public static SplitAssignmentsMessage create(TaskId taskId, List<SplitAssignment> splitAssignments)
    {
        List<MockSplitAssignment> mockSplitAssignments = new ArrayList<>(splitAssignments.size());
        for (SplitAssignment splitAssignment : splitAssignments) {
            mockSplitAssignments.add(MockSplitAssignment.create(splitAssignment));
        }
        return new SplitAssignmentsMessage(taskId, mockSplitAssignments);
    }
}
