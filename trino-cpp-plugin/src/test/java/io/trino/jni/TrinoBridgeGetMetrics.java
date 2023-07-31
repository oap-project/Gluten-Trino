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
package io.trino.jni;

import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.trino.execution.TaskInfo;
import io.trino.execution.TaskStatus;

public class TrinoBridgeGetMetrics
{
    private TrinoBridgeGetMetrics() {}

    static void testTaskStatus(TrinoBridge bridge, long handle)
    {
        String status = bridge.getTaskStatus(handle, "aaa");
        System.out.println(status);

        JsonCodec<TaskStatus> codec = new JsonCodecFactory().jsonCodec(TaskStatus.class);
        TaskStatus taskStatus = codec.fromJson(status);

        System.out.println(taskStatus.toString());
    }

    static void testTaskInfo(TrinoBridge bridge, long handle)
    {
        String info = bridge.getTaskInfo(handle, "aaa");
        System.out.println(info);

        JsonCodec<TaskInfo> codec = new JsonCodecFactory().jsonCodec(TaskInfo.class);
        TaskInfo taskInfo = codec.fromJson(info);

        System.out.println(taskInfo.toString());
    }

    public static void main(String[] args)
    {
        TrinoBridge bridge = new TrinoBridge();
        try {
            String configJson = "{\"maxOutputPageBytes\": 1024, \"maxWorkerThreads\": 1, \"maxDriversPerTask\": 1}";
            long handle = bridge.init(configJson, null);
            long task1 = bridge.createTestTask(handle, "aaa");
            System.out.println(task1 == 0);

            testTaskStatus(bridge, handle);
            testTaskInfo(bridge, handle);

            long task2 = bridge.createTestTask(handle, "aaa");
            System.out.println(task2 == 1);

            bridge.close(handle);
        }
        catch (Error e) {
            e.printStackTrace();
            System.out.println(e.toString());
        }
    }
}
