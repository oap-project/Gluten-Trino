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

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.util.ArrayList;
import java.util.List;

public class TrinoBridge
{
    public static native void registerConnector(String catalogProperties);

    // just for test.
    public static void main(String[] args)
    {
        TrinoBridge bridge = new TrinoBridge();
        try {
            String configJson = "{\"maxOutputPageBytes\": 1024, \"maxWorkerThreads\": 1, \"maxDriversPerTask\": 1}";
            long handle = bridge.init(configJson, null);
            long task1 = bridge.createTestTask(handle, "aaa");
            System.out.println(task1 == 0);

            String status = bridge.getTaskStatus(handle, "aaa");
            System.out.println(status);

            Thread.sleep(1000);
            List<Slice> slices = bridge.getBuffer(handle, "aaa", 0);

            System.out.println("slices num " + slices.size());
            long task2 = bridge.createTestTask(handle, "aaa");
            System.out.println(task2 == 1);

            bridge.close(handle);
        }
        catch (Error e) {
            e.printStackTrace();
            System.out.println(e.toString());
        }
        catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    // Declare the native method
    public native long init(String configJson, Object manager);

    public native void close(long handlePtr);

    public native void getBuffer(String id, long[] address, int[] length);

    public native long createTestTask(long handlePtr, String taskId);

    public native long createTask(long handlePtr, String taskId, String planFragment);

    public native void failedTask(long handlePtr, String taskId, String failedReason);

    public native void removeTask(long handlePtr, String taskId);

    public native void addSplits(long handlePtr, String taskId, String splitsInfo);

    public native void noMoreBuffers(long handlePtr, String taskId, int numPartitions);

    public native void registerOutputPartitionListener(long handlePtr, String taskId, int partitionId, long sequence, long maxBytes);

    public native int getBufferStep1(long handlePtr, String taskId, int partitionId);

    public native void getBufferStep2(long handlePtr, String taskId, int partitionId, int sliceNum, int[] lengths);

    public native void getBufferStep3(long handlePtr, String taskId, int partitionId, int sliceNum, long[] addresses);

    public List<Slice> getBuffer(long handlePtr, String taskId, int partitionId)
    {
        int sliceNum = getBufferStep1(handlePtr, taskId, partitionId);
        if (sliceNum == 0) {
            return new ArrayList<Slice>();
        }
        int[] lengthArray = new int[sliceNum + 1];
        getBufferStep2(handlePtr, taskId, partitionId, sliceNum, lengthArray);
        List<Slice> slices = new ArrayList<>();
        long[] addressArray = new long[sliceNum];
        for (int i = 0; i < sliceNum; i++) {
            Slice slice = Slices.allocateDirect(lengthArray[i]);
            addressArray[i] = slice.getAddress();
            slices.add(slice);
        }
        getBufferStep3(handlePtr, taskId, partitionId, sliceNum, addressArray);
        return slices;
    }

    public native String getTaskStatus(long handlePtr, String taskId);

    public native String getTaskInfo(long handlePtr, String taskId);

    static {
        System.loadLibrary("trino_bridge");
    }
}
