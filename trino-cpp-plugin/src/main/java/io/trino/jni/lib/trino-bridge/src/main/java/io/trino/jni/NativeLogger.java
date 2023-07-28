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

import io.airlift.log.Logger;

// This class used by Trino Native library for writing log in Trino format.
@SuppressWarnings("unused")
public class NativeLogger
{
    private static final Logger log = Logger.get(NativeLogger.class);

    private NativeLogger()
    {
    }

    public static void logDebug(String message)
    {
        log.debug(message);
    }

    public static void logInfo(String message)
    {
        log.info(message);
    }

    public static void logWarning(String message)
    {
        log.warn(message);
    }

    public static void logError(String message)
    {
        log.error(message);
    }
}
