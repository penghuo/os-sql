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
package io.trino.util;

import com.google.common.base.StandardSystemProperty;
import oshi.SystemInfo;

import static java.lang.Math.min;

public final class MachineInfo
{
    // cache physical processor count, so that it's not queried multiple times during tests
    private static volatile int physicalProcessorCount = -1;

    private MachineInfo() {}

    public static int getAvailablePhysicalProcessorCount()
    {
        if (physicalProcessorCount != -1) {
            return physicalProcessorCount;
        }

        // In OpenSearch plugin context, oshi's JNA native calls are blocked by the
        // security policy. Use Runtime.availableProcessors() as a safe fallback.
        physicalProcessorCount = Runtime.getRuntime().availableProcessors();
        return physicalProcessorCount;
    }
}
