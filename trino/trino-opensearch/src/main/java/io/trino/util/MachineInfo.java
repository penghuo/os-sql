/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.trino.util;

/**
 * Replacement for Trino's MachineInfo that does NOT use oshi for hardware detection.
 *
 * <p>The original Trino MachineInfo calls oshi's SystemInfo.getHardware().getProcessor()
 * which reads /proc/self/auxv and /sys/devices/system/cpu/ — both blocked by OpenSearch's
 * Java Agent security policy. This replacement returns Runtime.availableProcessors() instead.
 *
 * <p>This class is placed in the same package as the original (io.trino.util) so the Shadow
 * plugin relocates it to the same shaded location, replacing the original in the shadow jar.
 */
public final class MachineInfo {

  private MachineInfo() {}

  public static int getAvailablePhysicalProcessorCount() {
    return Runtime.getRuntime().availableProcessors();
  }
}
