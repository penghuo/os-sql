package org.apache.flink.runtime.rpc;

import java.util.HashSet;
import java.util.Set;

public class RpcUtils {

  /**
   * Extracts all {@link RpcGateway} interfaces implemented by the given clazz.
   *
   * @param clazz from which to extract the implemented RpcGateway interfaces
   * @return A set of all implemented RpcGateway interfaces
   */
  public static Set<Class<? extends RpcGateway>> extractImplementedRpcGateways(Class<?> clazz) {
    HashSet<Class<? extends RpcGateway>> interfaces = new HashSet<>();

    while (clazz != null) {
      for (Class<?> interfaze : clazz.getInterfaces()) {
        if (RpcGateway.class.isAssignableFrom(interfaze)) {
          interfaces.add((Class<? extends RpcGateway>) interfaze);
        }
      }

      clazz = clazz.getSuperclass();
    }

    return interfaces;
  }
}
