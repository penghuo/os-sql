/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport.asyncquery;

import java.io.IOException;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.env.Environment;
import org.opensearch.plugins.PluginInfo;
import org.opensearch.plugins.PluginsService;

/** Captures asynchronous query callers using the configured OpenSearch security mode. */
public final class PPLAsyncQuerySecurity {
  private static final String SECURITY_PLUGIN_NAME = "opensearch-security";
  private static final String SECURITY_DISABLED_SETTING = "plugins.security.disabled";

  private final boolean securityEnabled;

  /**
   * Detects whether the OpenSearch Security plugin is installed and enabled.
   *
   * @param environment node environment containing installed plugins and startup settings
   * @return caller identity provider for asynchronous PPL requests
   */
  public static PPLAsyncQuerySecurity fromEnvironment(Environment environment) {
    boolean installed;
    try {
      installed =
          PluginsService.findPluginDirs(environment.pluginsDir()).stream()
              .map(PPLAsyncQuerySecurity::readPlugin)
              .anyMatch(plugin -> SECURITY_PLUGIN_NAME.equals(plugin.getName()));
    } catch (IOException e) {
      throw new IllegalStateException("Unable to inspect installed OpenSearch plugins", e);
    }
    boolean disabled = environment.settings().getAsBoolean(SECURITY_DISABLED_SETTING, false);
    return new PPLAsyncQuerySecurity(installed && !disabled);
  }

  PPLAsyncQuerySecurity(boolean securityEnabled) {
    this.securityEnabled = securityEnabled;
  }

  /**
   * Captures the authenticated caller and fails closed when security is enabled but no identity is
   * present.
   *
   * @param threadContext current request thread context
   * @return immutable caller identity
   */
  public PPLAsyncQueryUser currentUser(ThreadContext threadContext) {
    return PPLAsyncQueryUser.current(threadContext, securityEnabled);
  }

  private static PluginInfo readPlugin(java.nio.file.Path pluginDirectory) {
    try {
      return PluginInfo.readFromProperties(pluginDirectory);
    } catch (IOException e) {
      throw new IllegalStateException(
          "Unable to read OpenSearch plugin metadata from " + pluginDirectory.getFileName(), e);
    }
  }
}
