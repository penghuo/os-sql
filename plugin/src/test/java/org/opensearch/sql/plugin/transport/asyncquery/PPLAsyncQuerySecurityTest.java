/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport.asyncquery;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.opensearch.OpenSearchSecurityException;
import org.opensearch.Version;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.env.Environment;

public class PPLAsyncQuerySecurityTest {

  @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void installedSecurityPluginRequiresAuthenticatedIdentity() throws Exception {
    Environment environment = environmentWithSecurityPlugin(Settings.EMPTY);
    PPLAsyncQuerySecurity security = PPLAsyncQuerySecurity.fromEnvironment(environment);

    assertThrows(
        OpenSearchSecurityException.class,
        () -> security.currentUser(new ThreadContext(Settings.EMPTY)));
  }

  @Test
  public void disabledSecurityPluginAllowsUnsecuredIdentity() throws Exception {
    Settings settings = Settings.builder().put("plugins.security.disabled", true).build();
    Environment environment = environmentWithSecurityPlugin(settings);
    PPLAsyncQuerySecurity security = PPLAsyncQuerySecurity.fromEnvironment(environment);

    assertFalse(security.currentUser(new ThreadContext(Settings.EMPTY)).securityEnabled());
  }

  private Environment environmentWithSecurityPlugin(Settings settings) throws Exception {
    Path plugins = temporaryFolder.newFolder("plugins").toPath();
    Path security = Files.createDirectory(plugins.resolve("opensearch-security"));
    Files.writeString(
        security.resolve("plugin-descriptor.properties"),
        String.join(
            System.lineSeparator(),
            "description=test security plugin",
            "version=1.0.0",
            "name=opensearch-security",
            "classname=org.opensearch.security.OpenSearchSecurityPlugin",
            "java.version=" + Runtime.version().feature(),
            "opensearch.version=" + Version.CURRENT,
            "has.native.controller=false"));
    Environment environment = mock(Environment.class);
    when(environment.pluginsDir()).thenReturn(plugins);
    when(environment.settings()).thenReturn(settings);
    return environment;
  }
}
