/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.spark.plugin

import org.opensearch.cluster.metadata.IndexNameExpressionResolver
import org.opensearch.cluster.node.DiscoveryNodes
import org.opensearch.common.settings.{ClusterSettings, IndexScopedSettings, Settings, SettingsFilter}
import org.opensearch.plugins._
import org.opensearch.rest.{RestController, RestHandler}
import org.opensearch.spark.plugin.rest.SubmitAction

import java.util
import java.util.function.Supplier

class SparkPlugin extends Plugin with ActionPlugin {
  override def getRestHandlers(settings: Settings,
                               restController: RestController,
                               clusterSettings: ClusterSettings,
                               indexScopedSettings: IndexScopedSettings,
                               settingsFilter: SettingsFilter,
                               indexNameExpressionResolver: IndexNameExpressionResolver,
                               nodesInCluster: Supplier[ DiscoveryNodes ]): util.List[ RestHandler ] = {
    util.Arrays.asList(new SubmitAction())
  }
}
