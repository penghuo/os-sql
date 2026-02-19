/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.opensearch.sql.ppl.PPLIntegTestCase;

/**
 * This test suite runs all Distributed integration tests without pushdown enabled. It mirrors
 * CalciteNoPushdownIT but with the distributed engine enabled.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
  DistributedAddTotalsCommandIT.class,
  DistributedAddColTotalsCommandIT.class,
  DistributedArrayFunctionIT.class,
  DistributedBinCommandIT.class,
  DistributedConvertTZFunctionIT.class,
  DistributedCsvFormatIT.class,
  DistributedDataTypeIT.class,
  DistributedDateTimeComparisonIT.class,
  DistributedDateTimeFunctionIT.class,
  DistributedDateTimeImplementationIT.class,
  DistributedDedupCommandIT.class,
  DistributedSortCommandIT.class,
  DistributedStatsCommandIT.class,
  DistributedWhereCommandIT.class,
  DistributedSearchCommandIT.class,
  DistributedTopCommandIT.class,
  DistributedRareCommandIT.class,
  DistributedRenameCommandIT.class,
  DistributedParseCommandIT.class,
  DistributedPPLGrokIT.class,
  DistributedPPLIPFunctionIT.class,
  DistributedPPLInSubqueryIT.class,
  DistributedPPLJoinIT.class,
  DistributedPPLJsonBuiltinFunctionIT.class,
  DistributedPPLLookupIT.class,
  DistributedPPLNestedAggregationIT.class,
  DistributedPPLParseIT.class,
  DistributedPPLPatternsIT.class,
  DistributedPPLPluginIT.class,
  DistributedPPLRenameIT.class,
  DistributedPPLScalarSubqueryIT.class,
  DistributedPPLSortIT.class,
  DistributedPPLSpathCommandIT.class,
  DistributedPPLStringBuiltinFunctionIT.class,
  DistributedPPLTrendlineIT.class,
  DistributedQueryAnalysisIT.class,
  DistributedRegexCommandIT.class,
  DistributedRexCommandIT.class,
  DistributedReplaceCommandIT.class,
  DistributedResourceMonitorIT.class,
  DistributedSettingsIT.class,
  DistributedShowDataSourcesCommandIT.class,
  DistributedSimpleQueryStringIT.class,
  DistributedStreamstatsCommandIT.class,
  DistributedSystemFunctionIT.class,
  DistributedTextFunctionIT.class,
  DistributedTimechartCommandIT.class,
  DistributedTimechartPerFunctionIT.class,
  DistributedTransposeCommandIT.class,
  DistributedTrendlineCommandIT.class,
  DistributedVisualizationFormatIT.class,
  DistributedRelevanceFunctionIT.class,
  DistributedQueryStringIT.class,
  DistributedReverseCommandIT.class,
  DistributedPrometheusDataSourceCommandsIT.class,
})
public class DistributedNoPushdownIT {
  private static boolean wasPushdownEnabled;

  @BeforeClass
  public static void disablePushdown() {
    wasPushdownEnabled = PPLIntegTestCase.GlobalPushdownConfig.enabled;
    PPLIntegTestCase.GlobalPushdownConfig.enabled = false;
  }

  @AfterClass
  public static void restorePushdown() {
    PPLIntegTestCase.GlobalPushdownConfig.enabled = wasPushdownEnabled;
  }
}
