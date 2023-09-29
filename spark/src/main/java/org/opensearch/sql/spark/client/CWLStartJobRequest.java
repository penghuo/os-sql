/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.client;

import static org.opensearch.sql.spark.data.constants.SparkConstants.DRIVER_ENV_ASSUME_ROLE_ARN_KEY;
import static org.opensearch.sql.spark.data.constants.SparkConstants.EXECUTOR_ENV_ASSUME_ROLE_ARN_KEY;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_INDEX_STORE_AUTH_KEY;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_INDEX_STORE_AWSREGION_KEY;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_INDEX_STORE_HOST_KEY;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_INDEX_STORE_PORT_KEY;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_INDEX_STORE_SCHEME_KEY;

import java.net.URI;
import java.net.URISyntaxException;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.spark.asyncquery.model.CWLSparkSubmitParameters;

public class CWLStartJobRequest {

  private final DataSourceMetadata dataSourceMetadata;

  public CWLStartJobRequest(DataSourceMetadata dataSourceMetadata) {
    this.dataSourceMetadata = dataSourceMetadata;
  }

  public String sparkParameters() {
    CWLSparkSubmitParameters sparkSubmitParameters = new CWLSparkSubmitParameters();
    sparkSubmitParameters.addParameter(DRIVER_ENV_ASSUME_ROLE_ARN_KEY, getDataSourceRoleARN());
    sparkSubmitParameters.addParameter(EXECUTOR_ENV_ASSUME_ROLE_ARN_KEY, getDataSourceRoleARN());

    // OpenSearch Configuration
    String opensearchuri = getOpenSearchUri();
    URI uri;
    try {
      uri = new URI(opensearchuri);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(
          String.format("Bad URI in indexstore configuration of the : %s datasoure.",
              dataSourceMetadata));
    }
    sparkSubmitParameters.addParameter(FLINT_INDEX_STORE_HOST_KEY, uri.getHost());
    sparkSubmitParameters.addParameter(
        FLINT_INDEX_STORE_PORT_KEY, String.valueOf(uri.getPort()));
    sparkSubmitParameters.addParameter(FLINT_INDEX_STORE_SCHEME_KEY, uri.getScheme());
    sparkSubmitParameters.addParameter(FLINT_INDEX_STORE_AUTH_KEY,
        dataSourceMetadata.getProperties().get("cloudwatchlog.indexstore.opensearch.auth"));
    sparkSubmitParameters.addParameter(FLINT_INDEX_STORE_AWSREGION_KEY,
        dataSourceMetadata.getProperties().get("cloudwatchlog.indexstore.opensearch.region"));


    // CWL
    sparkSubmitParameters.addParameter(
        "spark.sql.catalog." + dataSourceMetadata.getName(), "com.amazon.awslogscatalog.LogsCatalog");
    sparkSubmitParameters.addParameter(
        "spark.sql.catalog.accountId", dataSourceMetadata.getProperties().get("cloudwatchlog.accountId"));
    sparkSubmitParameters.addParameter(
        "spark.sql.catalog.accessRole", getDataSourceRoleARN());
    sparkSubmitParameters.addParameter(
        "spark.sql.catalog.region", dataSourceMetadata.getProperties().get("cloudwatchlog.region"));

    return sparkSubmitParameters.toString();
  }

  private String getDataSourceRoleARN() {
    return dataSourceMetadata.getProperties().get("cloudwatchlog.auth.role_arn");
  }

  private String getOpenSearchUri() {
    return dataSourceMetadata.getProperties().get("cloudwatchlog.indexstore.opensearch.uri");
  }
}
