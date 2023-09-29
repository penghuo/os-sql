/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.asyncquery.model;

import static org.opensearch.sql.spark.data.constants.SparkConstants.AWS_SNAPSHOT_REPOSITORY;
import static org.opensearch.sql.spark.data.constants.SparkConstants.DEFAULT_CLASS_NAME;
import static org.opensearch.sql.spark.data.constants.SparkConstants.EMR_ASSUME_ROLE_CREDENTIALS_PROVIDER;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_CREDENTIALS_PROVIDER_KEY;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_DEFAULT_AUTH;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_DEFAULT_HOST;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_DEFAULT_PORT;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_DEFAULT_REGION;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_DEFAULT_SCHEME;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_INDEX_STORE_AUTH_KEY;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_INDEX_STORE_AWSREGION_KEY;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_INDEX_STORE_HOST_KEY;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_INDEX_STORE_PORT_KEY;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_INDEX_STORE_SCHEME_KEY;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_SQL_EXTENSION;
import static org.opensearch.sql.spark.data.constants.SparkConstants.JAVA_HOME_LOCATION;
import static org.opensearch.sql.spark.data.constants.SparkConstants.SPARK_DRIVER_ENV_JAVA_HOME_KEY;
import static org.opensearch.sql.spark.data.constants.SparkConstants.SPARK_EXECUTOR_ENV_JAVA_HOME_KEY;
import static org.opensearch.sql.spark.data.constants.SparkConstants.SPARK_JAR_PACKAGES_KEY;
import static org.opensearch.sql.spark.data.constants.SparkConstants.SPARK_JAR_REPOSITORIES_KEY;
import static org.opensearch.sql.spark.data.constants.SparkConstants.SPARK_SQL_EXTENSIONS_KEY;
import static org.opensearch.sql.spark.data.constants.SparkConstants.SPARK_STANDALONE_PACKAGE;

import java.util.LinkedHashMap;
import java.util.Map;

public class CWLSparkSubmitParameters {
  private String className;
  private Map<String, String> config;
  public static final String SPACE = " ";
  public static final String EQUALS = "=";

  public CWLSparkSubmitParameters() {
    this.className = DEFAULT_CLASS_NAME;
    this.config = new LinkedHashMap<>();
    this.config.put(SPARK_JAR_PACKAGES_KEY, SPARK_STANDALONE_PACKAGE);
    this.config.put(SPARK_JAR_REPOSITORIES_KEY, AWS_SNAPSHOT_REPOSITORY);
    this.config.put(SPARK_DRIVER_ENV_JAVA_HOME_KEY, JAVA_HOME_LOCATION);
    this.config.put(SPARK_EXECUTOR_ENV_JAVA_HOME_KEY, JAVA_HOME_LOCATION);
    this.config.put(FLINT_INDEX_STORE_HOST_KEY, FLINT_DEFAULT_HOST);
    this.config.put(FLINT_INDEX_STORE_PORT_KEY, FLINT_DEFAULT_PORT);
    this.config.put(FLINT_INDEX_STORE_SCHEME_KEY, FLINT_DEFAULT_SCHEME);
    this.config.put(FLINT_INDEX_STORE_AUTH_KEY, FLINT_DEFAULT_AUTH);
    this.config.put(FLINT_INDEX_STORE_AWSREGION_KEY, FLINT_DEFAULT_REGION);
    this.config.put(FLINT_CREDENTIALS_PROVIDER_KEY, EMR_ASSUME_ROLE_CREDENTIALS_PROVIDER);
    this.config.put(SPARK_SQL_EXTENSIONS_KEY, FLINT_SQL_EXTENSION);
  }

  public void addParameter(String key, String value) {
    this.config.put(key, value);
  }

  @Override
  public String toString() {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append(" --class ");
    stringBuilder.append(this.className);
    stringBuilder.append(SPACE);
    for (String key : config.keySet()) {
      stringBuilder.append(" --conf ");
      stringBuilder.append(key);
      stringBuilder.append(EQUALS);
      stringBuilder.append(config.get(key));
      stringBuilder.append(SPACE);
    }
    return stringBuilder.toString();
  }
}
