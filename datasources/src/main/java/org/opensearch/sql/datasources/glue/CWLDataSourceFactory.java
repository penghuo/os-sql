package org.opensearch.sql.datasources.glue;

import static org.opensearch.sql.datasource.model.DataSourceType.CLOUDWATCHLOG;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import org.opensearch.sql.datasource.conf.DataSourceProperty;
import org.opensearch.sql.datasource.model.CatalogDataSource;
import org.opensearch.sql.datasource.model.DataSource;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.datasource.model.DataSourceType;
import org.opensearch.sql.datasources.utils.DatasourceValidationUtils;
import org.opensearch.sql.storage.DataSourceFactory;

public class CWLDataSourceFactory implements DataSourceFactory {
  // CWL configuration properties
  public static final String CWL_REGION = CLOUDWATCHLOG.getText() + ".region";
  public static final String CWL_ACCOUNT_ID = CLOUDWATCHLOG.getText() + ".accountId";
  public static final String CWL_AUTH_TYPE = CLOUDWATCHLOG.getText() + ".auth.type";
  public static final String CWL_AUTH_ROLE_ARN = CLOUDWATCHLOG.getText() + ".auth.role_arn";
  public static final String FLINT_URI = CLOUDWATCHLOG.getText() + ".indexstore.opensearch.uri";
  public static final String FLINT_AUTH = CLOUDWATCHLOG.getText() + ".indexstore.opensearch.auth";
  public static final String FLINT_REGION = CLOUDWATCHLOG.getText() + ".indexstore.opensearch.region";

  public static final DataSourceProperty.PropertyEntry P_CWL_REGION =
      DataSourceProperty.PropertyEntry.builder()
          .dataSourceType(CLOUDWATCHLOG)
          .postfix("region")
          .isMandatory(true).build();

  public static final DataSourceProperty.PropertyEntry P_CWL_ACCOUNT_ID =
      DataSourceProperty.PropertyEntry.builder()
          .dataSourceType(CLOUDWATCHLOG)
          .postfix("accountId")
          .isMandatory(true).build();

  @Override
  public DataSourceType getDataSourceType() {
    return CLOUDWATCHLOG;
  }

  @Override
  public DataSource createDataSource(DataSourceMetadata metadata) {
    DatasourceValidationUtils.validateLengthAndRequiredFields(
        metadata.getProperties(),
        Set.of(CWL_REGION, CWL_ACCOUNT_ID, CWL_AUTH_TYPE, CWL_AUTH_ROLE_ARN, FLINT_URI));
    return new CatalogDataSource(
        metadata.getName(),
        CLOUDWATCHLOG,
        DataSourceProperty.load(metadata.getProperties())
        );
  }
}
