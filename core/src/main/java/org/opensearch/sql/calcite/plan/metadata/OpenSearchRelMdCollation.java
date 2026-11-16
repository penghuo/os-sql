/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.plan.metadata;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.metadata.BuiltInMetadata;
import org.apache.calcite.rel.metadata.ChainedRelMetadataProvider;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import org.apache.calcite.rel.metadata.JaninoRelMetadataProvider;
import org.apache.calcite.rel.metadata.MetadataDef;
import org.apache.calcite.rel.metadata.MetadataHandler;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMdCollation;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;

/**
 * Collation metadata that reports no collations for an empty {@link Values}.
 *
 * <p>{@link RelMdCollation#collations(Values, RelMetadataQuery)} treats every column suffix of an
 * empty {@code Values} as sorted and returns n collations holding n(n+1)/2 field collations for n
 * columns. It recomputes them on every call, and each {@code Project} above copies them again,
 * which makes planning O(n^3) in the field count of a wide index whenever a filter folds to FALSE.
 * An empty relation needs no ordering guarantees, so this handler answers with no collations and
 * leaves every other relational expression to the default handlers.
 */
public class OpenSearchRelMdCollation implements MetadataHandler<BuiltInMetadata.Collation> {

  /** Provider that serves this handler ahead of Calcite's default metadata handlers. */
  public static final RelMetadataProvider PROVIDER =
      ChainedRelMetadataProvider.of(
          List.of(
              ReflectiveRelMetadataProvider.reflectiveSource(
                  new OpenSearchRelMdCollation(), BuiltInMetadata.Collation.Handler.class),
              DefaultRelMetadataProvider.INSTANCE));

  /**
   * Compiled handlers for {@link #PROVIDER}. Kept as a single instance because Janino caches the
   * generated handlers by provider and {@link ReflectiveRelMetadataProvider} has no value equality.
   */
  private static final JaninoRelMetadataProvider HANDLER_PROVIDER =
      JaninoRelMetadataProvider.of(PROVIDER);

  private OpenSearchRelMdCollation() {}

  @Override
  public MetadataDef<BuiltInMetadata.Collation> getDef() {
    return BuiltInMetadata.Collation.DEF;
  }

  /** Returns no collations for an empty {@link Values}; otherwise defers to Calcite. */
  public ImmutableList<RelCollation> collations(Values values, RelMetadataQuery mq) {
    if (values.getTuples().isEmpty()) {
      return ImmutableList.of();
    }
    return ImmutableList.copyOf(RelMdCollation.values(mq, values.getRowType(), values.getTuples()));
  }

  /**
   * Makes the cluster answer metadata queries with {@link #PROVIDER}.
   *
   * <p>The metadata query supplier is set as well as the provider: Calcite programs such as {@code
   * Programs.standard()} replace the cluster's provider while they run, but a cluster keeps its
   * supplier, so the handler stays in effect for the whole planning of the query.
   */
  public static void install(RelOptCluster cluster) {
    cluster.setMetadataProvider(PROVIDER);
    cluster.setMetadataQuerySupplier(() -> new RelMetadataQuery(HANDLER_PROVIDER));
    cluster.invalidateMetadataQuery();
  }
}
