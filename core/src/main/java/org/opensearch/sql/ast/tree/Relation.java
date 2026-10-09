/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.tree;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.expression.QualifiedName;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.executor.TimeBounds;

/** Logical plan node of Relation, the interface for building the searching sources. */
@ToString
@Getter
@EqualsAndHashCode(callSuper = false)
public class Relation extends UnresolvedPlan {
  private static final String COMMA = ",";

  /**
   * A relation could contain more than one table/index names, such as source=account1, account2
   * source=`account1`,`account2` source=`account*` They translated into union call with fields.
   * Note, this is a list, and {@link #getTableNames} returns a list. For displaying table names,
   * use {@link #getTableQualifiedName}.
   */
  private final List<UnresolvedExpression> tableNames;

  /**
   * Request-level time range the searched expression is narrowed to, or null when the request
   * declared none. Held here rather than written into {@link #tableNames}: the bounds apply to the
   * expression as a whole, and the name that carries them is only assembled in {@link
   * #getTableQualifiedName}.
   */
  @Nullable private final TimeBounds timeBounds;

  public Relation(List<UnresolvedExpression> tableNames) {
    this(tableNames, null);
  }

  public Relation(List<UnresolvedExpression> tableNames, @Nullable TimeBounds timeBounds) {
    this.tableNames = tableNames;
    this.timeBounds = timeBounds;
  }

  public Relation(UnresolvedExpression tableName) {
    this(Collections.singletonList(tableName));
  }

  public List<QualifiedName> getQualifiedNames() {
    return tableNames.stream().map(t -> (QualifiedName) t).collect(Collectors.toList());
  }

  /**
   * Get Qualified name preservs parts of the user given identifiers. This can later be utilized to
   * determine DataSource,Schema and Table Name during Analyzer stage. So Passing QualifiedName
   * directly to Analyzer Stage.
   *
   * <p>Carries {@link #timeBounds} when the request declared any, appended to the joined name so
   * the storage engine decodes it back whole.
   *
   * @return TableQualifiedName.
   */
  public QualifiedName getTableQualifiedName() {
    QualifiedName joined;
    if (tableNames.size() == 1) {
      joined = (QualifiedName) tableNames.get(0);
    } else {
      joined =
          new QualifiedName(
              tableNames.stream()
                  .map(UnresolvedExpression::toString)
                  .collect(Collectors.joining(COMMA)));
    }
    return timeBounds == null ? joined : withTimeBounds(joined);
  }

  /**
   * {@code name} with the bounds appended once, at the end. Encoding each source separately cannot
   * work: the sources are joined with {@link #COMMA}, which is also the separator inside an encoded
   * block, so only the trailing block would decode and every earlier one would be left behind in
   * the index expression.
   */
  private QualifiedName withTimeBounds(QualifiedName name) {
    List<String> parts = new ArrayList<>(name.getParts());
    int last = parts.size() - 1;
    parts.set(last, timeBounds.encodeInto(parts.get(last)));
    return new QualifiedName(parts);
  }

  @Override
  public List<UnresolvedPlan> getChild() {
    return ImmutableList.of();
  }

  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
    return nodeVisitor.visitRelation(this, context);
  }

  @Override
  public UnresolvedPlan attach(UnresolvedPlan child) {
    return this;
  }
}
