/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.operator.stream;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.net.InetAddresses;
import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.logging.log4j.core.Core;
import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.util.BytesRef;
import org.opensearch.search.internal.ContextIndexSearcher;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.planner.physical.PhysicalPlanNodeVisitor;

@EqualsAndHashCode
@ToString
public class SearchStream extends PhysicalPlan {
  private final List<ReferenceExpression> expressionList;
  private ContextIndexSearcher searcher;

  private Iterator<ExprValue> exprValueIterator;

  public SearchStream(List<ReferenceExpression> expressionList) {
    this.expressionList = expressionList;
  }

  @Override
  public <R, C> R accept(PhysicalPlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitSearchStream(this, context);
  }

  public void setContext(Object o) {
    this.searcher = (ContextIndexSearcher) o;
  }

  @Override
  public void open() {

    try {
      List<Integer> docIds = new ArrayList<>();
      searcher.search(
          new MatchAllDocsQuery(),
          new Collector() {
            @Override
            public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
              return new LeafCollector() {
                @Override
                public void setScorer(Scorable scorer) throws IOException {}

                @Override
                public void collect(int doc) throws IOException {
                  docIds.add(doc);
                }
              };
            }

            @Override
            public ScoreMode scoreMode() {
              return ScoreMode.COMPLETE_NO_SCORES;
            }
          });

      List<DocIdSetIterator> docValueIterators = new ArrayList<>();
      LeafReader leafReader = searcher.getIndexReader().leaves().get(0).reader();
      for (ReferenceExpression expr : expressionList) {
        if (expr.type() == ExprCoreType.STRING || expr.type() == OpenSearchDataType.OPENSEARCH_IP) {
          docValueIterators.add(DocValues.unwrapSingleton(DocValues.getSortedSet(leafReader, expr.path())));
        }
        else {
          docValueIterators.add(DocValues.unwrapSingleton(DocValues.getSortedNumeric(leafReader, expr.path())));
        }
      }

      List<ExprValue> tuples = new ArrayList<>();
      for (Integer docId : docIds) {
        ImmutableMap.Builder<String, Object> mapBuilder = new ImmutableMap.Builder<>();
        try {
          for(int i = 0; i < expressionList.size(); i++) {
            final DocIdSetIterator iterator = docValueIterators.get(i);
            docValueIterators.get(i).advance(docId);
              if (expressionList.get(i).type() == ExprCoreType.STRING || expressionList.get(i).type() == OpenSearchDataType.OPENSEARCH_TEXT_KEYWORD) {
                SortedDocValues stringIterator = (SortedDocValues) iterator;
                BytesRef bytesRef = stringIterator.lookupOrd(stringIterator.ordValue());
                mapBuilder.put(expressionList.get(i).path(), bytesRef.utf8ToString());
              } else if (expressionList.get(i).type() == OpenSearchDataType.OPENSEARCH_IP) {
                SortedDocValues stringIterator = (SortedDocValues) iterator;
                BytesRef bytesRef = stringIterator.lookupOrd(stringIterator.ordValue());
                mapBuilder.put(expressionList.get(i).path(), parseIP(bytesRef));
              } else {
                mapBuilder.put(expressionList.get(i).path(), ((NumericDocValues) iterator).longValue());
              }
          }
        } catch (Exception e) {
          continue;
        }
        tuples.add(ExprValueUtils.tupleValue(mapBuilder.build()));
      }

      exprValueIterator = tuples.listIterator();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() {

  }

  @Override
  public boolean hasNext() {
    return exprValueIterator.hasNext();
  }

  @Override
  public ExprValue next() {
    return exprValueIterator.next();
  }

  @Override
  public List<PhysicalPlan> getChild() {
    return ImmutableList.of();
  }

  private String parseIP(BytesRef value)
  {
    byte[] bytes = Arrays.copyOfRange(value.bytes, value.offset, value.offset + value.length);
    InetAddress inet = InetAddressPoint.decode(bytes);
    return InetAddresses.toAddrString(inet);
  }
}
