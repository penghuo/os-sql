/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.sql.plugin.streamexpression;

import java.io.IOException;
import java.util.List;
import org.opensearch.sql.plugin.streamexpression.comp.StreamComparator;
import org.opensearch.sql.plugin.streamexpression.stream.ParallelStream;
import org.opensearch.sql.plugin.streamexpression.stream.StreamContext;
import org.opensearch.sql.plugin.streamexpression.stream.TupleStream;
import org.opensearch.sql.plugin.streamexpression.stream.expr.Explanation;
import org.opensearch.sql.plugin.streamexpression.stream.expr.Expressible;
import org.opensearch.sql.plugin.streamexpression.stream.expr.StreamExpression;
import org.opensearch.sql.plugin.streamexpression.stream.expr.StreamExpressionNamedParameter;
import org.opensearch.sql.plugin.streamexpression.stream.expr.StreamExpressionParameter;
import org.opensearch.sql.plugin.streamexpression.stream.expr.StreamExpressionValue;
import org.opensearch.sql.plugin.streamexpression.stream.expr.StreamFactory;

public class Lang {

  public static void register(StreamFactory streamFactory) {
    streamFactory
        // source streams
        .withFunctionName("parallel", ParallelStream.class)
        .withFunctionName("input", LocalInputStream.class);
  }

  public static class LocalInputStream extends TupleStream implements Expressible {

    private final String result;

    public LocalInputStream(String result) {
      this.result = result;
    }

    public LocalInputStream(StreamExpression expression, StreamFactory factory) throws IOException {
      StreamExpressionNamedParameter resultParam = factory.getNamedOperand(expression,
          "result");
      this.result = ((StreamExpressionValue) resultParam.getParameter()).getValue();
    }

    @Override
    public void setStreamContext(StreamContext context) {
      // do nothing
    }

    @Override
    public List<TupleStream> children() {
      return null;
    }

    @Override
    public void open() throws IOException {
      // do nothing
    }

    @Override
    public void close() throws IOException {}

    @Override
    public Tuple read() throws IOException {
      return new Tuple("KEY", result);
    }

    @Override
    public StreamComparator getStreamSort() {
      return null;
    }

    @Override
    public StreamExpressionParameter toExpression(StreamFactory factory) throws IOException {
      StreamExpression expression = new StreamExpression(factory.getFunctionName(this.getClass()));
      expression.addParameter(
          new StreamExpressionNamedParameter("result", result));

      return expression;
    }

    @Override
    public Explanation toExplanation(StreamFactory factory) throws IOException {
      return new Explanation("LocalInputStream");
    }
  }
}
