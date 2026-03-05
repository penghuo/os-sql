/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.plugin.execution;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.opensearch.dqe.analyzer.AnalyzedQuery;
import org.opensearch.dqe.analyzer.sort.OperatorSelectionRule.PipelineStrategy;
import org.opensearch.dqe.analyzer.sort.PipelineDecision;
import org.opensearch.dqe.exchange.gather.ExchangeSourceOperator;
import org.opensearch.dqe.execution.driver.Pipeline;
import org.opensearch.dqe.execution.operator.LimitOperator;
import org.opensearch.dqe.execution.operator.Operator;
import org.opensearch.dqe.execution.operator.OperatorContext;
import org.opensearch.dqe.execution.operator.SortOperator;
import org.opensearch.dqe.execution.operator.TopNOperator;
import org.opensearch.dqe.memory.QueryMemoryBudget;

/**
 * Builds the coordinator-side operator pipeline from an {@link ExchangeSourceOperator} plus the
 * {@link PipelineDecision} from analysis.
 *
 * <p>The pipeline strategy determines which operators are added:
 *
 * <table>
 *   <tr><th>Strategy</th><th>Pipeline</th></tr>
 *   <tr><td>SCAN_ONLY</td><td>[ExchangeSource]</td></tr>
 *   <tr><td>LIMIT_ONLY</td><td>[ExchangeSource, Limit]</td></tr>
 *   <tr><td>FULL_SORT</td><td>[ExchangeSource, Sort]</td></tr>
 *   <tr><td>TOP_N</td><td>[ExchangeSource, TopN]</td></tr>
 *   <tr><td>TOP_N_WITH_OFFSET</td><td>[ExchangeSource, TopN(limit+offset), Limit(limit,offset)]</td></tr>
 * </table>
 */
public final class CoordinatorPipelineBuilder {

  private CoordinatorPipelineBuilder() {}

  /**
   * Build a coordinator pipeline.
   *
   * @param source the exchange source operator reading from the gather exchange
   * @param analyzed the analyzed query containing pipeline decision and output schema
   * @param budget the per-query memory budget
   * @return a Pipeline ready to be driven
   */
  public static Pipeline build(
      ExchangeSourceOperator source, AnalyzedQuery analyzed, QueryMemoryBudget budget) {

    PipelineDecision decision = analyzed.getPipelineDecision();
    PipelineStrategy strategy = decision.getStrategy();
    String queryId = source.getOperatorContext().getQueryId();
    int stageId = source.getOperatorContext().getStageId();

    List<Integer> outputChannels =
        IntStream.range(0, analyzed.getOutputColumnNames().size())
            .boxed()
            .collect(Collectors.toList());

    List<Operator> operators = new ArrayList<>();
    operators.add(source);
    int operatorId = 1; // source is 0

    switch (strategy) {
      case SCAN_ONLY:
        // No additional operators needed
        break;

      case LIMIT_ONLY: {
        long limit = decision.getLimit().orElse(0);
        long offset = decision.getOffset().orElse(0);
        OperatorContext ctx =
            new OperatorContext(queryId, stageId, 0, operatorId++, "Limit", budget);
        operators.add(new LimitOperator(ctx, lastOperator(operators), limit, offset));
        break;
      }

      case FULL_SORT: {
        OperatorContext ctx =
            new OperatorContext(queryId, stageId, 0, operatorId++, "Sort", budget);
        operators.add(
            new SortOperator(
                ctx, lastOperator(operators), decision.getSortSpecifications(), outputChannels));
        break;
      }

      case TOP_N: {
        long n = decision.getEffectiveTopN().orElse(decision.getLimit().orElse(Long.MAX_VALUE));
        OperatorContext ctx =
            new OperatorContext(queryId, stageId, 0, operatorId++, "TopN", budget);
        operators.add(
            new TopNOperator(
                ctx,
                lastOperator(operators),
                decision.getSortSpecifications(),
                n,
                outputChannels));
        break;
      }

      case TOP_N_WITH_OFFSET: {
        long limit = decision.getLimit().orElse(0);
        long offset = decision.getOffset().orElse(0);
        long effectiveN = decision.getEffectiveTopN().orElse(limit + offset);

        OperatorContext topNCtx =
            new OperatorContext(queryId, stageId, 0, operatorId++, "TopN", budget);
        operators.add(
            new TopNOperator(
                topNCtx,
                lastOperator(operators),
                decision.getSortSpecifications(),
                effectiveN,
                outputChannels));

        OperatorContext limitCtx =
            new OperatorContext(queryId, stageId, 0, operatorId++, "Limit", budget);
        operators.add(new LimitOperator(limitCtx, lastOperator(operators), limit, offset));
        break;
      }
    }

    return new Pipeline(operators);
  }

  private static Operator lastOperator(List<Operator> operators) {
    return operators.get(operators.size() - 1);
  }
}
