/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.plugin.routing;

import javax.annotation.Nullable;
import org.opensearch.dqe.parser.DqeErrorCode;
import org.opensearch.dqe.parser.DqeException;
import org.opensearch.dqe.plugin.explain.DqeExplainHandler;
import org.opensearch.dqe.plugin.orchestrator.DqeQueryOrchestrator;
import org.opensearch.dqe.plugin.request.DqeQueryRequest;
import org.opensearch.dqe.plugin.response.DqeQueryResponse;
import org.opensearch.dqe.plugin.settings.DqeSettings;

/**
 * Routes SQL requests to the DQE engine or the Calcite engine based on the cluster setting ({@code
 * plugins.sql.engine}) and the per-request {@code engine} field.
 *
 * <p>Resolution precedence:
 *
 * <ol>
 *   <li>Per-request {@code engine} field (if present and non-null)
 *   <li>Cluster setting {@code plugins.sql.engine}
 * </ol>
 *
 * <p>When the resolved engine is "dqe" but {@code plugins.dqe.enabled} is {@code false}, the router
 * throws {@link DqeException} with {@link DqeErrorCode#DQE_DISABLED}.
 */
public class EngineRouter {

  public static final String ENGINE_DQE = "dqe";
  public static final String ENGINE_CALCITE = "calcite";

  private final DqeSettings settings;
  @Nullable private final DqeQueryOrchestrator orchestrator;
  @Nullable private final DqeExplainHandler explainHandler;

  /**
   * Creates a new engine router.
   *
   * @param settings the DQE settings for reading cluster-level engine configuration
   * @param orchestrator the query orchestrator, or null if not yet available (PL-10)
   * @param explainHandler the explain handler, or null if not yet available (PL-5)
   */
  public EngineRouter(
      DqeSettings settings,
      @Nullable DqeQueryOrchestrator orchestrator,
      @Nullable DqeExplainHandler explainHandler) {
    this.settings = settings;
    this.orchestrator = orchestrator;
    this.explainHandler = explainHandler;
  }

  /**
   * Determine the effective engine for a request.
   *
   * @param requestEngineField the "engine" field from the request body, or null if absent
   * @return "dqe" or "calcite"
   */
  public String resolveEngine(String requestEngineField) {
    if (requestEngineField != null && !requestEngineField.isEmpty()) {
      String normalized = requestEngineField.toLowerCase();
      if (!ENGINE_DQE.equals(normalized) && !ENGINE_CALCITE.equals(normalized)) {
        throw new DqeException(
            "Invalid engine value: '"
                + requestEngineField
                + "'. Must be 'calcite' or 'dqe'.",
            DqeErrorCode.INVALID_REQUEST);
      }
      return normalized;
    }
    return settings.getEngine();
  }

  /**
   * Returns true if the resolved engine is "dqe" and DQE is enabled.
   *
   * @param requestEngineField the "engine" field from the request body, or null if absent
   * @return true if the request should be handled by DQE
   * @throws DqeException with DQE_DISABLED if engine=dqe but plugins.dqe.enabled=false
   */
  public boolean shouldUseDqe(String requestEngineField) {
    String engine = resolveEngine(requestEngineField);
    if (ENGINE_DQE.equals(engine)) {
      if (!settings.isDqeEnabled()) {
        throw new DqeException(
            "DQE engine is disabled. Set plugins.dqe.enabled=true to enable it.",
            DqeErrorCode.DQE_DISABLED);
      }
      return true;
    }
    return false;
  }

  /**
   * Execute a DQE query via the orchestrator.
   *
   * @param request the parsed DQE query request
   * @return the query response
   * @throws DqeException if the orchestrator is not yet available or on execution error
   */
  public DqeQueryResponse executeQuery(DqeQueryRequest request) throws DqeException {
    if (orchestrator == null) {
      throw new DqeException(
          "DQE query execution not yet implemented", DqeErrorCode.UNSUPPORTED_OPERATION);
    }
    return orchestrator.execute(request);
  }

  /**
   * Execute a DQE explain request via the explain handler.
   *
   * @param request the parsed DQE query request
   * @return the explain output as a formatted string
   * @throws DqeException if the explain handler is not yet available or on analysis error
   */
  public String executeExplain(DqeQueryRequest request) throws DqeException {
    if (explainHandler == null) {
      throw new DqeException(
          "DQE explain not yet implemented", DqeErrorCode.UNSUPPORTED_OPERATION);
    }
    return explainHandler.explain(request);
  }
}
