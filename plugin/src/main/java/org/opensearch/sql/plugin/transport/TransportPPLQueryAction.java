/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport;

import static org.opensearch.rest.BaseRestHandler.MULTI_ALLOW_EXPLICIT_INDEX;
import static org.opensearch.sql.executor.ExecutionEngine.ExplainResponse.normalizeLf;
import static org.opensearch.sql.lang.PPLLangSpec.PPL_SPEC;
import static org.opensearch.sql.protocol.response.format.JsonResponseFormatter.Style.PRETTY;

import java.util.Locale;
import java.util.Optional;
import java.util.function.Supplier;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.plugin.omni.OmniEngineService;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.common.utils.QueryContext;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.legacy.metrics.MetricName;
import org.opensearch.sql.legacy.metrics.Metrics;
import org.opensearch.sql.monitor.profile.QueryProfiling;
import org.opensearch.sql.opensearch.executor.OpenSearchQueryManager;
import org.opensearch.sql.opensearch.setting.OpenSearchSettings;
import org.opensearch.sql.ppl.domain.PPLQueryRequest;
import org.opensearch.sql.protocol.response.QueryResult;
import org.opensearch.sql.protocol.response.format.CsvResponseFormatter;
import org.opensearch.sql.protocol.response.format.Format;
import org.opensearch.sql.protocol.response.format.JsonResponseFormatter;
import org.opensearch.sql.protocol.response.format.RawResponseFormatter;
import org.opensearch.sql.protocol.response.format.ResponseFormatter;
import org.opensearch.sql.protocol.response.format.SimpleJsonResponseFormatter;
import org.opensearch.sql.protocol.response.format.VisualizationResponseFormatter;
import org.opensearch.sql.protocol.response.format.YamlResponseFormatter;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

/** Transport action for PPL queries — delegates to Omni (Trino-based) engine. */
public class TransportPPLQueryAction
    extends HandledTransportAction<ActionRequest, TransportPPLQueryResponse> {

  private final OmniEngineService omniEngineService;
  private final Supplier<Boolean> pplEnabled;

  @Inject
  public TransportPPLQueryAction(
      TransportService transportService,
      ActionFilters actionFilters,
      ClusterService clusterService,
      org.opensearch.common.settings.Settings clusterSettings,
      OmniEngineService omniEngineService) {
    super(PPLQueryAction.NAME, transportService, actionFilters, TransportPPLQueryRequest::new);
    this.omniEngineService = omniEngineService;
    OpenSearchSettings pluginSettings = new OpenSearchSettings(clusterService.getClusterSettings());
    this.pplEnabled =
        () ->
            MULTI_ALLOW_EXPLICIT_INDEX.get(clusterSettings)
                && (Boolean) pluginSettings.getSettingValue(Settings.Key.PPL_ENABLED);
  }

  @Override
  protected void doExecute(
      Task task, ActionRequest request, ActionListener<TransportPPLQueryResponse> listener) {
    if (!pplEnabled.get()) {
      listener.onFailure(
          new IllegalAccessException(
              "Either plugins.ppl.enabled or rest.action.multi.allow_explicit_index setting is"
                  + " false"));
      return;
    }

    TransportPPLQueryRequest transportRequest = TransportPPLQueryRequest.fromActionRequest(request);
    if (transportRequest.isGrammarRequest()) {
      listener.onResponse(new TransportPPLQueryResponse("{}"));
      return;
    }

    if (task instanceof PPLQueryTask pplQueryTask) {
      OpenSearchQueryManager.setCancellableTask(pplQueryTask);
    }
    Metrics.getInstance().getNumericalMetric(MetricName.PPL_REQ_TOTAL).increment();
    Metrics.getInstance().getNumericalMetric(MetricName.PPL_REQ_COUNT_TOTAL).increment();
    QueryContext.addRequestId();

    PPLQueryRequest pplRequest = transportRequest.toPPLQueryRequest();
    QueryContext.setProfile(pplRequest.profile());
    ActionListener<TransportPPLQueryResponse> clearingListener = wrapWithProfilingClear(listener);

    if (pplRequest.isExplainRequest()) {
      omniEngineService.explain(
          pplRequest, pplRequest.mode(), createExplainListener(pplRequest, clearingListener));
    } else {
      omniEngineService.execute(pplRequest, createQueryListener(pplRequest, clearingListener));
    }
  }

  private ResponseListener<ExecutionEngine.ExplainResponse> createExplainListener(
      PPLQueryRequest request, ActionListener<TransportPPLQueryResponse> listener) {
    return new ResponseListener<>() {
      @Override
      public void onResponse(ExecutionEngine.ExplainResponse response) {
        Optional<Format> isYamlFormat =
            Format.ofExplain(request.getFormat()).filter(f -> f.equals(Format.YAML));
        ResponseFormatter<ExecutionEngine.ExplainResponse> formatter;
        if (isYamlFormat.isPresent()) {
          formatter =
              new YamlResponseFormatter<>() {
                @Override
                protected Object buildYamlObject(ExecutionEngine.ExplainResponse r) {
                  return normalizeLf(r);
                }
              };
        } else {
          formatter =
              new JsonResponseFormatter<>(PRETTY) {
                @Override
                protected Object buildJsonObject(ExecutionEngine.ExplainResponse r) {
                  return r;
                }
              };
        }
        listener.onResponse(
            new TransportPPLQueryResponse(formatter.format(response), formatter.contentType()));
      }

      @Override
      public void onFailure(Exception e) {
        listener.onFailure(e);
      }
    };
  }

  private ResponseListener<ExecutionEngine.QueryResponse> createQueryListener(
      PPLQueryRequest pplRequest, ActionListener<TransportPPLQueryResponse> listener) {
    Format format = format(pplRequest);
    ResponseFormatter<QueryResult> formatter;
    if (format.equals(Format.CSV)) {
      formatter = new CsvResponseFormatter(pplRequest.sanitize());
    } else if (format.equals(Format.RAW)) {
      formatter = new RawResponseFormatter();
    } else if (format.equals(Format.VIZ)) {
      formatter = new VisualizationResponseFormatter(pplRequest.style());
    } else {
      formatter = new SimpleJsonResponseFormatter(JsonResponseFormatter.Style.PRETTY);
    }
    return new ResponseListener<>() {
      @Override
      public void onResponse(ExecutionEngine.QueryResponse response) {
        String body =
            formatter.format(
                new QueryResult(
                    response.getSchema(), response.getResults(), response.getCursor(), PPL_SPEC));
        listener.onResponse(new TransportPPLQueryResponse(body));
      }

      @Override
      public void onFailure(Exception e) {
        listener.onFailure(e);
      }
    };
  }

  private Format format(PPLQueryRequest pplRequest) {
    String format = pplRequest.getFormat();
    Optional<Format> optionalFormat = Format.of(format);
    if (optionalFormat.isPresent()) {
      return optionalFormat.get();
    } else {
      throw new IllegalArgumentException(
          String.format(Locale.ROOT, "response in %s format is not supported.", format));
    }
  }

  private ActionListener<TransportPPLQueryResponse> wrapWithProfilingClear(
      ActionListener<TransportPPLQueryResponse> delegate) {
    return new ActionListener<>() {
      @Override
      public void onResponse(TransportPPLQueryResponse transportPPLQueryResponse) {
        try {
          delegate.onResponse(transportPPLQueryResponse);
        } finally {
          QueryProfiling.clear();
        }
      }

      @Override
      public void onFailure(Exception e) {
        try {
          delegate.onFailure(e);
        } finally {
          QueryProfiling.clear();
        }
      }
    };
  }
}
