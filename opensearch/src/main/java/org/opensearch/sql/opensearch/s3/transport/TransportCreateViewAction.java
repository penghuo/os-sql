/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.s3.transport;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionFuture;
import org.opensearch.action.ActionListener;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.settings.Settings;
import org.opensearch.indices.IndicesService;
import org.opensearch.sql.opensearch.s3.OSS3Object;
import org.opensearch.sql.opensearch.s3.operator.S3Lister;
import org.opensearch.sql.opensearch.s3.operator.S3Scan;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportService;

public class TransportCreateViewAction
    extends HandledTransportAction<CreateViewRequest, CreateViewResponse> {

  private static final Logger LOG = LogManager.getLogger();

  private static int BATCH = 5000;

  public static final String ACTION_NAME = "n/view/create";

  private final ClusterService clusterService;
  private final TransportService transportService;
  private final IndicesService indicesService;
  private final NodeClient nodeClient;

  public static final String EXECUTOR = ThreadPool.Names.WRITE;

  @Inject
  public TransportCreateViewAction(
      String actionName,
      NodeClient nodeClient,
      ClusterService clusterService,
      IndicesService indicesService,
      TransportService transportService,
      ActionFilters actionFilters,
      String executor) {
    super(actionName, transportService, actionFilters, CreateViewRequest::new, executor);

    this.clusterService = clusterService;
    this.transportService = transportService;
    this.indicesService = indicesService;
    this.nodeClient = nodeClient;
    transportService.registerRequestHandler(
        ACTION_NAME,
        EXECUTOR,
        NodeCreateViewRequest::new,
        (request, channel, task) -> {
          DiscoveryNode localNode = clusterService.state().getNodes().getLocalNode();

          // create index
          CreateIndexRequest createIndexRequest =
              new CreateIndexRequest(request.indexName)
                  .settings(
                      Settings.builder()
                          .put("index.number_of_shards", 5)
                          .put("index.number_of_replicas", 0)
                          .put("index.routing.allocation.include._id", localNode.getId())
                          .put("index.translog.durability", "async")
                          .build())
                  .mapping(fieldsMapping(request.fieldsMapping));

          ActionFuture<CreateIndexResponse> indexResponseActionFuture =
              nodeClient.admin().indices().create(createIndexRequest);
          // send failure response
          CreateIndexResponse createIndexResponse = indexResponseActionFuture.get();
          if (!createIndexResponse.isAcknowledged()) {
            channel.sendResponse(new CreateViewResponse(false));
          }

          ThreadPool threadPool = nodeClient.threadPool();
          ExecutorService executorService = threadPool.executor("sql-worker");
//          CompletionService<Boolean> completionService =
//              new ExecutorCompletionService<>(executorService);

          List<CompletableFuture<Boolean>> futures = new ArrayList<>(request.partitions.size());

          for (OSS3Object partition : request.partitions) {
            futures.add(CompletableFuture.supplyAsync(() -> {
              S3Scan s3Scan =
                  new S3Scan(Collections.singletonList(partition));
              s3Scan.open();

              // bulk write
              while (true) {
                Optional<BulkRequest> bulkRequest = bulkRequest(s3Scan, request.indexName);
                if (!bulkRequest.isPresent()) {
                  break;
                }
                ActionFuture<BulkResponse> bulkResponseActionFuture =
                    nodeClient.bulk(bulkRequest.get());
                try {
                  BulkResponse bulkResponse = bulkResponseActionFuture.get();
                  LOG.info("bulk took: {}", bulkResponse.getTook());
                } catch (Exception e) {
                  throw new RuntimeException(e);
                }
              }
              s3Scan.close();
              return true;
            }, executorService));
          }

//          for (int i = 0; i < request.partitions.size(); i++) {
//            final int index = i;
//            completionService.submit(
//                () -> {
//                  // scan partition
//                  S3Scan s3Scan =
//                      new S3Scan(Collections.singletonList(request.partitions.get(index)));
//                  s3Scan.open();
//
//                  // bulk write
//                  while (true) {
//                    Optional<BulkRequest> bulkRequest = bulkRequest(s3Scan, request.indexName);
//                    if (!bulkRequest.isPresent()) {
//                      break;
//                    }
//                    ActionFuture<BulkResponse> bulkResponseActionFuture =
//                        nodeClient.bulk(bulkRequest.get());
//                    try {
//                      BulkResponse bulkResponse = bulkResponseActionFuture.get();
//                      LOG.info("bulk took: {}", bulkResponse.getTook());
//                    } catch (Exception e) {
//                      throw new RuntimeException(e);
//                    }
//                  }
//                  s3Scan.close();
//                  futures.get(index).complete(true);
//
//                  return true;
//                });
//          }

          CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();
          channel.sendResponse(new CreateViewResponse(true));
        });
  }

  private Map<String, Object> fieldsMapping(Map<String, Object> fieldsMapping) {
    Map<String, Object> properties = new HashMap<>();
    fieldsMapping.entrySet().stream()
        .map(
            entry ->
                properties.put(
                    entry.getKey(),
                    new HashMap<String, Object>() {
                      {
                        put("type", entry.getValue());
                      }
                    }));

    Map<String, Object> mapping = new HashMap<>();
    mapping.put("properties", properties);

    return mapping;
  }

  private Optional<BulkRequest> bulkRequest(S3Scan s3Scan, String indexName) {
    if (!s3Scan.hasNext()) {
      return Optional.empty();
    }

    int cnt = 0;
    BulkRequest bulkRequest = new BulkRequest();
    while (s3Scan.hasNext() && cnt++ < BATCH) {
      bulkRequest.add(new IndexRequest(indexName).source(s3Scan.next()));
    }

    return Optional.of(bulkRequest);
  }

  @Override
  protected void doExecute(
      Task task, CreateViewRequest request, ActionListener<CreateViewResponse> listener) {
    DiscoveryNodes nodes = clusterService.state().getNodes();

    try {
      final S3Lister s3Lister = new S3Lister(new URI(request.getTableName()));
      final Iterator<List<OSS3Object>> partition = s3Lister.partition(nodes.getSize()).iterator();

      for (DiscoveryNode node : nodes) {
        if (!partition.hasNext()) {
          break;
        }
        transportService.sendRequest(
            node,
            ACTION_NAME,
            new NodeCreateViewRequest(
                String.join("-", request.getIndexName(), node.getId().toLowerCase(Locale.ROOT)),
                request.getFieldsMapping(),
                partition.next()),
            new TransportResponseHandler<CreateViewResponse>() {
              @Override
              public void handleResponse(CreateViewResponse response) {
                listener.onResponse(response);
              }

              @Override
              public void handleException(TransportException exp) {
                listener.onFailure(exp);
              }

              @Override
              public String executor() {
                return EXECUTOR;
              }

              @Override
              public CreateViewResponse read(StreamInput in) throws IOException {
                return new CreateViewResponse(in);
              }
            });
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  static class NodeCreateViewRequest extends TransportRequest {
    private String indexName;

    private Map<String, Object> fieldsMapping;

    private List<OSS3Object> partitions;

    public NodeCreateViewRequest() {
      super();
    }

    public NodeCreateViewRequest(
        String indexName, Map<String, Object> fieldsMapping, List<OSS3Object> partitions) {
      super();
      this.indexName = indexName;
      this.fieldsMapping = fieldsMapping;
      this.partitions = partitions;
    }

    public NodeCreateViewRequest(StreamInput in) throws IOException {
      super(in);
      this.indexName = in.readString();
      this.fieldsMapping = in.readMap();
      this.partitions = in.readList(OSS3Object::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
      super.writeTo(out);
      out.writeString(indexName);
      out.writeMap(fieldsMapping);
      out.writeList(partitions);
    }
  }
}
