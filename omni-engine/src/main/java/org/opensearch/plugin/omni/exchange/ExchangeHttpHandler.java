/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.plugin.omni.exchange;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.json.JsonCodec;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.trino.Session;
import io.trino.execution.DynamicFiltersCollector.VersionedDynamicFilterDomains;
import io.trino.execution.SqlTaskManager;
import io.trino.execution.SqlTaskManager.SqlTaskWithResults;
import io.trino.execution.TaskId;
import io.trino.execution.TaskInfo;
import io.trino.execution.TaskStatus;
import io.trino.execution.buffer.BufferResult;
import io.trino.execution.buffer.PipelinedOutputBuffers.OutputBufferId;
import io.trino.metadata.SessionPropertyManager;
import io.trino.server.FailTaskRequest;
import io.trino.server.TaskUpdateRequest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.airlift.concurrent.MoreFutures.addTimeout;
import static io.trino.execution.buffer.PagesSerdeUtil.NO_CHECKSUM;
import static io.trino.execution.buffer.PagesSerdeUtil.calculateChecksum;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Netty HTTP handler serving both Trino control plane and data plane.
 *
 * Control plane routes (used by HttpRemoteTask):
 *   POST   /v1/task/{taskId}                    — create/update task
 *   GET    /v1/task/{taskId}                     — get task info (long-poll)
 *   GET    /v1/task/{taskId}/status              — get task status (long-poll)
 *   DELETE /v1/task/{taskId}                     — cancel/abort task
 *   POST   /v1/task/{taskId}/fail                — fail task
 *   GET    /v1/task/{taskId}/dynamicfilters      — get dynamic filters
 *
 * Data plane routes (used by HttpPageBufferClient):
 *   GET    /v1/task/{taskId}/results/{bufferId}/{token}              — fetch pages
 *   GET    /v1/task/{taskId}/results/{bufferId}/{token}/acknowledge  — acknowledge
 *   DELETE /v1/task/{taskId}/results/{bufferId}                      — destroy buffer
 */
@ChannelHandler.Sharable
public class ExchangeHttpHandler extends SimpleChannelInboundHandler<FullHttpRequest> {

    private static final Logger log = LogManager.getLogger(ExchangeHttpHandler.class);

    private static final int SERIALIZED_PAGES_MAGIC = 0xfea4f001;
    private static final String TRINO_PAGES_CONTENT_TYPE = "application/X-trino-pages";
    private static final String HDR_TASK_INSTANCE_ID = "X-Trino-Task-Instance-Id";
    private static final String HDR_PAGE_SEQUENCE_ID = "X-Trino-Page-Sequence-Id";
    private static final String HDR_PAGE_END_SEQUENCE_ID = "X-Trino-Page-End-Sequence-Id";
    private static final String HDR_BUFFER_COMPLETE = "X-Trino-Buffer-Complete";
    private static final String HDR_TASK_FAILED = "X-Trino-Task-Failed";
    private static final String HDR_MAX_SIZE = "X-Trino-Max-Size";
    private static final String HDR_MAX_WAIT = "X-Trino-Max-Wait";
    private static final String HDR_CURRENT_VERSION = "X-Trino-Current-Version";
    private static final DataSize DEFAULT_MAX_SIZE = DataSize.ofBytes(1024 * 1024);
    private static final long DEFAULT_MAX_WAIT_MS = 2000;
    private static final Duration ADDITIONAL_WAIT_TIME = new Duration(5, TimeUnit.SECONDS);

    // Data plane URI patterns
    private static final Pattern RESULTS_PATTERN =
            Pattern.compile("/v1/task/([^/]+)/results/(\\d+)/(\\d+)$");
    private static final Pattern ACKNOWLEDGE_PATTERN =
            Pattern.compile("/v1/task/([^/]+)/results/(\\d+)/(\\d+)/acknowledge$");
    private static final Pattern DESTROY_BUFFER_PATTERN =
            Pattern.compile("/v1/task/([^/]+)/results/(\\d+)$");

    // Control plane URI patterns
    private static final Pattern TASK_STATUS_PATTERN =
            Pattern.compile("/v1/task/([^/]+)/status$");
    private static final Pattern TASK_FAIL_PATTERN =
            Pattern.compile("/v1/task/([^/]+)/fail$");
    private static final Pattern TASK_DYNAMIC_FILTERS_PATTERN =
            Pattern.compile("/v1/task/([^/]+)/dynamicfilters$");
    private static final Pattern TASK_PATTERN =
            Pattern.compile("/v1/task/([^/]+)$");
    private static final Pattern STATS_PATTERN =
            Pattern.compile("/v1/stats$");

    // Request counters
    private static final java.util.concurrent.atomic.AtomicLong postTaskCount = new java.util.concurrent.atomic.AtomicLong();
    private static final java.util.concurrent.atomic.AtomicLong getTaskInfoCount = new java.util.concurrent.atomic.AtomicLong();
    private static final java.util.concurrent.atomic.AtomicLong getStatusCount = new java.util.concurrent.atomic.AtomicLong();
    private static final java.util.concurrent.atomic.AtomicLong getResultsCount = new java.util.concurrent.atomic.AtomicLong();
    private static final java.util.concurrent.atomic.AtomicLong acknowledgeCount = new java.util.concurrent.atomic.AtomicLong();
    private static final java.util.concurrent.atomic.AtomicLong destroyCount = new java.util.concurrent.atomic.AtomicLong();
    private static final java.util.concurrent.atomic.AtomicLong deleteTaskCount = new java.util.concurrent.atomic.AtomicLong();
    private static final java.util.concurrent.atomic.AtomicLong failTaskCount = new java.util.concurrent.atomic.AtomicLong();
    private static final java.util.concurrent.atomic.AtomicLong dynamicFiltersCount = new java.util.concurrent.atomic.AtomicLong();

    private final Supplier<SqlTaskManager> taskManagerSupplier;
    private final ScheduledExecutorService timeoutExecutor;
    private final ExecutorService responseExecutor;
    private final boolean dataIntegrityVerificationEnabled;
    private final SessionPropertyManager sessionPropertyManager;
    private final JsonCodec<TaskUpdateRequest> taskUpdateRequestCodec;
    private final JsonCodec<TaskInfo> taskInfoCodec;
    private final JsonCodec<TaskStatus> taskStatusCodec;
    private final JsonCodec<FailTaskRequest> failTaskRequestCodec;
    private final JsonCodec<VersionedDynamicFilterDomains> dynamicFilterDomainsCodec;

    public ExchangeHttpHandler(
            Supplier<SqlTaskManager> taskManagerSupplier,
            boolean dataIntegrityVerificationEnabled,
            SessionPropertyManager sessionPropertyManager,
            JsonCodec<TaskUpdateRequest> taskUpdateRequestCodec,
            JsonCodec<TaskInfo> taskInfoCodec,
            JsonCodec<TaskStatus> taskStatusCodec,
            JsonCodec<FailTaskRequest> failTaskRequestCodec,
            JsonCodec<VersionedDynamicFilterDomains> dynamicFilterDomainsCodec) {
        this.taskManagerSupplier = taskManagerSupplier;
        this.dataIntegrityVerificationEnabled = dataIntegrityVerificationEnabled;
        this.sessionPropertyManager = sessionPropertyManager;
        this.taskUpdateRequestCodec = taskUpdateRequestCodec;
        this.taskInfoCodec = taskInfoCodec;
        this.taskStatusCodec = taskStatusCodec;
        this.failTaskRequestCodec = failTaskRequestCodec;
        this.dynamicFilterDomainsCodec = dynamicFilterDomainsCodec;
        this.timeoutExecutor = Executors.newScheduledThreadPool(2, r -> {
            Thread t = new Thread(r, "exchange-timeout");
            t.setDaemon(true);
            return t;
        });
        // Dedicated response executor for page serialization and response writing.
        // Matches Trino's task.http-response-threads=100 (ServerMainModule.java).
        // Offloads heavy work (ByteBuf copy, acknowledge processing) from Netty event loop.
        this.responseExecutor = Executors.newFixedThreadPool(100, r -> {
            Thread t = new Thread(r, "exchange-response");
            t.setDaemon(true);
            return t;
        });
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request) {
        String uri = request.uri();
        // Strip query string for pattern matching
        String path = uri.contains("?") ? uri.substring(0, uri.indexOf('?')) : uri;
        HttpMethod method = request.method();

        try {
            if (method == HttpMethod.GET) {
                // Data plane — check specific patterns first (longer paths)
                Matcher ackMatcher = ACKNOWLEDGE_PATTERN.matcher(path);
                if (ackMatcher.matches()) { handleAcknowledge(ctx, ackMatcher); return; }

                Matcher resultsMatcher = RESULTS_PATTERN.matcher(path);
                if (resultsMatcher.matches()) { handleGetResults(ctx, request, resultsMatcher); return; }

                // Control plane
                Matcher statusMatcher = TASK_STATUS_PATTERN.matcher(path);
                if (statusMatcher.matches()) { handleGetTaskStatus(ctx, request, statusMatcher); return; }

                Matcher dfMatcher = TASK_DYNAMIC_FILTERS_PATTERN.matcher(path);
                if (dfMatcher.matches()) { handleGetDynamicFilters(ctx, request, dfMatcher); return; }

                Matcher taskMatcher = TASK_PATTERN.matcher(path);
                if (taskMatcher.matches()) { handleGetTaskInfo(ctx, request, taskMatcher); return; }

                Matcher statsMatcher = STATS_PATTERN.matcher(path);
                if (statsMatcher.matches()) { handleStats(ctx, request); return; }

            } else if (method == HttpMethod.POST) {
                Matcher failMatcher = TASK_FAIL_PATTERN.matcher(path);
                if (failMatcher.matches()) { handleFailTask(ctx, request, failMatcher); return; }

                Matcher taskMatcher = TASK_PATTERN.matcher(path);
                if (taskMatcher.matches()) { handleCreateOrUpdateTask(ctx, request, taskMatcher); return; }

            } else if (method == HttpMethod.DELETE) {
                Matcher destroyMatcher = DESTROY_BUFFER_PATTERN.matcher(path);
                if (destroyMatcher.matches()) { handleDestroyBuffer(ctx, destroyMatcher); return; }

                Matcher taskMatcher = TASK_PATTERN.matcher(path);
                if (taskMatcher.matches()) { handleDeleteTask(ctx, request, taskMatcher); return; }
            }
            sendError(ctx, HttpResponseStatus.NOT_FOUND, "Unknown route: " + method + " " + path);
        } catch (Exception e) {
            log.error("Exchange handler error for {} {}", method, path, e);
            sendError(ctx, HttpResponseStatus.INTERNAL_SERVER_ERROR, e.getMessage());
        }
    }

    // ── Control Plane Routes ──

    /** POST /v1/task/{taskId} — create or update task. Replicates TaskResource.createOrUpdateTask(). */
    private void handleCreateOrUpdateTask(ChannelHandlerContext ctx, FullHttpRequest request, Matcher matcher) {
        postTaskCount.incrementAndGet();
        TaskId taskId = TaskId.valueOf(matcher.group(1));
        log.info("TASK_DEBUG: POST_START taskId={} bodySize={} localAddr={} remoteAddr={}",
                taskId, request.content().readableBytes(), ctx.channel().localAddress(), ctx.channel().remoteAddress());
        byte[] body = new byte[request.content().readableBytes()];
        request.content().readBytes(body);
        TaskUpdateRequest taskUpdateRequest;
        try {
            taskUpdateRequest = taskUpdateRequestCodec.fromJson(body);
        } catch (Exception e) {
            log.error("TASK_DEBUG: POST_DESERIALIZE_FAIL taskId={} error={}", taskId, e.getMessage());
            sendError(ctx, HttpResponseStatus.INTERNAL_SERVER_ERROR, "Deserialize error: " + e.getMessage());
            return;
        }

        Session session;
        try {
            session = taskUpdateRequest.getSession().toSession(
                    sessionPropertyManager, taskUpdateRequest.getExtraCredentials(), taskUpdateRequest.getExchangeEncryptionKey());
        } catch (Exception e) {
            log.error("TASK_DEBUG: POST_SESSION_FAIL taskId={} error={}", taskId, e.getMessage());
            sendError(ctx, HttpResponseStatus.INTERNAL_SERVER_ERROR, "Session error: " + e.getMessage());
            return;
        }

        TaskInfo taskInfo;
        try {
            taskInfo = taskManagerSupplier.get().updateTask(
                    session, taskId, taskUpdateRequest.getStageSpan(),
                    taskUpdateRequest.getFragment(), taskUpdateRequest.getSplitAssignments(),
                    taskUpdateRequest.getOutputIds(), taskUpdateRequest.getDynamicFilterDomains(),
                    taskUpdateRequest.isSpeculative());
        } catch (Exception e) {
            log.error("TASK_DEBUG: POST_UPDATE_FAIL taskId={} error={}", taskId, e.getMessage(), e);
            sendError(ctx, HttpResponseStatus.INTERNAL_SERVER_ERROR, "UpdateTask error: " + e.getMessage());
            return;
        }

        log.info("TASK_DEBUG: POST_OK taskId={} instanceId={} state={}",
                taskId, taskInfo.getTaskStatus().getTaskInstanceId(), taskInfo.getTaskStatus().getState());

        if (shouldSummarize(request.uri())) {
            taskInfo = taskInfo.summarize();
        }
        sendJson(ctx, HttpResponseStatus.OK, taskInfoCodec.toJsonBytes(taskInfo));
    }

    /** GET /v1/task/{taskId}/status — long-poll task status. Replicates TaskResource.getTaskStatus(). */
    private void handleGetTaskStatus(ChannelHandlerContext ctx, FullHttpRequest request, Matcher matcher) {
        getStatusCount.incrementAndGet();
        TaskId taskId = TaskId.valueOf(matcher.group(1));
        String versionHeader = request.headers().get(HDR_CURRENT_VERSION);
        String maxWaitHeader = request.headers().get(HDR_MAX_WAIT);

        SqlTaskManager taskManager = taskManagerSupplier.get();

        if (versionHeader == null || maxWaitHeader == null) {
            TaskStatus ts = taskManager.getTaskStatus(taskId);
            log.info("TASK_DEBUG: GET_STATUS taskId={} instanceId={} state={} version={} localAddr={}",
                    taskId, ts.getTaskInstanceId(), ts.getState(), ts.getVersion(), ctx.channel().localAddress());
            sendJson(ctx, HttpResponseStatus.OK, taskStatusCodec.toJsonBytes(ts));
            return;
        }

        long currentVersion = Long.parseLong(versionHeader);
        Duration maxWait = Duration.valueOf(maxWaitHeader);
        Duration waitTime = randomizeWaitTime(maxWait);

        ListenableFuture<TaskStatus> future = taskManager.getTaskStatus(taskId, currentVersion);
        if (!future.isDone()) {
            future = addTimeout(future, () -> taskManager.getTaskStatus(taskId), waitTime, timeoutExecutor);
        }

        Futures.addCallback(future, new FutureCallback<TaskStatus>() {
            @Override
            public void onSuccess(TaskStatus status) {
                sendJson(ctx, HttpResponseStatus.OK, taskStatusCodec.toJsonBytes(status));
            }
            @Override
            public void onFailure(Throwable t) {
                sendError(ctx, HttpResponseStatus.INTERNAL_SERVER_ERROR, t.getMessage());
            }
        }, responseExecutor);
    }

    /** GET /v1/task/{taskId} — long-poll task info. Replicates TaskResource.getTaskInfo(). */
    private void handleGetTaskInfo(ChannelHandlerContext ctx, FullHttpRequest request, Matcher matcher) {
        getTaskInfoCount.incrementAndGet();
        TaskId taskId = TaskId.valueOf(matcher.group(1));
        String versionHeader = request.headers().get(HDR_CURRENT_VERSION);
        String maxWaitHeader = request.headers().get(HDR_MAX_WAIT);

        SqlTaskManager taskManager = taskManagerSupplier.get();

        if (versionHeader == null || maxWaitHeader == null) {
            TaskInfo info = taskManager.getTaskInfo(taskId);
            if (shouldSummarize(request.uri())) info = info.summarize();
            sendJson(ctx, HttpResponseStatus.OK, taskInfoCodec.toJsonBytes(info));
            return;
        }

        long currentVersion = Long.parseLong(versionHeader);
        Duration maxWait = Duration.valueOf(maxWaitHeader);
        Duration waitTime = randomizeWaitTime(maxWait);

        ListenableFuture<TaskInfo> future = taskManager.getTaskInfo(taskId, currentVersion);
        if (!future.isDone()) {
            future = addTimeout(future, () -> taskManager.getTaskInfo(taskId), waitTime, timeoutExecutor);
        }

        boolean summarize = shouldSummarize(request.uri());
        Futures.addCallback(future, new FutureCallback<TaskInfo>() {
            @Override
            public void onSuccess(TaskInfo info) {
                TaskInfo result = summarize ? info.summarize() : info;
                sendJson(ctx, HttpResponseStatus.OK, taskInfoCodec.toJsonBytes(result));
            }
            @Override
            public void onFailure(Throwable t) {
                sendError(ctx, HttpResponseStatus.INTERNAL_SERVER_ERROR, t.getMessage());
            }
        }, responseExecutor);
    }

    /** DELETE /v1/task/{taskId}?abort=true/false — cancel or abort task. Replicates TaskResource.deleteTask(). */
    private void handleDeleteTask(ChannelHandlerContext ctx, FullHttpRequest request, Matcher matcher) {
        deleteTaskCount.incrementAndGet();
        TaskId taskId = TaskId.valueOf(matcher.group(1));
        boolean abort = getQueryParamBool(request.uri(), "abort", true);

        TaskInfo taskInfo = abort
                ? taskManagerSupplier.get().abortTask(taskId)
                : taskManagerSupplier.get().cancelTask(taskId);

        if (shouldSummarize(request.uri())) {
            taskInfo = taskInfo.summarize();
        }
        sendJson(ctx, HttpResponseStatus.OK, taskInfoCodec.toJsonBytes(taskInfo));
    }

    /** POST /v1/task/{taskId}/fail — fail task with error. Replicates TaskResource.failTask(). */
    private void handleFailTask(ChannelHandlerContext ctx, FullHttpRequest request, Matcher matcher) {
        failTaskCount.incrementAndGet();
        TaskId taskId = TaskId.valueOf(matcher.group(1));
        byte[] body = new byte[request.content().readableBytes()];
        request.content().readBytes(body);
        FailTaskRequest failRequest = failTaskRequestCodec.fromJson(body);

        TaskInfo taskInfo = taskManagerSupplier.get().failTask(taskId, failRequest.getFailureInfo().toException());
        sendJson(ctx, HttpResponseStatus.OK, taskInfoCodec.toJsonBytes(taskInfo));
    }

    /** GET /v1/task/{taskId}/dynamicfilters — get dynamic filter domains. */
    private void handleGetDynamicFilters(ChannelHandlerContext ctx, FullHttpRequest request, Matcher matcher) {
        dynamicFiltersCount.incrementAndGet();
        TaskId taskId = TaskId.valueOf(matcher.group(1));
        String versionHeader = request.headers().get(HDR_CURRENT_VERSION);
        long currentVersion = versionHeader != null ? Long.parseLong(versionHeader) : 0;

        VersionedDynamicFilterDomains result = taskManagerSupplier.get()
                .acknowledgeAndGetNewDynamicFilterDomains(taskId, currentVersion);
        sendJson(ctx, HttpResponseStatus.OK, dynamicFilterDomainsCodec.toJsonBytes(result));
    }

    // ── Data Plane Routes ──

    /** GET /v1/task/{taskId}/results/{bufferId}/{token} — fetch pages with async long-poll. */
    private void handleGetResults(ChannelHandlerContext ctx, FullHttpRequest request, Matcher matcher) {
        getResultsCount.incrementAndGet();
        TaskId taskId = TaskId.valueOf(matcher.group(1));
        OutputBufferId bufferId = new OutputBufferId(Integer.parseInt(matcher.group(2)));
        long token = Long.parseLong(matcher.group(3));

        DataSize maxSize = DEFAULT_MAX_SIZE;
        String maxSizeHeader = request.headers().get(HDR_MAX_SIZE);
        if (maxSizeHeader != null) {
            try { maxSize = DataSize.valueOf(maxSizeHeader); } catch (Exception ignored) {}
        }

        long maxWaitMs = DEFAULT_MAX_WAIT_MS;
        String maxWaitHeader = request.headers().get(HDR_MAX_WAIT);
        if (maxWaitHeader != null) {
            try { maxWaitMs = Duration.valueOf(maxWaitHeader).toMillis(); } catch (Exception ignored) {}
        }
        Duration waitTime = randomizeWaitTime(new Duration(maxWaitMs, MILLISECONDS));

        SqlTaskManager taskManager = taskManagerSupplier.get();
        SqlTaskWithResults taskWithResults = taskManager.getTaskResults(taskId, bufferId, token, maxSize);
        ListenableFuture<BufferResult> future = taskWithResults.getResultsFuture();

        if (!future.isDone()) {
            BufferResult emptyResult = BufferResult.emptyResults(taskWithResults.getTaskInstanceId(), token, false);
            future = addTimeout(future, () -> emptyResult, waitTime, timeoutExecutor);
        }

        Futures.addCallback(future, new FutureCallback<BufferResult>() {
            @Override
            public void onSuccess(BufferResult result) {
                try {
                    if (result.getSerializedPages().isEmpty()) {
                        writeEmptyResponse(ctx, taskWithResults, result);
                    } else {
                        writePagesResponse(ctx, taskWithResults, result);
                    }
                } catch (Exception e) {
                    log.error("Error writing pages response", e);
                    sendError(ctx, HttpResponseStatus.INTERNAL_SERVER_ERROR, e.getMessage());
                }
            }
            @Override
            public void onFailure(Throwable t) {
                log.error("Exchange future failed for task {}", taskId, t);
                sendError(ctx, HttpResponseStatus.INTERNAL_SERVER_ERROR, t.getMessage());
            }
        }, responseExecutor);
    }

    private void handleAcknowledge(ChannelHandlerContext ctx, Matcher matcher) {
        acknowledgeCount.incrementAndGet();
        TaskId taskId = TaskId.valueOf(matcher.group(1));
        OutputBufferId bufferId = new OutputBufferId(Integer.parseInt(matcher.group(2)));
        long token = Long.parseLong(matcher.group(3));
        // Offload acknowledge processing to response executor to avoid blocking Netty event loop.
        // acknowledgeTaskResults() acquires locks in ClientBuffer/OutputBufferMemoryManager.
        responseExecutor.execute(() -> {
            try {
                taskManagerSupplier.get().acknowledgeTaskResults(taskId, bufferId, token);
                ctx.writeAndFlush(new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.NO_CONTENT));
            } catch (Exception e) {
                log.error("Error acknowledging results for task {}", taskId, e);
                sendError(ctx, HttpResponseStatus.INTERNAL_SERVER_ERROR, e.getMessage());
            }
        });
    }

    private void handleDestroyBuffer(ChannelHandlerContext ctx, Matcher matcher) {
        destroyCount.incrementAndGet();
        TaskId taskId = TaskId.valueOf(matcher.group(1));
        OutputBufferId bufferId = new OutputBufferId(Integer.parseInt(matcher.group(2)));
        taskManagerSupplier.get().destroyTaskResults(taskId, bufferId);
        ctx.writeAndFlush(new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.NO_CONTENT));
    }

    // ── Stats Route ──

    /** GET /v1/stats — returns JSON counters. ?reset=true resets after reading. */
    private void handleStats(ChannelHandlerContext ctx, FullHttpRequest request) {
        boolean reset = getQueryParamBool(request.uri(), "reset", false);
        String json;
        if (reset) {
            json = String.format(
                    "{\"post_task\":%d,\"get_task_info\":%d,\"get_status\":%d,\"get_results\":%d,\"acknowledge\":%d,\"destroy\":%d,\"delete_task\":%d,\"fail_task\":%d,\"dynamic_filters\":%d}",
                    postTaskCount.getAndSet(0), getTaskInfoCount.getAndSet(0), getStatusCount.getAndSet(0),
                    getResultsCount.getAndSet(0), acknowledgeCount.getAndSet(0), destroyCount.getAndSet(0),
                    deleteTaskCount.getAndSet(0), failTaskCount.getAndSet(0), dynamicFiltersCount.getAndSet(0));
        } else {
            json = String.format(
                    "{\"post_task\":%d,\"get_task_info\":%d,\"get_status\":%d,\"get_results\":%d,\"acknowledge\":%d,\"destroy\":%d,\"delete_task\":%d,\"fail_task\":%d,\"dynamic_filters\":%d}",
                    postTaskCount.get(), getTaskInfoCount.get(), getStatusCount.get(),
                    getResultsCount.get(), acknowledgeCount.get(), destroyCount.get(),
                    deleteTaskCount.get(), failTaskCount.get(), dynamicFiltersCount.get());
        }
        sendJson(ctx, HttpResponseStatus.OK, json.getBytes(StandardCharsets.UTF_8));
    }

    /** Returns current stats and resets all counters. */
    public static java.util.Map<String, Long> getAndResetStats() {
        return java.util.Map.of(
                "post_task", postTaskCount.getAndSet(0),
                "get_task_info", getTaskInfoCount.getAndSet(0),
                "get_status", getStatusCount.getAndSet(0),
                "get_results", getResultsCount.getAndSet(0),
                "acknowledge", acknowledgeCount.getAndSet(0),
                "destroy", destroyCount.getAndSet(0),
                "delete_task", deleteTaskCount.getAndSet(0),
                "fail_task", failTaskCount.getAndSet(0),
                "dynamic_filters", dynamicFiltersCount.getAndSet(0));
    }

    // ── Response Helpers ──

    private void writeEmptyResponse(ChannelHandlerContext ctx, SqlTaskWithResults taskWithResults, BufferResult result) {
        taskWithResults.recordHeartbeat();
        FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.NO_CONTENT);
        setExchangeHeaders(response, taskWithResults, result);
        ctx.writeAndFlush(response);
    }

    private void writePagesResponse(ChannelHandlerContext ctx, SqlTaskWithResults taskWithResults, BufferResult result) {
        taskWithResults.recordHeartbeat();
        List<Slice> pages = result.getSerializedPages();
        long checksum = dataIntegrityVerificationEnabled ? calculateChecksum(pages) : NO_CHECKSUM;

        int pagesSize = 0;
        for (Slice page : pages) pagesSize += page.length();
        int bodySize = 4 + 8 + 4 + pagesSize;

        ByteBuf body = Unpooled.buffer(bodySize);
        body.order(ByteOrder.LITTLE_ENDIAN);
        body.writeIntLE(SERIALIZED_PAGES_MAGIC);
        body.writeLongLE(checksum);
        body.writeIntLE(pages.size());
        for (Slice page : pages) body.writeBytes(page.getBytes());

        FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK, body);
        response.headers().set(HttpHeaderNames.CONTENT_TYPE, TRINO_PAGES_CONTENT_TYPE);
        response.headers().set(HttpHeaderNames.CONTENT_LENGTH, bodySize);
        setExchangeHeaders(response, taskWithResults, result);
        ctx.writeAndFlush(response);
    }

    private void setExchangeHeaders(FullHttpResponse response, SqlTaskWithResults taskWithResults, BufferResult result) {
        response.headers().set(HDR_TASK_INSTANCE_ID, taskWithResults.getTaskInstanceId());
        response.headers().set(HDR_PAGE_SEQUENCE_ID, result.getToken());
        response.headers().set(HDR_PAGE_END_SEQUENCE_ID, result.getNextToken());
        response.headers().set(HDR_BUFFER_COMPLETE, result.isBufferComplete());
        response.headers().set(HDR_TASK_FAILED, taskWithResults.isTaskFailedOrFailing());
    }

    private void sendJson(ChannelHandlerContext ctx, HttpResponseStatus status, byte[] json) {
        ByteBuf body = Unpooled.wrappedBuffer(json);
        FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status, body);
        response.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/json");
        response.headers().set(HttpHeaderNames.CONTENT_LENGTH, json.length);
        ctx.writeAndFlush(response);
    }

    private void sendError(ChannelHandlerContext ctx, HttpResponseStatus status, String message) {
        byte[] bytes = (message != null ? message : "Internal error").getBytes(StandardCharsets.UTF_8);
        ByteBuf body = Unpooled.wrappedBuffer(bytes);
        FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status, body);
        response.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain");
        response.headers().set(HttpHeaderNames.CONTENT_LENGTH, bytes.length);
        ctx.writeAndFlush(response);
    }

    private static Duration randomizeWaitTime(Duration maxWait) {
        long halfWait = maxWait.toMillis() / 2;
        return new Duration(halfWait + ThreadLocalRandom.current().nextLong(Math.max(halfWait, 1)), MILLISECONDS);
    }

    private static boolean shouldSummarize(String uri) {
        return uri.contains("summarize");
    }

    private static boolean getQueryParamBool(String uri, String param, boolean defaultValue) {
        QueryStringDecoder decoder = new QueryStringDecoder(uri);
        List<String> values = decoder.parameters().get(param);
        if (values == null || values.isEmpty()) return defaultValue;
        return Boolean.parseBoolean(values.get(0));
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("Exchange handler uncaught exception", cause);
        ctx.close();
    }
}
