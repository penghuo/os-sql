# Trino Distributed Execution Across OpenSearch Nodes

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace `DistributedQueryRunner` (in-process simulation) with real distributed execution where Trino stages are dispatched to remote OpenSearch nodes via TransportActions. No fallback mode — one code path for single-node and multi-node.

**Prerequisite:** Phase 1–4 of `2026-04-04-trino-opensearch-implementation-plan.md` complete. The following already exist:
- `TrinoEngine` — embedded Trino via `DistributedQueryRunner` (to be replaced)
- `OpenSearchNodeManager` — maps cluster topology to Trino `InternalNodeManager`
- `OpenSearchTaskExecutor` — split execution via OpenSearch thread pools
- `CircuitBreakerMemoryPool` — memory accounting
- `RestTrinoQueryAction` — `/_plugins/_trino_sql/v1/statement` REST endpoint
- 328+ Trino tests + 43 ClickBench queries passing

**Architecture:** Every OpenSearch node runs the full Trino engine (coordinator + worker, symmetric design). The node that receives the REST request becomes the coordinator for that query. The coordinator plans the query into stages, dispatches plan fragments to worker nodes via OpenSearch TransportActions, and collects results via the same transport layer. TRINO_PAGES binary is passed through as opaque bytes — no re-serialization.

**Key insight:** Single-node and multi-node run identical code. On a single node, all TransportActions are delivered via loopback (in-memory). On multiple nodes, they go over TCP. The only variable is how many nodes `OpenSearchNodeManager` reports to Trino's `NodeScheduler`.

**Design principles:**
- **NEVER FALLBACK.** No `DistributedQueryRunner`. No in-process fake multi-node. No feature flag to switch between old and new. `DistributedQueryRunner` is deleted from source, removed from dependencies, and purged from the shadow jar. If the transport engine doesn't work, we fix it — we don't fall back.
- No HTTP server. Transport only.
- No new thread pools beyond what exists (`trino_query`, `trino_exchange`, `trino_scheduler`).
- Trino kernel (parser, planner, optimizer, operators) is untouched. Only the infrastructure layer changes.

---

## Architecture

### Data Flow

```
POST /_plugins/_trino_sql/v1/statement (hits any node)
        │
        ▼
   TrinoEngine.executeAndSerializeJson()
        │
        ▼
   Parser → Analyzer → Optimizer → DistributedPlanner
        │
        ▼ produces PlanFragment per stage
   SqlQueryScheduler
        │
        ├─── Stage 0 (leaf scan): assign splits to nodes
        │    ├── Node A: TransportRemoteTask.start() → loopback trino:task/update
        │    ├── Node B: TransportRemoteTask.start() → network trino:task/update
        │    └── Node C: TransportRemoteTask.start() → network trino:task/update
        │
        ├─── Stage 1 (partial agg): depends on Stage 0 output
        │    Workers pull pages from Stage 0 via trino:task/results
        │    (hash partitioned shuffle across nodes)
        │
        └─── Stage 2 (root): runs on coordinator
             Pulls final pages from Stage 1 via trino:task/results
             Serializes to JSON → REST response
```

### Transport Actions

| Action Name | Direction | Payload | Purpose |
|---|---|---|---|
| `trino:task/update` | Coordinator → Worker | PlanFragment JSON + splits JSON + output buffer config | Create or update a task on a worker |
| `trino:task/results` | Downstream → Upstream | Request: taskId + bufferId + token + maxBytes | Fetch TRINO_PAGES binary from OutputBuffer |
| `trino:task/results/ack` | Downstream → Upstream | taskId + bufferId + token | Acknowledge consumed pages (flow control) |
| `trino:task/status` | Coordinator → Worker | taskId | Poll task state (RUNNING/FINISHED/FAILED) |
| `trino:task/cancel` | Coordinator → Worker | taskId | Cancel a running task |

### Component Ownership

```
Coordinator side (on the node that received REST request):
  TrinoEngine
    → SqlQueryScheduler
      → NodeScheduler (uses OpenSearchNodeManager for topology)
      → TransportRemoteTaskFactory → creates TransportRemoteTask per (stage, node)
      → TransportExchangeClient → pulls pages for root stage

Worker side (on every node including coordinator):
  OpenSearchSqlTaskManager
    → receives trino:task/update → creates SqlTask → creates Drivers
    → OpenSearchTaskExecutor → runs SplitRunner.processFor() cooperatively
    → OutputBuffer → serves pages via trino:task/results handler
    → CircuitBreakerMemoryPool → memory accounting
```

---

## TDD Progression

```
Phase 5: Distributed Execution
  Task 12 — OpenSearchSqlTaskManager: worker-side task lifecycle
  Task 13 — Transport Actions: control plane (task/update, task/status, task/cancel)
  Task 14 — Transport Actions: data plane (task/results, task/results/ack)
  Task 15 — TransportRemoteTask: coordinator dispatches work via transport
  Task 16 — TransportExchangeClient: page fetching via transport
  Task 17 — Integration: delete DistributedQueryRunner, wire real engine
  Task 18 — Multi-node integration tests: prove distributed execution

Phase 6: ClickBench Benchmarks (single-node and multi-node)
  Task 19 — Re-run ClickBench Iceberg benchmark on single-node (transport engine)
  Task 20 — Run ClickBench Iceberg benchmark on multi-node Docker cluster
```

**Critical invariant:** After every task, all 328+ Trino tests + 43 ClickBench queries still pass. No regressions. During Tasks 12–16, tests run on `DistributedQueryRunner` temporarily (new components are built but not yet wired). Task 17 is the cutover: `DistributedQueryRunner` is deleted, tests run on the transport engine, and there is NO way to go back. If tests fail after Task 17, fix the transport engine — never reintroduce `DistributedQueryRunner`.

---

## File Structure

### New files in `trino/trino-opensearch/`

```
trino/trino-opensearch/src/main/java/org/opensearch/sql/trino/
├── execution/
│   ├── OpenSearchTaskExecutor.java          # EXISTS — split scheduling
│   └── OpenSearchSqlTaskManager.java        # NEW — worker-side task lifecycle
├── transport/
│   ├── TrinoTaskUpdateAction.java           # NEW — action type constant
│   ├── TrinoTaskUpdateRequest.java          # NEW — serializable request
│   ├── TrinoTaskUpdateResponse.java         # NEW — serializable response
│   ├── TransportTrinoTaskUpdateAction.java  # NEW — handler (worker side)
│   ├── TrinoTaskStatusAction.java           # NEW — action type
│   ├── TrinoTaskStatusRequest.java          # NEW
│   ├── TrinoTaskStatusResponse.java         # NEW
│   ├── TransportTrinoTaskStatusAction.java  # NEW — handler
│   ├── TrinoTaskCancelAction.java           # NEW — action type
│   ├── TrinoTaskCancelRequest.java          # NEW
│   ├── TransportTrinoTaskCancelAction.java  # NEW — handler
│   ├── TrinoTaskResultsAction.java          # NEW — action type
│   ├── TrinoTaskResultsRequest.java         # NEW
│   ├── TrinoTaskResultsResponse.java        # NEW — contains TRINO_PAGES bytes
│   ├── TransportTrinoTaskResultsAction.java # NEW — handler
│   ├── TrinoTaskResultsAckAction.java       # NEW — action type
│   ├── TrinoTaskResultsAckRequest.java      # NEW
│   ├── TransportTrinoTaskResultsAckAction.java # NEW — handler
│   ├── TransportRemoteTask.java             # NEW — RemoteTask via transport
│   ├── TransportRemoteTaskFactory.java      # NEW — factory
│   ├── TransportExchangeClient.java         # NEW — page fetching via transport
│   ├── TransportExchangeClientFactory.java  # NEW — factory
│   └── TransportPageBufferClient.java       # NEW — per-node page fetcher
├── bootstrap/
│   ├── TrinoEngine.java                     # MODIFY — delete DistributedQueryRunner
│   └── TrinoEngineModule.java               # NEW — Guice module wiring all components
├── node/
│   └── OpenSearchNodeManager.java           # EXISTS — cluster topology
├── memory/
│   └── CircuitBreakerMemoryPool.java        # EXISTS — memory pool
├── plugin/
│   ├── TrinoSettings.java                   # EXISTS
│   └── (registered in SQLPlugin.java)       # MODIFY — add transport actions
└── rest/
    └── RestTrinoQueryAction.java            # EXISTS — REST endpoint
```

### New test files

```
trino/trino-opensearch/src/test/java/org/opensearch/sql/trino/
├── execution/
│   ├── OpenSearchTaskExecutorTest.java      # EXISTS
│   └── OpenSearchSqlTaskManagerTest.java    # NEW
├── transport/
│   ├── TrinoTaskUpdateSerializationTest.java    # NEW — round-trip tests
│   ├── TrinoTaskResultsSerializationTest.java   # NEW
│   ├── TransportRemoteTaskTest.java             # NEW — mock transport
│   └── TransportExchangeClientTest.java         # NEW — mock transport

trino/trino-integ-test/src/test/java/org/opensearch/sql/trino/
├── DistributedExecutionIT.java              # NEW — multi-node cluster tests
└── DistributedClickBenchIT.java             # NEW — ClickBench on multi-node
```

---

## Task 12: OpenSearchSqlTaskManager — Worker-Side Task Lifecycle

**Goal:** Every OpenSearch node can receive a plan fragment, create drivers, execute splits, and serve output pages. This is the worker-side component.

**Files:**
- Create: `trino/.../execution/OpenSearchSqlTaskManager.java`
- Create: `trino/.../execution/OpenSearchSqlTaskManagerTest.java`

**Design:** Wrap Trino's `SqlTaskManager` — do not reimplement. `SqlTaskManager` manages:
- Task creation from `PlanFragment` + splits
- Driver lifecycle (create, run, finish)
- OutputBuffer management (pages produced by operators)
- Task state machine (PLANNED → RUNNING → FLUSHING → FINISHED/FAILED)
- Memory tracking per task

We need to instantiate `SqlTaskManager` with our OpenSearch-backed components:
- `OpenSearchTaskExecutor` for split scheduling
- `CircuitBreakerMemoryPool` for memory
- Trino's own `LocalExecutionPlanner` for creating drivers from plan fragments

- [ ] **Step 1: Write unit tests**

```java
package org.opensearch.sql.trino.execution;

/**
 * Tests for OpenSearchSqlTaskManager.
 * Verifies that plan fragments can be submitted and executed locally.
 */
public class OpenSearchSqlTaskManagerTest {

    // Test: submit a simple plan fragment (table scan + project), verify task transitions to FINISHED
    @Test
    public void submitPlanFragmentProducesFinishedTask() { ... }

    // Test: submit plan fragment with splits, verify splits are processed
    @Test
    public void splitsAreProcessed() { ... }

    // Test: read output pages from completed task
    @Test
    public void outputPagesReadable() { ... }

    // Test: cancel a running task
    @Test
    public void cancelTransitionsToAborted() { ... }

    // Test: task reports correct state at each lifecycle point
    @Test
    public void taskStateTransitions() { ... }

    // Test: memory is reserved and released during task execution
    @Test
    public void memoryAccountingDuringExecution() { ... }
}
```

- [ ] **Step 2: Implement OpenSearchSqlTaskManager**

```java
package org.opensearch.sql.trino.execution;

/**
 * Worker-side task manager. Receives plan fragments via transport actions,
 * creates local Trino tasks, executes them, and serves output pages.
 *
 * Wraps Trino's SqlTaskManager — does NOT reimplement task lifecycle.
 */
public class OpenSearchSqlTaskManager {

    private final SqlTaskManager delegate;

    public OpenSearchSqlTaskManager(
            LocalExecutionPlanner planner,
            OpenSearchTaskExecutor taskExecutor,
            CircuitBreakerMemoryPool memoryPool,
            NodeInfo nodeInfo) {
        // Construct SqlTaskManager with our OpenSearch-backed components
        this.delegate = new SqlTaskManager(
            planner,
            taskExecutor,
            memoryPool,
            ...);
    }

    /**
     * Called by TransportTrinoTaskUpdateAction when a coordinator dispatches work.
     * Creates or updates a local task from the given plan fragment and splits.
     */
    public TaskInfo updateTask(
            TaskId taskId,
            PlanFragment fragment,
            List<SplitAssignment> splits,
            OutputBuffers outputBuffers) {
        return delegate.updateTask(
            /* session */ ...,
            taskId,
            Optional.of(fragment),
            splits,
            outputBuffers,
            /* dynamicFilterDomains */ Map.of());
    }

    /**
     * Called by TransportTrinoTaskResultsAction.
     * Returns a page of results from the given output buffer.
     */
    public BufferResult getTaskResults(TaskId taskId, int bufferId, long token, long maxBytes) {
        return delegate.getTaskResults(taskId, new OutputBufferId(bufferId), token, maxBytes);
    }

    /** Called by TransportTrinoTaskResultsAckAction. Advances the ack token for flow control. */
    public void acknowledgeTaskResults(TaskId taskId, int bufferId, long token) {
        delegate.acknowledgeTaskResults(taskId, new OutputBufferId(bufferId), token);
    }

    /** Called by TransportTrinoTaskStatusAction. */
    public TaskInfo getTaskInfo(TaskId taskId) {
        return delegate.getTaskInfo(taskId);
    }

    /** Called by TransportTrinoTaskCancelAction. */
    public TaskInfo cancelTask(TaskId taskId) {
        return delegate.cancelTask(taskId);
    }
}
```

- [ ] **Step 3: Instantiate in TrinoPlugin.createComponents()**

The `OpenSearchSqlTaskManager` must be created on every node during plugin initialization. It needs Trino's `LocalExecutionPlanner` which requires the full type system, function registry, and planner context. These come from the Trino engine bootstrap.

```java
// In TrinoPlugin.createComponents()
OpenSearchSqlTaskManager taskManager = new OpenSearchSqlTaskManager(
    planner, taskExecutor, memoryPool, nodeInfo);
// Register as a component so transport actions can inject it
```

- [ ] **Step 4: Run all existing tests — no regressions**

```bash
./gradlew :trino:trino-opensearch:test
./gradlew :trino:trino-integ-test:test --tests "*TestTrino*"
```

- [ ] **Step 5: Commit**

```bash
git commit -s -m "feat(trino): OpenSearchSqlTaskManager — worker-side task lifecycle"
```

---

## Task 13: Transport Actions — Control Plane

**Goal:** Coordinator can create/update tasks, poll status, and cancel tasks on any node in the cluster.

**Files:**
- Create: `trino/.../transport/TrinoTaskUpdateAction.java`
- Create: `trino/.../transport/TrinoTaskUpdateRequest.java`
- Create: `trino/.../transport/TrinoTaskUpdateResponse.java`
- Create: `trino/.../transport/TransportTrinoTaskUpdateAction.java`
- Create: `trino/.../transport/TrinoTaskStatusAction.java`
- Create: `trino/.../transport/TrinoTaskStatusRequest.java`
- Create: `trino/.../transport/TrinoTaskStatusResponse.java`
- Create: `trino/.../transport/TransportTrinoTaskStatusAction.java`
- Create: `trino/.../transport/TrinoTaskCancelAction.java`
- Create: `trino/.../transport/TrinoTaskCancelRequest.java`
- Create: `trino/.../transport/TransportTrinoTaskCancelAction.java`
- Create: `trino/.../transport/TrinoTaskUpdateSerializationTest.java`
- Modify: `plugin/.../SQLPlugin.java` — register actions in `getActions()`

### Serialization Strategy

Trino's `PlanFragment` is a complex object graph. Use Trino's own Jackson `ObjectMapper` (which has all type serializers registered) to serialize to JSON bytes. Pass these bytes through OpenSearch's `StreamOutput` as opaque byte arrays. The worker deserializes using the same Trino `ObjectMapper`.

This avoids implementing custom serialization for every Trino internal type. The JSON format is also version-stable within a Trino release.

- [ ] **Step 1: Write serialization round-trip tests**

```java
package org.opensearch.sql.trino.transport;

/**
 * Verify PlanFragment and split assignments survive serialization round-trip
 * through OpenSearch's StreamOutput/StreamInput.
 */
public class TrinoTaskUpdateSerializationTest {

    // Test: PlanFragment → JSON bytes → StreamOutput → StreamInput → JSON bytes → PlanFragment
    @Test
    public void planFragmentSurvivesRoundTrip() {
        PlanFragment original = createSimpleScanFragment();
        TrinoTaskUpdateRequest request = new TrinoTaskUpdateRequest(
            TaskId.valueOf("query.0.0.0"),
            serializeFragment(original),
            serializeSplits(List.of()),
            serializeOutputBuffers(OutputBuffers.INITIAL_EMPTY_OUTPUT_BUFFERS));

        // Write to StreamOutput, read back from StreamInput
        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        TrinoTaskUpdateRequest deserialized = new TrinoTaskUpdateRequest(out.bytes().streamInput());

        // Verify round-trip fidelity
        PlanFragment recovered = deserializeFragment(deserialized.getPlanFragmentJson());
        assertEquals(original.getId(), recovered.getId());
        assertEquals(original.getPartitioning(), recovered.getPartitioning());
    }

    // Test: large payload (realistic PlanFragment with many operators)
    @Test
    public void largePlanFragmentSurvivesRoundTrip() { ... }

    // Test: splits with Trino connector-specific split data
    @Test
    public void splitAssignmentsSurviveRoundTrip() { ... }
}
```

- [ ] **Step 2: Implement TrinoTaskUpdateAction (action type)**

```java
package org.opensearch.sql.trino.transport;

import org.opensearch.action.ActionType;

public class TrinoTaskUpdateAction extends ActionType<TrinoTaskUpdateResponse> {
    public static final String NAME = "cluster:internal/trino/task/update";
    public static final TrinoTaskUpdateAction INSTANCE = new TrinoTaskUpdateAction();

    private TrinoTaskUpdateAction() {
        super(NAME, TrinoTaskUpdateResponse::new);
    }
}
```

- [ ] **Step 3: Implement TrinoTaskUpdateRequest**

```java
package org.opensearch.sql.trino.transport;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

/**
 * Request to create or update a Trino task on a worker node.
 * Carries PlanFragment, split assignments, and output buffer config
 * as JSON bytes (serialized by Trino's own ObjectMapper).
 */
public class TrinoTaskUpdateRequest extends ActionRequest {

    private final String taskId;
    private final byte[] planFragmentJson;
    private final byte[] splitAssignmentsJson;
    private final byte[] outputBuffersJson;
    private final byte[] sessionJson;

    public TrinoTaskUpdateRequest(String taskId, byte[] planFragmentJson,
            byte[] splitAssignmentsJson, byte[] outputBuffersJson, byte[] sessionJson) {
        this.taskId = taskId;
        this.planFragmentJson = planFragmentJson;
        this.splitAssignmentsJson = splitAssignmentsJson;
        this.outputBuffersJson = outputBuffersJson;
        this.sessionJson = sessionJson;
    }

    public TrinoTaskUpdateRequest(StreamInput in) throws IOException {
        super(in);
        this.taskId = in.readString();
        this.planFragmentJson = in.readByteArray();
        this.splitAssignmentsJson = in.readByteArray();
        this.outputBuffersJson = in.readByteArray();
        this.sessionJson = in.readByteArray();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(taskId);
        out.writeByteArray(planFragmentJson);
        out.writeByteArray(splitAssignmentsJson);
        out.writeByteArray(outputBuffersJson);
        out.writeByteArray(sessionJson);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    // Getters
    public String getTaskId() { return taskId; }
    public byte[] getPlanFragmentJson() { return planFragmentJson; }
    public byte[] getSplitAssignmentsJson() { return splitAssignmentsJson; }
    public byte[] getOutputBuffersJson() { return outputBuffersJson; }
    public byte[] getSessionJson() { return sessionJson; }
}
```

- [ ] **Step 4: Implement TrinoTaskUpdateResponse**

```java
package org.opensearch.sql.trino.transport;

import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

/**
 * Response from a task update. Contains serialized TaskInfo JSON
 * (task state, stats, error if any).
 */
public class TrinoTaskUpdateResponse extends ActionResponse {

    private final byte[] taskInfoJson;

    public TrinoTaskUpdateResponse(byte[] taskInfoJson) {
        this.taskInfoJson = taskInfoJson;
    }

    public TrinoTaskUpdateResponse(StreamInput in) throws IOException {
        super(in);
        this.taskInfoJson = in.readByteArray();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeByteArray(taskInfoJson);
    }

    public byte[] getTaskInfoJson() { return taskInfoJson; }
}
```

- [ ] **Step 5: Implement TransportTrinoTaskUpdateAction (handler)**

```java
package org.opensearch.sql.trino.transport;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.sql.trino.execution.OpenSearchSqlTaskManager;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

/**
 * Handles trino:task/update on the worker node.
 * Deserializes the plan fragment and delegates to OpenSearchSqlTaskManager.
 */
public class TransportTrinoTaskUpdateAction
        extends HandledTransportAction<TrinoTaskUpdateRequest, TrinoTaskUpdateResponse> {

    private final OpenSearchSqlTaskManager taskManager;
    private final TrinoJsonCodec codec;  // Trino's ObjectMapper wrapper

    @Inject
    public TransportTrinoTaskUpdateAction(
            TransportService transportService,
            ActionFilters actionFilters,
            OpenSearchSqlTaskManager taskManager,
            TrinoJsonCodec codec) {
        super(TrinoTaskUpdateAction.NAME, transportService, actionFilters,
              TrinoTaskUpdateRequest::new);
        this.taskManager = taskManager;
        this.codec = codec;
    }

    @Override
    protected void doExecute(Task task, TrinoTaskUpdateRequest request,
                            ActionListener<TrinoTaskUpdateResponse> listener) {
        try {
            PlanFragment fragment = codec.deserializePlanFragment(request.getPlanFragmentJson());
            List<SplitAssignment> splits = codec.deserializeSplits(
                request.getSplitAssignmentsJson());
            OutputBuffers buffers = codec.deserializeOutputBuffers(
                request.getOutputBuffersJson());
            Session session = codec.deserializeSession(request.getSessionJson());

            TaskInfo info = taskManager.updateTask(
                TaskId.valueOf(request.getTaskId()),
                fragment, splits, buffers, session);

            listener.onResponse(
                new TrinoTaskUpdateResponse(codec.serializeTaskInfo(info)));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }
}
```

- [ ] **Step 6: Implement status and cancel actions (same pattern)**

`TrinoTaskStatusAction`, `TrinoTaskStatusRequest`, `TrinoTaskStatusResponse`, `TransportTrinoTaskStatusAction` — follows same pattern as update. Request carries `taskId`, response carries `TaskInfo` JSON.

`TrinoTaskCancelAction`, `TrinoTaskCancelRequest`, `TransportTrinoTaskCancelAction` — request carries `taskId`, response is empty or carries final `TaskInfo`.

- [ ] **Step 7: Create TrinoJsonCodec — centralized serialization**

```java
package org.opensearch.sql.trino.transport;

/**
 * Wraps Trino's ObjectMapper for serializing/deserializing PlanFragment,
 * SplitAssignment, OutputBuffers, TaskInfo, and Session.
 *
 * Uses Trino's registered type serializers — do NOT write custom serialization.
 * The ObjectMapper is obtained from the Trino engine's Guice injector.
 */
public class TrinoJsonCodec {

    private final ObjectMapper trinoObjectMapper;

    public TrinoJsonCodec(ObjectMapper trinoObjectMapper) {
        this.trinoObjectMapper = trinoObjectMapper;
    }

    public byte[] serializePlanFragment(PlanFragment fragment) { ... }
    public PlanFragment deserializePlanFragment(byte[] json) { ... }

    public byte[] serializeSplits(List<SplitAssignment> splits) { ... }
    public List<SplitAssignment> deserializeSplits(byte[] json) { ... }

    public byte[] serializeOutputBuffers(OutputBuffers buffers) { ... }
    public OutputBuffers deserializeOutputBuffers(byte[] json) { ... }

    public byte[] serializeTaskInfo(TaskInfo info) { ... }
    public TaskInfo deserializeTaskInfo(byte[] json) { ... }

    public byte[] serializeSession(Session session) { ... }
    public Session deserializeSession(byte[] json) { ... }
}
```

- [ ] **Step 8: Register actions in SQLPlugin.getActions()**

```java
// In SQLPlugin.getActions(), add:
new ActionHandler<>(TrinoTaskUpdateAction.INSTANCE, TransportTrinoTaskUpdateAction.class),
new ActionHandler<>(TrinoTaskStatusAction.INSTANCE, TransportTrinoTaskStatusAction.class),
new ActionHandler<>(TrinoTaskCancelAction.INSTANCE, TransportTrinoTaskCancelAction.class),
```

- [ ] **Step 9: Run all existing tests — no regressions**

```bash
./gradlew :trino:trino-opensearch:test
./gradlew :trino:trino-integ-test:test --tests "*TestTrino*"
```

Actions are registered but not yet called by the engine. Tests still run on `DistributedQueryRunner` temporarily — this is the build phase, not the final state. Task 17 deletes `DistributedQueryRunner` permanently.

- [ ] **Step 10: Commit**

```bash
git commit -s -m "feat(trino): transport actions for task control plane (update/status/cancel)"
```

---

## Task 14: Transport Actions — Data Plane

**Goal:** Downstream stages can fetch TRINO_PAGES binary from upstream OutputBuffers via transport.

**Files:**
- Create: `trino/.../transport/TrinoTaskResultsAction.java`
- Create: `trino/.../transport/TrinoTaskResultsRequest.java`
- Create: `trino/.../transport/TrinoTaskResultsResponse.java`
- Create: `trino/.../transport/TransportTrinoTaskResultsAction.java`
- Create: `trino/.../transport/TrinoTaskResultsAckAction.java`
- Create: `trino/.../transport/TrinoTaskResultsAckRequest.java`
- Create: `trino/.../transport/TransportTrinoTaskResultsAckAction.java`
- Create: `trino/.../transport/TrinoTaskResultsSerializationTest.java`
- Modify: `plugin/.../SQLPlugin.java` — register actions

**Performance-critical.** Every shuffled page flows through `trino:task/results`. The TRINO_PAGES binary (Trino's columnar in-memory format) is passed through as opaque `byte[]` — never deserialized by OpenSearch.

- [ ] **Step 1: Write serialization tests**

```java
public class TrinoTaskResultsSerializationTest {

    // Test: request round-trip
    @Test
    public void requestSurvivesRoundTrip() {
        TrinoTaskResultsRequest request = new TrinoTaskResultsRequest(
            "query.0.0.0", 0, 42L, 1024 * 1024);

        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        TrinoTaskResultsRequest deserialized = new TrinoTaskResultsRequest(
            out.bytes().streamInput());

        assertEquals("query.0.0.0", deserialized.getTaskId());
        assertEquals(0, deserialized.getBufferId());
        assertEquals(42L, deserialized.getToken());
        assertEquals(1024 * 1024, deserialized.getMaxSizeBytes());
    }

    // Test: response with TRINO_PAGES bytes round-trip (opaque pass-through)
    @Test
    public void responseWithPageBytesSurvivesRoundTrip() {
        byte[] trinoPages = new byte[]{0x01, 0x02, 0x03, ...}; // sample binary
        TrinoTaskResultsResponse response = new TrinoTaskResultsResponse(
            42L, 43L, false, trinoPages);

        BytesStreamOutput out = new BytesStreamOutput();
        response.writeTo(out);
        TrinoTaskResultsResponse deserialized = new TrinoTaskResultsResponse(
            out.bytes().streamInput());

        assertArrayEquals(trinoPages, deserialized.getPages());
        assertEquals(42L, deserialized.getToken());
        assertEquals(43L, deserialized.getNextToken());
        assertFalse(deserialized.isBufferComplete());
    }

    // Test: response with empty pages (buffer not ready yet)
    @Test
    public void emptyResponseWhenBufferNotReady() { ... }

    // Test: response with bufferComplete=true (no more pages)
    @Test
    public void bufferCompleteSignalsEnd() { ... }
}
```

- [ ] **Step 2: Implement TrinoTaskResultsRequest**

```java
public class TrinoTaskResultsRequest extends ActionRequest {

    private final String taskId;
    private final int bufferId;
    private final long token;         // sequence number for exactly-once delivery
    private final long maxSizeBytes;  // flow control: max bytes to return

    // Constructor, StreamInput constructor, writeTo, getters
}
```

- [ ] **Step 3: Implement TrinoTaskResultsResponse**

```java
public class TrinoTaskResultsResponse extends ActionResponse {

    private final long token;
    private final long nextToken;
    private final boolean bufferComplete;
    private final byte[] pages;  // TRINO_PAGES binary — opaque, never deserialized

    // Constructor, StreamInput constructor, writeTo, getters
}
```

- [ ] **Step 4: Implement TransportTrinoTaskResultsAction (handler)**

```java
public class TransportTrinoTaskResultsAction
        extends HandledTransportAction<TrinoTaskResultsRequest, TrinoTaskResultsResponse> {

    private final OpenSearchSqlTaskManager taskManager;

    @Override
    protected void doExecute(Task task, TrinoTaskResultsRequest request,
                            ActionListener<TrinoTaskResultsResponse> listener) {
        try {
            BufferResult result = taskManager.getTaskResults(
                TaskId.valueOf(request.getTaskId()),
                request.getBufferId(),
                request.getToken(),
                request.getMaxSizeBytes());

            // Serialize pages to TRINO_PAGES binary via Trino's PagesSerdeUtil
            byte[] pagesBytes = PagesSerdeUtil.serialize(result.getPages());

            listener.onResponse(new TrinoTaskResultsResponse(
                result.getToken(),
                result.getNextToken(),
                result.isBufferComplete(),
                pagesBytes));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }
}
```

- [ ] **Step 5: Implement ack action (same pattern)**

`TrinoTaskResultsAckAction` + `TrinoTaskResultsAckRequest` + handler. Request carries `taskId`, `bufferId`, `token`. Response is empty. Handler calls `taskManager.acknowledgeTaskResults(...)`.

- [ ] **Step 6: Register in SQLPlugin.getActions()**

```java
new ActionHandler<>(TrinoTaskResultsAction.INSTANCE, TransportTrinoTaskResultsAction.class),
new ActionHandler<>(TrinoTaskResultsAckAction.INSTANCE, TransportTrinoTaskResultsAckAction.class),
```

- [ ] **Step 7: Run all existing tests — no regressions**

- [ ] **Step 8: Commit**

```bash
git commit -s -m "feat(trino): transport actions for data plane (results/ack)"
```

---

## Task 15: TransportRemoteTask — Replace HttpRemoteTask

**Goal:** The coordinator dispatches plan fragments to worker nodes via OpenSearch TransportActions instead of HTTP.

**Files:**
- Create: `trino/.../transport/TransportRemoteTask.java`
- Create: `trino/.../transport/TransportRemoteTaskFactory.java`
- Create: `trino/.../transport/TransportRemoteTaskTest.java`

**`RemoteTask` is Trino's interface for coordinator→worker communication.** The coordinator creates one `RemoteTask` per (stage, worker node). Trino's production implementation is `HttpRemoteTask`. Ours is `TransportRemoteTask`.

- [ ] **Step 1: Write tests with mock transport**

```java
public class TransportRemoteTaskTest {

    // Test: start() sends trino:task/update to the target node
    @Test
    public void startSendsTaskUpdateToTargetNode() {
        MockTransportService transport = createMockTransport();
        DiscoveryNode targetNode = createTestNode("node-1");

        TransportRemoteTask task = new TransportRemoteTask(
            transport, targetNode, taskId, planFragment, session, codec);
        task.start();

        List<CapturedRequest> requests =
            transport.getCapturedRequests(TrinoTaskUpdateAction.NAME);
        assertEquals(1, requests.size());
        assertEquals(targetNode, requests.get(0).getTargetNode());
    }

    // Test: addSplits() sends incremental update
    @Test
    public void addSplitsSendsIncrementalUpdate() { ... }

    // Test: getTaskInfo() returns latest known state
    @Test
    public void getTaskInfoReturnsLatestState() { ... }

    // Test: cancel() sends cancel action
    @Test
    public void cancelSendsCancelAction() { ... }

    // Test: transport failure triggers task failure callback
    @Test
    public void transportFailureTriggersTaskFailure() { ... }

    // Test: response updates task state
    @Test
    public void responseUpdatesTaskState() { ... }
}
```

- [ ] **Step 2: Implement TransportRemoteTask**

```java
package org.opensearch.sql.trino.transport;

/**
 * Coordinator-side RemoteTask that dispatches work to a worker node
 * via OpenSearch TransportActions. Replaces Trino's HttpRemoteTask.
 *
 * Lifecycle:
 *   1. start() — sends PlanFragment + initial splits via trino:task/update
 *   2. addSplits() — batches new splits, sends via trino:task/update
 *   3. Periodically polls status via trino:task/status (or uses push from response)
 *   4. cancel()/abort() — sends trino:task/cancel
 */
public class TransportRemoteTask implements RemoteTask {

    private final TransportService transportService;
    private final DiscoveryNode targetNode;
    private final TaskId taskId;
    private final PlanFragment planFragment;
    private final Session session;
    private final TrinoJsonCodec codec;
    private final StateMachine<TaskInfo> taskInfo;

    // Pending splits batched for next update
    private final SetMultimap<PlanNodeId, Split> pendingSplits;
    private final Set<PlanNodeId> noMoreSplits;

    @Override
    public void start() {
        sendUpdate();
    }

    @Override
    public void addSplits(Multimap<PlanNodeId, Split> splits) {
        pendingSplits.putAll(splits);
        sendUpdate();  // batch and send
    }

    @Override
    public void noMoreSplits(PlanNodeId sourceId) {
        noMoreSplits.add(sourceId);
        sendUpdate();
    }

    @Override
    public void cancel() {
        transportService.sendRequest(
            targetNode,
            TrinoTaskCancelAction.NAME,
            new TrinoTaskCancelRequest(taskId.toString()),
            new ActionListenerResponseHandler<>(...));
    }

    private void sendUpdate() {
        // Drain pending splits into the request
        List<SplitAssignment> assignments = drainPendingSplits();

        TrinoTaskUpdateRequest request = new TrinoTaskUpdateRequest(
            taskId.toString(),
            codec.serializePlanFragment(planFragment),
            codec.serializeSplits(assignments),
            codec.serializeOutputBuffers(outputBuffers),
            codec.serializeSession(session));

        transportService.sendRequest(
            targetNode,
            TrinoTaskUpdateAction.NAME,
            request,
            new ActionListenerResponseHandler<>(
                ActionListener.wrap(
                    response -> {
                        TaskInfo info = codec.deserializeTaskInfo(response.getTaskInfoJson());
                        taskInfo.set(info);
                    },
                    this::failTask),
                TrinoTaskUpdateResponse::new));
    }

    // ... remaining RemoteTask interface methods
}
```

- [ ] **Step 3: Implement TransportRemoteTaskFactory**

```java
/**
 * Factory bound in Guice. SqlQueryScheduler calls this to create
 * RemoteTask instances for each (stage, worker) pair.
 */
public class TransportRemoteTaskFactory implements RemoteTaskFactory {

    private final TransportService transportService;
    private final OpenSearchNodeManager nodeManager;
    private final TrinoJsonCodec codec;

    @Override
    public RemoteTask createRemoteTask(
            Session session,
            TaskId taskId,
            InternalNode node,
            PlanFragment fragment,
            Multimap<PlanNodeId, Split> initialSplits,
            OutputBuffers outputBuffers,
            ...) {

        // Map Trino InternalNode to OpenSearch DiscoveryNode
        DiscoveryNode osNode = nodeManager.toDiscoveryNode(node);

        return new TransportRemoteTask(
            transportService, osNode, taskId, fragment, session, codec, ...);
    }
}
```

- [ ] **Step 4: Run all existing tests — no regressions**

Factory is created but not yet wired. Existing path unchanged.

- [ ] **Step 5: Commit**

```bash
git commit -s -m "feat(trino): TransportRemoteTask dispatches work via transport actions"
```

---

## Task 16: TransportExchangeClient — Page Fetching via Transport

**Goal:** Downstream stages fetch TRINO_PAGES from upstream OutputBuffers via transport instead of HTTP.

**Files:**
- Create: `trino/.../transport/TransportPageBufferClient.java`
- Create: `trino/.../transport/TransportExchangeClient.java`
- Create: `trino/.../transport/TransportExchangeClientFactory.java`
- Create: `trino/.../transport/TransportExchangeClientTest.java`

- [ ] **Step 1: Write tests**

```java
public class TransportExchangeClientTest {

    // Test: fetches pages from a single upstream node
    @Test
    public void fetchesPagesFromSingleSource() { ... }

    // Test: fetches pages from multiple upstream nodes (merge)
    @Test
    public void fetchesPagesFromMultipleSources() { ... }

    // Test: handles buffer complete signal
    @Test
    public void detectsBufferComplete() { ... }

    // Test: sends ack after consuming pages
    @Test
    public void sendsAckAfterConsumption() { ... }

    // Test: retries on transport failure
    @Test
    public void retriesOnTransientFailure() { ... }

    // Test: respects maxSizeBytes flow control
    @Test
    public void respectsFlowControl() { ... }
}
```

- [ ] **Step 2: Implement TransportPageBufferClient**

```java
/**
 * Fetches pages from a single upstream node's OutputBuffer via transport.
 * One instance per (upstream task, buffer ID) pair.
 *
 * Implements token-based exactly-once delivery:
 *   - Request with token N
 *   - Receive pages [N, N+K)
 *   - Ack token N+K
 *   - Next request with token N+K
 */
public class TransportPageBufferClient {

    private final TransportService transportService;
    private final DiscoveryNode sourceNode;
    private final String taskId;
    private final int bufferId;
    private long currentToken = 0;

    /**
     * Fetch next batch of pages. Calls listener with TRINO_PAGES bytes.
     */
    public void getPages(long maxBytes,
                         ActionListener<TrinoTaskResultsResponse> listener) {
        transportService.sendRequest(
            sourceNode,
            TrinoTaskResultsAction.NAME,
            new TrinoTaskResultsRequest(taskId, bufferId, currentToken, maxBytes),
            new ActionListenerResponseHandler<>(
                ActionListener.wrap(
                    response -> {
                        currentToken = response.getNextToken();
                        sendAck(response.getToken());
                        listener.onResponse(response);
                    },
                    listener::onFailure),
                TrinoTaskResultsResponse::new));
    }

    private void sendAck(long token) {
        transportService.sendRequest(
            sourceNode,
            TrinoTaskResultsAckAction.NAME,
            new TrinoTaskResultsAckRequest(taskId, bufferId, token),
            EmptyTransportResponseHandler.INSTANCE);
    }
}
```

- [ ] **Step 3: Implement TransportExchangeClient**

```java
/**
 * Merges pages from multiple upstream TransportPageBufferClients.
 * Replaces Trino's HTTP-based ExchangeClient.
 *
 * Used by exchange operators in downstream stages to pull data from
 * upstream stages running on (potentially remote) nodes.
 */
public class TransportExchangeClient implements Closeable {

    private final List<TransportPageBufferClient> sources;
    private final LinkedBlockingQueue<byte[]> pageQueue;

    /**
     * Add a source (upstream node + task + buffer).
     * Called by the scheduler when it knows which nodes are running upstream stages.
     */
    public void addSource(DiscoveryNode node, String taskId, int bufferId) {
        TransportPageBufferClient client = new TransportPageBufferClient(
            transportService, node, taskId, bufferId);
        sources.add(client);
        // Start fetching pages asynchronously
        scheduleFetch(client);
    }

    /**
     * Get next page. Blocks until a page is available or all sources are complete.
     * Returns null when all sources are exhausted.
     */
    public byte[] getNextPage() { ... }

    private void scheduleFetch(TransportPageBufferClient client) { ... }
}
```

- [ ] **Step 4: Implement TransportExchangeClientFactory**

```java
public class TransportExchangeClientFactory {
    // Factory method called by Trino's exchange operator setup
    public TransportExchangeClient create(...) {
        return new TransportExchangeClient(transportService, ...);
    }
}
```

- [ ] **Step 5: Run all existing tests — no regressions**

- [ ] **Step 6: Commit**

```bash
git commit -s -m "feat(trino): TransportExchangeClient fetches pages via transport"
```

---

## Task 17: Integration — Delete DistributedQueryRunner, Wire Real Engine

**Goal:** Remove `DistributedQueryRunner`. Wire `TransportRemoteTaskFactory` and `TransportExchangeClientFactory` into the Trino engine. All queries now use transport-based execution.

**Files:**
- Modify: `trino/.../bootstrap/TrinoEngine.java` — complete rewrite
- Create: `trino/.../bootstrap/TrinoEngineModule.java` — Guice wiring
- Modify: `trino/.../plugin/TrinoPlugin.java` — initialization
- Modify: `trino/.../rest/RestTrinoQueryAction.java` — if needed

This is the hardest task. It replaces the entire execution backend.

- [ ] **Step 1: Create TrinoEngineModule — Guice wiring for all components**

```java
package org.opensearch.sql.trino.bootstrap;

/**
 * Guice module that wires all Trino engine components together.
 * Replaces the implicit wiring that DistributedQueryRunner did internally.
 *
 * Binds:
 *   InternalNodeManager     → OpenSearchNodeManager
 *   TaskExecutor            → OpenSearchTaskExecutor
 *   MemoryPool              → CircuitBreakerMemoryPool
 *   RemoteTaskFactory       → TransportRemoteTaskFactory
 *   ExchangeClientFactory   → TransportExchangeClientFactory
 *   SqlTaskManager          → OpenSearchSqlTaskManager
 *
 * Plus Trino's own internal modules:
 *   SqlParser, TypeRegistry, FunctionManager, PlanOptimizers,
 *   LocalExecutionPlanner, SplitManager, PageSourceManager, etc.
 *
 * These come from Trino's ServerMainModule — we include the parts we need
 * and override the infrastructure bindings with our OpenSearch implementations.
 */
public class TrinoEngineModule implements Module {
    @Override
    public void configure(Binder binder) {
        // Our OpenSearch implementations
        binder.bind(InternalNodeManager.class).to(OpenSearchNodeManager.class);
        binder.bind(TaskExecutor.class).to(OpenSearchTaskExecutor.class);
        binder.bind(MemoryPool.class).toInstance(
            CircuitBreakerMemoryPool.create(computeMemorySize()));
        binder.bind(RemoteTaskFactory.class).to(TransportRemoteTaskFactory.class);

        // Trino kernel — include modules from ServerMainModule that provide:
        // SqlParser, TypeManager, FunctionManager, StatementAnalyzerFactory,
        // PlanOptimizers, LocalExecutionPlanner, SplitManager, PageSourceManager,
        // TransactionManager, NodeScheduler, SqlQueryScheduler, DispatchManager
        install(new TrinoKernelModule());  // extracts non-server parts of ServerMainModule
    }
}
```

- [ ] **Step 2: Rewrite TrinoEngine — no DistributedQueryRunner**

```java
package org.opensearch.sql.trino.bootstrap;

/**
 * Embedded Trino engine. Uses Trino's real query engine with OpenSearch
 * transport for inter-node communication. No DistributedQueryRunner.
 * No HTTP server. No simulation.
 *
 * On single-node: all transport actions are loopback (in-memory).
 * On multi-node: transport actions go over TCP.
 * Same code path. Same components. Only topology differs.
 */
public class TrinoEngine implements Closeable {

    private final Injector injector;
    private final DispatchManager dispatchManager;
    private final SqlParser sqlParser;
    private final SessionPropertyManager sessionPropertyManager;

    TrinoEngine(Injector injector) {
        this.injector = injector;
        this.dispatchManager = injector.getInstance(DispatchManager.class);
        this.sqlParser = injector.getInstance(SqlParser.class);
        this.sessionPropertyManager = injector.getInstance(SessionPropertyManager.class);
    }

    /**
     * Execute SQL and return Trino client protocol JSON.
     * Uses DispatchManager → SqlQueryScheduler → TransportRemoteTask pipeline.
     */
    public String executeAndSerializeJson(String sql, String catalog, String schema) {
        String queryId = UUID.randomUUID().toString().replace("-", "");

        try {
            Session session = createSession(catalog, schema);

            // Submit to DispatchManager — this triggers the full pipeline:
            //   parse → analyze → plan → schedule → dispatch tasks via transport
            ListenableFuture<?> submitted = dispatchManager.createQuery(queryId, session, sql);
            submitted.get();  // wait for dispatch

            // Wait for query completion
            QueryInfo info = waitForCompletion(queryId);

            // Collect results from OutputBuffer of root stage
            return serializeToTrinoJson(queryId, info);
        } catch (Exception e) {
            return serializeErrorJson(queryId, e);
        }
    }

    @Override
    public void close() {
        injector.getInstance(LifeCycleManager.class).stop();
    }
}
```

- [ ] **Step 3: Initialize in TrinoPlugin.createComponents()**

```java
// Replace the old DistributedQueryRunner-based initialization with:
TrinoEngineModule module = new TrinoEngineModule(
    nodeManager, taskExecutor, memoryPool,
    transportService, clusterService);
Injector injector = Guice.createInjector(module);
engine = injector.getInstance(TrinoEngine.class);
taskManager = injector.getInstance(OpenSearchSqlTaskManager.class);
```

- [ ] **Step 4: Run single-node tests — must all pass**

```bash
# All 328+ Trino tests
./gradlew :trino:trino-integ-test:test --tests "*TestTrino*"

# All 43 ClickBench queries
./gradlew :trino:trino-integ-test:test --tests "*ClickBenchIT*"

# No external server test
./gradlew :trino:trino-integ-test:test --tests "*NoExternalServerTest*"
```

On single node, all transport actions go through loopback. This validates the full stack: serialization, task manager, exchange, everything.

- [ ] **Step 5: Delete ALL DistributedQueryRunner references — zero tolerance**

This is not optional. Every reference must be removed. No `if/else`, no feature flag, no "use DistributedQueryRunner when transport is unavailable." One code path.

```bash
# Search ENTIRE repo — not just trino/
grep -r "DistributedQueryRunner" --include="*.java" --include="*.gradle" --include="*.md" .
```

Specific files that currently reference `DistributedQueryRunner` (as of 2026-04-05):

| File | Action |
|---|---|
| `trino/trino-opensearch/src/main/java/.../TrinoEngine.java` | **Rewritten in Step 2** — no `DistributedQueryRunner` import, field, or usage |
| `trino/trino-opensearch/build.gradle` | **Remove** `shade "io.trino:trino-testing:${trinoVersion}"` and corresponding `compileOnly`. The transport engine does not need `trino-testing`. If other test utilities from `trino-testing` are needed, extract only those classes. |
| `trino/trino-integ-test/.../TestTrinoJoinQueries.java` | **Remove** `@Disabled("Requires DistributedQueryRunner...")` annotation and the comment. The transport engine supports full distributed execution — this test must now pass, not be skipped. |
| `trino/trino-integ-test/.../NoExternalServerTest.java` | **Update** comment "Works with DistributedQueryRunner which supports full distributed execution" → "Works with transport-based Trino engine which supports full distributed execution" |
| `trino/benchmark/RESULTS.md` | **Update** "embedded in OpenSearch via shadow jar + DistributedQueryRunner" → historical note only, or remove |
| `docs/benchmark/*.md` | **Update** any references to `DistributedQueryRunner` as historical context |

After cleanup:

```bash
# MUST return zero results from Java and Gradle files
grep -r "DistributedQueryRunner" --include="*.java" --include="*.gradle" .
# Result: (empty)

# Markdown files may mention it in historical context only (e.g., "previously used")
# but NEVER as a current or fallback execution path
```

- [ ] **Step 6: Remove trino-testing shade dependency if no longer needed**

```bash
# In trino/trino-opensearch/build.gradle:
# DELETE these lines:
#   shade("io.trino:trino-testing:${trinoVersion}") { ... }
#   compileOnly "io.trino:trino-testing:${trinoVersion}"
#
# The trino-integ-test module may still need trino-testing for test utilities
# (MaterializedResult, etc.) — that's fine, it's a TEST dependency there.
# But the MAIN module (trino-opensearch) must not depend on trino-testing at runtime.
```

Verify the shadow jar no longer bundles `DistributedQueryRunner`:

```bash
./gradlew :trino:trino-opensearch:shadowJar
jar tf trino/trino-opensearch/build/libs/trino-opensearch-*.jar | grep -i DistributedQueryRunner
# MUST return zero results
```

- [ ] **Step 7: Run full test suite — final validation**

```bash
./gradlew :trino:trino-opensearch:test
./gradlew :trino:trino-integ-test:test
```

ALL tests must pass on the transport engine. No skipped tests with `@Disabled` referencing `DistributedQueryRunner`. If a test previously required `DistributedQueryRunner` internals (like casting `QueryRunner` to `DistributedQueryRunner`), it must be rewritten to work with the transport engine or use `OpenSearchTrinoQueryRunner`.

- [ ] **Step 8: Commit**

```bash
git commit -s -m "feat(trino): delete DistributedQueryRunner, wire real transport-based engine

BREAKING: DistributedQueryRunner removed entirely. All execution uses
OpenSearch TransportActions. No fallback. Single-node and multi-node
run identical code paths — only topology differs."
```

---

## Task 18: Multi-Node Integration Tests — Prove Distributed Execution

**Goal:** Prove queries actually distribute across OpenSearch nodes. Three levels of proof: thread pool metrics, task registry, and transport action interception.

**Files:**
- Create: `trino/trino-integ-test/src/test/java/org/opensearch/sql/trino/DistributedExecutionIT.java`
- Create: `trino/trino-integ-test/src/test/java/org/opensearch/sql/trino/DistributedClickBenchIT.java`
- Modify: `trino/trino-integ-test/build.gradle` — add multi-node test cluster config

- [ ] **Step 1: Configure multi-node test cluster**

```groovy
// In trino/trino-integ-test/build.gradle
// Add a separate test task for multi-node tests
task distributedTest(type: Test) {
    useJUnitPlatform {
        includeTags 'distributed'
    }
    // OpenSearch test framework cluster config
    systemProperty 'tests.cluster.size', '3'
}

testClusters.distributedTest {
    plugin ':opensearch-sql-plugin'
    testDistribution = 'archive'
    numberOfNodes = 3
    setting 'plugins.trino.enabled', 'true'
}
```

- [ ] **Step 2: Add node stats REST endpoint**

Add a simple diagnostic endpoint to `RestTrinoQueryAction` (or a new handler):

```
GET /_plugins/_trino_sql/v1/node/stats
Returns: {
  "nodeId": "abc123",
  "tasksReceived": 7,
  "splitsProcessed": 42,
  "pagesProduced": 100,
  "bytesShuffled": 1048576
}
```

This is implemented in `OpenSearchSqlTaskManager` using atomic counters.

- [ ] **Step 3: Write DistributedExecutionIT**

```java
package org.opensearch.sql.trino;

import org.junit.jupiter.api.Tag;

/**
 * Proves queries distribute across OpenSearch nodes.
 * Runs on a 3-node cluster. Every test verifies ACTUAL distribution,
 * not just correctness.
 */
@Tag("distributed")
public class DistributedExecutionIT extends OpenSearchIntegTestCase {

    @Override
    protected int numberOfNodes() { return 3; }

    // ── Correctness Tests ──────────────────────────────────────

    @Test
    public void selectOneWorksOnEveryNode() throws Exception {
        // Send "SELECT 1" to each node, verify correct result
        for (String nodeId : getNodeIds()) {
            String result = executeOnNode(nodeId, "SELECT 1");
            assertContains(result, "\"data\":[[1]]");
        }
    }

    @Test
    public void aggregationQueryReturnsCorrectResult() throws Exception {
        String result = execute(
            "SELECT regionkey, count(*) FROM tpch.tiny.nation "
            + "GROUP BY regionkey ORDER BY regionkey");
        assertRowCount(result, 5);
    }

    @Test
    public void hashJoinReturnsCorrectResult() throws Exception {
        String result = execute(
            "SELECT n.name, r.name FROM tpch.tiny.nation n "
            + "JOIN tpch.tiny.region r ON n.regionkey = r.regionkey "
            + "ORDER BY n.name LIMIT 5");
        assertRowCount(result, 5);
    }

    // ── Distribution Proof: Thread Pool Metrics ────────────────

    @Test
    public void remoteNodesProcessSplits() throws Exception {
        // Record baseline trino_query completed count on all nodes
        Map<String, Long> before = getAllNodeTrinoQueryCounts();

        // Send query to node 0 only — this node is the coordinator
        String coordinatorId = getNodeIds().get(0);
        executeOnNode(coordinatorId,
            "SELECT orderkey, count(*) FROM tpch.sf1.lineitem GROUP BY orderkey");

        // Check: at least one NON-coordinator node did work
        Map<String, Long> after = getAllNodeTrinoQueryCounts();
        long remoteNodesWithWork = getNodeIds().stream()
            .filter(id -> !id.equals(coordinatorId))
            .filter(id -> after.get(id) > before.getOrDefault(id, 0L))
            .count();

        assertTrue(
            "Expected remote nodes to process splits, but none did. "
            + "Before: " + before + ", After: " + after,
            remoteNodesWithWork >= 1);
    }

    // ── Distribution Proof: Task Registry ──────────────────────

    @Test
    public void remoteNodesReceivedTasks() throws Exception {
        // Reset stats on all nodes
        resetAllNodeStats();

        // Execute on coordinator
        String coordinatorId = getNodeIds().get(0);
        executeOnNode(coordinatorId,
            "SELECT regionkey, count(*) FROM tpch.tiny.nation GROUP BY regionkey");

        // Verify at least one non-coordinator received tasks
        long remoteTaskTotal = getNodeIds().stream()
            .filter(id -> !id.equals(coordinatorId))
            .mapToLong(id -> getNodeStats(id).get("tasksReceived").asLong())
            .sum();

        assertTrue(
            "Remote nodes should have received tasks, got total: " + remoteTaskTotal,
            remoteTaskTotal > 0);
    }

    // ── Distribution Proof: Transport Action Count ─────────────

    @Test
    public void transportActionsFired() throws Exception {
        // Query node stats endpoint which tracks transport action counts
        resetAllNodeStats();

        execute("SELECT n.name, r.name FROM tpch.tiny.nation n "
            + "JOIN tpch.tiny.region r ON n.regionkey = r.regionkey");

        // Sum trino:task/update actions received across all nodes
        long totalTaskUpdates = getNodeIds().stream()
            .mapToLong(id -> getNodeStats(id).get("taskUpdatesReceived").asLong())
            .sum();

        // A GROUP BY or JOIN query produces at least 2 stages,
        // each dispatched to at least 1 node
        assertTrue(
            "Expected at least 2 task update actions, got " + totalTaskUpdates,
            totalTaskUpdates >= 2);

        // Sum trino:task/results actions (data plane)
        long totalResultsFetched = getNodeIds().stream()
            .mapToLong(id -> getNodeStats(id).get("resultsFetched").asLong())
            .sum();

        assertTrue(
            "Expected at least 1 results fetch (shuffle), got " + totalResultsFetched,
            totalResultsFetched >= 1);
    }

    // ── Fault Tolerance ────────────────────────────────────────

    @Test
    public void queryFailsCleanlyWhenWorkerDisappears() throws Exception {
        // Start a query, stop a worker mid-execution, verify coordinator
        // returns an error (not hang indefinitely)
        // This tests the transport failure → task failure → query failure path
    }

    // ── Helper Methods ─────────────────────────────────────────

    private String executeOnNode(String nodeId, String sql) { ... }
    private String execute(String sql) { ... }  // sends to any node
    private Map<String, Long> getAllNodeTrinoQueryCounts() { ... }
    private JsonNode getNodeStats(String nodeId) { ... }
    private void resetAllNodeStats() { ... }
}
```

- [ ] **Step 4: Write DistributedClickBenchIT**

```java
@Tag("distributed")
public class DistributedClickBenchIT extends OpenSearchIntegTestCase {

    @Override
    protected int numberOfNodes() { return 3; }

    @Test
    public void allClickBenchQueriesPassDistributed() throws Exception {
        List<String> queries = loadQueries("/clickbench/queries.sql");
        assertEquals(43, queries.size());

        int passed = 0;
        List<String> failures = new ArrayList<>();

        for (int i = 0; i < queries.size(); i++) {
            long start = System.currentTimeMillis();
            try {
                String result = execute(queries.get(i));
                assertContains(result, "\"state\":\"FINISHED\"");
                long elapsed = System.currentTimeMillis() - start;
                passed++;
                System.out.printf("Q%d: PASS (%dms)%n", i, elapsed);
            } catch (Exception e) {
                failures.add("Q" + i + ": " + e.getMessage());
                System.out.printf("Q%d: FAIL — %s%n", i, e.getMessage());
            }
        }

        System.out.printf("%n=== Distributed ClickBench: %d/%d ===%n",
            passed, queries.size());
        assertEquals("All 43 ClickBench queries should pass distributed",
            43, passed);
    }
}
```

- [ ] **Step 5: Run multi-node tests**

```bash
./gradlew :trino:trino-integ-test:distributedTest
```

- [ ] **Step 6: Run ALL tests — single-node and multi-node**

```bash
# Single-node (existing)
./gradlew :trino:trino-integ-test:test

# Multi-node (new)
./gradlew :trino:trino-integ-test:distributedTest
```

Both must pass. Same code path. Different topology.

- [ ] **Step 7: Commit**

```bash
git commit -s -m "feat(trino): multi-node integration tests proving distributed execution"
```

---

## Task 19: ClickBench Iceberg Benchmark — Single-Node (Transport Engine)

**Goal:** Re-run the full ClickBench Iceberg benchmark on a single OpenSearch node using the new transport-based engine. Compare against the previous `DistributedQueryRunner` baseline (82s total, 42/43 beat Spark) to verify no performance regression.

**Prerequisite:** Task 17 complete — `DistributedQueryRunner` deleted, transport engine wired.

**Files:**
- Modify: `trino/benchmark/run_clickbench_iceberg.sh` — if any flags need updating
- Create: `docs/benchmark/YYYYMMDD_clickbench_iceberg_transport_single_node.md` — results

**Environment (must match previous benchmark):**

| Setting | Value |
|---|---|
| Machine | Same machine as previous benchmark (32 CPUs, 123GB RAM) |
| Heap | 32GB JVM |
| Table | `iceberg.clickbench.hits` (99,997,497 rows, Parquet + Zstd) |
| Endpoint | `/_plugins/_trino_sql/v1/statement` |
| Runs | Best of 3 |

- [ ] **Step 1: Build plugin with transport engine**

```bash
./gradlew :opensearch-sql-plugin:bundlePlugin
```

- [ ] **Step 2: Verify Iceberg warehouse exists**

```bash
# Reuse existing warehouse from previous benchmark
ls /tmp/iceberg-clickbench-warehouse/clickbench/hits/
# If missing, recreate:
# python3 trino/benchmark/create_iceberg_table.py /tmp/iceberg-clickbench-warehouse
```

- [ ] **Step 3: Run benchmark**

```bash
OPENSEARCH_HEAP=32g TRIES=3 \
  ICEBERG_WAREHOUSE=/tmp/iceberg-clickbench-warehouse \
  ./trino/benchmark/run_clickbench_iceberg.sh
```

- [ ] **Step 4: Record results and compare against DistributedQueryRunner baseline**

Create `docs/benchmark/YYYYMMDD_clickbench_iceberg_transport_single_node.md` with:

```markdown
# ClickBench Benchmark: Transport Engine — Single Node

## Comparison: Transport Engine vs DistributedQueryRunner

| Query | Transport (s) | DistributedQR (s) | Delta |
|-------|--------------|-------------------|-------|
| Q0    | ...          | 0.04              | ...   |
| ...   | ...          | ...               | ...   |
| Total | ...          | 82                | ...   |
```

**Acceptance criteria:**
- 43/43 queries pass
- Total time within 10% of previous baseline (82s ± 8s)
- No individual query > 2x slower than baseline
- If regression found, profile and fix before proceeding

- [ ] **Step 5: Commit results**

```bash
git commit -s -m "perf(trino): ClickBench single-node benchmark with transport engine"
```

---

## Task 20: ClickBench Iceberg Benchmark — Multi-Node Docker Cluster

**Goal:** Run ClickBench on a real multi-node OpenSearch cluster using Docker Compose. Measure performance scaling from 1 to 3 nodes. Prove distributed execution delivers real speedup on analytical queries.

**Prerequisite:** Task 17 and Task 18 complete. Task 19 provides single-node baseline.

**Files:**
- Create: `trino/benchmark/docker/docker-compose.yml` — 3-node OpenSearch cluster
- Create: `trino/benchmark/docker/opensearch.yml` — shared config
- Create: `trino/benchmark/run_clickbench_iceberg_multinode.sh` — benchmark script
- Create: `docs/benchmark/YYYYMMDD_clickbench_iceberg_transport_multi_node.md` — results

### Docker Compose Setup

A 3-node OpenSearch cluster where each node has the SQL plugin with Trino enabled. All nodes share the Iceberg warehouse via a Docker volume mount.

- [ ] **Step 1: Create Docker Compose config**

```yaml
# trino/benchmark/docker/docker-compose.yml
version: '3.8'

services:
  opensearch-node1:
    image: opensearchproject/opensearch:3.6.0
    container_name: opensearch-node1
    environment:
      - cluster.name=clickbench-cluster
      - node.name=opensearch-node1
      - discovery.seed_hosts=opensearch-node1,opensearch-node2,opensearch-node3
      - cluster.initial_cluster_manager_nodes=opensearch-node1
      - bootstrap.memory_lock=true
      - "OPENSEARCH_JAVA_OPTS=-Xms16g -Xmx16g"
      - plugins.trino.enabled=true
      - plugins.trino.catalog.iceberg.warehouse=/data/iceberg-warehouse
      - plugins.security.disabled=true
    ulimits:
      memlock: { soft: -1, hard: -1 }
      nofile: { soft: 65536, hard: 65536 }
    volumes:
      - opensearch-data1:/usr/share/opensearch/data
      - ${ICEBERG_WAREHOUSE:-/tmp/iceberg-clickbench-warehouse}:/data/iceberg-warehouse:ro
      - ${PLUGIN_ZIP}:/tmp/opensearch-sql-plugin.zip:ro
    ports:
      - "9200:9200"
      - "9600:9600"
    networks:
      - opensearch-net
    # Install plugin on first boot
    entrypoint: >
      bash -c '
        if [ ! -d /usr/share/opensearch/plugins/opensearch-sql ]; then
          /usr/share/opensearch/bin/opensearch-plugin install --batch file:///tmp/opensearch-sql-plugin.zip
        fi
        /usr/share/opensearch/opensearch-docker-entrypoint.sh
      '

  opensearch-node2:
    image: opensearchproject/opensearch:3.6.0
    container_name: opensearch-node2
    environment:
      - cluster.name=clickbench-cluster
      - node.name=opensearch-node2
      - discovery.seed_hosts=opensearch-node1,opensearch-node2,opensearch-node3
      - cluster.initial_cluster_manager_nodes=opensearch-node1
      - bootstrap.memory_lock=true
      - "OPENSEARCH_JAVA_OPTS=-Xms16g -Xmx16g"
      - plugins.trino.enabled=true
      - plugins.trino.catalog.iceberg.warehouse=/data/iceberg-warehouse
      - plugins.security.disabled=true
    ulimits:
      memlock: { soft: -1, hard: -1 }
      nofile: { soft: 65536, hard: 65536 }
    volumes:
      - opensearch-data2:/usr/share/opensearch/data
      - ${ICEBERG_WAREHOUSE:-/tmp/iceberg-clickbench-warehouse}:/data/iceberg-warehouse:ro
      - ${PLUGIN_ZIP}:/tmp/opensearch-sql-plugin.zip:ro
    networks:
      - opensearch-net
    entrypoint: >
      bash -c '
        if [ ! -d /usr/share/opensearch/plugins/opensearch-sql ]; then
          /usr/share/opensearch/bin/opensearch-plugin install --batch file:///tmp/opensearch-sql-plugin.zip
        fi
        /usr/share/opensearch/opensearch-docker-entrypoint.sh
      '

  opensearch-node3:
    image: opensearchproject/opensearch:3.6.0
    container_name: opensearch-node3
    environment:
      - cluster.name=clickbench-cluster
      - node.name=opensearch-node3
      - discovery.seed_hosts=opensearch-node1,opensearch-node2,opensearch-node3
      - cluster.initial_cluster_manager_nodes=opensearch-node1
      - bootstrap.memory_lock=true
      - "OPENSEARCH_JAVA_OPTS=-Xms16g -Xmx16g"
      - plugins.trino.enabled=true
      - plugins.trino.catalog.iceberg.warehouse=/data/iceberg-warehouse
      - plugins.security.disabled=true
    ulimits:
      memlock: { soft: -1, hard: -1 }
      nofile: { soft: 65536, hard: 65536 }
    volumes:
      - opensearch-data3:/usr/share/opensearch/data
      - ${ICEBERG_WAREHOUSE:-/tmp/iceberg-clickbench-warehouse}:/data/iceberg-warehouse:ro
      - ${PLUGIN_ZIP}:/tmp/opensearch-sql-plugin.zip:ro
    networks:
      - opensearch-net
    entrypoint: >
      bash -c '
        if [ ! -d /usr/share/opensearch/plugins/opensearch-sql ]; then
          /usr/share/opensearch/bin/opensearch-plugin install --batch file:///tmp/opensearch-sql-plugin.zip
        fi
        /usr/share/opensearch/opensearch-docker-entrypoint.sh
      '

volumes:
  opensearch-data1:
  opensearch-data2:
  opensearch-data3:

networks:
  opensearch-net:
```

- [ ] **Step 2: Create multi-node benchmark script**

```bash
# trino/benchmark/run_clickbench_iceberg_multinode.sh
#!/bin/bash
# ClickBench Benchmark — Multi-Node Docker Cluster
#
# Runs 43 ClickBench queries against a 3-node OpenSearch cluster
# with Trino distributed execution across all nodes.
#
# Prerequisites:
#   - Docker and docker-compose installed
#   - Iceberg warehouse created: python3 trino/benchmark/create_iceberg_table.py
#   - Plugin built: ./gradlew :opensearch-sql-plugin:bundlePlugin
#
# Usage:
#   ./trino/benchmark/run_clickbench_iceberg_multinode.sh
#
# Environment variables:
#   NODES            — number of nodes (default: 3)
#   TRIES            — runs per query (default: 3)
#   ICEBERG_WAREHOUSE — path to Iceberg warehouse (default: /tmp/iceberg-clickbench-warehouse)
#   HEAP_PER_NODE    — JVM heap per node (default: 16g)
#   SKIP_DOCKER      — set to 1 to use existing cluster (set OS_PORT)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
QUERIES_FILE="${REPO_DIR}/trino/trino-integ-test/src/test/resources/clickbench/queries.sql"
NODES="${NODES:-3}"
TRIES="${TRIES:-3}"
WAREHOUSE="${ICEBERG_WAREHOUSE:-/tmp/iceberg-clickbench-warehouse}"
HEAP="${HEAP_PER_NODE:-16g}"
SKIP_DOCKER="${SKIP_DOCKER:-0}"

echo "=== ClickBench Benchmark (Multi-Node: ${NODES} nodes) ==="
echo "Warehouse: ${WAREHOUSE}"
echo "Heap/node: ${HEAP}"
echo "Tries:     ${TRIES}"
echo ""

# ---------- Step 1: Build plugin ----------
PLUGIN_ZIP=$(ls "${REPO_DIR}"/plugin/build/distributions/opensearch-sql-*.zip 2>/dev/null | head -1)
if [ -z "${PLUGIN_ZIP}" ]; then
    echo "Step 0: Building plugin..."
    (cd "${REPO_DIR}" && ./gradlew :opensearch-sql-plugin:bundlePlugin -q)
    PLUGIN_ZIP=$(ls "${REPO_DIR}"/plugin/build/distributions/opensearch-sql-*.zip | head -1)
fi
echo "Plugin: ${PLUGIN_ZIP}"

if [ "${SKIP_DOCKER}" != "1" ]; then
    # ---------- Step 2: Start Docker cluster ----------
    echo "Step 1: Starting ${NODES}-node Docker cluster..."
    export PLUGIN_ZIP ICEBERG_WAREHOUSE="${WAREHOUSE}"

    COMPOSE_FILE="${SCRIPT_DIR}/docker/docker-compose.yml"
    docker-compose -f "${COMPOSE_FILE}" down -v 2>/dev/null || true
    docker-compose -f "${COMPOSE_FILE}" up -d

    # Wait for cluster to be green
    echo "  Waiting for cluster health..."
    for i in $(seq 1 120); do
        HEALTH=$(curl -s "http://localhost:9200/_cluster/health" 2>/dev/null || true)
        if echo "${HEALTH}" | grep -q '"number_of_nodes":'${NODES}; then
            echo "  Cluster green with ${NODES} nodes"
            break
        fi
        if [ $i -eq 120 ]; then
            echo "ERROR: Cluster did not form. Check: docker-compose logs"
            exit 1
        fi
        sleep 2
    done

    OS_PORT=9200
else
    OS_PORT="${OS_PORT:-9200}"
    echo "Using existing cluster on port ${OS_PORT}"
fi

ENDPOINT="http://localhost:${OS_PORT}/_plugins/_trino_sql/v1/statement"

# ---------- Step 3: Verify cluster and Iceberg table ----------
echo ""
echo "Step 2: Verifying cluster..."
CLUSTER_INFO=$(curl -s "http://localhost:${OS_PORT}/_cluster/health")
NODE_COUNT=$(echo "${CLUSTER_INFO}" | python3 -c "import json,sys; print(json.load(sys.stdin)['number_of_nodes'])")
echo "  Nodes: ${NODE_COUNT}"

echo "  Verifying Iceberg table..."
VERIFY=$(curl -s -X POST "${ENDPOINT}" \
    -H "Content-Type: application/json" \
    -H "X-Trino-Catalog: iceberg" -H "X-Trino-Schema: clickbench" \
    -d '{"query":"SELECT COUNT(*) FROM iceberg.clickbench.hits"}' \
    --max-time 120)
COUNT=$(echo "$VERIFY" | python3 -c "import json,sys; print(json.load(sys.stdin)['data'][0][0])")
echo "  Rows: ${COUNT}"

# ---------- Step 4: Collect pre-benchmark node stats ----------
echo ""
echo "Step 3: Collecting pre-benchmark node stats..."
for node_name in $(curl -s "http://localhost:${OS_PORT}/_cat/nodes?h=name" | tr -d ' '); do
    echo "  ${node_name}: ready"
done

# ---------- Step 5: Run queries ----------
echo ""
echo "Step 4: Running 43 ClickBench queries (${TRIES} tries each)..."
echo ""

python3 - "${ENDPOINT}" "${TRIES}" "${QUERIES_FILE}" "${NODES}" << 'PYEOF'
import json, urllib.request, time, sys

endpoint = sys.argv[1]
tries = int(sys.argv[2])
queries_file = sys.argv[3]
num_nodes = sys.argv[4]

queries_raw = open(queries_file).readlines()
queries = [q.strip().rstrip(';') for q in queries_raw if q.strip() and not q.strip().startswith('--')]

def run_sql(sql, catalog="iceberg", schema="clickbench", timeout=600):
    body = json.dumps({"query": sql}).encode()
    req = urllib.request.Request(endpoint, data=body,
        headers={"Content-Type": "application/json",
                 "X-Trino-Catalog": catalog, "X-Trino-Schema": schema})
    resp = urllib.request.urlopen(req, timeout=timeout).read().decode()
    return json.loads(resp)

# Header
print(f"{'Query':<7} ", end="")
for t in range(1, tries+1):
    print(f"{'Try'+str(t):>10}", end="")
if tries > 1:
    print(f"  {'Min':>8} {'Med':>8} {'Max':>8}", end="")
print(f"  {'Status':<6}")
print("-" * (7 + tries*10 + (24 if tries > 1 else 0) + 8))

passed = failed = 0
total = 0
results = []

for i, raw in enumerate(queries):
    sql = raw
    times = []
    ok = True
    err_msg = ""

    for t in range(tries):
        s = time.time()
        try:
            r = run_sql(sql, timeout=600)
            e = time.time() - s
            if r.get("stats",{}).get("state") == "FINISHED":
                times.append(e)
            else:
                ok = False
                err_msg = r.get("error",{}).get("message","?")[:60]
                times.append(-1)
        except Exception as ex:
            ok = False
            err_msg = str(ex)[:60]
            times.append(-1)

    print(f"Q{i:<5} ", end="")
    for t_val in times:
        if t_val >= 0:
            print(f"{t_val:>9.1f}s", end="")
        else:
            print(f"{'FAIL':>10}", end="")

    if ok:
        sorted_t = sorted(times)
        mn = sorted_t[0]
        if tries > 1:
            med, mx = sorted_t[len(sorted_t)//2], sorted_t[-1]
            print(f"  {mn:>7.1f}s {med:>7.1f}s {mx:>7.1f}s", end="")
        print(f"  OK")
        passed += 1
        total += (mn if tries > 1 else times[0])
        results.append((i, mn if tries > 1 else times[0], "OK"))
    else:
        if tries > 1:
            print(f"  {'':>8} {'':>8} {'':>8}", end="")
        print(f"  FAIL ({err_msg})")
        failed += 1
        results.append((i, -1, f"FAIL: {err_msg}"))

print("-" * (7 + tries*10 + (24 if tries > 1 else 0) + 8))
print(f"\nPassed: {passed}/43, Failed: {failed}/43")
if tries > 1:
    print(f"Total time: {total:.0f}s (best of {tries})")
else:
    print(f"Total time: {total:.0f}s")
print(f"Average: {total/max(passed,1):.1f}s per query")
print(f"Cluster: {num_nodes} nodes")

results_json = {
    "passed": passed, "failed": failed, "total_time": total,
    "nodes": int(num_nodes),
    "queries": [{"query": i, "time": t, "status": s} for i, t, s in results]
}
with open("/tmp/clickbench-iceberg-multinode-results.json", "w") as f:
    json.dump(results_json, f, indent=2)
print(f"\nResults saved to /tmp/clickbench-iceberg-multinode-results.json")
PYEOF

# ---------- Step 6: Collect post-benchmark node stats ----------
echo ""
echo "Step 5: Post-benchmark node stats (proof of distribution)..."
echo ""
# Query the node stats endpoint on each node
for node_name in $(curl -s "http://localhost:${OS_PORT}/_cat/nodes?h=name" | tr -d ' '); do
    STATS=$(curl -s "http://localhost:${OS_PORT}/_plugins/_trino_sql/v1/node/stats")
    echo "  ${node_name}: ${STATS}"
done

# ---------- Step 7: Cleanup ----------
if [ "${SKIP_DOCKER}" != "1" ]; then
    echo ""
    read -p "Stop Docker cluster? [y/N] " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        docker-compose -f "${SCRIPT_DIR}/docker/docker-compose.yml" down -v
        echo "Cluster stopped."
    else
        echo "Cluster left running. Stop with: docker-compose -f ${SCRIPT_DIR}/docker/docker-compose.yml down -v"
    fi
fi

echo "Done."
```

- [ ] **Step 3: Build plugin and create Iceberg warehouse**

```bash
./gradlew :opensearch-sql-plugin:bundlePlugin

# If warehouse doesn't exist yet on this machine:
python3 trino/benchmark/create_iceberg_table.py /tmp/iceberg-clickbench-warehouse
```

- [ ] **Step 4: Run benchmark — 3 nodes**

```bash
NODES=3 TRIES=3 \
  ICEBERG_WAREHOUSE=/tmp/iceberg-clickbench-warehouse \
  ./trino/benchmark/run_clickbench_iceberg_multinode.sh
```

- [ ] **Step 5: Run benchmark — 1 node (Docker, for apples-to-apples comparison)**

Run a single-node Docker cluster with the same heap (16g per node) to isolate the scaling effect from hardware differences:

```bash
# Single-node Docker with same 16g heap
NODES=1 TRIES=3 \
  ICEBERG_WAREHOUSE=/tmp/iceberg-clickbench-warehouse \
  ./trino/benchmark/run_clickbench_iceberg_multinode.sh
```

- [ ] **Step 6: Record results — 3-way comparison**

Create `docs/benchmark/YYYYMMDD_clickbench_iceberg_transport_multi_node.md`:

```markdown
# ClickBench Benchmark: Transport Engine — Multi-Node Scaling

## Environment

| Setting | 1-Node (bare metal) | 1-Node (Docker) | 3-Node (Docker) |
|---|---|---|---|
| Machine | 32 CPUs, 123GB | Docker, 16g heap | 3x Docker, 16g each |
| Total heap | 32GB | 16GB | 48GB |
| Total CPUs | 32 | (shared) | (shared) |
| Engine | Transport | Transport | Transport |

## Results

| Query | 1-Node BM (s) | 1-Node Docker (s) | 3-Node Docker (s) | 3-Node Speedup |
|-------|---------------|-------------------|-------------------|----------------|
| Q0    | ...           | ...               | ...               | ...            |
| ...   |               |                   |                   |                |
| Total | ...           | ...               | ...               | ...            |

## Scaling Analysis

- Queries that benefit most from distribution: GROUP BY, JOIN (hash-partitioned)
- Queries that don't scale: point lookups, simple scans (I/O bound, single split)
- Bottleneck on 3 nodes: network shuffle overhead for wide aggregations

## Distribution Proof

Node stats after 3-node benchmark:
| Node | Tasks Received | Splits Processed | Pages Produced | Bytes Shuffled |
|------|---------------|-----------------|---------------|---------------|
| node1 (coordinator) | ... | ... | ... | ... |
| node2 (worker) | ... | ... | ... | ... |
| node3 (worker) | ... | ... | ... | ... |
```

**Acceptance criteria:**
- 43/43 queries pass on all configurations
- 3-node Docker shows measurable speedup over 1-node Docker for GROUP BY/JOIN queries
- Node stats prove all 3 nodes processed tasks (non-zero `tasksReceived` on workers)

- [ ] **Step 7: Commit results**

```bash
git commit -s -m "perf(trino): ClickBench multi-node benchmark — 3-node Docker cluster"
```

---

## Task Dependency Graph

```
Task 12: OpenSearchSqlTaskManager (worker-side)
    │
    ├──→ Task 13: Transport Control Plane (task/update, status, cancel)
    │         │
    │         └──→ Task 15: TransportRemoteTask (coordinator-side)
    │                   │
    ├──→ Task 14: Transport Data Plane (task/results, ack)      │
    │         │                                                  │
    │         └──→ Task 16: TransportExchangeClient              │
    │                   │                                        │
    │                   └──→ Task 17: Integration ←──────────────┘
    │                             │
    │                             ├──→ Task 18: Multi-Node Tests
    │                             │
    │                             ├──→ Task 19: ClickBench Single-Node (transport)
    │                             │
    │                             └──→ Task 20: ClickBench Multi-Node (Docker)
    │                                           (depends on 18 + 19)
    │
    Tasks 13 and 14 can run IN PARALLEL (independent transport actions)
    Tasks 15 and 16 can run IN PARALLEL (coordinator-side, independent)
    Tasks 18 and 19 can run IN PARALLEL (independent test/benchmark)
    Task 20 depends on 18 (multi-node proven) and 19 (single-node baseline)
```

---

## Success Criteria

| Metric | Target |
|---|---|
| `DistributedQueryRunner` in Java/Gradle files | **0 references** (grep returns empty) |
| `DistributedQueryRunner` in shadow jar | **0 classes** (jar tf returns empty) |
| `trino-testing` as runtime/shade dependency in trino-opensearch | **Removed** (test-only in trino-integ-test is OK) |
| `@Disabled` annotations referencing `DistributedQueryRunner` | **0** (all tests run, none skipped) |
| Feature flags for engine selection | **None exist** (no `plugins.trino.use_transport` or similar) |
| Single-node: all Trino tests | 328+ pass on transport engine |
| Single-node: ClickBench | 43/43 pass on transport engine |
| Multi-node: correctness tests | All pass |
| Multi-node: `remoteNodesProcessSplits` | At least 1 non-coordinator node did work |
| Multi-node: `remoteNodesReceivedTasks` | Remote task count > 0 |
| Multi-node: `transportActionsFired` | task/update >= 2, task/results >= 1 |
| Multi-node: ClickBench | 43/43 pass |
| No Trino HTTP server | Port 8080 free on all nodes |
| No fallback mode | One code path for 1-node and N-node |
| ClickBench single-node (transport) | Total within 10% of previous baseline (82s) |
| ClickBench 3-node Docker | 43/43 pass, measurable speedup on GROUP BY/JOIN queries |
| ClickBench 3-node distribution proof | All worker nodes show `tasksReceived > 0` |

---

## Risk Register

| Risk | Impact | Mitigation |
|---|---|---|
| Trino's `SqlTaskManager` has many constructor dependencies | HIGH | Extract from `ServerMainModule`'s Guice bindings. Add iteratively — each missing binding error reveals the next. |
| `PlanFragment` JSON serialization breaks with shaded classes | HIGH | The shadow jar relocates `io.trino` → `org.opensearch.sql.trino.shaded.trino`. Trino's Jackson serializers use class names. Ensure the `ObjectMapper` is constructed AFTER relocation (inside the shadow jar classloader). Test serialization round-trip first (Task 13 Step 1). |
| TRINO_PAGES binary too large for default transport message size | HIGH | OpenSearch default `transport.tcp.max_content_length` is 100MB. ClickBench queries may shuffle more. Monitor page sizes; if exceeded, implement chunked transfer or increase the limit. |
| `DispatchManager` initialization pulls in HTTP server dependencies | HIGH | Extract the query submission path without `DispatchManager` if needed. Alternative: use `SqlQueryManager` directly (lower-level, skips the HTTP dispatch layer). |
| Race conditions between transport response and task state updates | MEDIUM | Use Trino's `StateMachine` (already battle-tested) for task state. All updates go through `StateMachine.set()` which handles concurrent access. |
| Network partition causes hung queries | MEDIUM | Set transport timeout on all `sendRequest` calls. Implement query-level timeout that cancels all tasks if exceeded. |
| Performance regression from JSON serialization of PlanFragment | LOW | PlanFragment serialization is one-time per (stage, node). Not on the hot path. Data plane uses opaque binary pass-through. Profile if tests show slowdown. |
| Docker benchmark: Iceberg warehouse read-only mount on all nodes | MEDIUM | All nodes mount the warehouse as `:ro`. If Trino tries to write metadata, it fails. Ensure Iceberg catalog uses `TESTING_FILE_METASTORE` (read-only) or pre-create all metadata. Alternative: use a shared read-write NFS volume. |
| Docker benchmark: CPU/memory contention with 3 nodes on one host | MEDIUM | 3 nodes × 16g = 48g heap + OS overhead. Ensure host has ≥ 64GB RAM. For CPU: Docker shares host CPUs. Results will be conservative vs real multi-host deployment. Document this in results. |
| Transport engine performance regression vs DistributedQueryRunner | MEDIUM | Task 19 catches this early. If > 10% regression: profile serialization overhead, check if transport loopback adds latency vs in-process calls. Optimize hot paths. **NEVER reintroduce DistributedQueryRunner as fallback.** |
| Tests fail after Task 17 cutover | HIGH | Fix the transport engine. Do NOT revert to `DistributedQueryRunner`. Do NOT add a feature flag. Do NOT keep both paths. The `DistributedQueryRunner` is fake multi-node simulation — keeping it masks real bugs in the transport layer. |
