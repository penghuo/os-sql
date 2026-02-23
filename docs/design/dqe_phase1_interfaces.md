# DQE Phase 1: Interface Contracts

**Status**: Frozen
**Date**: 2026-02-23
**Scope**: Phase 1 — All module public interfaces agreed upon before implementation
**Reference**: `docs/design/dqe_design.md`, `docs/design/dqe_phase1_tasks.md`

---

## 1. Conventions

- **Package root**: `org.opensearch.dqe.<module>`
- **Trino types**: Source code uses `io.trino.*` imports. Shadow plugin rewrites to `org.opensearch.dqe.shaded.io.trino.*` at packaging time. All modules code against unshaded imports during development.
- **Exceptions**: All DQE exceptions extend `DqeException` (defined in dqe-parser). Error codes are centralized in `DqeErrorCode` enum.
- **Nullability**: `@Nullable` annotations on parameters/return types that may be null. Non-annotated parameters are assumed non-null.
- **Immutability**: Handle classes (`DqeTableHandle`, `DqeColumnHandle`, `DqeShardSplit`, `AnalyzedQuery`, `DqeQueryRequest`, `DqeQueryResponse`) are immutable.

---

## 2. Module Dependency Graph

```
dqe-parser:     standalone (shaded Trino trino-parser only)
dqe-types:      depends on dqe-parser (for DqeException/DqeErrorCode), shaded Trino types
dqe-metadata:   depends on dqe-types, dqe-parser (exceptions)
dqe-analyzer:   depends on dqe-metadata, dqe-types, dqe-parser
dqe-memory:     depends on dqe-parser (exceptions)
dqe-execution:  depends on dqe-analyzer, dqe-types, dqe-metadata, dqe-memory, dqe-parser
dqe-exchange:   depends on dqe-execution, dqe-types, dqe-metadata, dqe-memory, dqe-parser
dqe-plugin:     depends on all modules
dqe-integ-test: depends on dqe-plugin (test scope)
```

**Note**: `trinoVersion=439` (not 479 as originally spec'd). Trino 479 requires Java 25; our environment has Java 21. Trino 439 is the latest Java 21-compatible version. APIs are identical for Phase 1 usage. Upgrade to 479+ when Java 25 is available.

---

## 3. Error Code Catalog

All error codes are defined in `org.opensearch.dqe.parser.DqeErrorCode` (19 codes):

```java
public enum DqeErrorCode {
    // Parser (dqe-parser)
    PARSING_ERROR,

    // Analyzer (dqe-analyzer)
    UNSUPPORTED_OPERATION,
    TYPE_MISMATCH,
    ACCESS_DENIED,
    ANALYSIS_ERROR,

    // Metadata (dqe-metadata)
    TABLE_NOT_FOUND,
    SHARD_NOT_AVAILABLE,

    // Execution (dqe-execution)
    EXECUTION_ERROR,
    QUERY_CANCELLED,
    QUERY_TIMEOUT,
    PIT_EXPIRED,

    // Memory (dqe-memory)
    EXCEEDED_QUERY_MEMORY_LIMIT,
    CIRCUIT_BREAKER_TRIPPED,
    TOO_MANY_CONCURRENT_QUERIES,

    // Exchange (dqe-exchange)
    EXCHANGE_BUFFER_TIMEOUT,
    STAGE_EXECUTION_FAILED,

    // Plugin (dqe-plugin)
    DQE_DISABLED,
    INVALID_REQUEST,

    // General
    INTERNAL_ERROR
}
```

---

## 4. dqe-parser (`org.opensearch.dqe.parser`)

Owner: dev-foundation

### DqeSqlParser

```java
public class DqeSqlParser {
    public DqeSqlParser()
    public Statement parse(String sql) throws DqeParsingException
}
```

### DqeException (base for ALL DQE exceptions)

```java
public class DqeException extends RuntimeException {
    public DqeException(String message, DqeErrorCode errorCode)
    public DqeException(String message, DqeErrorCode errorCode, Throwable cause)
    public DqeErrorCode getErrorCode()
}
```

### DqeParsingException

```java
public class DqeParsingException extends DqeException {
    public DqeParsingException(String message, int lineNumber, int columnNumber)
    public DqeParsingException(String message, int lineNumber, int columnNumber, Throwable cause)
    public int getLineNumber()
    public int getColumnNumber()
}
```

### DqeUnsupportedOperationException

```java
public class DqeUnsupportedOperationException extends DqeException {
    public DqeUnsupportedOperationException(String construct)
    public DqeUnsupportedOperationException(String construct, String detail)
    public String getConstruct()
}
```

### DqeTypeMismatchException

```java
public class DqeTypeMismatchException extends DqeException {
    public DqeTypeMismatchException(String expression, String expectedType, String actualType)
    public String getExpression()
    public String getExpectedType()
    public String getActualType()
}
```

### DqeAccessDeniedException

```java
public class DqeAccessDeniedException extends DqeException {
    public DqeAccessDeniedException(String resource)
    public DqeAccessDeniedException(String resource, String detail)
    public String getResource()
}
```

---

## 5. dqe-types (`org.opensearch.dqe.types`)

Owner: dev-types

### DqeType

```java
public final class DqeType {
    // Package-private constructor; use DqeTypes factory
    public Type getTrinoType()
    public String getDisplayName()        // "VARCHAR", "BIGINT", "TIMESTAMP(3)", etc.
    public boolean isSortable()
    public boolean isNumeric()
    public boolean isComparable()
    public boolean isParameterized()
    public boolean isRowType()
    public boolean isArrayType()
    public boolean isMapType()
    public DqeType getElementType()       // for ARRAY; throws if not array
    public List<RowField> getRowFields()  // for ROW; throws if not row
    public int getPrecision()             // for DECIMAL, TIMESTAMP
    public int getScale()                 // for DECIMAL

    public static final class RowField {
        public RowField(String name, DqeType type)
        public String getName()
        public DqeType getType()
    }
}
```

### DqeTypes (static factory)

```java
public final class DqeTypes {
    public static final DqeType VARCHAR;
    public static final DqeType BIGINT;
    public static final DqeType INTEGER;
    public static final DqeType SMALLINT;
    public static final DqeType TINYINT;
    public static final DqeType DOUBLE;
    public static final DqeType REAL;
    public static final DqeType BOOLEAN;
    public static final DqeType VARBINARY;
    public static final DqeType TIMESTAMP_MILLIS;    // TIMESTAMP(3)
    public static final DqeType TIMESTAMP_NANOS;     // TIMESTAMP(9)

    public static DqeType decimal(int precision, int scale)
    public static DqeType array(DqeType elementType)
    public static DqeType row(List<DqeType.RowField> fields)
    public static DqeType map(DqeType keyType, DqeType valueType)
    public static DqeType timestamp(int precision)
    public static DqeType fromTrinoType(Type trinoType)
}
```

### OpenSearchTypeMappingResolver (`org.opensearch.dqe.types.mapping`)

```java
public class OpenSearchTypeMappingResolver {
    public OpenSearchTypeMappingResolver(MultiFieldResolver multiFieldResolver,
                                         DateFormatResolver dateFormatResolver)
    public ResolvedField resolve(String fieldName, String osTypeName,
                                  Map<String, Object> mappingProperties)
    public Map<String, ResolvedField> resolveAll(Map<String, Object> indexMapping)
}
```

### ResolvedField (`org.opensearch.dqe.types.mapping`)

```java
public final class ResolvedField {
    public ResolvedField(String fieldPath, DqeType type, boolean sortable,
                         @Nullable String keywordSubField, boolean hasFielddata,
                         boolean isArray)
    public String getFieldPath()
    public DqeType getType()
    public boolean isSortable()
    public @Nullable String getKeywordSubField()
    public boolean hasFielddata()
    public boolean isArray()
}
```

### MultiFieldResolver (`org.opensearch.dqe.types.mapping`)

```java
public class MultiFieldResolver {
    public Optional<MultiFieldInfo> resolve(String fieldName, Map<String, Object> fieldMapping)

    public static final class MultiFieldInfo {
        public MultiFieldInfo(String keywordSubFieldPath, boolean parentHasFielddata)
        public String getKeywordSubFieldPath()
        public boolean parentHasFielddata()
    }
}
```

### DqeTypeCoercion

```java
public final class DqeTypeCoercion {
    public static Optional<DqeType> getCommonSuperType(DqeType left, DqeType right)
    public static boolean canCoerce(DqeType source, DqeType target)
    public static boolean canCast(DqeType source, DqeType target)
}
```

### TypeWidening

```java
public final class TypeWidening {
    public static Optional<DqeType> widen(String fieldName, Set<DqeType> types)
}
```

### DateFormatResolver (`org.opensearch.dqe.types.mapping`)

```java
public class DateFormatResolver {
    public DateFormatInfo resolve(@Nullable String formatProperty)
    public static final Set<String> SUPPORTED_BUILTIN_FORMATS;

    public static final class DateFormatInfo {
        public DateFormatInfo(DateTimeFormatter formatter, int timestampPrecision)
        public DateTimeFormatter getFormatter()
        public int getTimestampPrecision()  // 3 for millis, 9 for nanos
    }
}
```

### ArrayDetectionStrategy / ArrayDetector (`org.opensearch.dqe.types.array`)

```java
public interface ArrayDetectionStrategy {
    Set<String> detectArrayFields(String indexName, Set<String> fieldPaths);
}

public class ArrayDetector {
    public ArrayDetector(ArrayDetectionMode mode, int sampleSize, Client client)
    public Set<String> detect(String indexName, Set<String> fieldPaths)

    public enum ArrayDetectionMode { META_ANNOTATION, SAMPLING, NONE }
}
```

### SearchHitToPageConverter (`org.opensearch.dqe.types.converter`)

```java
public class SearchHitToPageConverter {
    public SearchHitToPageConverter(List<ColumnDescriptor> columns, int batchSize)
    public Page convert(SearchHit[] hits)

    public static final class ColumnDescriptor {
        public ColumnDescriptor(String fieldPath, DqeType type,
                                @Nullable DateFormatResolver.DateFormatInfo dateFormat)
        public String getFieldPath()
        public DqeType getType()
        public @Nullable DateFormatResolver.DateFormatInfo getDateFormat()
    }
}
```

---

## 6. dqe-metadata (`org.opensearch.dqe.metadata`)

Owner: dev-types

### DqeTableHandle

```java
public final class DqeTableHandle implements Writeable {
    public DqeTableHandle(String indexName, @Nullable String indexPattern,
                          List<String> resolvedIndices, long schemaGeneration,
                          @Nullable String pitId)
    public DqeTableHandle(StreamInput in) throws IOException

    public String getIndexName()
    public @Nullable String getIndexPattern()
    public List<String> getResolvedIndices()
    public long getSchemaGeneration()
    public @Nullable String getPitId()
    public DqeTableHandle withPitId(String pitId)

    @Override public void writeTo(StreamOutput out) throws IOException
}
```

### DqeColumnHandle

```java
public final class DqeColumnHandle implements Writeable {
    public DqeColumnHandle(String fieldName, String fieldPath, DqeType type,
                           boolean sortable, @Nullable String keywordSubField,
                           boolean isArray)
    public DqeColumnHandle(StreamInput in) throws IOException

    public String getFieldName()
    public String getFieldPath()
    public DqeType getType()
    public boolean isSortable()
    public @Nullable String getKeywordSubField()
    public boolean isArray()

    @Override public void writeTo(StreamOutput out) throws IOException
}
```

### DqeShardSplit

```java
public final class DqeShardSplit implements Writeable {
    public DqeShardSplit(int shardId, String nodeId, String indexName, boolean primary)
    public DqeShardSplit(StreamInput in) throws IOException

    public int getShardId()
    public String getNodeId()
    public String getIndexName()
    public boolean isPrimary()

    @Override public void writeTo(StreamOutput out) throws IOException
}
```

**Note**: All handle classes implement `Writeable` for transport serialization (required by dqe-exchange).

### DqeMetadata

```java
public class DqeMetadata {
    public DqeMetadata(ClusterService clusterService,
                       OpenSearchTypeMappingResolver typeMappingResolver,
                       ArrayDetector arrayDetector, StatisticsCache statisticsCache,
                       Client client)

    public DqeTableHandle getTableHandle(String schema, String tableName)
    public List<DqeColumnHandle> getColumnHandles(DqeTableHandle table)
    public List<DqeShardSplit> getSplits(DqeTableHandle table)
    public DqeTableStatistics getStatistics(DqeTableHandle table)
    public SchemaSnapshot createSnapshot(DqeTableHandle table)
}
```

### SchemaSnapshot

```java
public final class SchemaSnapshot {
    public SchemaSnapshot(DqeTableHandle tableHandle, List<DqeColumnHandle> columns,
                          long createdAtMillis)
    public DqeTableHandle getTableHandle()
    public List<DqeColumnHandle> getColumns()
    public long getCreatedAtMillis()
    public Optional<DqeColumnHandle> getColumn(String fieldPath)
    public DqeColumnHandle getColumn(int ordinal)
    public int getColumnCount()
}
```

### DqeTableStatistics

```java
public final class DqeTableStatistics {
    public static final DqeTableStatistics UNKNOWN;
    public DqeTableStatistics(long rowCount, long indexSizeBytes, long generation)
    public long getRowCount()
    public long getIndexSizeBytes()
    public long getGeneration()
    public boolean isUnknown()
}
```

### StatisticsCache

```java
public class StatisticsCache {
    public StatisticsCache(long ttlMillis)
    public Optional<DqeTableStatistics> get(String indexName)
    public void put(String indexName, DqeTableStatistics statistics)
    public void invalidate(String indexName)
    public void invalidateAll()
}
```

### ShardSelector

```java
public class ShardSelector {
    public ShardSelector(String localNodeId)
    public ShardRouting select(IndexShardRoutingTable shardRoutingTable)
}
```

### Module-specific exceptions

```java
public class DqeTableNotFoundException extends DqeException {
    public DqeTableNotFoundException(String tableName)
    public String getTableName()
}

public class DqeShardNotAvailableException extends DqeException {
    public DqeShardNotAvailableException(String indexName, int shardId)
    public String getIndexName()
    public int getShardId()
}
```

---

## 7. dqe-analyzer (`org.opensearch.dqe.analyzer`)

Owner: dev-analyzer

### DqeAnalyzer

```java
public class DqeAnalyzer {
    public DqeAnalyzer()
    public AnalyzedQuery analyze(Statement statement, DqeMetadata metadata,
                                  SecurityContext securityContext)
}
```

### AnalyzedQuery (primary output — consumed by dqe-execution and dqe-plugin)

```java
public class AnalyzedQuery {
    public static Builder builder()

    // Source table
    public DqeTableHandle getTable()

    // Output schema
    public List<String> getOutputColumnNames()
    public List<DqeType> getOutputColumnTypes()
    public List<TypedExpression> getOutputExpressions()

    // Predicates
    public Optional<PredicateAnalysisResult> getPredicateAnalysis()
    public boolean hasWhereClause()

    // Projections
    public RequiredColumns getRequiredColumns()

    // Sorting and limiting
    public List<SortSpecification> getSortSpecifications()
    public OptionalLong getLimit()
    public OptionalLong getOffset()
    public PipelineDecision getPipelineDecision()

    // SELECT * detection
    public boolean isSelectAll()

    public static class Builder { /* all field setters + build() */ }
}
```

### Scope / ScopeResolver / ResolvedField (`org.opensearch.dqe.analyzer.scope`)

```java
public class Scope {
    public Scope(DqeTableHandle table, List<DqeColumnHandle> columns,
                 Optional<String> tableAlias)
    public DqeTableHandle getTable()
    public List<DqeColumnHandle> getColumns()
    public Optional<String> getTableAlias()
    public String getTableName()
    public void addColumnAlias(String alias, DqeType type)
}

public class ScopeResolver {
    public ScopeResolver()
    public ResolvedField resolveColumn(String columnName, Scope scope)
    public ResolvedField resolveQualifiedColumn(String qualifier, String columnName,
                                                 Scope scope)
}

public class ResolvedField {
    public ResolvedField(DqeTableHandle table, DqeColumnHandle column, DqeType type)
    public DqeTableHandle getTable()
    public DqeColumnHandle getColumn()
    public DqeType getType()
    public String getFieldName()
}
```

### ExpressionTypeChecker / TypedExpression (`org.opensearch.dqe.analyzer.type`)

```java
public class ExpressionTypeChecker {
    public ExpressionTypeChecker(DqeTypeCoercion typeCoercion)
    public TypedExpression check(Expression expression, Scope scope)
}

public class TypedExpression {
    public TypedExpression(Expression expression, DqeType type)
    public Expression getExpression()
    public DqeType getType()
    public boolean isColumnReference()
    public boolean isLiteral()
}
```

### UnsupportedConstructValidator (`org.opensearch.dqe.analyzer.validation`)

```java
public class UnsupportedConstructValidator {
    public UnsupportedConstructValidator()
    public void validate(Statement statement)
}
```

### PredicateAnalyzer / PushdownPredicate (`org.opensearch.dqe.analyzer.predicate`)

```java
public class PredicateAnalyzer {
    public PredicateAnalyzer()
    public PredicateAnalysisResult analyze(TypedExpression whereClause, Scope scope)
}

public class PredicateAnalysisResult {
    public PredicateAnalysisResult(List<PushdownPredicate> pushdownPredicates,
                                    List<TypedExpression> residualPredicates)
    public List<PushdownPredicate> getPushdownPredicates()
    public List<TypedExpression> getResidualPredicates()
    public boolean isFullyPushedDown()
}

public class PushdownPredicate {
    public enum PredicateType {
        TERM_EQUALITY, RANGE, BETWEEN, IN_SET, EXISTS, NOT_EXISTS,
        LIKE_PATTERN, BOOL_AND, BOOL_OR, BOOL_NOT
    }

    public PushdownPredicate(PredicateType type, String fieldName, DqeType fieldType,
                              Object value, List<PushdownPredicate> children)
    public PredicateType getType()
    public String getFieldName()
    public DqeType getFieldType()
    public Object getValue()
    public List<PushdownPredicate> getChildren()

    // Factory methods
    public static PushdownPredicate termEquality(String field, DqeType type, Object value)
    public static PushdownPredicate range(String field, DqeType type, Object lower,
                                           boolean lowerInclusive, Object upper,
                                           boolean upperInclusive)
    public static PushdownPredicate between(String field, DqeType type, Object lower,
                                             Object upper)
    public static PushdownPredicate inSet(String field, DqeType type, List<Object> values)
    public static PushdownPredicate exists(String field)
    public static PushdownPredicate notExists(String field)
    public static PushdownPredicate likePattern(String field, String pattern)
    public static PushdownPredicate and(List<PushdownPredicate> children)
    public static PushdownPredicate or(List<PushdownPredicate> children)
    public static PushdownPredicate not(PushdownPredicate child)
}

public class PushdownClassifier {
    public PushdownClassifier()
    public Optional<PushdownPredicate> classify(TypedExpression expression, Scope scope)
    public boolean isColumnLiteralComparison(TypedExpression expression)
}
```

### ProjectionAnalyzer / RequiredColumns (`org.opensearch.dqe.analyzer.projection`)

```java
public class ProjectionAnalyzer {
    public ProjectionAnalyzer()
    public RequiredColumns computeRequiredColumns(List<TypedExpression> selectExpressions,
            TypedExpression whereClause, List<TypedExpression> orderByExpressions,
            Scope scope)
}

public class RequiredColumns {
    public RequiredColumns(Set<DqeColumnHandle> columns)
    public Set<DqeColumnHandle> getColumns()
    public String[] getFieldNames()
    public int size()
    public boolean isAllColumns()
    public static RequiredColumns allColumns(List<DqeColumnHandle> allTableColumns)
}
```

### OrderByAnalyzer / SortSpecification / OperatorSelectionRule (`org.opensearch.dqe.analyzer.sort`)

```java
public class OrderByAnalyzer {
    public OrderByAnalyzer(ExpressionTypeChecker typeChecker)
    public List<SortSpecification> analyze(List<SortItem> sortItems, Scope scope)
}

public class SortSpecification {
    public enum SortDirection { ASC, DESC }
    public enum NullOrdering { NULLS_FIRST, NULLS_LAST }

    public SortSpecification(DqeColumnHandle column, DqeType type,
                              SortDirection direction, NullOrdering nullOrdering)
    public DqeColumnHandle getColumn()
    public DqeType getType()
    public SortDirection getDirection()
    public NullOrdering getNullOrdering()
}

public class OperatorSelectionRule {
    public enum PipelineStrategy {
        SCAN_ONLY, FULL_SORT, TOP_N, TOP_N_WITH_OFFSET, LIMIT_ONLY
    }
    public OperatorSelectionRule()
    public PipelineDecision decide(List<SortSpecification> sortSpecs,
                                    OptionalLong limit, OptionalLong offset)
}

public class PipelineDecision {
    public PipelineDecision(PipelineStrategy strategy, List<SortSpecification> sortSpecs,
                             OptionalLong limit, OptionalLong offset,
                             OptionalLong effectiveTopN)
    public PipelineStrategy getStrategy()
    public List<SortSpecification> getSortSpecifications()
    public OptionalLong getLimit()
    public OptionalLong getOffset()
    public OptionalLong getEffectiveTopN()
}
```

### SecurityContext (`org.opensearch.dqe.analyzer.security`)

```java
public interface SecurityContext {
    boolean hasIndexReadPermission(String indexName)
    String getUserName()
}
```

### DqeAnalysisException

```java
public class DqeAnalysisException extends DqeException {
    public DqeAnalysisException(String message)
    public DqeAnalysisException(String message, Throwable cause)
}
```

---

## 8. dqe-memory (`org.opensearch.dqe.memory`)

Owner: dev-transport

### DqeMemoryTracker

```java
public class DqeMemoryTracker {
    public DqeMemoryTracker(CircuitBreaker circuitBreaker, CircuitBreaker parentBreaker)
    public void reserve(long bytes, String label) throws CircuitBreakingException
    public void release(long bytes, String label)
    public long getUsedBytes()
    public long getLimitBytes()
    public long getRemainingBytes()
}
```

### DqeCircuitBreakerRegistrar

```java
public class DqeCircuitBreakerRegistrar {
    public static DqeMemoryTracker register(CircuitBreakerService service,
                                             long breakerLimitBytes)
    public static void deregister(CircuitBreakerService service)
}
```

### QueryMemoryBudget

```java
public class QueryMemoryBudget {
    public QueryMemoryBudget(String queryId, long budgetBytes,
                              DqeMemoryTracker memoryTracker)
    public void reserve(long bytes, String label) throws DqeException
    public void release(long bytes, String label)
    public long getUsedBytes()
    public long getBudgetBytes()
    public long getRemainingBytes()
    public String getQueryId()
    public void releaseAll()
}
```

### QueryCleanup

```java
public class QueryCleanup {
    public QueryCleanup(QueryMemoryBudget budget)
    public void registerCleanupAction(Runnable action)
    public void cleanup()
    public boolean isCleaned()
}
```

### AdmissionController

```java
public class AdmissionController {
    public AdmissionController(int maxConcurrentQueries)
    public boolean tryAcquire()
    public void release()
    public int getRunningQueryCount()
    public int getMaxConcurrentQueries()
    public boolean isAtCapacity()
}
```

---

## 9. dqe-execution (`org.opensearch.dqe.execution`)

Owner: dev-execution

### Operator (`org.opensearch.dqe.execution.operator`)

```java
public interface Operator extends AutoCloseable {
    Page getOutput()
    boolean isFinished()
    void finish()
    @Override void close()
    OperatorContext getOperatorContext()
}
```

### OperatorContext

```java
public class OperatorContext {
    public OperatorContext(String queryId, int stageId, int pipelineId,
                          int operatorId, String operatorType,
                          QueryMemoryBudget memoryBudget)

    public String getQueryId()
    public int getStageId()
    public int getPipelineId()
    public int getOperatorId()
    public String getOperatorType()

    // Memory
    public void reserveMemory(long bytes)
    public void releaseMemory(long bytes)
    public long getReservedBytes()

    // Cancellation
    public boolean isInterrupted()
    public void setInterrupted(boolean interrupted)
    public void checkInterrupted() throws DqeQueryCancelledException

    // Stats
    public long getInputPositions()
    public long getOutputPositions()
    public void addInputPositions(long positions)
    public void addOutputPositions(long positions)
    public void addInputDataSizeBytes(long bytes)
    public void addOutputDataSizeBytes(long bytes)
}
```

### OperatorFactory

```java
public interface OperatorFactory {
    Operator createOperator(OperatorContext operatorContext)
    OperatorFactory duplicate()
}
```

### ShardScanOperator (`org.opensearch.dqe.execution.operator.scan`)

```java
public class ShardScanOperator implements Operator {
    public ShardScanOperator(OperatorContext operatorContext,
                              SearchRequestBuilder searchRequestBuilder,
                              PitHandle pitHandle, Client client, int batchSize)
}

public class SearchRequestBuilder {
    public SearchRequestBuilder(String indexName, int shardId,
                                 List<String> requiredColumns,
                                 @Nullable QueryBuilder pushedDownQuery,
                                 @Nullable List<SortBuilder<?>> sortBuilders,
                                 int batchSize)
    public SearchRequest buildInitialRequest(PitHandle pitHandle)
    public SearchRequest buildSearchAfterRequest(PitHandle pitHandle,
                                                  Object[] searchAfterValues)
}
```

### PredicateToQueryDslConverter (`org.opensearch.dqe.execution.predicate`)

```java
public class PredicateToQueryDslConverter {
    public PredicateToQueryDslConverter()
    public @Nullable QueryBuilder convert(PushdownPredicate predicate)
}
```

### FilterOperator / ExpressionEvaluator

```java
public class FilterOperator implements Operator {
    public FilterOperator(OperatorContext operatorContext, Operator source,
                          ExpressionEvaluator filterPredicate)
}

public class ExpressionEvaluator {
    public ExpressionEvaluator(TypedExpression expression,
                                Map<String, Integer> inputColumns)
    public Object evaluate(Page page, int position)
    public Block evaluateAll(Page page)
    public DqeType getOutputType()
}
```

### ProjectOperator

```java
public class ProjectOperator implements Operator {
    public ProjectOperator(OperatorContext operatorContext, Operator source,
                           List<ExpressionEvaluator> projections)
}
```

### SortOperator

```java
public class SortOperator implements Operator {
    public SortOperator(OperatorContext operatorContext, Operator source,
                         List<SortSpecification> sortSpecifications,
                         List<Integer> outputChannels)
}
```

### TopNOperator

```java
public class TopNOperator implements Operator {
    public TopNOperator(OperatorContext operatorContext, Operator source,
                         List<SortSpecification> sortSpecifications, long n,
                         List<Integer> outputChannels)
}
```

### LimitOperator

```java
public class LimitOperator implements Operator {
    public LimitOperator(OperatorContext operatorContext, Operator source,
                          long limit, long offset)
}
```

### Pipeline / Driver / DriverRunner (`org.opensearch.dqe.execution.driver`)

```java
public class Pipeline {
    public Pipeline(List<Operator> operators)
    public Operator getSource()
    public Operator getOutput()
    public List<Operator> getOperators()
    public void close()
}

public class Driver implements Closeable {
    public Driver(Pipeline pipeline, Consumer<Page> outputConsumer)
    public boolean process()
    public boolean isFinished()
    @Override public void close()
    public List<OperatorContext> getOperatorContexts()
}

public class DriverRunner {
    public DriverRunner(ThreadPool threadPool)
    public void enqueueDriver(Driver driver, DriverCompletionCallback completionCallback)
    public void cancelDrivers(String queryId)
    public void shutdown()
}

@FunctionalInterface
public interface DriverCompletionCallback {
    void onComplete(Driver driver, @Nullable Exception failure);
}
```

### PitManager / PitHandle (`org.opensearch.dqe.execution.pit`)

```java
public class PitManager {
    public PitManager(Client client)
    public PitHandle createPit(String indexName, TimeValue keepAlive)
    public void releasePit(PitHandle pitHandle)
    public void releaseAllPits(String queryId)
    public void registerPit(String queryId, PitHandle pitHandle)
}

public class PitHandle {
    public PitHandle(String pitId, String indexName, TimeValue keepAlive)
    public String getPitId()
    public String getIndexName()
    public TimeValue getKeepAlive()
    public boolean isReleased()
}
```

### DqeQueryTask / QueryCancellationHandler / QueryTimeoutScheduler (`org.opensearch.dqe.execution.task`)

```java
public class DqeQueryTask extends CancellableTask {
    public DqeQueryTask(long id, String type, String action, String description,
                         TaskId parentTaskId, Map<String, String> headers,
                         String queryId, String sqlQuery)
    public String getQueryId()
    public String getSqlQuery()
    @Override public boolean shouldCancelChildrenOnCancellation()  // returns true
}

public class QueryCancellationHandler {
    public QueryCancellationHandler()
    public void registerOperatorContexts(String queryId, List<OperatorContext> contexts)
    public void cancelQuery(String queryId, String reason)
    public void deregisterQuery(String queryId)
    public boolean isCancelled(String queryId)
}

public class QueryTimeoutScheduler {
    public QueryTimeoutScheduler(ThreadPool threadPool,
                                  QueryCancellationHandler cancellationHandler)
    public Cancellable scheduleTimeout(String queryId, TimeValue timeout)
    public void cancelTimeout(String queryId)
}

public class DqeQueryCancelledException extends DqeException {
    public DqeQueryCancelledException(String queryId, String reason)
    public String getQueryId()
    public String getReason()
}
```

---

## 10. dqe-exchange (`org.opensearch.dqe.exchange`)

Owner: dev-transport

### Transport Actions (`org.opensearch.dqe.exchange.action`)

```java
// Push data pages
public class DqeExchangePushAction extends ActionType<DqeExchangePushResponse> {
    public static final String NAME = "internal:dqe/exchange/push";
}
public class DqeExchangePushRequest extends TransportRequest implements Writeable {
    public DqeExchangePushRequest(DqeExchangeChunk chunk)
    public DqeExchangeChunk getChunk()
}
public class DqeExchangePushResponse extends TransportResponse implements Writeable {
    public DqeExchangePushResponse(boolean accepted, long bufferRemainingBytes)
    public boolean isAccepted()
    public long getBufferRemainingBytes()
}

// Execute stage on data node
public class DqeStageExecuteAction extends ActionType<DqeStageExecuteResponse> {
    public static final String NAME = "internal:dqe/stage/execute";
}
public class DqeStageExecuteRequest extends TransportRequest implements Writeable {
    public DqeStageExecuteRequest(String queryId, int stageId,
                                    byte[] serializedPlanFragment,
                                    List<DqeShardSplit> shardSplits,
                                    String coordinatorNodeId,
                                    long queryMemoryBudgetBytes)
    public String getQueryId()
    public int getStageId()
    public byte[] getSerializedPlanFragment()
    public List<DqeShardSplit> getShardSplits()
    public String getCoordinatorNodeId()
    public long getQueryMemoryBudgetBytes()
}
public class DqeStageExecuteResponse extends TransportResponse implements Writeable {
    public DqeStageExecuteResponse(String queryId, int stageId, boolean accepted)
}

// Cancel stage
public class DqeStageCancelAction extends ActionType<DqeStageCancelResponse> {
    public static final String NAME = "internal:dqe/stage/cancel";
}
public class DqeStageCancelRequest extends TransportRequest implements Writeable {
    public DqeStageCancelRequest(String queryId, int stageId)
}
public class DqeStageCancelResponse extends TransportResponse implements Writeable {
    public DqeStageCancelResponse(String queryId, int stageId, boolean acknowledged)
}
```

### Serialization (`org.opensearch.dqe.exchange.serde`)

```java
public class DqeDataPage implements Writeable {
    public DqeDataPage(Page page)
    public DqeDataPage(StreamInput in) throws IOException
    public Page getPage()
    public long getUncompressedSizeInBytes()
    public long getCompressedSizeInBytes()
    public int getPositionCount()
    @Override public void writeTo(StreamOutput out) throws IOException
}

public class PageSerializer {
    public static byte[] serialize(Page page)
    public static long estimateSerializedSize(Page page)
}

public class PageDeserializer {
    public static Page deserialize(byte[] compressedData, long uncompressedSize)
}
```

### Message Framing

```java
public class DqeExchangeChunk implements Writeable {
    public DqeExchangeChunk(String queryId, int stageId, int partitionId,
                              long sequenceNumber, List<DqeDataPage> pages,
                              boolean isLast, long uncompressedBytes)
    public DqeExchangeChunk(StreamInput in) throws IOException
    public String getQueryId()
    public int getStageId()
    public int getPartitionId()
    public long getSequenceNumber()
    public List<DqeDataPage> getPages()
    public boolean isLast()
    public long getUncompressedBytes()
    public long getCompressedBytes()
    @Override public void writeTo(StreamOutput out) throws IOException
}
```

### Exchange Buffer (`org.opensearch.dqe.exchange.buffer`)

```java
public class ExchangeBuffer implements Closeable {
    public ExchangeBuffer(long maxBufferBytes, long producerTimeoutMs,
                          DqeMemoryTracker memoryTracker, String queryId)
    public void addChunk(DqeExchangeChunk chunk) throws DqeException
    public @Nullable DqeExchangeChunk pollChunk() throws InterruptedException
    public @Nullable DqeExchangeChunk pollChunk(long timeoutMs) throws InterruptedException
    public void setNoMoreData()
    public void abort()
    public boolean isFinished()
    public long getBufferedBytes()
    public int getBufferedChunkCount()
    @Override public void close()
}

public class BackpressureController {
    public BackpressureController(long maxBufferBytes, long producerTimeoutMs)
    public boolean awaitCapacity(long requiredBytes) throws InterruptedException
    public void notifyConsumed(long freedBytes)
    public long getBufferedBytes()
    public boolean isFull()
    public void abort()
}
```

### Gather Exchange (`org.opensearch.dqe.exchange.gather`)

```java
public class GatherExchangeSource implements Closeable {
    public GatherExchangeSource(String queryId, int stageId,
                                 int expectedProducerCount, ExchangeBuffer buffer)
    public @Nullable Page getNextPage() throws InterruptedException
    public boolean isFinished()
    public void abort()
    @Override public void close()
}

public class GatherExchangeSink implements Closeable {
    public GatherExchangeSink(String queryId, int stageId, int partitionId,
                               String coordinatorNodeId,
                               TransportService transportService, long maxChunkBytes)
    public void addPage(Page page) throws DqeException
    public void finish() throws DqeException
    public void abort()
    public long getSequenceNumber()
    @Override public void close()
}
```

### Stage Management (`org.opensearch.dqe.exchange.stage`)

```java
public class StageScheduler {
    public StageScheduler(TransportService transportService)
    public void scheduleStage(String queryId, int stageId,
                               byte[] serializedPlanFragment,
                               List<DqeShardSplit> shardSplits,
                               String coordinatorNodeId,
                               long queryMemoryBudgetBytes,
                               ActionListener<StageScheduleResult> listener)
    public void cancelQuery(String queryId, ActionListener<Void> listener)
    public void cancelStage(String queryId, int stageId, ActionListener<Void> listener)
    public Set<String> getActiveNodes(String queryId)
}

public class StageScheduleResult {
    public StageScheduleResult(String queryId, int stageId, int dispatchedTaskCount)
    public String getQueryId()
    public int getStageId()
    public int getDispatchedTaskCount()
}

public class StageExecutionHandler extends TransportRequestHandler<DqeStageExecuteRequest> {
    public StageExecutionHandler(ThreadPool threadPool,
                                  TransportService transportService,
                                  DqeMemoryTracker memoryTracker)
    @Override public void messageReceived(DqeStageExecuteRequest request,
                                           TransportChannel channel, Task task)
}

public class StageCancelHandler extends TransportRequestHandler<DqeStageCancelRequest> {
    public StageCancelHandler(StageExecutionHandler executionHandler)
    @Override public void messageReceived(DqeStageCancelRequest request,
                                           TransportChannel channel, Task task)
}
```

---

## 11. dqe-plugin (`org.opensearch.dqe.plugin`)

Owner: dev-plugin

### DqeEnginePlugin

```java
public class DqeEnginePlugin {
    public DqeEnginePlugin(Settings settings, ClusterService clusterService,
                           ThreadPool threadPool, NodeClient client)
    public List<ActionPlugin.ActionHandler<?, ?>> getActions()
    public List<ExecutorBuilder<?>> getExecutorBuilders(Settings settings)
    public List<Setting<?>> getSettings()
    public void initialize()
    public EngineRouter getEngineRouter()
    public RestHandler getStatsHandler()
    public void close()
    public boolean isEnabled()
}
```

### EngineRouter

```java
public class EngineRouter {
    public EngineRouter(DqeSettings settings, DqeQueryOrchestrator orchestrator,
                        DqeExplainHandler explainHandler)
    public String resolveEngine(String requestEngineField)
    public boolean shouldUseDqe(String requestEngineField)
    public DqeQueryResponse executeQuery(DqeQueryRequest request) throws DqeException
    public String executeExplain(DqeQueryRequest request) throws DqeException
}
```

### DqeQueryRequest (`org.opensearch.dqe.plugin.request`)

```java
public class DqeQueryRequest {
    public String getQuery()
    public String getEngine()
    public int getFetchSize()
    public Map<String, String> getSessionProperties()
    public String getQueryId()
    public Optional<Long> getQueryMaxMemoryBytes()
    public Optional<Duration> getQueryTimeout()
    public static Builder builder()
}
```

### DqeRequestParser (`org.opensearch.dqe.plugin.request`)

```java
public class DqeRequestParser {
    public DqeRequestParser(DqeSettings settings)
    public DqeQueryRequest parse(String requestBody) throws DqeException
    public DqeQueryRequest parse(XContentParser parser) throws DqeException
}
```

### DqeQueryResponse (`org.opensearch.dqe.plugin.response`)

```java
public class DqeQueryResponse {
    public String getEngine()
    public List<ColumnSchema> getSchema()
    public List<List<Object>> getData()
    public QueryStats getStats()
    public static Builder builder()

    public static class ColumnSchema {
        public ColumnSchema(String name, String type)
        public String getName()
        public String getType()
    }

    public static class QueryStats {
        public String getState()
        public String getQueryId()
        public long getElapsedMs()
        public long getRowsProcessed()
        public long getBytesProcessed()
        public int getStages()
        public int getShardsQueried()
        public static Builder builder()
    }
}
```

### DqeResponseFormatter (`org.opensearch.dqe.plugin.response`)

```java
public class DqeResponseFormatter {
    public DqeResponseFormatter()
    public String formatSuccess(DqeQueryResponse response)
    public String formatError(DqeException exception, String queryId)
    public void writeSuccess(DqeQueryResponse response, XContentBuilder builder)
    public void writeError(DqeException exception, String queryId, XContentBuilder builder)
}
```

### DqeExplainHandler / PlanPrinter (`org.opensearch.dqe.plugin.explain`)

```java
public class DqeExplainHandler {
    public DqeExplainHandler(DqeSqlParser parser, DqeAnalyzer analyzer,
                              DqeMetadata metadata, PlanPrinter planPrinter)
    public String explain(DqeQueryRequest request) throws DqeException
    public ExplainResult explainStructured(DqeQueryRequest request) throws DqeException
}

public class PlanPrinter {
    public PlanPrinter()
    public String print(AnalyzedQuery plan)
    public String printWithStats(AnalyzedQuery plan, DqeQueryResponse.QueryStats stats)
}
```

### DqeSettings (`org.opensearch.dqe.plugin.settings`)

```java
public class DqeSettings {
    public DqeSettings(ClusterSettings clusterSettings)

    // Static Setting definitions
    public static final Setting<String> SQL_ENGINE_SETTING;               // "plugins.sql.engine" default "calcite"
    public static final Setting<Boolean> DQE_ENABLED_SETTING;             // "plugins.dqe.enabled" default true
    public static final Setting<Integer> MAX_CONCURRENT_QUERIES_SETTING;  // default 10
    public static final Setting<TimeValue> QUERY_TIMEOUT_SETTING;         // default "5m"
    public static final Setting<String> MEMORY_BREAKER_LIMIT_SETTING;     // default "20%"
    public static final Setting<ByteSizeValue> QUERY_MAX_MEMORY_SETTING;  // default "256mb"
    public static final Setting<ByteSizeValue> EXCHANGE_BUFFER_SIZE_SETTING;  // default "32mb"
    public static final Setting<ByteSizeValue> EXCHANGE_CHUNK_SIZE_SETTING;   // default "1mb"
    public static final Setting<TimeValue> EXCHANGE_TIMEOUT_SETTING;          // default "60s"
    public static final Setting<TimeValue> EXCHANGE_BACKPRESSURE_TIMEOUT_SETTING; // default "30s"
    public static final Setting<Integer> SCAN_BATCH_SIZE_SETTING;         // default 1000
    public static final Setting<TimeValue> SLOW_QUERY_LOG_THRESHOLD_SETTING;  // default "10s"
    public static final Setting<Integer> WORKER_POOL_SIZE_SETTING;        // default min(4, cores/2)
    public static final Setting<Integer> EXCHANGE_POOL_SIZE_SETTING;      // default min(2, cores/4)
    public static final Setting<Integer> COORDINATOR_POOL_SIZE_SETTING;   // default min(2, cores/4)

    // Dynamic getters
    public String getEngine()
    public boolean isDqeEnabled()
    public int getMaxConcurrentQueries()
    public TimeValue getQueryTimeout()
    public ByteSizeValue getQueryMaxMemory()
    public ByteSizeValue getExchangeBufferSize()
    public ByteSizeValue getExchangeChunkSize()
    public TimeValue getExchangeTimeout()
    public TimeValue getExchangeBackpressureTimeout()
    public int getScanBatchSize()
    public TimeValue getSlowQueryLogThreshold()

    public static List<Setting<?>> getAllSettings()
}
```

### DqeMetrics (`org.opensearch.dqe.plugin.metrics`)

```java
public class DqeMetrics {
    public DqeMetrics()
    public void recordQuerySubmitted()
    public void recordQuerySucceeded(long wallTimeMs, long rowsReturned)
    public void recordQueryFailed(String errorCategory)
    public void recordQueryCancelled()
    public void incrementActiveQueries()
    public void decrementActiveQueries()
    public void recordRowsScanned(long rows)
    public void recordBytesScanned(long bytes)
    public void updateMemoryUsed(long bytes)
    public void updateBreakerLimit(long bytes)
    public void recordBreakerTripped()
    public MetricsSnapshot getSnapshot()
    public void reset()
}
```

### DqeQueryOrchestrator (`org.opensearch.dqe.plugin.orchestrator`)

```java
public class DqeQueryOrchestrator {
    public DqeQueryOrchestrator(
        DqeSqlParser parser, DqeAnalyzer analyzer, DqeMetadata metadata,
        StageScheduler stageScheduler, AdmissionController admissionController,
        DqeMemoryTracker memoryTracker, PitManager pitManager,
        DqeSettings settings, DqeMetrics metrics,
        SlowQueryLogger slowQueryLogger, DqeAuditLogger auditLogger,
        ThreadPool threadPool)

    public DqeQueryResponse execute(DqeQueryRequest request) throws DqeException
    public void cancel(String queryId)
}
```

### SlowQueryLogger / DqeAuditLogger (`org.opensearch.dqe.plugin.logging`)

```java
public class SlowQueryLogger {
    public SlowQueryLogger(DqeSettings settings)
    public void maybeLog(DqeQueryRequest request, DqeQueryResponse.QueryStats stats,
                          long memoryPeakBytes)
    public boolean isSlowQuery(long elapsedMs)
}

public class DqeAuditLogger {
    public DqeAuditLogger()
    public void logSuccess(DqeQueryRequest request, List<String> indicesAccessed)
    public void logFailure(DqeQueryRequest request, DqeException exception)
    public void logCancellation(DqeQueryRequest request, List<String> indicesAccessed)
}
```

---

## 12. Resolved Cross-Module Alignment

### R1: DqeErrorCode expansion
**Issue**: Original enum from dev-foundation only had 8 codes. Other modules need 12+ codes.
**Resolution**: Expanded to 20 codes (see Section 3). All modules use codes from this single enum.

### R2: AnalyzedQuery naming
**Issue**: dev-analyzer calls it `AnalyzedQuery`; dev-plugin called it `AnalyzedPlan`.
**Resolution**: Use `AnalyzedQuery` consistently. Dev-plugin will use `AnalyzedQuery`.

### R3: DqeAnalyzer.analyze() signature
**Issue**: dev-plugin omitted `SecurityContext` parameter.
**Resolution**: Use `analyze(Statement, DqeMetadata, SecurityContext)` — plugin layer creates `SecurityContext` from request context and passes it in.

### R4: Handle classes implement Writeable
**Issue**: dev-transport needs `DqeShardSplit`, `DqeTableHandle`, `DqeColumnHandle` to implement `Writeable` for transport serialization.
**Resolution**: All handle classes in dqe-metadata implement `Writeable` with `StreamInput`/`StreamOutput` constructors.

### R5: SortSpecification location
**Issue**: dev-execution asked whether to move to dqe-types.
**Resolution**: Keep in dqe-analyzer (`org.opensearch.dqe.analyzer.sort`). The dependency `dqe-execution -> dqe-analyzer` is allowed by the module graph, so no circular dependency. dqe-execution uses `SortSpecification` from dqe-analyzer directly.

### R6: PushdownPredicate keyword sub-field info
**Issue**: dev-execution needs `.keyword` sub-field paths for term queries on text fields.
**Resolution**: `PushdownPredicate` carries the `fieldName` which is the field to query against. The `PredicateAnalyzer` in dqe-analyzer already resolves `.keyword` sub-field selection during classification — if a text field has a keyword sub-field, the `fieldName` in the `PushdownPredicate` will be `field.keyword`. The `DqeColumnHandle.getKeywordSubField()` metadata enables this resolution.

### R7: Plan fragment serialization
**Issue**: `DqeStageExecuteRequest` carries `byte[] serializedPlanFragment` but no format is defined.
**Resolution**: For Phase 1 (single-stage gather only), the plan fragment carries the `AnalyzedQuery` serialized via Java serialization or a simple custom `Writeable` format. Dev-transport and dev-execution co-own this: dev-execution provides a `PlanFragmentSerializer` that writes the essential fields (table handle, required columns, pushdown predicate, sort spec, limit, column handles) and a corresponding deserializer used by `StageExecutionHandler`.

### R8: GatherExchangeSource lifecycle
**Issue**: dev-plugin expected it as a singleton dependency; dev-transport defines per-query construction.
**Resolution**: `GatherExchangeSource` is created per-query by the orchestrator (not a singleton). The orchestrator creates an `ExchangeBuffer` + `GatherExchangeSource` before dispatching stages, then reads pages from it after dispatch.

### R9: AdmissionController API
**Issue**: dev-transport exposes `tryAcquire() -> boolean`; dev-plugin expected throwing version.
**Resolution**: Keep `tryAcquire() -> boolean`. The orchestrator calls `tryAcquire()` and throws `DqeException(TOO_MANY_CONCURRENT_QUERIES)` if it returns false. This keeps the admission controller simple and the error handling in the orchestrator.

### R10: OperatorFactory for Phase 1
**Issue**: dev-execution asks if OperatorFactory should be skipped.
**Resolution**: Keep `OperatorFactory` as an interface for forward compatibility. Phase 1 pipeline assembly in `StageExecutionHandler` may use direct construction, but the interface exists for Phase 2+ when operators need duplication for parallel pipelines.

---

## 13. Cross-Module Interface Summary

```
Caller → Callee                    Interface Method
─────────────────────────────────────────────────────────────────
Plugin  → Parser                   DqeSqlParser.parse(sql) -> Statement
Plugin  → Analyzer                 DqeAnalyzer.analyze(stmt, meta, sec) -> AnalyzedQuery
Plugin  → Metadata                 DqeMetadata.getTableHandle/getColumnHandles/getSplits
Plugin  → Execution                PitManager.createPit/releasePit
Plugin  → Exchange                 StageScheduler.scheduleStage/cancelQuery
Plugin  → Memory                   AdmissionController.tryAcquire/release
Plugin  → Memory                   QueryMemoryBudget.new/releaseAll

Exchange → Execution               Driver.new/process/close
Exchange → Execution               DriverRunner.enqueueDriver/cancelDrivers
Exchange → Memory                  DqeMemoryTracker.reserve/release (ExchangeBuffer)

Execution → Analyzer               PushdownPredicate (PredicateToQueryDslConverter)
Execution → Analyzer               TypedExpression (ExpressionEvaluator)
Execution → Analyzer               SortSpecification (SortOperator/TopNOperator)
Execution → Analyzer               RequiredColumns (SearchRequestBuilder)
Execution → Types                  SearchHitToPageConverter.convert
Execution → Types                  DqeType (comparators, evaluators)
Execution → Metadata               DqeShardSplit (ShardScanOperator targeting)
Execution → Memory                 QueryMemoryBudget.reserve/release (OperatorContext)

Analyzer → Metadata                DqeMetadata.getTableHandle/getColumnHandles
Analyzer → Types                   DqeType, DqeTypeCoercion

Metadata → Types                   OpenSearchTypeMappingResolver, MultiFieldResolver, TypeWidening
```
