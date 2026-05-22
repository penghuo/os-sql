# SqlNode Redesign — Inventory

Status: 2026-05-22, scoped to `feat/sqlnode_review` at commit `29e026d6f6`.

## A. PPLFuncImpTable register call graph

### File size & shape
- `core/.../expression/function/PPLFuncImpTable.java` — 1555 lines
- Total registration calls: **266**
- Internal API surface:
  - `register(BuiltinFunctionName, FunctionImp, PPLTypeChecker)` — base registration
  - `registerOperator(BuiltinFunctionName, SqlOperator...)` — wraps Calcite operator
  - `registerOperator(BuiltinFunctionName, SqlOperator, PPLTypeChecker)` — explicit type checker
  - `registerExternalOperator(BuiltinFunctionName, SqlOperator)` — runtime override
  - `registerExternalAggOperator(BuiltinFunctionName, SqlUserDefinedAggFunction)` — agg override

### What the data actually is
Every `registerOperator(name, op[, checker])` call boils down to a triple:
```
(BuiltinFunctionName, SqlOperator, PPLTypeChecker→SqlOperandTypeChecker)
```
The `FunctionImp` wrapper at line 700-702 is just `(builder, args) -> builder.makeCall(operator, args)`. **Pure overhead.** Calcite's validator already does this when given an operator table.

### Categorical breakdown (266 entries)

| Category | Count (approx) | Examples | Maps to |
|---|---|---|---|
| Calcite std operators | ~90 | AND, OR, NOT, PLUS, MINUS, MULTIPLY, ASCII, LOWER, COS, SIN, EXP, FLOOR, CEIL, COALESCE | `SqlStdOperatorTable.X` |
| SqlLibraryOperators | ~20 | REGEXP, CONCAT, REVERSE, MD5, SHA1, LOG2, REGEXP_REPLACE_PG_4 | `SqlLibraryOperators.X` |
| PPLBuiltinOperators (UDF) | ~80 | COSH, SINH, SPAN, WIDTH_BUCKET, EARLIEST, LATEST, ENHANCED_COALESCE, IP comparisons | `ImplementorUDF` instances |
| Aggregations | ~25 | SUM, AVG, COUNT, MIN, MAX, STDDEV_POP, VARSAMP, FIRST, LAST, TAKE | `SqlUserDefinedAggFunction` |
| Window functions | ~10 | ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD, NTH_VALUE | `SqlStdOperatorTable.X` (window) |
| Datetime UDFs | ~30 | YEAR, DATEDIFF, DATE_ADD, FROM_UNIXTIME, STR_TO_DATE | UDF (mostly `ImplementorUDF`) |
| Collection UDFs | ~10 | ARRAY, ARRAY_LENGTH, FILTER, FORALL, REDUCE, TRANSFORM | `ImplementorUDF` |
| Internal helpers | ~10 | INTERNAL_ITEM, INTERNAL_REGEXP_REPLACE_*, INTERNAL_PATTERN | UDF |
| External (storage layer) | 2 | GEOIP, DISTINCT_COUNT_APPROX | `SqlUserDefinedFunction` / `SqlUserDefinedAggFunction` |

### External callers of PPLFuncImpTable (production)

| File | Usage | Notes |
|---|---|---|
| `OpenSearchExecutionEngine.java:334,348` | `registerExternalOperator/registerExternalAggOperator` for GEOIP, DISTINCT_COUNT_APPROX | Late binding — runtime injection |
| `QualifiedNameResolver.java:303` | `resolve(rexBuilder, INTERNAL_ITEM, field, name)` | Builds `RexCall(INTERNAL_ITEM, ...)` for nested-field navigation. **Production-dead under SqlNode** — SqlNode uses `tryMapOrStructItemAccess` directly. |
| `PlanUtils.java:343` | `resolveAgg(name, distinct, field, args, context)` | Aggregation analyzer entry point. **Production-dead under SqlNode** — SqlNode emits agg via `SqlBasicCall` directly. |
| `binning/time/*Handler.java` | `resolve(...)` for span computation | Used by `WidthBucketFunction` UDF only. |
| `MVIndexFunctionImp.java` | `resolve(...)` inside a UDF impl | Used by `MVIndex` UDF only. |
| `UnifiedFunctionCalciteAdapter.java:89` | `resolve(rexBuilder, name, args)` | API-layer adapter; only the SQL path uses it. |
| `PplToSqlNode.java` | 4 references — **all comments** | SqlNode does NOT call PPLFuncImpTable.resolve. |

**Conclusion**: SqlNode never invokes `PPLFuncImpTable.resolve`. The 266 registrations exist purely so that `PplToSqlNode`'s emit path can later resolve through `SqlValidator → buildOperatorTable() → operator lookup` — which already works without `PPLFuncImpTable` because `SqlNodePlanner.buildOperatorTable` already includes `PPLBuiltinOperators.instance()`, `SqlStdOperatorTable.instance()`, and `SqlLibraryOperators` directly.

`QualifiedNameResolver`, `PlanUtils.resolveAgg`, and the `binning/time/*Handler` caller chain are all part of v2's RexBuilder-based emit path — none of them are reached under SqlNode.

### Implication for Phase A
The existing operator-table assembly in `SqlNodePlanner.buildOperatorTable` (lines 255-328) **already does** what Phase A's `PplOperatorTable` was supposed to do. PPLFuncImpTable's true residual responsibility under SqlNode is:
1. Late-binding `registerExternalOperator` for GEOIP / DISTINCT_COUNT_APPROX (2 calls from `OpenSearchExecutionEngine`)
2. Hosting `PPLBuiltinOperators` instances that are built lazily inside `populate()`

**Phase A simplifies to**:
- Move `PPLBuiltinOperators` instance construction out of `populate()` into static initializers (so the operator table can pick them up without `PPLFuncImpTable` ever running)
- Replace `OpenSearchExecutionEngine.registerExternalOperator/AggOperator` calls with direct registration into `OpenSearchExecutionEngine.OperatorTable` (already exists, line 361-380)
- Delete `PPLFuncImpTable.java` and the dead `QualifiedNameResolver`, dead `PlanUtils.resolveAgg`, dead `binning/time/*Handler`, dead `MVIndexFunctionImp`

## B. CoercionUtils external callers

### Direct callsites
1. `PPLFuncImpTable.java:505-506` (`validateFunctionArgs`) — agg arg coercion. **Dead under SqlNode** (PPLFuncImpTable is never invoked).
2. `PPLFuncImpTable.java:625` (`resolveWithCoercion` for comparators) — **dead under SqlNode**.
3. `PPLFuncImpTable.java:641` (`resolveWithCoercion` for non-comparators) — **dead under SqlNode**.
4. None outside PPLFuncImpTable.

### Implication for Phase B
**CoercionUtils is dead the moment PPLFuncImpTable is deleted.** No `PplTypeCoercionImpl` needed — Calcite's stock `TypeCoercionImpl` (already enabled via `withTypeCoercionEnabled(true)` at SqlNodePlanner.java:76, 426) handles the cases that matter:
- STRING ↔ NUMERIC: Calcite's stock coercion handles it (verified by passing ITs)
- DATE+TIME → TIMESTAMP: never actually triggered by any IT (no PPL command produces this combination)

If a regression surfaces, we can install `PplTypeCoercionFactory` then. But Phase B should start with: **delete CoercionUtils, run ITs, see what (if anything) breaks**. Don't pre-build a `PplTypeCoercionImpl` for hypothetical needs.

## C. ExprDate / ExprTime / ExprTimestamp UDT references

### Where the UDT mapping happens
`OpenSearchTypeFactory.convertExprTypeToRelDataType` (or a similar method) maps:
- `ExprCoreType.DATE` → custom UDT wrapping native DATE
- `ExprCoreType.TIME` → custom UDT wrapping native TIME
- `ExprCoreType.TIMESTAMP` → custom UDT wrapping native TIMESTAMP

### Callsites that branch on these UDTs
TODO: enumerate. Estimate: 30-50 callsites in `DateTimeFunctions.java`, datetime UDAFs (`AvgDateAggFunction`?), `OpenSearchExecutionEngine.processValue` (struct/record decoding), explain-output formatters.

### Implication for Phase C
1. Make `convertExprTypeToRelDataType` return `SqlTypeName.DATE` / `TIME` / `TIMESTAMP` directly.
2. Change every callsite that does `if (type instanceof ExprDate)` to `if (type.getSqlTypeName() == SqlTypeName.DATE)`.
3. Delete `ExprDate.java`, `ExprTime.java`, `ExprTimestamp.java`.
4. Keep `ExprDateValue`, `ExprTimeValue`, `ExprTimestampValue` (runtime values, separate concern).

## Revised execution order

The original plan over-engineered Phase A/B. Real plan:

1. **Confirm dead code** — verify SqlNode never reaches `PPLFuncImpTable.resolve` (done, see above).
2. **Move external registrations** — `OpenSearchExecutionEngine` registers GEOIP and DISTINCT_COUNT_APPROX directly into its own `OperatorTable`, not via `PPLFuncImpTable`.
3. **Delete `PPLFuncImpTable` + its dead-cluster siblings** (`QualifiedNameResolver`, `PlanUtils.resolveAgg`, `binning/time/*Handler`, `MVIndexFunctionImp` if dead, `UnifiedFunctionCalciteAdapter` if not used by SqlNode).
4. **Delete `CoercionUtils`** (becomes dead with #3).
5. **Run ITs.** Fix anything that broke.
6. **Phase C**: switch UDT to native types, delete UDT classes.
7. **Run ITs again.**
