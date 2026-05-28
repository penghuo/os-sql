# RelNode → SQL → RelNode Round-Trip Guideline

This guideline codifies the principles, rules and best practices for routing a PPL-built `RelNode` through Calcite's `RelToSqlConverter` → SQL parser → `SqlValidator` → `SqlToRelConverter` pipeline (the `SqlNodePipeline` round trip).

The goal of the round trip is to make `SqlValidator` the single source of type truth, replacing PPL's home-grown coercion plumbing. The guideline below is what we learned from making that work in practice.

## Top-line principles

1. **The SQL string is not a typed wire format.** Anything the visitor knows that Calcite's parser cannot recover from textual SQL is data you will lose. Plan for the loss, don't hope it survives.

2. **Fix the latent bug, not the symptom.** A round-trip failure usually exposes a pre-existing problem that was hidden by the visitor-only path. Bypass guards mask bugs; they do not fix them. Diagnose to the actual layer.

3. **Lean on Calcite's machinery; don't fight it.** If the validator has a coercion path, an operator table, or a library binding, prefer making it work over patching its output. Post-passes that rewrite Calcite's choices are last-resort.

4. **Each layer owns its concerns.** The unparser unparses, the validator validates, the pushdown analyzer matches operators. If a fix lives in the wrong layer, you will pay for it on every future touch.

5. **Pre-passes preserve invariants the SQL string can't carry; post-passes fight Calcite. Prefer pre-passes.** When you must add a post-pass, narrow it.

## Decision flow when a round-trip breaks

```
Round-trip failure
  │
  ├── Validator: "No match found for function signature X"
  │     → Operator-table coverage. Add the right SqlLibrary, register
  │       the operator, or expand the lookup. Not a post-pass problem.
  │
  ├── Validator: "Cannot apply 'F' to arguments of type ..."
  │     → Operator metadata is lying. Trace getOperandMetadata vs.
  │       Calcite's checkOperandTypes contract. Fix the metadata.
  │
  ├── Parser fails outright on the unparsed SQL
  │     → Dialect emits a form the parser doesn't accept. Override
  │       the dialect's unparseCall for that operator/kind.
  │
  ├── Plan validates but executes wrong (semantics drift)
  │     → Type information lost in unparse. Add a narrow pre-pass
  │       wrapping the affected RexNodes in CAST(... AS T).
  │
  └── Pushdown stops firing after round-trip
        → Operator identity changed. Update the pushdown analyzer's
          pattern match to accept both forms; do NOT rewrite the plan
          back to the visitor's operator.
```

## Rules

### R1: SQL textual literals lose type precision. Wrap with CAST when it matters.

SQL has no syntax for "this literal is FLOAT, not DOUBLE" or "this string is VARCHAR, not CHAR". The unparser writes `0.06f` as `6E-2`; the parser types `6E-2` as DOUBLE because that's what SQL says about exponential literals. Likewise, `'foo'` is `CHAR(3)` to the parser, never VARCHAR.

The only mechanism that survives the round trip is an explicit CAST. Use `RexBuilder.makeAbstractCast(targetType, literal)` (not `makeCast`, which constant-folds back to a plain `RexLiteral`).

**Live example:** `SqlNodePipeline.wrapFloatLiteralsForRoundTrip` wraps every FLOAT/REAL `RexLiteral` so PPL's `0.06f - 0.01f` keeps its FLOAT result type end-to-end.

**When NOT to wrap:** Don't wrap *every* VARCHAR literal globally. OpenSearch pushdown analyzers pattern-match on `RexInputRef = RexLiteral` pairs (e.g. `name = 'Jake'`), and a CAST defeats the match. Scope your wrapper narrowly — to a specific operator's operands, a specific Rex shape, or a specific call site.

### R2: Pick the right `SqlLibrary` so the validator can resolve every operator the dialect emits.

A `SqlOperator` exists in Calcite only if the validator's `SqlOperatorTable` exposes it. If the dialect unparses `SAFE_CAST(x AS T)` (BigQuery) or `MAP(k, v)` (Spark), the validator must have those libraries loaded — else the round-tripped SQL fails with "No match found for function signature X".

`SqlNodePipeline.buildOperatorTable` loads `SqlLibrary.BIG_QUERY` (for `SAFE_CAST`) and `SqlLibrary.SPARK` (for `MAP` and other Spark functions). Add libraries when adding dialect-specific unparse forms.

**Verification:** when a round-trip fails with "No match found", grep `SqlLibraryOperators` for the operator name. If it's there, check its `@LibraryOperator(libraries = ...)` annotation and confirm the library is in `buildOperatorTable`.

### R3: `OperandTypes.family(List, optional)` does NOT support variadic operands.

Calcite's `FamilyOperandTypeChecker.checkOperandTypes` performs this check first:

```java
if (families.size() != callBinding.getOperandCount()) return false;
```

The `optional` predicate is consulted only by `getOperandCountRange()`. So `family(MAP*14, i -> i > 0 && i < 14)` does not mean "1..14 MAP operands accepted" — it means "exactly 14 operands required, and 13 of them happen to be reported as optional in error messages".

To express variadic-of-N MAP operands, OR strictly-sized family checkers:

```java
SqlSingleOperandTypeChecker checker = OperandTypes.family(MAP);  // 1 operand
for (int k = 2; k <= MAX_OPERANDS; k++) {
  checker = checker.or(familyOfMaps(k));                          // 2..MAX
}
```

PPL's `PPLFamilyTypeCheckerWrapper` honours the `optional` predicate via `getOperandCountRange().isValidCount(...)`, so the broken form silently passes the visitor-only path. The SqlValidator round trip will surface the latent bug. Fix the metadata, not the round trip.

**Live example:** `RelevanceQueryFunction.getOperandMetadata` was the broken form; we fixed the metadata rather than working around it in `SqlNodePipeline`.

### R4: Pushdown rules pattern-match on operator identity. Round-trip can swap that identity.

The visitor builds `SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR` (a `SqlSpecialOperator`, kind `MAP_VALUE_CONSTRUCTOR`, syntax SPECIAL). After unparse + reparse + validate, the same call binds to `SqlLibraryOperators.MAP` (a `SqlBasicFunction`, kind `OTHER_FUNCTION`, syntax FUNCTION). Same semantics, different `SqlOperator` instance.

Pushdown rules typically check `call.getOperator() == X`. After the round trip the check fails, push-down never fires, and the call falls through to enumerable codegen — which for relevance UDFs throws `UnsupportedOperationException("only supported when pushed down")`.

**Fix in the consumer, not the pipeline.** The pushdown analyzer is the rule's owner. Have it accept both operator identities (or check `SqlKind`, or compare canonical "is this a key/value pair list?" semantics):

```java
// PredicateAnalyzer.expectMapCall
if (call.getOperator() == SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR
    || call.getOperator() == SqlLibraryOperators.MAP) { ... }
```

**Anti-pattern:** A `SqlNodePipeline` post-pass that rewrites `SqlLibraryOperators.MAP` back to `SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR`. We tried this; it works, but it puts the OpenSearch pushdown analyzer's matching concern inside the round-trip pipeline. Don't.

### R5: Bypass guards mask bugs. Never grow the bypass list.

A guard like `if (containsX(plan)) return original;` says "I don't know what's wrong, but I know skipping the round trip works around it." Each guard hides:
- A latent bug in operator metadata (R3).
- A missing library binding (R2).
- A dialect's unparse choice the parser can't handle.
- A pushdown rule that's over-restrictive on operator identity (R4).
- A type-information loss that needs a CAST wrapper (R1).

Track the failures, diagnose to the actual layer, and fix there. The temporary inconvenience of a broken IT is worth the long-term gain of a correctly-typed pipeline.

**Process:** when a new round-trip failure shows up, the first question is "which of R1–R4 is this?" Resist adding to a guard list.

### R6: A round-trip-only fix belongs in `SqlNodePipeline`. A semantics fix belongs at the source.

Use `SqlNodePipeline` pre/post-passes only when the fix has no meaning outside the round trip. `wrapFloatLiteralsForRoundTrip` is round-trip-only (wrapping a FLOAT literal in CAST inside the visitor would be pointless — the visitor already has the FLOAT type). Operator-metadata bugs (R3) are round-trip-amplified but exist independently — fix them at the operator definition.

**Test:** would the fix make the visitor-only path more correct, more consistent, or fix a known bug? If yes, fix at the source. If the fix is purely "bridge an information-loss across SQL serialisation", it belongs in `SqlNodePipeline`.

### R7: Trace before you theorise.

Operator identity, RelDataType, RexLiteral type-name — these are easy to test directly with a temporary unit test under `ppl/src/test/java/org/opensearch/sql/ppl/calcite/`. Extend `CalcitePPLAbstractTest`, build the visitor RelNode via `getRelNode(ppl)`, and dump the relevant fields. Cleanup after.

**Why this matters:** Calcite has multiple type-checker contracts, multiple operator-table layers, multiple coercion paths. Reasoning about which one is firing is far more error-prone than running it. We made several wrong diagnoses in this session that a 30-line trace test would have caught immediately.

## Cookbook: common failure shapes

### "No match found for function signature X(...)"

Validator can't find operator X. Steps:

1. Grep `SqlLibraryOperators` and `SqlStdOperatorTable` for X.
2. If in a library: check `buildOperatorTable` includes that library.
3. If `MAP_VALUE_CONSTRUCTOR` specifically: confirm `SqlLibrary.SPARK` is loaded (Spark's MAP function name-resolves the unparsed `MAP(k, v)`).
4. If a PPL UDF: confirm `PPLBuiltinOperators.instance()` exposes it.

### "Cannot apply 'F' to arguments of type 'F(<X>)'"

Operand-type check failed. Steps:

1. Read `F.getOperandMetadata().getInnerTypeChecker()`.
2. If it's `family(...)` with an `optional` predicate: it's broken (R3). Replace with strictly-sized OR-chain.
3. If it's `wrapUDT(...)` or `PPLFamilyTypeChecker`: trace its `checkOperandTypes` to see which operand is rejected.
4. Type-information drift (R1): if the visitor's RexCall has e.g. `(VARCHAR, VARCHAR)` operands but post-trip is `(CHAR(N), CHAR(N))`, a CAST pre-pass may be needed if the checker is fixed-type-strict.

### Plan validates but execution throws "supported only when pushed down"

Operator identity changed (R4). Steps:

1. Compare the visitor's RexCall operator vs. the post-trip operator. They differ.
2. Find the pushdown rule's pattern match. Update it to accept both forms.
3. Don't rewrite the plan back to the visitor's operator in `SqlNodePipeline`.

### Plan validates and executes but wrong result/schema

Type-information drift the validator didn't catch (R1). Steps:

1. Compare visitor-side RelDataType vs. post-trip RelDataType for the affected column.
2. If literal-typing differs (FLOAT → DOUBLE, VARCHAR → CHAR(N)): add a narrow CAST pre-pass for that RexNode shape.
3. Verify the CAST survives via `makeAbstractCast`, not `makeCast`.

## Anti-patterns we removed

- **Bypass guards in `revalidate()`.** ~16 of them. Each masked a real bug somewhere else. Removed entirely.
- **`restoreMapValueConstructor` post-pass.** Rewrote post-trip Spark `MAP` back to `MAP_VALUE_CONSTRUCTOR` so OpenSearch pushdown matched. Replaced by widening the pushdown rule's pattern match (R4).
- **`wrapMapConstructorOperands` pre-pass.** Cast all VARCHAR/CHAR operands of `MAP_VALUE_CONSTRUCTOR` to VARCHAR. Was redundant once `RelevanceQueryFunction.getOperandMetadata` (R3) was fixed; the validator's family check on `MAP` outer-type is component-type-agnostic.

## File index

- `core/src/main/java/org/opensearch/sql/calcite/sqlnode/SqlNodePipeline.java` — pipeline entry, operator-table assembly, FLOAT-literal wrapper, parser/validator wiring.
- `core/src/main/java/org/opensearch/sql/calcite/sqlnode/OpenSearchSparkSqlDialect.java` — dialect overrides (POSITION, TRIM, REAL/VARCHAR cast specs).
- `core/src/main/java/org/opensearch/sql/calcite/sqlnode/PplConvertletTable.java` — PPL-specific Sql → Rex conversion overrides.
- `core/src/main/java/org/opensearch/sql/expression/function/udf/RelevanceQueryFunction.java` — operand metadata example (R3).
- `opensearch/src/main/java/org/opensearch/sql/opensearch/request/PredicateAnalyzer.java` — pushdown pattern-match consumer (R4).
- `docs/dev/sqlnode-pipeline-it-status.md` — IT-by-IT round-trip status tracker.
