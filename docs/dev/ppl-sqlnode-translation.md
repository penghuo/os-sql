# PPL AST → SqlNode Translation Guidelines

This document captures the design conventions for `PPLToSqlNodeVisitor` (`core/src/main/java/org/opensearch/sql/calcite/sqlnode/`). It is the path every PPL query takes after parsing: PPL AST → SqlNode → Calcite SqlValidator → SqlToRelConverter → RelNode.

The guidelines below are not aspirational; each one was extracted from a concrete pitfall the visitor hit and resolved.

## Architecture

```
UnresolvedPlan (PPL AST)
   │
   ├── stripImplicitMetaProjects()      ← AST pre-pass
   │
   ▼
PPLToSqlNodeVisitor                     ← compositional visitor
   │   uses: SqlBuilder DSL
   │   threads: Frame state
   ▼
SqlNode tree
   │
   ▼
SqlValidator → SqlToRelConverter        ← Calcite handles this
   │
   ▼
RelNode
   │
   ├── stripMetadataFields()            ← post-RelNode shuttle
   │
   ▼
RelNode (final)
```

Three responsibilities, three layers:

1. **AST pre-pass** removes AstBuilder-injected markers that don't belong in SqlNode form.
2. **Visitor** does pure structural translation — AST shape to SqlNode shape — with the `Frame` carrying just enough state for cross-pipe disambiguation.
3. **Post-RelNode shuttle** enforces final-output policies (e.g. metadata field stripping) that the validator can't express.

Each layer should hold the concern it's best at. Mixing them is the recurring smell.

## Compositional Visitor

Each `visit*` method **returns** the SqlNode for its subtree. Parent visitors compose children's outputs:

```java
@Override
public SqlNode visitProject(Project node, Frame frame) {
  SqlNode from = node.getChild().get(0).accept(this, frame);   // recurse
  // ... build SELECT around `from` ...
  return SqlBuilder.select(items).from(from).withFields(...).wrap(frame);
}
```

Anti-patterns:

- **Mutating a shared "in-flight pipeline" state** (the v2 `Pipeline` pattern). Composability dies; every visitor has to know what the predecessor left in the bag. This visitor explicitly avoided it.
- **Returning `Void` and storing the SqlNode on the frame.** Same problem.
- **Reaching into the AST sibling.** Each `visit*` walks ITS subtree and returns. The parent decides composition.

## Frame State

`Frame` is the per-translation state object passed through the visitor. Keep it minimal:

```java
static final class Frame {
  List<String> currentFields;   // what columns are visible to the next pipe
  JoinHints joinHints;          // bind-bare-to-LEFT state from upstream join, or null
}
```

### Rules

1. **One field per concern.** Don't add three coupled fields when one record will do. The original three-field form (`joinLeftAlias`, `joinRightAlias`, `joinAmbiguousColumns`) had an "all-or-none" invariant the type system couldn't enforce — every reader had to null-check three things and hope the writer set all three. Bundling into a `JoinHints` record made the invariant a type.

2. **Don't keep dead state.** `Frame.sourceTable` was set by `visitRelation` and never read elsewhere. Dead state calcifies — the next change will preserve it "in case." Delete it.

3. **State is scope-bounded, not query-bounded.** `joinHints` is valid only between `visitJoin` and the next visitor that wraps the pipeline. Not the whole translation. The wrap terminal (`SqlBuilder.select(...).wrap(frame)`) clears it unconditionally — bug-by-omission becomes impossible.

4. **Each AST recursion gets its own Frame when scope is independent.** `visitJoin` walks the right side with `Frame rightFrame = new Frame()` so the right's local state doesn't leak back into the parent.

## SqlBuilder DSL

Centralise SqlNode construction AND its frame transition in one place. Each builder method represents one SqlNode shape:

```java
SqlBuilder.select(items)                                    // SELECT
    .from(from)
    .withFields(deduped)
    .wrap(frame)                                            // seals scope

SqlBuilder.join()                                           // JOIN
    .left(L).right(R).type(jt).on(cond)
    .joinHints(leftAlias, rightAlias, ambiguous)            // optional
    .build(frame)                                           // leaves scope open

SqlBuilder.aliasAs(inner, "tt")                             // <inner> AS tt
SqlBuilder.relation(tableParts, fields, frame)              // bare table
```

### Two terminal styles

- **`.wrap(frame)`** — used by SELECT. The wrapping SELECT seals off prior alias scope, so `joinHints` is cleared. Use when materialising the in-flight pipeline into a settled subquery.
- **`.build(frame)`** — used by JOIN, AS, relation. The produced SqlNode is a participant in an enclosing scope, not a fresh one. Frame state is set (or left alone), not torn down.

### Why centralise

Direct `new SqlSelect(...)` / `new SqlJoin(...)` scattered in the visitor + manual frame field assignment creates the bug pattern: forget the assignment, ship a latent bug. The builder makes both responsibilities atomic — you can't construct the node without addressing the frame transition.

## AST Pre-pass

The AstBuilder injects markers that exist only as v2-engine signals. They must NOT be materialised as SqlNode wrappers.

### Example: `Project(AllFieldsExcludeMeta)`

PPL's AstBuilder wraps every join side, every subsearch, and every top-level query as `Project(AllFieldsExcludeMeta, child)` to mean "drop OpenSearch metadata fields from this subtree." Materialising this as `SELECT cols FROM X` in SqlNode form **hides the table identifier** `X` from outer scope, breaking downstream `X.col` references.

Strip these in a pre-pass before visiting:

```java
public SqlNode translate(UnresolvedPlan plan) {
  return stripImplicitMetaProjects(plan).accept(this, new Frame());
}
```

The visitor then handles only user-written projections. Metadata stripping moves to `QueryService.stripMetadataFields` (a post-RelNode shuttle on the final row type).

**Heuristic**: if AstBuilder injects a node that doesn't correspond to a user-written pipe, strip it pre-translation. Don't try to recognise-and-elide it inside the visitor — every visitor that touches its child has to learn the special case.

## Metadata Field Policy

PPL's rule (per `docs/user/ppl/general/identifiers.md`): metadata fields (`_id`, `_index`, `_score`, `_maxscore`, `_sort`, `_routing`, `_uid`) are excluded from output by default. Only explicit references keep them.

Where this lives in the visitor:

| Case | Location | Mechanism |
|---|---|---|
| `source=X` (bare query) | `QueryService.stripMetadataFields` post-RelNode shuttle | drops meta from final row type |
| `\| fields *` | `resolveSelectedFields` AllFields branch | returns `nonMeta` |
| `\| fields a*` (wildcard) | `resolveSelectedFields` wildcard expansion | matches against `nonMeta` |
| `\| fields _id, name` | `resolveSelectedFields` explicit-name branch | passes through unfiltered |
| `\| fields - _id` | `resolveSelectedFields` exclusion branch | starts from `nonMeta` |
| `where _id='1'` | not handled here | filter ref allowed; output strips via the shuttle |

The metadata filter in `resolveSelectedFields` is **the `*`-expansion rule**, not a general policy — it only fires when the user wrote `*` or a wildcard. Explicit names bypass it. Don't conflate name resolution with metadata policy in other places.

## Comment Etiquette

Bad:

```java
// Multi-part identifiers like `t1.name` need to be split into [t1, name] so the validator
// sees a qualified reference, not a single dotted-string column. PPL uses `.` for both
// alias.column qualification and struct-field navigation; at this point the join layer
// has already settled the alias scope, so split-on-dot is safe for the shapes we accept.
```

This comment is wrong (split-on-dot is NOT safe — the validator handles alias-vs-struct disambiguation; we just emit multi-part) and the wrongness propagated assumptions into other parts of the code. **A wrong comment is worse than no comment.**

Rules:

- Explain WHY the code is shaped this way, not WHAT it does.
- If the rule comes from a Calcite quirk, name the quirk: "Calcite labels a multi-part ref `t1.name` as just `name`; we add `AS \"t1.name\"` to preserve the user-typed header."
- If the rule comes from PPL semantics, cite the doc: "PPL's `*` excludes metadata — see `docs/user/ppl/general/identifiers.md`."
- Never write a comment that hand-waves a question you actually need to think through. The next reader will trust it and propagate the bug.

## When To Use The Validator vs Decide Yourself

The validator handles a lot of resolution work for free. Don't reimplement it:

- **Multi-part identifier resolution.** `[t1, name]` — is `t1` an alias or a STRUCT column with a `name` field? The validator decides. Emit the split parts; let it work.
- **Type checking and coercion.** `CAST(x AS INTEGER)` — the validator handles operand types. Emit the SqlNode shape.
- **Wildcard expansion in SELECT.** `SELECT *` expands to the FROM's row type. Emit `SqlIdentifier.star(POS)`; let it expand.

But the validator does NOT do these — visitor must:

- **PPL's bind-bare-to-LEFT post-join.** SQL says ambiguous; PPL says left wins. Visitor qualifies.
- **Implicit metadata-field exclusion.** `*` in PPL means "non-metadata"; in SQL means "everything." Visitor pre-filters.
- **Header label preservation for dotted projections.** `t1.name AS "t1.name"` — the AS alias survives Calcite's auto-uniquification.

If you're tempted to reimplement validator logic, stop and check first — there's usually a simpler emission shape that lets it do the work.

## Adding a New Visitor

Checklist for `visitX`:

1. **What SqlNode shape does this produce?** SELECT, JOIN, AS, relation, or something new (UNION, ORDER BY, ...)?
2. **Use SqlBuilder if it covers your shape.** If not, decide whether to extend `SqlBuilder` or construct directly. Extend when there are ≥2 call sites that need the same shape.
3. **Frame state changes — wrap or build?** If you produce a settled SELECT subquery, terminate with `.wrap(frame)`. If you participate in an enclosing scope, terminate with `.build(frame)` (or set frame state explicitly with a clear method name).
4. **Does PPL semantics differ from SQL here?** If yes, document the divergence in a comment with a concrete example. If no, let the validator handle it.
5. **Do you need to strip an AstBuilder marker?** If the AstBuilder wraps your AST node with a v2-engine signal that doesn't translate, add it to `stripImplicitMetaProjects` pre-pass.
6. **Test against an IT.** Drive by a `Calcite*IT` integration test, not unit tests.

## Anti-patterns Encountered

| Anti-pattern | Symptom | Fix |
|---|---|---|
| Mutable Pipeline shared state | Each visitor has to know what predecessors left in the bag | Compositional visitor — children return SqlNodes |
| Three coupled Frame fields with informal "all-or-none" invariant | Bug class: set two, forget the third | One record (`JoinHints`) — type-enforced |
| `frame.x = ...; frame.y = ...; frame.z = null;` scattered in visitors | New visitor adds field; old visitors don't reset | Centralise in builder terminal (`.wrap(frame)`) |
| Materialising AstBuilder markers as SqlNode wrappers | Table identifier disappears from outer scope | Strip in pre-pass; enforce policy post-RelNode |
| Wrong comment that becomes load-bearing | Future reader trusts the assertion, propagates bug | Delete or rewrite — wrong comments worse than missing ones |
| Pre-resolving validator's job (alias vs struct, type coercion) | Visitor logic balloons; struct columns crash | Emit the shape; let validator decide |

## See Also

- Source files: `core/src/main/java/org/opensearch/sql/calcite/sqlnode/`
- v2 visitor (legacy, being replaced): `PplToSqlNode.java` — read for context only
- AstBuilder injection sites: `ppl/src/main/java/org/opensearch/sql/ppl/parser/AstBuilder.java` (search `projectExceptMeta`)
- Metadata field doc: `docs/user/ppl/general/identifiers.md`
