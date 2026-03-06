# DQE Phase 1 Test Migration & TDD Bug Fix

**Date**: 2026-03-06
**Status**: Approved

## Problem

Basic DQE engine queries are broken. Example: `SELECT category FROM dqe_test_logs WHERE abs(status)=200 LIMIT 10` returns empty results. The engine has no comprehensive integration test suite to catch regressions.

## Root Cause

`ExpressionCompiler.compileFunctionCall()` resolves functions with implicit type coercion (e.g., `abs(BIGINT)` matches `abs(DOUBLE)` via coercion) but never wraps arguments in `CastBlockExpression`. The raw BIGINT Block is passed directly to `abs()` which expects DOUBLE, producing wrong/empty results.

## Scope

Migrate 103 phase1 test cases from `trino-v5/scripts/dqe-test/` into `wukong/dqe/integ-test/`, then run a TDD loop to fix all DQE engine bugs until all tests pass.

### In Scope
- 6 phase1 categories: basic_select (15), where_predicates (25), type_specific (20), order_by_limit (8), multi_shard (13), expressions (10)
- Python validate.py test framework migration
- DQE Java engine bug fixes

### Out of Scope
- `error_cases/` (15 cases) — excluded per requirements
- Phase 2+ test cases
- Changes to test expected values without stated reason + human approval

## Design

### 1. Test Framework

Replace `dqe/integ-test/` with the trino-v5 Python-based framework:

- `validate.py` — adapted to POST to `/_plugins/_trino_sql` (no `engine` param)
- `setup-data.sh` — creates indices with mappings and bulk-loads test data
- `data/mappings/*.json` — index mappings for 7 test indices
- `data/bulk/*.ndjson` — test documents
- `cases/phase1/{category}/Q*.json` — 103 test case files
- `run-phase1-tests.sh` — orchestrates test execution

### 2. TDD Loop

1. Build wukong: `./gradlew :dqe:compileJava`
2. Bootstrap OpenSearch with DQE plugin
3. Load test data: `setup-data.sh`
4. Run all tests: `run-phase1-tests.sh`
5. Identify failures → fix DQE engine code
6. Repeat from step 1 until all 103 tests pass

### 3. Known Bug Fix: Implicit Cast in ExpressionCompiler

In `ExpressionCompiler.compileFunctionCall()`:
- After `registry.resolve(funcName, argTypes)` returns a `ResolvedFunction`
- Compare `resolved.parameterTypes()` with actual `argTypes`
- For each mismatch, wrap the argument `BlockExpression` in `CastBlockExpression(arg, targetType)`
- Pass the cast-wrapped arguments to `ScalarFunctionExpression`

### 4. Test Change Policy

Any modification to test case expected values requires:
- A documented technical reason
- Human approval before committing

## Key Files

### Test Framework (to create/copy)
- `dqe/integ-test/validate.py`
- `dqe/integ-test/setup-data.sh`
- `dqe/integ-test/run-phase1-tests.sh`
- `dqe/integ-test/data/mappings/*.json`
- `dqe/integ-test/data/bulk/*.ndjson`
- `dqe/integ-test/cases/phase1/*/Q*.json`

### DQE Engine (to fix)
- `dqe/src/main/java/org/opensearch/sql/dqe/function/expression/ExpressionCompiler.java`
- `dqe/src/main/java/org/opensearch/sql/dqe/function/expression/CastBlockExpression.java`
- `dqe/src/main/java/org/opensearch/sql/dqe/function/BuiltinFunctions.java`
- Other files as discovered during TDD loop
