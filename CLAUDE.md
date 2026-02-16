# Repository Guidelines

## Project Structure & Modules
- `core/`: Calcite integration, expressions, and analysis utilities.
- `sql/` and `ppl/`: Grammars, parsers, and AST builders for SQL and PPL.
- `opensearch/`: Storage access, query pushdown rules, and scripting.
- `protocol/`: Response formatting (JSON, JDBC, CSV).
- `plugin/`: OpenSearch plugin entry points and REST wiring.
- `async-query/`: Spark-backed asynchronous execution.
- `integ-test/`, `doctest/`, `yamlRestTest/`: Integration and doc-style tests.
- `docs/`, `scripts/`, `build-tools/`: Documentation, helper scripts, and build utilities.

## Build, Test, and Development Commands
- `./gradlew clean build`: Full build and unit test suite.
- `./gradlew :core:test`: Module-specific tests for the core engine.
- `./gradlew :integ-test:integTest -DignorePrometheus`: Integration tests against OpenSearch.
- `./gradlew doctest -DignorePrometheus`: Validate documentation examples.
- `./gradlew yamlRestTest`: Run YAML REST tests.
- Use `./gradlew -Dtests.seed=<seed>` to reproduce flaky tests when needed.

## Coding Style & Naming Conventions
- Java 11+ with Lombok; follow existing visitor and analyzer patterns.
- Keep package paths aligned with modules (e.g., `org.opensearch.sql.core.*`).
- Prefer descriptive class and method names; avoid one-letter identifiers.
- Match surrounding indentation (Java/Kotlin: 4 spaces; scripts: bash style).
- Keep public APIs documented with Javadoc; align comments with Calcite terminology.

## Testing Guidelines
- Unit tests use JUnit 5 and live under each module’s `src/test/java`.
- Name tests after the unit under test (`ClassNameTests`) and favor readable `@DisplayName`.
- For integration coverage, place REST and end-to-end cases in `integ-test` or `yamlRestTest` with clear dataset setup/cleanup.
- Run the narrowest relevant Gradle target before opening a PR; include seeds or commands used to reproduce failures.

## Commit & Pull Request Guidelines
- Write concise, imperative commit subjects; reference GitHub issues (`#123`) when applicable.
- One logical change per commit; keep diffs focused on the described scope.
- Pull requests should summarize intent, key design choices, and test results in the description.
- Add release-note entries when behavior changes are user-visible; include config updates or compatibility notes as bullets.
- Request reviews from owners of affected modules (`core`, `opensearch`, `sql`, etc.) and link any relevant design docs.

## Security & Configuration Tips
- Avoid committing cluster credentials; prefer environment variables or local config files ignored by Git.
- When changing query execution or script generation, consider least-privilege access and cross-cluster compatibility.
- Validate new endpoints and pushdown rules against the default security plugin configuration before merging.

## Issue Triage

Use the PPL triage skill for triaging GitHub issues. See `.claude/skills/ppl-triage.md` for detailed instructions.

### Quick Start
```bash
# Start local OpenSearch
./gradlew :opensearch-sql:run &

# Use the helper script
.claude/skills/ppl-triage-helper.sh setup-test
.claude/skills/ppl-triage-helper.sh ppl-explain 'source=testindex | where field1 = "xxxx"'
```

### Issue Types
- **Bug Report**: Verify with explain API, show DSL queries, provide reproduction steps
- **Feature Request**: Check scope, link related issues, estimate complexity
- **Documentation**: Identify gaps, suggest concrete examples
- **Question/Support**: Answer directly, point to correct syntax

### Key APIs for Debugging
- PPL Explain: `POST /_plugins/_ppl/_explain {"query": "..."}`
- SQL Explain: `POST /_plugins/_sql/_explain {"query": "..."}`
- PPL Execute: `POST /_plugins/_ppl {"query": "..."}`
- SQL Execute: `POST /_plugins/_sql {"query": "..."}`

