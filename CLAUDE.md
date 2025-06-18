# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build and Development Commands

### Core Build Commands
- `./gradlew build` - Full build including tests, formatting, and packaging
- `./gradlew assemble` - Generate jar and zip files in build/distributions folder
- `./gradlew compileJava` - Compile all Java source files
- `./gradlew clean` - Clean all build artifacts

### Testing Commands
- `./gradlew test` - Run all unit tests
- `./gradlew :integ-test:integTest` - Run integration tests (takes time)
- `./gradlew :integ-test:yamlRestTest` - Run REST integration tests
- `./gradlew :doctest:doctest` - Run documentation tests
- `./gradlew :integ-test:integTest -Dtests.class="*QueryIT"` - Run specific integration test class
- `./gradlew :<module_name>:test` - Run tests for specific module

### Code Quality Commands
- `./gradlew spotlessCheck` - Check code formatting
- `./gradlew spotlessApply` - Apply code formatting automatically
- `./gradlew pitest` - Run PiTest mutation testing

### Grammar and Parser Commands
- `./gradlew generateGrammarSource` - (Re-)Generate ANTLR parser from grammar files

### Module-Specific Commands
- `./gradlew :<module_name>:build` - Build specific module (e.g., `./gradlew :core:build`)
- `./gradlew :opensearch-sql-plugin:run` - Quick start OpenSearch with plugin installed

## Project Architecture

### Multi-Engine Architecture
The project has evolved through three major engine versions:
- **V1 Engine**: Original NLPchina-based implementation
- **V2 Engine**: Enhanced custom engine with better correctness and extensibility
- **V3 Engine**: Apache Calcite integration for advanced SQL processing and optimization

### Core Modules (from settings.gradle)
- `plugin` - OpenSearch plugin integration and REST endpoints
- `core` - Core query engine and expression evaluation
- `sql` - SQL language processor with ANTLR grammar
- `ppl` - PPL (Piped Processing Language) processor
- `opensearch` - OpenSearch storage engine implementation
- `prometheus` - Prometheus data source integration
- `protocol` - Request/response protocol handling
- `common` - Shared utilities and common functionality
- `legacy` - Backward compatibility with older versions
- `async-query-core` / `async-query` - Asynchronous query execution
- `datasources` - Multi-datasource federation capabilities
- `language-grammar` - ANTLR grammar definitions
- `integ-test` - Integration and comparison tests
- `doctest` - Executable documentation tests

### Query Processing Flow
```
SQL/PPL Query → Parser (ANTLR) → AST → Analyzer → Query Planner → Physical Plan → Execution → Results
```

### Key Grammar Files
- `sql/src/main/antlr/OpenSearchSQLParser.g4` - SQL grammar
- `ppl/src/main/antlr/OpenSearchPPLParser.g4` - PPL grammar
- `language-grammar/src/main/antlr4/` - Shared grammar definitions

### Engine Selection Logic
- V3 Engine (Calcite-based): Default for PPL queries in 3.0.0+
- V2 Engine: Fallback for unsupported V3 features
- V1 Engine: Legacy support only

### Testing Architecture
- **Unit Tests**: Standard JUnit tests in each module
- **Integration Tests**: Uses OpenSearch test framework with in-memory clusters
- **Comparison Tests**: Framework comparing results with other databases
- **Doc Tests**: Executable documentation ensuring accuracy
- **Mutation Tests**: PiTest for code quality validation

## Development Guidelines

### Java Version
- **Build**: Requires Java 21 (defined in build.gradle)
- **Runtime**: Compatible with Java 11+ for OpenSearch compatibility

### Code Formatting
- Uses Google Java Format via Spotless plugin
- 2-space indentation, 100-character line limit
- Automatic import organization and unused import removal
- Always run `./gradlew spotlessApply` before committing

### Language Features
- **SQL**: Supports complex queries, joins, subqueries, window functions, aggregations
- **PPL**: Splunk-like pipe syntax with commands like `search`, `where`, `stats`, `eval`
- **Multi-datasource**: Query across OpenSearch, Prometheus, S3

### Working with Grammar Files
- After modifying .g4 files, run `./gradlew generateGrammarSource`
- Generated parser files are in `build/generated-src/antlr/main/`
- Don't edit generated files directly

### Testing Best Practices
- Write integration tests for new SQL/PPL features
- Use comparison test framework for correctness validation
- Test against multiple data types and edge cases
- Include doc tests for user-facing features

### Module Dependencies
- `core` provides base query engine functionality
- `sql` and `ppl` depend on `core` for execution
- `opensearch` module provides storage engine implementation
- `plugin` module orchestrates all components

### Fallback Mechanisms
- V3 → V2 fallback for unsupported Calcite features
- V2 → V1 fallback for legacy compatibility
- Check logs for "Fallback to V2 query engine" messages

### Performance Considerations
- Use pushdown optimization when possible
- Consider memory usage for large result sets
- Async query support for long-running operations
- Cursor-based pagination for large datasets

### Timezone Handling
- Timezone parameter support is available in PPL queries
- Use "?timezone=ZONE_ID" in REST API requests (e.g., "?timezone=America/Los_Angeles")
- Dates are stored as UTC and displayed in the specified timezone
- Date arithmetic functions respect session timezone (date_add, date_sub, etc.)
- Default timezone is UTC