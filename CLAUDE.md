# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Apache Pinot is a real-time distributed OLAP datastore for ultra-low latency analytics over streaming and batch data.

**Core runtime roles:**
- **Broker** (`pinot-broker`): query routing and scatter-gather execution
- **Server** (`pinot-server`): segment storage and query execution
- **Controller** (`pinot-controller`): cluster metadata management via Apache Helix
- **Minion** (`pinot-minion`): async background tasks (segment conversion, purge, etc.)

**Key shared modules:**
- `pinot-spi` / `pinot-common`: shared SPI interfaces and utilities
- `pinot-segment-spi` / `pinot-segment-local`: segment generation, indexes, storage
- `pinot-query-planner` / `pinot-query-runtime`: multi-stage query (MSQ) engine
- `pinot-plugins`: all plugins — input formats (Avro, Parquet, ORC, JSON, etc.), filesystems (S3, GCS, HDFS, ADLS), stream ingestion (Kafka, Pulsar, Kinesis), metrics reporters
- `pinot-connectors`: external integrations (Spark, Flink)
- `pinot-tools`: CLI and quickstart scripts
- `pinot-integration-tests`: end-to-end cluster validation

The Pinot UI is a React.js frontend stored in `pinot-controller/src/main/resources/`.

## Build Commands

Use the Maven wrapper (`./mvnw`). JDK 11+ required (`pinot-clients` targets Java 8).

```bash
# Full build
./mvnw clean install

# Fast dev build (disables slow plugins)
./mvnw verify -Ppinot-fastdev

# Full binary distribution (for quickstart)
./mvnw clean install -DskipTests -Pbin-dist -Pbuild-shaded-jar
```

### Targeted Rebuild (fastest for iterative dev)

Only rebuild changed modules — skips ~150 unrelated modules:

```bash
# Step 1: install changed modules
./mvnw install -T 1C \
  -pl pinot-spi,pinot-common,pinot-segment-spi,pinot-segment-local,pinot-core,pinot-tools \
  -DskipTests -Dlicense.skip -Dcheckstyle.skip -Drat.ignoreErrors=true -q

# Step 2: rebuild distribution JAR
./mvnw package -pl pinot-tools -Pbin-dist \
  -DskipTests -Dlicense.skip -Dcheckstyle.skip -Drat.ignoreErrors=true -q

# Step 3: copy fresh JARs into distribution lib (Maven assembly may use stale classes)
cp pinot-core/target/pinot-core-*.jar               pinot-tools/target/pinot-tools-pkg/lib/
cp pinot-segment-local/target/pinot-segment-local-*.jar  pinot-tools/target/pinot-tools-pkg/lib/
```

Add `--offline` / `-o` to steps 1 and 2 when no new dependencies were added. Adjust `-pl` to include only the modules you touched.

Common module groups:
- SPI / shared: `pinot-spi,pinot-common,pinot-segment-spi`
- Segment / index work: `pinot-segment-local`
- Query execution: `pinot-core`
- Quickstart / CLI: `pinot-tools`

## Test Commands

### Build a module with all its dependencies

```bash
./mvnw -pl pinot-server -am test -DskipTests
```

The `-am` flag builds the module and all modules it depends on.

### Single unit test

Always rebuild the module first, then run the test:

```bash
./mvnw install -pl <module> -DskipTests -Dlicense.skip -Dcheckstyle.skip -Drat.ignoreErrors=true && \
./mvnw test -pl <module> -Dtest=<TestClassName> \
  -Dlicense.skip -Dcheckstyle.skip -Drat.ignoreErrors=true
```

Example:
```bash
./mvnw test -pl pinot-segment-local \
  -Dtest=SparseMapSegmentCreationTest \
  -Dlicense.skip -Dcheckstyle.skip -Drat.ignoreErrors=true
```

### Single integration test

Integration tests start a full embedded cluster (ZK + controller + broker + server). Run only for cross-role validation. They take ~30s each.

```bash
./mvnw test -pl pinot-integration-tests \
  -Dtest=OfflineClusterIntegrationTest \
  -Dsurefire.failIfNoSpecifiedTests=false \
  -Dlicense.skip -Dcheckstyle.skip -Drat.ignoreErrors=true
```

**Note:** `-Dsurefire.failIfNoSpecifiedTests=false` is required for integration tests — without it Maven fails if the test class doesn't match surefire's default includes.

### Quickstart

```bash
pkill -9 -f "pinot" 2>/dev/null; sleep 2
rm -rf /tmp/QuickStart 2>/dev/null
pinot-tools/target/pinot-tools-pkg/bin/pinot-admin.sh QuickStart -type <TYPE>
```

Add a unique build marker (e.g. `[BUILD=v5]`) to a startup log line in the quickstart class to confirm the distribution picked up your latest changes.

## Code Style

After making Java changes:

```bash
./mvnw license:format -pl <module>   # only needed for new files — run BEFORE spotless
./mvnw spotless:apply -pl <module>
./mvnw checkstyle:check -pl <module>
```

Or run all at once:
```bash
./mvnw spotless:apply checkstyle:check \
  -pl pinot-spi,pinot-segment-local,pinot-core \
  -Dlicense.skip
```

Checkstyle rules live under `config/`. Checkstyle enforces a **120 character line limit** — long `printStatus()` calls and string concatenations are common offenders. Break them across lines.

## Coding Conventions

- All source files require the ASF license header; add with `./mvnw license:format`.
- Methods and parameters are non-null by default unless annotated with `@javax.annotation.Nullable`.
- Use SLF4J for logging; do not use `System.out` or `System.err`.
- Use explicit class imports, not inline fully qualified names.
- Add class-level Javadoc for new classes; document thread-safety guarantees. Javadoc supports both `/** ... */` and `///` syntax (JEP-467).
- Keep diffs minimal: do not reformat unrelated code.
- Favor early returns and guard clauses; avoid nesting beyond 2–3 levels.
- Propagate exceptions with useful context; never swallow errors silently.

## Architecture Notes

- **Query changes** often touch both broker planning (`pinot-broker`, `pinot-query-planner`) and server execution (`pinot-server`, `pinot-query-runtime`); verify both.
- **Segment/index changes** live under `pinot-segment-local` and `pinot-segment-spi`.
- **Config or API changes** should update relevant configs and docs.
- Preserve backward compatibility across mixed-version broker/server/controller deployments.
- Timezones: when defining `STANDARD_TIMEZONES`, use format `PDT(America/Los_Angeles)`, sorted by UTC offset.

## Common Pitfalls

- **Dictionary sort order matters.** Any dictionary that claims `isSorted() = true` must sort values in the type's natural order (numeric for INT/LONG/FLOAT/DOUBLE, lexicographic for STRING). Lexicographic sorting of numeric strings (e.g. `"5" > "42"`) silently breaks range predicates and BETWEEN filters via `insertionIndexOf()`.
- **TreeMap keys are always lexicographic.** When building dictId mappings or value dictionaries from a `TreeMap<String, ...>`, re-sort the extracted keys numerically for numeric types before assigning dictIds.
- **Rebuild before testing.** The `install` step for changed modules must complete before running tests — Maven won't automatically rebuild transitive dependencies within `-pl`. Use `./mvnw install -pl <modules> -DskipTests` then `./mvnw test -pl <module> -Dtest=<Test>`.
- **Stale JARs in distribution.** After `./mvnw package -Pbin-dist`, the assembled `pinot-tools-pkg/lib/` may contain stale JARs. Copy freshly built JARs manually (see Targeted Rebuild step 3) before running QuickStart.
- **Integration test data is self-contained.** Integration tests embed their own test data as string arrays — they do not read from `src/main/resources`. QuickStart data files and integration test data are independent and must be updated separately.
