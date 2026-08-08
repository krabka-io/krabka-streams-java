# Build and release

## Prerequisites

- JDK 17 or later on the `PATH`. Gradle's toolchain support provisions Java 17 for
  compilation, so a newer JDK can drive the build.
- No Gradle installation. The wrapper pins Gradle 9.6.1.
- Node.js and npm on the `PATH` for the Markdown tasks only. The JVM build never
  needs them.

## Common tasks

```shell
./gradlew build                              # compile, test, and assemble every module
./gradlew test                               # unit tests only
./gradlew :krabka-streams-columnar:test      # one module
./gradlew javadoc                            # javadoc for every module
./gradlew clean
./gradlew publishToMavenLocal                # install into ~/.m2 for local consumption
```

On Windows, use `gradlew.bat`.

The integration suite is a separate task and needs live services:

```shell
KRABKA_INTEGRATION_BOOTSTRAP=localhost:9092 \
  ./gradlew :krabka-streams-test-utils:integrationTest

KRABKA_INTEGRATION_SCHEMA_REGISTRY=http://localhost:8081 \
  ./gradlew :krabka-streams-test-utils:integrationTest
```

Both variables are declared as task inputs, so changing either invalidates the task
instead of reporting `UP-TO-DATE`. Without them the tests are skipped, not failed.

## Markdown formatting and linting

Documentation is formatted with [prettier](https://prettier.io) and linted with
[markdownlint](https://github.com/DavidAnson/markdownlint). Both run through Gradle:

```shell
./gradlew formatMarkdown          # rewrite every Markdown file in prettier's style
./gradlew checkMarkdownFormat     # fail if any file is not formatted
./gradlew lintMarkdown            # run the markdownlint rule set
```

`installMarkdownTools` runs `npm ci` and is a dependency of the other three. It is
up to date as long as `package.json` and `package-lock.json` are unchanged, so the
install cost is paid once.

Formatting rewrites pipe tables so their columns line up as plain text, normalizes
emphasis to `_underscores_`, collapses stray blank lines, strips trailing whitespace,
and ends every file with a single newline. Prose wrapping is left alone
(`proseWrap: preserve`), so hand-wrapped paragraphs keep the line breaks the author
chose.

| File                       | Purpose                                       |
| -------------------------- | --------------------------------------------- |
| `package.json`             | Pins the prettier and markdownlint versions   |
| `package-lock.json`        | Locks the full dependency tree for `npm ci`   |
| `.prettierrc.json`         | Print width 88, preserved prose wrapping, LF  |
| `.prettierignore`          | Keeps build output and the Gradle wrapper out |
| `.markdownlint-cli2.jsonc` | Rule set, with the reasons for each exception |

The lint configuration turns off the rules prettier already owns, such as emphasis and
list style, so the two tools cannot disagree. `MD013` limits prose to 100 columns and
exempts tables, headings, and code blocks, because aligned tables are wider than any
sensible prose limit.

The same commands work without Gradle, which is convenient inside an editor:

```shell
npm ci
npm run format:markdown
npm run lint:markdown
```

## Build configuration

The root `build.gradle.kts` configures every subproject; the module build files only
declare dependencies and module-specific test flags.

| Setting                  | Value                                          | Where                   |
| ------------------------ | ---------------------------------------------- | ----------------------- |
| Group                    | `io.krabka`                                    | root build              |
| Version                  | `1.0.0`                                        | `gradle.properties`     |
| Java toolchain           | 17                                             | root build              |
| Compiler                 | `--release 17`, UTF-8, `-Xlint:all`, `-Werror` | root build              |
| Javadoc                  | UTF-8, `Xdoclint:all,-missing`                 | root build              |
| Test framework           | JUnit Platform, `junit-bom:5.13.4`             | root build              |
| Sources and Javadoc jars | always built                                   | root build              |
| Arrow JVM flag           | `--add-opens=java.base/java.nio=ALL-UNNAMED`   | columnar and test-utils |

`-Werror` means any warning fails the build, including deprecations, raw types,
unchecked casts, and `this`-escapes. Keep `@SuppressWarnings` scoped to the smallest
element that needs it.

`gradle.properties` holds the version and enables the build cache, parallel execution,
and the configuration
cache, with a 2 GB daemon heap. Configuration cache rules out reading environment
variables at configuration time, which is why the `integrationTest` task declares them
through `providers.environmentVariable(...)`.

Adding a module means creating the directory, adding it to `settings.gradle.kts`, and
adding a POM description to the `moduleDescriptions` map in the root build. Publishing
calls `getValue`, so a missing entry fails the build.

## Version bumping

The version lives in one place, the `version` property in `gradle.properties`, and
three Gradle tasks move it:

```shell
./gradlew bumpPatch     # X.Y.Z becomes X.Y.(Z+1)
./gradlew bumpMinor     # X.Y.Z becomes X.(Y+1).0
./gradlew bumpMajor     # X.Y.Z becomes (X+1).0.0
```

Each task does three things:

1. Rewrites `version` in `gradle.properties`. A minor bump resets the patch number and
   a major bump resets both, so the result is always a clean semantic version.
2. Replaces every standalone reference to the old version in `README.md`, `PARITY.md`,
   and `docs/*.md`. That covers the dependency coordinates, the Maven snippet, and the
   sentences that name the current release. A reference only matches when it stands
   alone, so a `v` prefix or a `-SNAPSHOT` suffix keeps a version out of the rewrite.
3. Opens a `CHANGELOG.md` section for the new version, dated today, with a placeholder
   line to replace. Older entries are never touched, because they describe releases
   that already happened.

The tasks are finalized by `formatMarkdown`, since a longer version string changes the
width of an aligned table.

Two details worth knowing before you run one. Prose that describes a past release does
not belong outside `CHANGELOG.md`, because the bump rewrites it; say what the current
version does instead. And the replacement is textual, so if a version ever collides
with a dependency version quoted in the docs, that quote moves too. Read the diff
before committing.

Override the date with `-PreleaseDate`, which is useful when the release lands on a
different day than the bump:

```shell
./gradlew bumpMinor -PreleaseDate=2026-09-01
```

## Continuous integration

Three workflows live in `.github/workflows`.

### `ci.yml`

Runs on every pull request and on pushes to `main`, with two jobs.

**`build`** runs a matrix over Java 17 and 21, both running `./gradlew build`. This is
the gate for compilation, unit tests, and Javadoc.

**`markdown`** sets up Node 22 alongside Java and runs
`./gradlew checkMarkdownFormat lintMarkdown`. Keeping it in its own job is what lets the
JVM jobs stay free of a Node toolchain, and it is why the Markdown tasks are not wired
into `check`: `./gradlew build` would otherwise fail on a machine without npm.

### `integration.yml`

Runs on the same triggers, with two jobs.

**`broker-integration`** runs a matrix over `apache-kafka-4.3.1` and `krabka-0.3.8`. Each
job starts the broker in a container, waits for the startup line in its logs, finalizes
`streams.version=1` where needed, and runs `integrationTest` with
`KRABKA_INTEGRATION_BOOTSTRAP=localhost:9092`. Broker logs are always dumped afterwards.
`fail-fast: false` keeps one broker's failure from cancelling the other. Timeout: 20
minutes.

**`schema-registry-integration`** starts a krabka broker and the krabka schema
registry 0.3.8 on a shared Docker network, waits for `GET /subjects` to succeed, and runs
`integrationTest` with `KRABKA_INTEGRATION_SCHEMA_REGISTRY=http://localhost:8081`.
Timeout: 15 minutes.

These two jobs are the reference for running the services locally. The listener,
feature, and replication-factor flags are easy to get subtly wrong.

### `release.yml`

Triggered by a tag matching `v*`, with `concurrency` set so two releases of the same ref
cannot overlap and in-progress runs are never cancelled.

Steps:

1. Assert that `MAVEN_CENTRAL_USERNAME`, `MAVEN_CENTRAL_PASSWORD`, `SIGNING_KEY`, and
   `SIGNING_PASSWORD` are all non-empty, before anything is built.
2. `./gradlew build publishAllPublicationsToCentralPortalRepository`, which builds,
   signs, and uploads to the Central Portal staging endpoint.
3. `POST` to the Central Portal `manual/upload/defaultRepository/io.krabka` endpoint with
   `publishing_type=automatic` to promote the deployment.
4. `gh release create <tag> --verify-tag --generate-notes`.

## Publishing

Every subproject applies `maven-publish` and `signing`, and produces a `mavenJava`
publication with the main jar, a sources jar, and a Javadoc jar.

The POM carries the module description from the root build, the project URL, the
Apache-2.0 license, the `krabka-io` developer entry, and SCM coordinates, which is the
metadata Maven Central requires.

Signing uses in-memory PGP keys:

```kotlin
val key = System.getenv("SIGNING_KEY")
if (!key.isNullOrBlank()) {
    useInMemoryPgpKeys(key, System.getenv("SIGNING_PASSWORD"))
    sign(publications)
}
```

A blank or missing `SIGNING_KEY` disables signing entirely, which is what makes
`publishToMavenLocal` work on a developer machine without a key.

The publishing repository is named `centralPortal` and points at the OSSRH staging API,
authenticated with `MAVEN_CENTRAL_USERNAME` and `MAVEN_CENTRAL_PASSWORD`. Those are
Central Portal user tokens, not your Sonatype account password.

### Cutting a release

1. Run `./gradlew bumpPatch`, `bumpMinor`, or `bumpMajor`.
2. Replace the placeholder line in the new `CHANGELOG.md` section with the real
   changes, and read the rest of the diff.
3. Review [PARITY.md](../PARITY.md).
4. Merge to `main` and confirm `ci.yml` and `integration.yml` are green.
5. Tag `vX.Y.Z` and push the tag. `release.yml` does the rest.

To dry-run the publication without uploading anything:

```shell
./gradlew publishToMavenLocal
```

then inspect `~/.m2/repository/io/krabka/`.

## Repository conventions

- `.gitattributes` pins line endings: LF for `.java`, `.kts`, `.md`, and `gradlew`;
  CRLF for `.bat`.
- `.gitignore` covers `.gradle/`, `build/`, `**/build/`, `.idea/`, `*.iml`, `.vscode/`,
  and `out/`.
- The Gradle wrapper jar is committed, as Gradle intends.
- History is merged pull requests from `agent/*` branches; `main` is the default branch.
