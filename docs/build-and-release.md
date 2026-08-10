# Build and release

## Prerequisites

- JDK 17 or later on the `PATH`. Gradle's toolchain support provisions Java 17 for
  compilation, so a newer JDK can drive the build.
- No Gradle installation. The wrapper pins Gradle 9.6.1.
- Bazelisk for Bazel builds. `.bazelversion` pins Bazel 9.2.0 and Bazel downloads its
  Java 17 toolchain.
- Node.js and npm on the `PATH` for the Markdown tasks only. The JVM build never
  needs them.

## Common tasks

```shell
./gradlew build                              # compile, test, and assemble every module
./gradlew test                               # unit tests only
./gradlew :krabka-streams-columnar:test      # one module
./gradlew javadoc                            # javadoc for every module
./gradlew javadocJar                         # package every module's javadoc
./gradlew javadocSite                        # assemble the GitHub Pages documentation site
./gradlew clean
./gradlew publishToMavenLocal                # install into ~/.m2 for local consumption
bazel build //...                            # compile every module
bazel test //...                             # unit, formatting, and lint tests
bazel test //... --config=remote             # run those actions on BuildBuddy RBE
```

On Windows, use `gradlew.bat`.

The remote config sends build events and actions to BuildBuddy Cloud. Authenticate local
builds by creating an ignored `user.bazelrc` with the API key from BuildBuddy Quickstart:

```text
build --remote_header=x-buildbuddy-api-key=YOUR_API_KEY
```

GitHub Actions reads the same credential from the `BUILDBUDDY_API_KEY` repository secret.
The integration workflow stays local because its tests connect to services on the runner.

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
| Version                  | `1.2.0`                                        | `gradle.properties`     |
| Java toolchain           | 17                                             | root build              |
| Compiler                 | `--release 17`, UTF-8, `-Xlint:all`, `-Werror` | root build              |
| Javadoc                  | UTF-8, `Xdoclint:all`, `-Werror`               | root build              |
| Test framework           | JUnit Platform, `junit-bom:5.13.4`             | root build              |
| Sources and Javadoc jars | always built                                   | root build              |
| Shaded jar               | `all` classifier                               | root build              |
| JPMS name                | stable `Automatic-Module-Name`                 | root build              |
| Version platform         | `krabka-streams-bom`                           | root build              |
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

Four workflows live in `.github/workflows`.

### `ci.yml`

Runs on every pull request and on pushes to `main`. The `build` job runs
`bazel test //...` with Java 17 and 21, compiling every module and running the Java,
formatting, and lint tests. Bazel supplies Java, Node, and the npm tools; the workflow
does not install those toolchains separately.

### `integration.yml`

Runs on the same triggers, with two jobs.

**`broker-integration`** runs a matrix over `apache-kafka-4.3.1` and `krabka-0.3.8`. Each
job starts the broker in a container, waits for the startup line in its logs, finalizes
`streams.version=1` where needed, and runs the Bazel `integration_tests` target with
`KRABKA_INTEGRATION_BOOTSTRAP=localhost:9092`. Broker logs are always dumped afterwards.
`fail-fast: false` keeps one broker's failure from cancelling the other. Timeout: 20
minutes.

**`schema-registry-integration`** starts a krabka broker and the krabka schema
registry 0.3.8 on a shared Docker network, waits for `GET /subjects` to succeed, and runs
the same Bazel target with `KRABKA_INTEGRATION_SCHEMA_REGISTRY=http://localhost:8081`.
Timeout: 15 minutes.

These two jobs are the reference for running the services locally. The listener,
feature, and replication-factor flags are easy to get subtly wrong.

### `pages.yml`

Runs on pushes to `main` and manually through `workflow_dispatch`, publishing the
documentation site to GitHub Pages at
<https://krabka-io.github.io/krabka-streams-java/>.

The `build` job runs `bazel build //:javadoc-site`, which assembles
`bazel-bin/javadoc-site`: the landing page from `docs/site/index.html` (with
`@VERSION@` replaced by the current version) plus one aggregated Javadoc run over
every module under `api/`. The rules live in `tools/javadoc.bzl` and run javadoc
from Bazel's pinned `remotejdk25_linux`, so the published site is hermetic — it
never depends on the runner's installed Java — and the JDK 25 doclet's
`--syntax-highlight` option bundles highlight.js to color the `{@code}` examples.
Aggregation makes cross-module references link and gives the whole API one search
index; `docs/site/javadoc-theme.css` restyles the standard doclet and retints the
highlight tokens to the krabka palette, and `docs/site/overview.html` supplies the
overview text. The Gradle equivalents remain: `./gradlew javadocSite` renders the
same site locally, and the published `-javadoc.jar`s run the same JDK 25 javadoc
tool through Gradle's toolchains while compilation stays on Java 17. The `deploy` job publishes
the uploaded artifact with `actions/deploy-pages`; it alone holds the
`pages: write` and `id-token: write` permissions, and a `pages` concurrency group
keeps deployments sequential without cancelling one in flight.

Pages is configured with the "GitHub Actions" source. That is a one-time repository
setting; re-enable it with
`gh api repos/krabka-io/krabka-streams-java/pages -X POST -f build_type=workflow`
if the repository is ever recreated.

### `release.yml`

Triggered by a tag matching `v*`, with `concurrency` set so two releases of the same ref
cannot overlap and in-progress runs are never cancelled.

Steps:

1. Assert that `MAVEN_CENTRAL_USERNAME`, `MAVEN_CENTRAL_PASSWORD`, `SIGNING_KEY`, and
   `SIGNING_PASSWORD` are all non-empty, before anything is built.
2. `bazel test //...` verifies the release sources.
3. `./gradlew javadocJar` runs doclint with warnings as errors and packages the package
   summaries and examples.
4. `./gradlew publishAllPublicationsToCentralPortalRepository` creates, signs, and uploads
   the Maven artifacts. Gradle remains the publisher because it owns the POM, sources,
   Javadoc, and signing configuration.
5. `POST` to the Central Portal `manual/upload/defaultRepository/io.krabka` endpoint with
   `publishing_type=automatic` to promote the deployment.
6. `gh release create <tag> --verify-tag --generate-notes`.

## Publishing

Every subproject applies `maven-publish` and `signing`, and produces a `mavenJava`
publication with the main, sources, Javadoc, and `all` jars. The root project publishes
the `krabka-streams-bom` platform. Each Javadoc JAR contains its package overview and a
usage example, and the release workflow packages those JARs before any upload begins.

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
