# CLAUDE.md

## Building and testing

Use Bazel, not `./gradlew`, for building and testing in this environment:

```shell
bazel build //...
bazel test //...
```

The Gradle build pins a Java 17 toolchain, and the host JVM might not be compatible
(no local JDK 17, and toolchain auto-download is not configured). Bazel provisions
its own remote JDK, so it always works. CI runs Bazel too, so a green
`bazel test //...` is the merge bar; Gradle-only tasks (Javadoc lint, publishing,
`bumpMinor`) run in release workflows with their own JDK.

If a Gradle-only step must be checked locally, run the tool from Bazel's downloaded
JDK under `$(bazel info output_base)/external/rules_java++toolchains+remotejdk17_*`
instead of the host JVM.
