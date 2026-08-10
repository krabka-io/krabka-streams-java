"""Hermetic aggregated Javadoc and documentation-site rules.

The Gradle build renders the same outputs for local use and Maven publishing;
these rules exist so CI can build the documentation site through Bazel with a
pinned remote JDK instead of whatever the runner happens to have installed. The
JDK is an explicit attribute (`jdk`), normally `@remotejdk25_linux//:jdk`, whose
doclet provides `--syntax-highlight`.
"""

load("@rules_java//java/common:java_common.bzl", "java_common")
load("@rules_java//java/common:java_info.bzl", "JavaInfo")

JavaRuntimeInfo = java_common.JavaRuntimeInfo

def _quote(value):
    return "\"" + value.replace("\\", "\\\\").replace("\"", "\\\"") + "\""

def _aggregate_javadoc_impl(ctx):
    java_runtime = ctx.attr.jdk[JavaRuntimeInfo]
    out = ctx.actions.declare_directory(ctx.label.name)
    classpath = depset(transitive = [dep[JavaInfo].transitive_compile_time_jars for dep in ctx.attr.deps])

    lines = [
        "-encoding UTF-8",
        "-docencoding UTF-8",
        "-charset UTF-8",
        "-quiet",
        "-Xdoclint:all",
        "-Werror",
        "--syntax-highlight",
        "--add-stylesheet " + ctx.file.stylesheet.path,
        "-overview " + ctx.file.overview.path,
        "-bottom " + _quote(ctx.attr.bottom),
    ]
    for title, packages in ctx.attr.groups.items():
        lines.append("-group " + _quote(title) + " " + packages)
    lines.append("-classpath " + _quote(":".join([jar.path for jar in classpath.to_list()])))
    for src in ctx.files.srcs:
        lines.append(src.path)
    argfile = ctx.actions.declare_file(ctx.label.name + ".args")
    ctx.actions.write(argfile, "\n".join(lines) + "\n")

    command = """set -euo pipefail
version=$(sed -n 's/^version=//p' {properties})
{javadoc} @{argfile} \
    -doctitle "krabka streams for Java $version" \
    -windowtitle "krabka streams for Java $version API" \
    -d {out}
""".format(
        properties = ctx.file.properties.path,
        javadoc = java_runtime.java_home + "/bin/javadoc",
        argfile = argfile.path,
        out = out.path,
    )
    ctx.actions.run_shell(
        outputs = [out],
        inputs = depset(
            ctx.files.srcs + [argfile, ctx.file.stylesheet, ctx.file.overview, ctx.file.properties],
            transitive = [classpath, java_runtime.files],
        ),
        command = command,
        mnemonic = "AggregateJavadoc",
        progress_message = "Generating aggregated Javadoc with a hermetic JDK",
    )
    return [DefaultInfo(files = depset([out]))]

aggregate_javadoc = rule(
    implementation = _aggregate_javadoc_impl,
    doc = "Runs one javadoc invocation over every module's sources with a pinned JDK.",
    attrs = {
        "srcs": attr.label_list(allow_files = [".java"], doc = "Every module's main sources."),
        "deps": attr.label_list(providers = [JavaInfo], doc = "The module libraries, for the classpath."),
        "jdk": attr.label(providers = [JavaRuntimeInfo], doc = "The JDK whose javadoc runs."),
        "stylesheet": attr.label(allow_single_file = True),
        "overview": attr.label(allow_single_file = True),
        "properties": attr.label(allow_single_file = True, doc = "gradle.properties, for the version."),
        "groups": attr.string_dict(doc = "Overview group title to colon-separated package list."),
        "bottom": attr.string(doc = "Footer HTML for every page."),
    },
)

def _javadoc_site_impl(ctx):
    out = ctx.actions.declare_directory(ctx.label.name)
    api = ctx.attr.api[DefaultInfo].files.to_list()[0]
    command = """set -euo pipefail
version=$(sed -n 's/^version=//p' {properties})
mkdir -p {out}/api
cp -RL {api}/. {out}/api/
sed "s/@VERSION@/$version/g" {index} > {out}/index.html
touch {out}/.nojekyll
""".format(
        properties = ctx.file.properties.path,
        api = api.path,
        index = ctx.file.index.path,
        out = out.path,
    )
    ctx.actions.run_shell(
        outputs = [out],
        inputs = depset([api, ctx.file.index, ctx.file.properties]),
        command = command,
        mnemonic = "JavadocSite",
        progress_message = "Assembling the documentation site",
    )
    return [DefaultInfo(files = depset([out]))]

javadoc_site = rule(
    implementation = _javadoc_site_impl,
    doc = "Wraps the aggregated Javadoc with the landing page for GitHub Pages.",
    attrs = {
        "api": attr.label(doc = "The aggregate_javadoc target."),
        "index": attr.label(allow_single_file = True, doc = "The landing page with @VERSION@ markers."),
        "properties": attr.label(allow_single_file = True, doc = "gradle.properties, for the version."),
    },
)
