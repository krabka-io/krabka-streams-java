import org.gradle.api.publish.maven.MavenPublication
import org.gradle.api.tasks.compile.JavaCompile
import org.gradle.api.tasks.javadoc.Javadoc
import org.gradle.jvm.toolchain.JavaLanguageVersion
import org.gradle.jvm.tasks.Jar

plugins {
    `java-platform`
    `jvm-toolchains`
    `maven-publish`
    signing
}

group = "io.krabka"
// The version lives in gradle.properties so `bumpPatch`, `bumpMinor`, and `bumpMajor`
// can rewrite it. Gradle applies it to this project and every subproject.

val moduleDescriptions = mapOf(
    "krabka-streams" to "Apache Kafka Streams API and krabka defaults",
    "krabka-streams-schema-serde" to "Schema registry serdes for krabka streams",
    "krabka-streams-columnar" to "Apache Arrow batch processing for krabka streams",
    "krabka-streams-columnar-schema" to "Avro and Protobuf Arrow bridges for krabka streams",
    "krabka-streams-test-utils" to "Test helpers for krabka streams",
)

val automaticModuleNames = mapOf(
    "krabka-streams" to "io.krabka.streams",
    "krabka-streams-schema-serde" to "io.krabka.streams.schema.serde",
    "krabka-streams-columnar" to "io.krabka.streams.columnar",
    "krabka-streams-columnar-schema" to "io.krabka.streams.columnar.schema",
    "krabka-streams-test-utils" to "io.krabka.streams.test.utils",
)

// Compilation stays on the Java 17 toolchain, but Javadoc runs with JDK 25's tool:
// its doclet adds --syntax-highlight (JDK-8348282), which bundles highlight.js and
// highlights {@snippet} and <pre>{@code} fragments without any custom scripting.
val javadocLanguageVersion = JavaLanguageVersion.of(25)
val rootJavaToolchains = extensions.getByType<JavaToolchainService>()

subprojects {
    group = rootProject.group
    version = rootProject.version

    apply(plugin = "java-library")
    apply(plugin = "maven-publish")
    apply(plugin = "signing")

    repositories {
        mavenCentral()
    }

    extensions.configure<JavaPluginExtension> {
        toolchain.languageVersion.set(JavaLanguageVersion.of(17))
        withSourcesJar()
        withJavadocJar()
    }

    tasks.withType<JavaCompile>().configureEach {
        options.encoding = "UTF-8"
        options.release.set(17)
        options.compilerArgs.addAll(listOf("-Xlint:all", "-Werror", "-parameters"))
    }

    val javaToolchains = extensions.getByType<JavaToolchainService>()
    tasks.withType<Javadoc>().configureEach {
        javadocTool.set(javaToolchains.javadocToolFor {
            languageVersion.set(javadocLanguageVersion)
        })
        options.encoding = "UTF-8"
        (options as StandardJavadocDocletOptions).apply {
            addBooleanOption("Xdoclint:all", true)
            addBooleanOption("Werror", true)
            addBooleanOption("-syntax-highlight", true)
            addFileOption("-add-stylesheet", rootProject.file("docs/site/javadoc-theme.css"))
        }
        inputs.file(rootProject.file("docs/site/javadoc-theme.css"))
    }

    tasks.withType<Jar>().configureEach {
        manifest.attributes["Automatic-Module-Name"] = automaticModuleNames.getValue(project.name)
    }

    val shadedJar = tasks.register<Jar>("shadedJar") {
        dependsOn(project.configurations.getByName("runtimeClasspath").buildDependencies)
        archiveClassifier.set("all")
        duplicatesStrategy = DuplicatesStrategy.EXCLUDE
        from(project.extensions.getByType<SourceSetContainer>()["main"].output)
        from({ project.configurations.getByName("runtimeClasspath").map(project::zipTree) })
        exclude("META-INF/*.SF", "META-INF/*.DSA", "META-INF/*.RSA", "module-info.class")
    }

    tasks.withType<Test>().configureEach {
        useJUnitPlatform()
    }

    dependencies {
        "testImplementation"(platform("org.junit:junit-bom:5.13.4"))
        "testImplementation"("org.junit.jupiter:junit-jupiter")
        "testImplementation"("com.google.testparameterinjector:test-parameter-injector-junit5:1.22")
        "testImplementation"("org.assertj:assertj-core:3.27.7")
        "testRuntimeOnly"("org.junit.platform:junit-platform-launcher")
    }

    extensions.configure<PublishingExtension> {
        publications {
            create<MavenPublication>("mavenJava") {
                from(components["java"])
                artifact(shadedJar)
                artifactId = project.name
                pom {
                    name.set(project.name)
                    description.set(moduleDescriptions.getValue(project.name))
                    url.set("https://github.com/krabka-io/krabka-streams-java")
                    licenses {
                        license {
                            name.set("Apache License, Version 2.0")
                            url.set("https://www.apache.org/licenses/LICENSE-2.0.txt")
                            distribution.set("repo")
                        }
                    }
                    developers {
                        developer {
                            id.set("krabka-io")
                            name.set("krabka-io")
                            url.set("https://github.com/krabka-io")
                        }
                    }
                    scm {
                        connection.set("scm:git:https://github.com/krabka-io/krabka-streams-java.git")
                        developerConnection.set("scm:git:ssh://git@github.com/krabka-io/krabka-streams-java.git")
                        url.set("https://github.com/krabka-io/krabka-streams-java")
                    }
                }
            }
        }
        repositories {
            maven {
                name = "centralPortal"
                url = uri("https://ossrh-staging-api.central.sonatype.com/service/local/staging/deploy/maven2/")
                credentials {
                    username = System.getenv("MAVEN_CENTRAL_USERNAME")
                    password = System.getenv("MAVEN_CENTRAL_PASSWORD")
                }
            }
        }
    }

    extensions.configure<SigningExtension> {
        val key = System.getenv("SIGNING_KEY")
        val password = System.getenv("SIGNING_PASSWORD")
        if (!key.isNullOrBlank()) {
            useInMemoryPgpKeys(key, password)
            sign(extensions.getByType<PublishingExtension>().publications)
        }
    }
}

dependencies {
    constraints {
        api(project(":krabka-streams"))
        api(project(":krabka-streams-schema-serde"))
        api(project(":krabka-streams-columnar"))
        api(project(":krabka-streams-columnar-schema"))
        api(project(":krabka-streams-test-utils"))
    }
}

publishing {
    publications {
        create<MavenPublication>("krabkaStreamsBom") {
            from(components["javaPlatform"])
            artifactId = "krabka-streams-bom"
            pom {
                name.set("krabka-streams-bom")
                description.set("Dependency constraints for krabka streams modules")
                url.set("https://github.com/krabka-io/krabka-streams-java")
                licenses {
                    license {
                        name.set("Apache License, Version 2.0")
                        url.set("https://www.apache.org/licenses/LICENSE-2.0.txt")
                        distribution.set("repo")
                    }
                }
                developers {
                    developer {
                        id.set("krabka-io")
                        name.set("krabka-io")
                        url.set("https://github.com/krabka-io")
                    }
                }
                scm {
                    connection.set("scm:git:https://github.com/krabka-io/krabka-streams-java.git")
                    developerConnection.set("scm:git:ssh://git@github.com/krabka-io/krabka-streams-java.git")
                    url.set("https://github.com/krabka-io/krabka-streams-java")
                }
            }
        }
    }
    repositories {
        maven {
            name = "centralPortal"
            url = uri("https://ossrh-staging-api.central.sonatype.com/service/local/staging/deploy/maven2/")
            credentials {
                username = System.getenv("MAVEN_CENTRAL_USERNAME")
                password = System.getenv("MAVEN_CENTRAL_PASSWORD")
            }
        }
    }
}

signing {
    val key = System.getenv("SIGNING_KEY")
    if (!key.isNullOrBlank()) {
        useInMemoryPgpKeys(key, System.getenv("SIGNING_PASSWORD"))
        sign(publishing.publications)
    }
}

// Markdown formatting and linting. The tools are prettier and markdownlint-cli2,
// pinned in package.json and configured in .prettierrc.json and
// .markdownlint-cli2.jsonc. They need Node.js and npm on the PATH.
val npmCommand = if (System.getProperty("os.name").startsWith("Windows")) "npm.cmd" else "npm"
val markdownGroup = "markdown"

val installMarkdownTools = tasks.register<Exec>("installMarkdownTools") {
    description = "Installs the pinned prettier and markdownlint-cli2 versions."
    group = markdownGroup
    workingDir = layout.projectDirectory.asFile
    commandLine(npmCommand, "ci", "--no-audit", "--no-fund")
    inputs.file(layout.projectDirectory.file("package.json"))
    inputs.file(layout.projectDirectory.file("package-lock.json"))
    outputs.file(layout.projectDirectory.file("node_modules/.package-lock.json"))
}

val markdownInputs = fileTree(layout.projectDirectory) {
    include("**/*.md")
    include(".prettierrc.json")
    include(".prettierignore")
    include(".markdownlint-cli2.jsonc")
    exclude("node_modules/**", "**/build/**", ".gradle/**")
}

val formatMarkdown = tasks.register<Exec>("formatMarkdown") {
    description = "Rewrites every Markdown file in prettier's style, aligning tables."
    group = markdownGroup
    dependsOn(installMarkdownTools)
    workingDir = layout.projectDirectory.asFile
    commandLine(npmCommand, "run", "format:markdown")
}

tasks.register<Exec>("checkMarkdownFormat") {
    description = "Fails when a Markdown file is not in prettier's style."
    group = markdownGroup
    dependsOn(installMarkdownTools)
    mustRunAfter(formatMarkdown)
    workingDir = layout.projectDirectory.asFile
    commandLine(npmCommand, "run", "checkFormat:markdown")
    inputs.files(markdownInputs)
}

tasks.register<Exec>("lintMarkdown") {
    description = "Runs markdownlint over every Markdown file."
    group = markdownGroup
    dependsOn(installMarkdownTools)
    mustRunAfter(formatMarkdown)
    workingDir = layout.projectDirectory.asFile
    commandLine(npmCommand, "run", "lint:markdown")
    inputs.files(markdownInputs)
}

// Documentation site. `aggregateJavadoc` runs one Javadoc pass over every module so
// cross-module references link and one search index covers the whole API;
// `javadocSite` wraps it with the landing page under docs/site/ for GitHub Pages.
val documentationGroup = "documentation"

// The aggregate Javadoc classpath is a root-level configuration because resolving
// another project's compileClasspath from here is unsafe under the configuration
// cache. Runtime usage carries every module's api and implementation dependencies.
repositories {
    mavenCentral()
}

val aggregateJavadocClasspath: Configuration by configurations.creating {
    isCanBeConsumed = false
    attributes {
        attribute(
            org.gradle.api.attributes.Usage.USAGE_ATTRIBUTE,
            objects.named(org.gradle.api.attributes.Usage::class.java, org.gradle.api.attributes.Usage.JAVA_RUNTIME),
        )
        attribute(
            org.gradle.api.attributes.Category.CATEGORY_ATTRIBUTE,
            objects.named(org.gradle.api.attributes.Category::class.java, org.gradle.api.attributes.Category.LIBRARY),
        )
        attribute(
            org.gradle.api.attributes.LibraryElements.LIBRARY_ELEMENTS_ATTRIBUTE,
            objects.named(
                org.gradle.api.attributes.LibraryElements::class.java,
                org.gradle.api.attributes.LibraryElements.JAR,
            ),
        )
    }
}

dependencies {
    subprojects.forEach { subproject ->
        aggregateJavadocClasspath(project(subproject.path))
    }
}

val aggregateJavadoc = tasks.register<Javadoc>("aggregateJavadoc") {
    description = "Generates one Javadoc run over every module's public API."
    group = documentationGroup
    javadocTool.set(rootJavaToolchains.javadocToolFor {
        languageVersion.set(javadocLanguageVersion)
    })
    setDestinationDir(layout.buildDirectory.dir("docs/aggregate-javadoc").get().asFile)
    subprojects.forEach { subproject ->
        source(subproject.extensions.getByType<SourceSetContainer>()["main"].allJava)
    }
    classpath = aggregateJavadocClasspath
    title = "krabka streams for Java $version"
    options.encoding = "UTF-8"
    (options as StandardJavadocDocletOptions).apply {
        docEncoding = "UTF-8"
        charSet = "UTF-8"
        windowTitle = "krabka streams for Java $version API"
        addBooleanOption("Xdoclint:all", true)
        addBooleanOption("Werror", true)
        addBooleanOption("-syntax-highlight", true)
        addFileOption("-add-stylesheet", rootProject.file("docs/site/javadoc-theme.css"))
        overview = rootProject.file("docs/site/overview.html").absolutePath
        group("Kafka Streams", "io.krabka.streams")
        group("Schema registry and serdes", "io.krabka.streams.schema")
        group("Columnar processing", "io.krabka.streams.columnar")
        group("Avro and Protobuf Arrow bridges", "io.krabka.streams.columnar.schema")
        group("Test utilities", "io.krabka.streams.test")
        bottom = "<a href=\"https://github.com/krabka-io/krabka-streams-java\">krabka-streams-java</a>" +
            " is licensed under the Apache License 2.0."
    }
    inputs.files(
        rootProject.file("docs/site/javadoc-theme.css"),
        rootProject.file("docs/site/overview.html"),
    )
}

tasks.register<Sync>("javadocSite") {
    description = "Assembles the static documentation site for GitHub Pages."
    group = documentationGroup
    val siteVersion = version.toString()
    into(layout.buildDirectory.dir("javadoc-site"))
    from(aggregateJavadoc) {
        into("api")
    }
    from(layout.projectDirectory.file("docs/site/index.html")) {
        filter { line -> line.replace("@VERSION@", siteVersion) }
    }
    // GitHub Pages serves the artifact as-is, but .nojekyll keeps any future
    // Jekyll-based serving from hiding the doclet's underscore-prefixed files.
    doLast {
        destinationDir.resolve(".nojekyll").writeText("")
    }
}

// Version bumping. `bumpPatch`, `bumpMinor`, and `bumpMajor` rewrite the version in
// gradle.properties, update every place the documentation names the current version,
// and open a CHANGELOG section for the new release.
abstract class BumpVersionTask : DefaultTask() {
    /** `major`, `minor`, or `patch`. */
    @get:Input
    abstract val part: Property<String>

    /** Release date for the new CHANGELOG heading. Defaults to today. */
    @get:Input
    @get:Optional
    abstract val releaseDate: Property<String>

    @get:Internal
    abstract val repositoryRoot: DirectoryProperty

    @TaskAction
    fun bump() {
        val root = repositoryRoot.get().asFile
        val properties = File(root, "gradle.properties")
        val currentLine = Regex("(?m)^version=(.+)$").find(properties.readText())
            ?: throw GradleException("gradle.properties has no version property")
        val current = currentLine.groupValues[1].trim()
        val next = next(current)

        properties.writeText(properties.readText().replaceFirst(currentLine.value, "version=$next"))
        logger.lifecycle("version $current -> $next")

        val occurrence = Regex("(?<![\\w.-])" + Regex.escape(current) + "(?![\\w.-])")
        documentationFiles(root).forEach { file ->
            val original = file.readText()
            val rewritten = original.replace(occurrence, next)
            if (rewritten != original) {
                file.writeText(rewritten)
                val count = occurrence.findAll(original).count()
                logger.lifecycle("  ${file.toRelativeString(root)}: $count reference(s)")
            }
        }

        openChangelogSection(File(root, "CHANGELOG.md"), next)
        logger.lifecycle("Describe the release in CHANGELOG.md, then review the diff before committing.")
    }

    private fun next(current: String): String {
        val match = Regex("^(\\d+)\\.(\\d+)\\.(\\d+)$").find(current)
            ?: throw GradleException("version `$current` is not major.minor.patch")
        val (major, minor, patch) = match.destructured
        return when (val requested = part.get()) {
            "major" -> "${major.toInt() + 1}.0.0"
            "minor" -> "$major.${minor.toInt() + 1}.0"
            "patch" -> "$major.$minor.${patch.toInt() + 1}"
            else -> throw GradleException("unknown version part `$requested`")
        }
    }

    /**
     * Every Markdown file that names the current version, except CHANGELOG.md, whose
     * older entries describe releases that already happened.
     */
    private fun documentationFiles(root: File): List<File> =
        (listOf(File(root, "README.md"), File(root, "PARITY.md"))
            + (File(root, "docs").listFiles { file -> file.extension == "md" }?.toList() ?: emptyList()))
            .filter { it.isFile }
            .sorted()

    private fun openChangelogSection(changelog: File, version: String) {
        if (!changelog.isFile) {
            return
        }
        val text = changelog.readText()
        if (Regex("(?m)^## " + Regex.escape(version) + "\\b").containsMatchIn(text)) {
            logger.lifecycle("  CHANGELOG.md already has a $version section")
            return
        }
        val date = releaseDate.getOrElse(java.time.LocalDate.now().toString())
        val heading = Regex("(?m)^# .+$").find(text)
            ?: throw GradleException("CHANGELOG.md has no top-level heading")
        val section = "\n\n## $version - $date\n\n- _Describe the changes in this release._"
        changelog.writeText(
            text.substring(0, heading.range.last + 1) + section + text.substring(heading.range.last + 1).trimStart('\n').let { "\n\n$it" },
        )
        logger.lifecycle("  CHANGELOG.md: opened the $version section")
    }
}

val versionGroup = "versioning"
val releaseDateProperty = providers.gradleProperty("releaseDate")

listOf("major", "minor", "patch").forEach { part ->
    tasks.register<BumpVersionTask>("bump${part.replaceFirstChar { it.uppercase() }}") {
        description = "Increases the $part version and updates the documentation."
        group = versionGroup
        this.part.set(part)
        releaseDate.set(releaseDateProperty)
        repositoryRoot.set(layout.projectDirectory)
        finalizedBy(formatMarkdown)
    }
}
