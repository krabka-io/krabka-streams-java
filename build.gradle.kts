import org.gradle.api.publish.maven.MavenPublication
import org.gradle.api.tasks.compile.JavaCompile
import org.gradle.api.tasks.javadoc.Javadoc
import org.gradle.jvm.toolchain.JavaLanguageVersion

plugins {
    base
}

group = "io.krabka"
version = "1.0.0"

val moduleDescriptions = mapOf(
    "krabka-streams" to "Apache Kafka Streams API and krabka defaults",
    "krabka-streams-schema-serde" to "Schema registry serdes for krabka streams",
    "krabka-streams-columnar" to "Apache Arrow batch processing for krabka streams",
    "krabka-streams-test-utils" to "Test helpers for krabka streams",
)

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
        options.compilerArgs.addAll(listOf("-Xlint:all", "-Werror"))
    }

    tasks.withType<Javadoc>().configureEach {
        options.encoding = "UTF-8"
        (options as StandardJavadocDocletOptions).addBooleanOption("Xdoclint:all,-missing", true)
    }

    tasks.withType<Test>().configureEach {
        useJUnitPlatform()
    }

    dependencies {
        "testImplementation"(platform("org.junit:junit-bom:5.13.4"))
        "testImplementation"("org.junit.jupiter:junit-jupiter")
        "testRuntimeOnly"("org.junit.platform:junit-platform-launcher")
    }

    extensions.configure<PublishingExtension> {
        publications {
            create<MavenPublication>("mavenJava") {
                from(components["java"])
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
