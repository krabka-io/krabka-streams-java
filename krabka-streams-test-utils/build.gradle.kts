plugins {
    `java-library`
}

dependencies {
    api(project(":krabka-streams"))
    api(project(":krabka-streams-schema-serde"))
    api(project(":krabka-streams-columnar"))
    api(project(":krabka-streams-columnar-schema"))
    api("org.apache.kafka:kafka-streams-test-utils:4.3.1")
    testCompileOnly("org.checkerframework:checker-qual:3.49.5")
}

tasks.withType<Test>().configureEach {
    jvmArgs("--add-opens=java.base/java.nio=ALL-UNNAMED")
}

val integrationTestSourceSet = sourceSets.create("integrationTest")

configurations[integrationTestSourceSet.implementationConfigurationName]
    .extendsFrom(configurations.testImplementation.get())
configurations[integrationTestSourceSet.runtimeOnlyConfigurationName]
    .extendsFrom(configurations.testRuntimeOnly.get())

tasks.register<Test>("integrationTest") {
    description = "Runs tests against services set by the KRABKA_INTEGRATION environment variables."
    group = LifecycleBasePlugin.VERIFICATION_GROUP
    testClassesDirs = integrationTestSourceSet.output.classesDirs
    classpath = integrationTestSourceSet.runtimeClasspath
    shouldRunAfter(tasks.test)
    inputs.property(
        "integrationBootstrap",
        providers.environmentVariable("KRABKA_INTEGRATION_BOOTSTRAP").orElse(""),
    )
    inputs.property(
        "integrationSchemaRegistry",
        providers.environmentVariable("KRABKA_INTEGRATION_SCHEMA_REGISTRY").orElse(""),
    )
    jvmArgs("--add-opens=java.base/java.nio=ALL-UNNAMED")
    useJUnitPlatform()
}
