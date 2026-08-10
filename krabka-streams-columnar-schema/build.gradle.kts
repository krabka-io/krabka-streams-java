plugins {
    `java-library`
}

dependencies {
    api(project(":krabka-streams-columnar"))
    api(project(":krabka-streams-schema-serde"))
    implementation("com.google.protobuf:protobuf-java-util:4.33.5")
}

tasks.withType<Test>().configureEach {
    jvmArgs("--add-opens=java.base/java.nio=ALL-UNNAMED")
}
