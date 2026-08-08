plugins {
    `java-library`
}

dependencies {
    api(project(":krabka-streams"))
    api(project(":krabka-streams-schema-serde"))
    api(project(":krabka-streams-columnar"))
    api("org.apache.kafka:kafka-streams-test-utils:4.3.1")
}
