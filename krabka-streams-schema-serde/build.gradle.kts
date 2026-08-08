plugins {
    `java-library`
}

dependencies {
    api(project(":krabka-streams"))
    api("org.apache.avro:avro:1.12.1")
    api("com.google.protobuf:protobuf-java:4.33.5")
    api("com.fasterxml.jackson.core:jackson-databind:2.22.0")
}
