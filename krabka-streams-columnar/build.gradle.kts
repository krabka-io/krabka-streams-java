plugins {
    `java-library`
}

dependencies {
    api(project(":krabka-streams"))
    api("org.apache.arrow:arrow-vector:19.0.0")
    runtimeOnly("org.apache.arrow:arrow-memory-netty:19.0.0")
}
