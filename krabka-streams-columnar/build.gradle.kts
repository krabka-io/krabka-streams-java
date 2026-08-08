plugins {
    `java-library`
}

dependencies {
    api(project(":krabka-streams"))
    api("org.apache.arrow:arrow-vector:19.0.0")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.22.0")
    runtimeOnly("org.apache.arrow:arrow-memory-netty:19.0.0")
    testCompileOnly("org.checkerframework:checker-qual:3.49.5")
}

tasks.withType<Test>().configureEach {
    jvmArgs("--add-opens=java.base/java.nio=ALL-UNNAMED")
}
