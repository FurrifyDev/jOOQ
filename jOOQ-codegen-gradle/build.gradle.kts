plugins {
    id("java")
    id("groovy")
    id("com.gradle.plugin-publish") version "2.1.0"
}

java {
    sourceCompatibility = JavaVersion.VERSION_25
    targetCompatibility = JavaVersion.VERSION_25
}

group = "org.jooq"
version = "3.21.0-SNAPSHOT"

repositories {
    mavenLocal()
    mavenCentral()
}

publishing {
    repositories {
        maven {
            val releasesRepoUrl = uri("https://repo.snepdragon.net/releases")
            val snapshotsRepoUrl = uri("https://repo.snepdragon.net/snapshots")

            name = "snepdragon"
            url = if (version.toString().endsWith("SNAPSHOT")) snapshotsRepoUrl else releasesRepoUrl

            credentials(PasswordCredentials::class)
        }
    }
}

dependencies {
    implementation(gradleApi())
    implementation("$group:jooq-codegen:$version")
}

tasks.withType<Javadoc> {
    (options as StandardJavadocDocletOptions).addStringOption("Xdoclint:none", "-quiet")
}

gradlePlugin {
    website = "https://jooq.org"
    vcsUrl = "https://github.com/jOOQ/jOOQ"
    plugins {
        create("simplePlugin") {
            id = "${group}.jooq-codegen-gradle"
            displayName = "jooq-codegen-gradle"
            description = "jOOQ code generation plugin for Gradle"
            tags.set(listOf("jooq"))
            implementationClass = "org.jooq.codegen.gradle.CodegenPlugin"
            version = project.version.toString()
        }
    }
}
