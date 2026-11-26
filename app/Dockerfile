# Multi-stage build. Built remotely with `az acr build`, so no local Java, Maven or Docker is needed.

# ---- build: compile the app from source inside the image ----
FROM mcr.microsoft.com/openjdk/jdk:21-ubuntu AS build
WORKDIR /workspace

# Resolve dependencies first so this layer is cached while only sources change.
COPY .mvn/ .mvn/
COPY mvnw pom.xml ./
RUN sh mvnw -B -q dependency:go-offline

COPY src/ src/
RUN sh mvnw -B -q -DskipTests package

# ---- runtime ----
FROM mcr.microsoft.com/openjdk/jdk:21-ubuntu
WORKDIR /app
COPY --from=build /workspace/target/app.jar /app/app.jar

# Numeric non-root user (lets Kubernetes verify runAsNonRoot). The app only writes to /tmp.
USER 10001:10001
EXPOSE 8080
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75", "-jar", "/app/app.jar"]
