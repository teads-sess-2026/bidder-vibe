# ── Build stage: compile & package the Spring Boot bidder ────────────────────
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /app

# Cache dependencies: resolve against pom before copying sources.
# Note: no -q and no `|| true` — a resolution failure must surface here rather
# than being hidden and blowing up later at the COPY --from=build step.
COPY pom.xml .
RUN mvn -B dependency:go-offline

COPY src src
RUN mvn -B clean package -DskipTests

# ── Runtime stage: slim JRE with just the fat jar ────────────────────────────
FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /app/target/*.jar app.jar
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "app.jar"]
