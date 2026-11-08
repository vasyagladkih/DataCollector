FROM eclipse-temurin:25-jdk-alpine AS builder
WORKDIR /app

COPY gradlew settings.gradle build.gradle ./
COPY gradle ./gradle
COPY common ./common
COPY control-plane ./control-plane
COPY data-ingestor ./data-ingestor

RUN chmod +x ./gradlew && ./gradlew :control-plane:bootJar --no-daemon -x test

FROM eclipse-temurin:25-jre-alpine
WORKDIR /app

COPY --from=builder /app/control-plane/build/libs/control-plane-0.0.1-SNAPSHOT.jar app.jar

EXPOSE 8081
ENTRYPOINT ["java", "-jar", "app.jar"]
