# syntax=docker/dockerfile:1.7

FROM maven:3.9.11-eclipse-temurin-21 AS build

ARG MODULE
ARG ARTIFACT
WORKDIR /workspace

COPY pom.xml ./
COPY common/pom.xml common/pom.xml
COPY services/transaction-service/pom.xml services/transaction-service/pom.xml
COPY services/transaction-generator/pom.xml services/transaction-generator/pom.xml
COPY services/outbox-publisher/pom.xml services/outbox-publisher/pom.xml
COPY services/streaming-processor/pom.xml services/streaming-processor/pom.xml
COPY services/lake-writer/pom.xml services/lake-writer/pom.xml
COPY services/warehouse-writer/pom.xml services/warehouse-writer/pom.xml
COPY services/batch-processor/pom.xml services/batch-processor/pom.xml
COPY services/analytics-service/pom.xml services/analytics-service/pom.xml

RUN --mount=type=cache,target=/root/.m2 \
    test -n "${MODULE}" \
    && test -n "${ARTIFACT}" \
    && mvn -B -ntp -pl "${MODULE}" -am dependency:go-offline

COPY common/src common/src
COPY services services

RUN --mount=type=cache,target=/root/.m2 \
    mvn -B -ntp -pl "${MODULE}" -am -Dmaven.test.skip=true package \
    && cp "${MODULE}/target/${ARTIFACT}" /tmp/application.jar

FROM eclipse-temurin:21.0.9_10-jre

RUN groupadd --gid 10001 banking \
    && useradd --uid 10001 --gid banking --create-home --shell /usr/sbin/nologin banking

WORKDIR /app
COPY --from=build --chown=banking:banking /tmp/application.jar /app/application.jar

USER banking
ENTRYPOINT ["java", "-jar", "/app/application.jar"]
