FROM maven:3.6.3-jdk-11 AS builder
COPY src /usr/src/app/src
COPY pom.xml /usr/src/app
RUN mvn -f /usr/src/app/pom.xml clean package

FROM openjdk:8
WORKDIR /
COPY --from=builder /usr/src/app/target/pii-detection*jar-with-dependencies.jar pii-detection.jar
EXPOSE 5000
CMD java -jar pii-detection.jar