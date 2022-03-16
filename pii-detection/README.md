## Pii Detection 
This module adds pii detection functionality to rtdl system as a seperate stateful function. 
PII detection algorithm identifies **SSN** and **phone number** patterns in the value and applies masking where 
PII values are replaced with **###**

### How the module works?
* Message that's been received through `/ingest` function in _ingest-service.go_ is produced to Kafka topic, **ingress**.
* _pii-detection_ consumes Kafka topic, **pii-detection**
* `PiiDetectionFn` checks incoming message and identifies if it includes **SSN** and **phone number** patterns
* The result is produced to Kafka topic, **ingest**

### The flow
`ingest` -> `pii-detection` -> `statefun-functions`

### How to add stateful function (pii-detection) in the chain
In order to introduce a stateful function between the `ingest` and the `statefun-functions` services without 
touching the code of either:

1. Introduce another topic in Kafka and let `stateful-functions` use that as the input topic rather than `ingress` (**pii-detection**)
2. Implement a stateful function that serves its own service which reads from `ingress` and 
writes to the Kafka topic that `stateful-functions` is listening to (**PiiDetectionFn**)
3. Add **module.yaml** including ingress, egress configuration as below


       kind: io.statefun.endpoints.v2/http
       spec:
           functions: com.rtdl.sf.pii/*
           urlPathTemplate: http://pii-detection:5000/
           transport:
               type: io.statefun.transports.v1/async
       ---
       kind: io.statefun.kafka.v1/ingress
       spec:
           id: com.rtdl.sf.pii/ingress
           address: redpanda:29092
           consumerGroupId: pii-group
           startupPosition:
               type: latest
       
           topics:
             - topic: ingress
               valueType: com.rtdl.sf.pii/IncomingMessage
               targets:
                 - com.rtdl.sf.pii/pii-detection
       ---
       kind: io.statefun.kafka.v1/egress
       spec:
           id: com.rtdl.sf.pii/egress
           address: redpanda:29092
           deliverySemantic:
           type: exactly-once
           transactionTimeout: 5sec

4. Modify **docker-compose.yml** as below


       statefun-worker:
       ......
       volumes:
       ......
       - ./pii-detection/module.yaml:/opt/statefun/modules/pii-detection/module.yaml
       
       
       statefun-manager:
       ......
       volumes:
       ......
       - ./pii-detection/module.yaml:/opt/statefun/modules/pii-detection/module.yaml


       pii-detection:
           build:
               context: ./pii-detection
           expose:
             - 5000
5. Add **Dockerfile** as below


       FROM maven:3.6.3-jdk-11 AS builder
       COPY src /usr/src/app/src
       COPY pom.xml /usr/src/app
       RUN mvn -f /usr/src/app/pom.xml clean package

       FROM openjdk:8
       WORKDIR /
       COPY --from=builder /usr/src/app/target/pii-detection*jar-with-dependencies.jar pii-detection.jar
       EXPOSE 5000
       CMD java -jar pii-detection.jar
