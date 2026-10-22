# kafka-spring-cloud-streams

Analytique de visites de pages en temps réel avec Spring Cloud Stream et Apache Kafka Streams.

## Présentation

Application Spring Boot événementielle qui simule des visites de pages, les publie sur Kafka, calcule un comptage fenêtré (5 s) par page avec Kafka Streams et diffuse le résultat au navigateur en Server-Sent Events.

## Technologies

- Java 21, Maven
- Spring Boot 3.4.5, Spring Cloud 2024.0.2
- Spring Cloud Stream (Kafka binder), Kafka Streams

## Run

```bash
docker compose up -d
./mvnw spring-boot:run
```

Endpoints : `GET /publish?name=P1` et `GET /analytics` (SSE).
