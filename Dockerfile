FROM maven:3.9-eclipse-temurin-17

COPY common/target/*.jar .

RUN curl -o opentelemetry-javaagent.jar -L https://github.com/open-telemetry/opentelemetry-java-instrumentation/releases/download/v1.29.0/opentelemetry-javaagent.jar

COPY run_app.sh .

RUN chmod +x run_app.sh

ENTRYPOINT ["bash", "run_app.sh"]
