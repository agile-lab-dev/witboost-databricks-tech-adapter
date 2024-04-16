# OpenTelemetry Instrumentation

[OpenTelemetry](https://opentelemetry.io/docs/concepts/) is an observability framework designed to aid in the generation and collection of application telemetry data such as metrics, logs, and traces.

One of the biggest advantages of using OpenTelemetry is that it is vendor-agnostic. It can export data in multiple formats which you can send to a backend of your choice.

This project includes support for sending metrics and traces with OpenTelemetry, making it easy to integrate it in you observability stack of choice.

### Setup Automatic Instrumentation

Automatic instrumentation uses a [Java agent](https://github.com/open-telemetry/opentelemetry-java-instrumentation) that can be attached to any Java application. It dynamically injects bytecode to capture telemetry from many popular libraries and frameworks. This reduces the amount of work required to integrate OpenTelemetry into the application code.

When the agent starts, which is done immediately after the JVM starts, it logs the following:

```
[otel.javaagent 2023-08-31 15:40:32:492 +0200] [main] INFO io.opentelemetry.javaagent.tooling.VersionLogger - opentelemetry-javaagent - version: 1.29.0
```

The Docker image includes the OpenTelemetry agent jar and it is enabled by default. Ensure that whatever OpenTelemetry endpoint you are pointing to is accessible from Docker, otherwise it won't work. The agent will print out warnings about being unable to reach the endpoint, so it's easy to spot:

```
[otel.javaagent 2023-08-31 15:40:46:765 +0200] [OkHttp http://localhost:5555/...] WARN io.opentelemetry.exporter.internal.grpc.GrpcExporter - Failed to export spans. Server responded with gRPC status code 2. Error message: Failed to connect to localhost/[0:0:0:0:0:0:0:1]:5555
[otel.javaagent 2023-08-31 15:40:46:808 +0200] [OkHttp http://localhost:5555/...] WARN io.opentelemetry.exporter.internal.grpc.GrpcExporter - Failed to export metrics. Server responded with gRPC status code 2. Error message: Failed to connect to localhost/[0:0:0:0:0:0:0:1]:5555
```

If running outside of Docker you can still use the agent; in order to do so, first [download the agent jar](https://github.com/open-telemetry/opentelemetry-java-instrumentation/releases/latest/download/opentelemetry-javaagent.jar) and then either:
- attach it to the Maven JVM by setting MAVEN_OPTS and then use the `spring-boot:run` goal:
  ```bash
  export MAVEN_OPTS="-javaagent:path/to/opentelemetry-javaagent.jar"
  mvn -pl common spring-boot:run
  ```
- build the fat jar and launch the application with a JVM with the agent attached:
  ```bash
  mvn package spring-boot:repackage
  java -javaagent:path/to/opentelemetry-javaagent.jar -jar common/target/databricks-specific-provisioner.jar
  ```

The second option avoids altering your Maven setup, so that's what we recommend using.

The agent is highly configurable (see [doc](https://opentelemetry.io/docs/instrumentation/java/automatic/agent-config/)). Here’s an example of agent configuration via environment variables:

```
OTEL_TRACES_EXPORTER=otlp \
OTEL_METRICS_EXPORTER=otlp \
OTEL_SERVICE_NAME=databricks-specific-provisioner \
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:5555 \
```

Here’s an explanation of what each environment variables does:

- `OTEL_SERVICE_NAME`  sets the name of the service associated with your telemetry, and is sent to your [Observability backend](https://opentelemetry.io/ecosystem/vendors/).

- `OTEL_TRACES_EXPORTER` specifies which traces exporter to use. In this case, traces are being exported  with `otlp`.

- `OTEL_METRICS_EXPORTER` specifies which metrics exporter to use. In this case, metrics are being exported to `otlp`.

- `OTEL_EXPORTER_OTLP_ENDPOINT` sets the endpoint where telemetry is exported to. If omitted, the default `Collector` endpoint will be used, which is `0.0.0.0:4317` for gRPC and `0.0.0.0:4318` for HTTP.

#### Example observability backend using Docker Compose

> **Note**
Ensure that [Docker Compose](https://docs.docker.com/compose/) is installed on your machine.

You can find a basic observability backend built with Grafana, Tempo, Prometheus and the OpenTelemetry Collector in the `otel` directory. You can run it with:

```bash
docker compose up
```

This will run the collector and all other services. With the agent setup and configuration outlined above the application will send telemetry data to this backend so you can immediately start to experiment with it, given that the application is running outside of Docker.

If you want to test locally using Docker, you can use the local machine hostname as the endpoint for the telemetry exporter, like this:

```bash
docker run --name java-sp-container \
-e OTEL_EXPORTER_OTLP_ENDPOINT=http://$(hostname -f):5555 \
-p 8888:8888 databricks-specific-provisioner
```

### Grafana

If ran locally, Grafana is available on `localhost:3000`; for more information on how to use Grafana refer to the official [Grafana documentation](https://grafana.com/docs/?plcmt=learn-nav).

#### Grafana dashboards

Grafana allows to create dashboards combining traces and metrics received from the application. As a starting point, we provide a default basic dashboard showing successful v.s. failed HTTP requests. This dashboard is loaded as a volume on the grafana container and can be found in the local directory
`otel/o11y-backend/grafana/dashboards`.

If you want to modify this default dashboard, once Grafana is running, create a new dashboard or modify an existing one and export it as a JSON file (see [Grafana documentation](https://grafana.com/docs/grafana/latest/dashboards/manage-dashboards/#export-and-import-dashboards) on this matter.) You can add this JSON dashboard in the provided folder to be loaded at startup, as all JSON dashboards in this folder will be loaded at container startup.

Dashboards loaded in this fashion cannot be deleted from the Grafana UI.


### Application Metric example

Since Automatic Instrumentation captures telemetry from already established libraries and frameworks, it cannot be used directly to introduce custom metrics on the application. However, the Micrometer library offers the interface to create and update user-defined metrics, and these will be automatically collected and propagated. 

We created a small example on how to create custom application metrics. First, is necessary to import the appropriate Micrometer MeterRegistry, which creates metrics like counters, gauges, timers, etc. The registry is added to the class via dependency injection.

```java
import io.micrometer.core.instrument.MeterRegistry;

/* Counts the kind of request that is handled by incrementing the metric provisioner.operation
 * Tags are being used to differentiate between operation types */
@Controller
public class ProvisionerController {

  private final MeterRegistry registry;

  public ProvisionerController(MeterRegistry registry) {
    this.registry = registry;
  }
}
```

After that, we can create diverse metrics using counters created with the MeterRegistry instance. We can define a metric name and then a set of key-value pairs that represent tags. The counters will be discriminated based on these tags.

```java
...

@Controller
public class ProvisionerController {
    
  ...

  @RequestMapping(method = RequestMethod.POST, value = "/v1/provision")
  public ResponseEntity<ProvisioningStatus> provision(
          @RequestBody ProvisioningRequest provisionRequest) {
    log.info("Received provision request on /v1/provision");
    registry.counter("provisioner.operation", "kind", "provision").increment();
    ProvisioningStatus result = ...;
    return ResponseEntity.ok(result);
  }

  @RequestMapping(method = RequestMethod.POST, value = "/v1/unprovision")
  public ResponseEntity<ProvisioningStatus> unprovision(
          @RequestBody ProvisioningRequest provisionRequest) {
    log.info("Received unprovision request on /v1/unprovision");
    registry.counter("provisioner.operation", "kind", "unprovision").increment();
    ProvisioningStatus result = ...;
    return ResponseEntity.ok(result);
  }
}
```

For more details about metrics and other meters, check the [OpenTelemetry documentation](https://opentelemetry.io/docs/concepts/signals/metrics/) and Micrometer implementation [documentation](https://micrometer.io/docs/concepts).
