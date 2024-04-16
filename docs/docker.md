# Running with Docker

You can use the below instructions if you want to run your app as a docker image.

The docker image uses a bash script (`run_app.sh` located in the root folder) to execute the provisioner jar. If you're implementing your own provisioner, be sure to modify the name of the jar that is being executed, otherwise the docker image will fail to find your application jar, and it won't start.

> **Warning:** Don't use the wildcard statement `*.jar` as the order of the jar expansion is not deterministic.

### Build Docker image

```bash
docker build -t databricks-specific-provisioner .
```

### Container execution

At this point, we can run the docker image as a container via the `run` command and the name associated with the image during the build. This automatically enables OpenTelemetry automatic instrumentation:


```bash
docker run -d --name java-sp-container -p 8888:8888 databricks-specific-provisioner
```

However, for OpenTelemetry to work correctly, some environment variables need to be set in order to send collected data to an **Observability backend** (e.g. Prometheus).

- If you already have a Collector up and running, replace `<CollectorURL>` with its URL (e.g. `http://172.20.0.1:5555`).

```bash
docker run -d --name java-sp-container \
-e OTEL_EXPORTER_OTLP_ENDPOINT=<CollectorURL> \
-e OTEL_METRICS_EXPORTER=otlp \
-e OTEL_SERVICE_NAME=databricks-specific-provisioner \
-e OTEL_TRACES_EXPORTER=otlp \
-p 8888:8888 databricks-specific-provisioner
```

- If you are running the collector in your local host (for example, by running the provided [docker-compose file](opentelemetry.md)) then you can replace `<CollectorURL>` with the hostname address, and the final command will look like the following:

```bash
docker run -d --name java-sp-container \
-e OTEL_EXPORTER_OTLP_ENDPOINT=http://$(hostname -f):5555 \
-e OTEL_METRICS_EXPORTER=otlp \
-e OTEL_SERVICE_NAME=databricks-specific-provisioner \
-e OTEL_TRACES_EXPORTER=otlp \
-p 8888:8888 databricks-specific-provisioner
```
