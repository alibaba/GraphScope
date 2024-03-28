# How to instrument

GraphScope leverage [OpenTelemetry](https://opentelemetry.io/) for instrument.

## Java

### Automatic instrument
1.  Download the latest [`opentelemetry-javaagent.jar`](https://github.com/open-telemetry/opentelemetry-java-instrumentation/releases/latest/download/opentelemetry-javaagent.jar)﻿ agent file
2. Configure the following environment variables to set the service and protocol details.
```bash
export JAVA_TOOL_OPTIONS="-javaagent:/PATH/TO/opentelemetry-javaagent.jar"
export OTEL_SERVICE_NAME="frontend-service"
```
3. Start the application as usual
```bash
java -jar application.jar
```