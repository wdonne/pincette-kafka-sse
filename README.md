# Kafka Server-Sent Events

With this [SSE](https://html.spec.whatwg.org/multipage/server-sent-events.html#server-sent-events)-endpoint, events can be served from a Kafka topic. Event-driven applications that only see Kafka can reach the end-user by forwarding events to this topic.

When a user connects to the endpoint, a specific Kafka consumer group is created that is reused for subsequent connections. This turns SSE into a persistent flow, even when there are connection problems. The level of persistence is determined by the retention period of the Kafka topic. It makes sense to express this in minutes or hours instead of days.

The current Java Kafka client creates a limitation in the sense that each consumer group requires a separate thread. Therefore, one instance of the endpoint can serve only around 500 connections in a practical way. This limitation will be lifted as soon as we have a non-blocking Kafka client that doesn't need extra threads.

## Configuration

The configuration is managed by the [Lightbend Config package](https://github.com/lightbend/config). By default it will try to load `conf/application.conf`. An alternative configuration may be loaded by adding `-Dconfig.resource=myconfig.conf`, where the file is also supposed to be in the `conf` directory, or `-Dconfig.file=/conf/myconfig.conf`. If no configuration file is available it will load a default one from the resources. The following entries are available:

|Entry|Mandatory|Description|
|---|---|---|
|eventName|No|The name that is given to the SSE-events. The default value is `message`.|
|eventNameField|No|The name of the field in the events that is used to extract the name for the SSE-event. If it is not set, then it falls back to the `eventName` field.|
|fallbackCookie|No|The cookie that is consumed when no bearer token could be found on the `Authorization` header. If you rely on this, then make sure the cookie is an `HttpOnly` cookie. The default value is `access_token`.|
|jwtPublicKey|No|A public key in PEM format. If it is present, the signature of the bearer tokens will be verified. If you don't use it, you should deploy the server behind a gateway that does the verification.|
|kafka|Yes|All Kafka settings come below this entry. So for example, the setting `bootstrap.servers` would go to the entry `kafka.bootstrap.servers`.|
|namespace|No|A name to distinguish several deployments in the same environment. The default value is `sse`.|
|otlp.grpc|No|The OpenTelemetry endpoint for logs and metrics. It should be a URL like `http://localhost:4317`.|
|subscriptionsField|No|The name of the field in the events that is used to extract the subscriptions. It should be an array of strings. If the username of the current user is in it, the event will also be sent there. The default value is `_subscriptions`.|
|topic|Yes|The Kafka topic that is consumed.|
|tracesTopic|No|The Kafka topic to which the event traces are sent.|
|usernameField|No|The name of the field in the events that is used to extract the user name. The default value is `_jwt.sub`.|

## Telemetry

A few OpenTelemetry observable counters are emitted every minute. The following table shows the counters.

|Counter|Description|
|---|---|
|http.server.average_duration_millis|The average request duration in the measured interval.|
|http.server.average_request_bytes|The average request body size in bytes in the measured interval.|
|http.server.average_response_bytes|The average response body size in bytes in the measured interval.|
|http.server.sse_events|The number of SSE events that are sent to clients during the measured interval.|
|http.server.requests|The number of requests during the measured interval.|

The following attributes are added to the counters, except for the counter `http.server.sse_events`, which has only the attribute `instance`.

|Attribute|Description|
|---|---|
|aggregate|The name of the aggregate the request was about.|
|http.request.method|The request method.|
|http.response.status_code|The status code of the response.|
|instance|The UUID of the Kafka SSE instance.|

The logs are also sent to the OpenTelemetry endpoint.

The event traces are JSON messages, as described in [JSON Streams Telemetry](https://jsonstreams.io/docs/logging.html). They are sent to the Kafka topic set in the `tracesTopic` configuration field.

## Building and Running

You can build the tool with `mvn clean package`. This will produce a self-contained JAR-file in the `target` directory with the form `pincette-kafka-sse-<version>-jar-with-dependencies.jar`. You can launch this JAR with `java -jar`.

## Docker

Docker images can be found at [https://hub.docker.com/repository/docker/wdonne/pincette-kafka-sse](https://hub.docker.com/repository/docker/wdonne/pincette-kafka-sse).

## Kubernetes

You can mount the configuration in a `ConfigMap` and `Secret` combination. The `ConfigMap` should be mounted at `/conf/application.conf`. You then include the secret in the configuration from where you have mounted it. See also [https://github.com/lightbend/config/blob/main/HOCON.md#include-syntax](https://github.com/lightbend/config/blob/main/HOCON.md#include-syntax).