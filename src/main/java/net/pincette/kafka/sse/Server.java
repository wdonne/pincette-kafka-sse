package net.pincette.kafka.sse;

import static io.netty.buffer.Unpooled.wrappedBuffer;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpResponseStatus.UNAUTHORIZED;
import static io.opentelemetry.api.common.AttributeKey.stringKey;
import static java.lang.System.exit;
import static java.lang.System.getenv;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.Duration.ofSeconds;
import static java.time.Instant.now;
import static java.util.Optional.ofNullable;
import static java.util.UUID.randomUUID;
import static java.util.logging.Level.SEVERE;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.Stream.concat;
import static net.pincette.config.Util.configValue;
import static net.pincette.jes.JsonFields.CORR;
import static net.pincette.jes.JsonFields.JWT;
import static net.pincette.jes.JsonFields.SUB;
import static net.pincette.jes.JsonFields.SUBSCRIPTIONS;
import static net.pincette.jes.tel.OtelUtil.metrics;
import static net.pincette.jes.util.Kafka.createReliableProducer;
import static net.pincette.jes.util.Kafka.fromConfig;
import static net.pincette.json.JsonUtil.getArray;
import static net.pincette.json.JsonUtil.getString;
import static net.pincette.json.JsonUtil.string;
import static net.pincette.json.JsonUtil.toJsonPointer;
import static net.pincette.kafka.sse.Common.LOGGER;
import static net.pincette.kafka.sse.Common.SSE;
import static net.pincette.kafka.sse.Common.VERSION;
import static net.pincette.kafka.sse.Common.namespace;
import static net.pincette.netty.http.JWTVerifier.verify;
import static net.pincette.netty.http.PipelineHandler.handle;
import static net.pincette.netty.http.Util.getBearerToken;
import static net.pincette.netty.http.Util.simpleResponse;
import static net.pincette.netty.http.Util.wrapMetrics;
import static net.pincette.netty.http.Util.wrapTracing;
import static net.pincette.rs.Box.box;
import static net.pincette.rs.Chain.with;
import static net.pincette.rs.Filter.filter;
import static net.pincette.rs.LambdaSubscriber.lambdaSubscriber;
import static net.pincette.rs.PassThrough.passThrough;
import static net.pincette.rs.Probe.probeValue;
import static net.pincette.rs.Util.empty;
import static net.pincette.rs.Util.generate;
import static net.pincette.rs.Util.onCancelProcessor;
import static net.pincette.rs.Util.onErrorProcessor;
import static net.pincette.rs.kafka.KafkaPublisher.publisher;
import static net.pincette.rs.kafka.KafkaSubscriber.subscriber;
import static net.pincette.rs.kafka.Util.fromPublisher;
import static net.pincette.rs.kafka.Util.fromSubscriber;
import static net.pincette.rs.streams.Message.message;
import static net.pincette.rs.streams.Streams.streams;
import static net.pincette.util.Collections.map;
import static net.pincette.util.Pair.pair;
import static net.pincette.util.Util.tryToDoSilent;

import com.typesafe.config.Config;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpRequest;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.ObservableLongCounter;
import io.opentelemetry.api.metrics.ObservableLongUpDownCounter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Stream;
import javax.json.JsonObject;
import net.pincette.jes.tel.EventTrace;
import net.pincette.jes.tel.HttpMetrics;
import net.pincette.jes.util.Kafka;
import net.pincette.json.JsonUtil;
import net.pincette.kafka.json.JsonDeserializer;
import net.pincette.kafka.json.JsonSerializer;
import net.pincette.netty.http.HeaderHandler;
import net.pincette.netty.http.HttpServer;
import net.pincette.netty.http.Metrics;
import net.pincette.netty.http.RequestHandler;
import net.pincette.rs.DequePublisher;
import net.pincette.rs.Mapper;
import net.pincette.rs.Merge;
import net.pincette.rs.kafka.ConsumerEvent;
import net.pincette.rs.streams.Message;
import net.pincette.util.ImmutableBuilder;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class Server implements AutoCloseable {
  private static final String ACCESS_TOKEN = "access_token";
  private static final String DEFAULT_EVENT_NAME = "message";
  private static final String DEFAULT_SUBSCRIPTIONS_FIELD = SUBSCRIPTIONS;
  private static final String DEFAULT_USERNAME_FIELD = JWT + "." + SUB;
  private static final String EVENT_NAME = "eventName";
  private static final String EVENT_NAME_FIELD = "eventNameField";
  private static final String FALLBACK_COOKIE = "fallbackCookie";
  private static final String HTTP_SERVER_ACTIVE_REQUESTS = "http.server.active_requests";
  private static final String HTTP_SERVER_SSE_EVENTS = "http.server.sse_events";
  private static final String INSTANCE_ATTRIBUTE = "instance";
  private static final String INSTANCE_ENV = "INSTANCE";
  private static final String INSTANCE =
      ofNullable(getenv(INSTANCE_ENV)).orElse(randomUUID().toString());
  private static final String JWT_PUBLIC_KEY = "jwtPublicKey";
  private static final String KAFKA = "kafka";
  private static final String MIME_TYPE = "text/event-stream";
  private static final String SUBSCRIPTIONS_FIELD = "subscriptionsField";
  private static final String TOPIC_NAME = "topic";
  private static final String TRACES_TOPIC = "tracesTopic";
  private static final String USERNAME_FIELD = "usernameField";

  private long activeRequests;
  private ObservableLongUpDownCounter activeRequestsCounter;
  private final Config config;
  private final KafkaConsumer<String, JsonObject> consumer;
  private final Map<String, List<DequePublisher<JsonObject>>> connections = new HashMap<>();
  private ObservableLongCounter eventCounter;
  private final BiConsumer<ConsumerEvent, KafkaConsumer<String, JsonObject>> eventHandler;
  private final EventTrace eventTrace;
  private long events;
  private final Function<JsonObject, String> getEventName;
  private final Function<JsonObject, Stream<String>> getSubscriptions;
  private final Function<JsonObject, String> getUsername;
  private HttpServer httpServer;
  private final Attributes attributes = Attributes.of(stringKey(INSTANCE_ATTRIBUTE), INSTANCE);
  private final Map<String, String> attributesMap = map(pair(INSTANCE_ATTRIBUTE, INSTANCE));
  private final Map<String, Object> kafkaConfig;
  private Meter meter;
  private final int port;
  private final String topic;
  private final String tracesTopic;

  private Server(
      final int port,
      final Config config,
      final BiConsumer<ConsumerEvent, KafkaConsumer<String, JsonObject>> eventHandler) {
    this.port = port;
    this.config = config;
    this.eventHandler = eventHandler;
    kafkaConfig = config != null ? fromConfig(config, KAFKA) : null;
    consumer = consumer(kafkaConfig);
    topic = config != null ? config.getString(TOPIC_NAME) : null;
    tracesTopic = tracesTopic(config);
    getUsername = config != null ? getUsername(config) : null;
    getSubscriptions = config != null ? getSubscriptions(config) : null;
    getEventName = config != null ? getEventName(config) : null;
    eventTrace = eventTrace(config);
  }

  public Server() {
    this(-1, null, null);
  }

  private static KafkaConsumer<String, JsonObject> consumer(final Map<String, Object> config) {
    return ofNullable(config)
        .map(c -> Kafka.consumer(INSTANCE, c, new StringDeserializer(), new JsonDeserializer()))
        .orElse(null);
  }

  private static EventTrace eventTrace(final Config config) {
    return ofNullable(config)
        .filter(c -> tracesTopic(c) != null)
        .map(
            c ->
                new EventTrace()
                    .withServiceNamespace(namespace(c))
                    .withServiceName(SSE)
                    .withServiceVersion(VERSION)
                    .withName(SSE))
        .orElse(null);
  }

  private static String fallbackCookie(final Config config) {
    return configValue(config::getString, FALLBACK_COOKIE).orElse(ACCESS_TOKEN);
  }

  private static Function<JsonObject, String> getEventName(final Config config) {
    final var eventName = configValue(config::getString, EVENT_NAME).orElse(DEFAULT_EVENT_NAME);
    final Function<JsonObject, String> fromName = json -> eventName;

    return configValue(config::getString, EVENT_NAME_FIELD)
        .map(JsonUtil::toJsonPointer)
        .map(p -> getEventNameFromMessage(p, eventName))
        .orElse(fromName);
  }

  private static Function<JsonObject, String> getEventNameFromMessage(
      final String pointer, final String defaultName) {
    return json -> getString(json, pointer).orElse(defaultName);
  }

  private static Function<JsonObject, Stream<String>> getSubscriptions(final Config config) {
    final var field = getSubscriptionsField(config);

    return json -> getArray(json, field).map(JsonUtil::strings).orElseGet(Stream::empty);
  }

  private static String getSubscriptionsField(final Config config) {
    return toJsonPointer(
        configValue(config::getString, SUBSCRIPTIONS_FIELD).orElse(DEFAULT_SUBSCRIPTIONS_FIELD));
  }

  private static Optional<String> getUsername(
      final HttpRequest request, final String fallbackCookie) {
    return getBearerToken(request, fallbackCookie)
        .flatMap(net.pincette.jwt.Util::getJwtPayload)
        .flatMap(p -> getString(p, "/" + SUB));
  }

  private static Function<JsonObject, String> getUsername(final Config config) {
    final var field = getUsernameField(config);

    return json -> getString(json, field).orElse(null);
  }

  private static String getUsernameField(final Config config) {
    return toJsonPointer(
        configValue(config::getString, USERNAME_FIELD).orElse(DEFAULT_USERNAME_FIELD));
  }

  private static HeaderHandler headerHandler(final Config config) {
    final var fallbackCookie = fallbackCookie(config);

    return configValue(config::getString, JWT_PUBLIC_KEY)
        .map(key -> verify(key, fallbackCookie))
        .orElse(h -> h);
  }

  private static Subscriber<Metrics> metricsSubscriber(final Meter meter) {
    return HttpMetrics.subscriber(meter, path -> null, INSTANCE);
  }

  private static void panic(final Throwable throwable) {
    LOGGER.log(SEVERE, "panic", throwable);
    exit(1);
  }

  private static KafkaProducer<String, JsonObject> producer(final Map<String, Object> config) {
    return createReliableProducer(config, new StringSerializer(), new JsonSerializer());
  }

  private static String tracesTopic(final Config config) {
    return ofNullable(config).flatMap(c -> configValue(c::getString, TRACES_TOPIC)).orElse(null);
  }

  private ObservableLongUpDownCounter activeRequestsCounter() {
    return ofNullable(meter)
        .map(
            m ->
                m.upDownCounterBuilder(HTTP_SERVER_ACTIVE_REQUESTS)
                    .buildWithCallback(
                        measurement -> measurement.record(activeRequests, attributes)))
        .orElse(null);
  }

  public void close() {
    if (activeRequestsCounter != null) {
      tryToDoSilent(activeRequestsCounter::close);
    }

    if (eventCounter != null) {
      tryToDoSilent(eventCounter::close);
    }

    httpServer.close();
  }

  private void closeConnection(final String username, final DequePublisher<JsonObject> queue) {
    --activeRequests;
    connections.get(username).remove(queue);

    if (connections.get(username).isEmpty()) {
      connections.remove(username);
    }

    LOGGER.info(() -> "User " + username + " closed connection");
  }

  private Subscriber<? super Message<String, JsonObject>> consumeEvents() {
    return lambdaSubscriber(
        m ->
            getConnections(m.value)
                .filter(p -> !p.isClosed())
                .forEach(p -> p.getDeque().offerFirst(m.value)),
        () -> {},
        Server::panic);
  }

  private void createServer() {
    meter = metrics(namespace(config), SSE, VERSION, config).map(m -> m.getMeter(SSE)).orElse(null);
    httpServer =
        new HttpServer(
            port,
            wrapTracing(
                handle(headerHandler(config))
                    .finishWith(
                        ofNullable(meter)
                            .map(m -> wrapMetrics(handler(), metricsSubscriber(m)))
                            .orElseGet(this::handler)),
                LOGGER));

    if (meter != null) {
      activeRequestsCounter = activeRequestsCounter();
      eventCounter = eventCounter();
    }
  }

  private ObservableLongCounter eventCounter() {
    return ofNullable(meter)
        .map(
            m ->
                m.counterBuilder(HTTP_SERVER_SSE_EVENTS)
                    .buildWithCallback(measurement -> measurement.record(events, attributes)))
        .orElse(null);
  }

  private Stream<DequePublisher<JsonObject>> getConnections(final JsonObject event) {
    return concat(Stream.of(getUsername.apply(event)), getSubscriptions.apply(event))
        .collect(toSet())
        .stream()
        .map(connections::get)
        .filter(Objects::nonNull)
        .flatMap(List::stream);
  }

  private RequestHandler handler() {
    final var fallbackCookie = fallbackCookie(config);

    return (request, requestBody, response) ->
        getUsername(request, fallbackCookie)
            .map(u -> simpleResponse(response, OK, MIME_TYPE, newConnection(u)))
            .orElseGet(() -> simpleResponse(response, UNAUTHORIZED, empty()));
  }

  private Publisher<ByteBuf> newConnection(final String username) {
    final DequePublisher<JsonObject> publisher = new DequePublisher<>();

    LOGGER.info(() -> "SSE connection for user " + username);
    ++activeRequests;
    connections.computeIfAbsent(username, u -> new ArrayList<>()).add(publisher);

    return sseStream(username, publisher);
  }

  public CompletionStage<Boolean> run() {
    createServer();

    return httpServer.run();
  }

  private Publisher<ByteBuf> sseStream(
      final String username, final DequePublisher<JsonObject> events) {
    return with(Merge.of(
            with(events)
                .map(
                    json ->
                        trace(
                            "event: "
                                + getEventName.apply(json)
                                + "\ndata: "
                                + string(json)
                                + "\n\n",
                            username))
                .map(probeValue(m -> ++this.events))
                .get(),
            with(generate(() -> ":\n")).throttle(1).get()))
        .buffer(100, ofSeconds(1))
        .map(s -> wrappedBuffer(s.getBytes(UTF_8)))
        .map(onErrorProcessor(t -> closeConnection(username, events)))
        .map(onCancelProcessor(() -> closeConnection(username, events)))
        .get();
  }

  public void start() {
    final var streams =
        ImmutableBuilder.create(
                () ->
                    streams(
                            fromPublisher(publisher(() -> consumer).withEventHandler(eventHandler)),
                            fromSubscriber(subscriber(() -> producer(kafkaConfig))))
                        .from(topic, passThrough())
                        .subscribe(consumeEvents()))
            .updateIf(
                s -> eventTrace != null,
                s ->
                    s.from(topic, box(Mapper.map(this::traceMessage), filter(Objects::nonNull)))
                        .to(tracesTopic))
            .build();
    final var thread = new Thread(streams::start, "Streams");

    thread.start();
    run().toCompletableFuture().join();
    streams.stop();
  }

  private <T> T trace(final T v, final String username) {
    LOGGER.finest(
        () -> INSTANCE + (username != null ? (": " + username) : "") + ": " + v.toString());

    return v;
  }

  private Message<String, JsonObject> traceMessage(final Message<String, JsonObject> message) {
    return getString(message.value, "/" + CORR)
        .map(corr -> pair(corr, getUsername.apply(message.value)))
        .filter(pair -> pair.second != null && connections.containsKey(pair.second))
        .map(
            pair ->
                message(
                    pair.first,
                    eventTrace
                        .withTraceId(pair.first)
                        .withTimestamp(now())
                        .withAttributes(attributesMap)
                        .withUsername(pair.second)
                        .toJson()
                        .build()))
        .orElse(null);
  }

  public Server withConfig(final Config config) {
    return new Server(port, config, eventHandler);
  }

  public Server withEventHandler(
      final BiConsumer<ConsumerEvent, KafkaConsumer<String, JsonObject>> eventHandler) {
    return new Server(port, config, eventHandler);
  }

  public Server withPort(final int port) {
    return new Server(port, config, eventHandler);
  }
}
