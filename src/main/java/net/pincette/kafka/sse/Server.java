package net.pincette.kafka.sse;

import static io.netty.buffer.Unpooled.wrappedBuffer;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpResponseStatus.UNAUTHORIZED;
import static io.opentelemetry.api.common.AttributeKey.stringKey;
import static java.lang.Boolean.FALSE;
import static java.lang.System.getenv;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.Duration.ofSeconds;
import static java.time.Instant.now;
import static java.util.Comparator.comparingLong;
import static java.util.Optional.ofNullable;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.logging.Level.SEVERE;
import static java.util.regex.Pattern.compile;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.maxBy;
import static java.util.stream.Collectors.toSet;
import static net.pincette.config.Util.configValue;
import static net.pincette.jes.JsonFields.CORR;
import static net.pincette.jes.JsonFields.JWT;
import static net.pincette.jes.JsonFields.SUB;
import static net.pincette.jes.JsonFields.SUBSCRIPTIONS;
import static net.pincette.jes.tel.OtelUtil.metrics;
import static net.pincette.jes.util.Kafka.adminConfig;
import static net.pincette.jes.util.Kafka.consumerGroupOffsets;
import static net.pincette.jes.util.Kafka.createReliableProducer;
import static net.pincette.jes.util.Kafka.describeConsumerGroups;
import static net.pincette.jes.util.Kafka.fromConfig;
import static net.pincette.jes.util.Kafka.send;
import static net.pincette.jes.util.Kafka.topicPartitionOffsets;
import static net.pincette.jes.util.Kafka.topicPartitions;
import static net.pincette.json.JsonUtil.createObjectBuilder;
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
import static net.pincette.rs.Chain.with;
import static net.pincette.rs.Probe.probeValue;
import static net.pincette.rs.Util.empty;
import static net.pincette.rs.Util.generate;
import static net.pincette.rs.kafka.ConsumerEvent.STARTED;
import static net.pincette.util.Collections.map;
import static net.pincette.util.Collections.merge;
import static net.pincette.util.Collections.set;
import static net.pincette.util.Pair.pair;
import static net.pincette.util.ScheduledCompletionStage.composeAsyncAfter;
import static net.pincette.util.StreamUtil.rangeExclusive;
import static net.pincette.util.StreamUtil.tail;
import static net.pincette.util.Util.tryToDoSilent;
import static net.pincette.util.Util.tryToDoWithRethrow;
import static net.pincette.util.Util.tryToGetSilent;
import static org.apache.kafka.clients.CommonClientConfigs.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.CommonClientConfigs.GROUP_INSTANCE_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.common.GroupState.DEAD;
import static org.apache.kafka.common.GroupState.EMPTY;
import static org.apache.kafka.common.GroupState.UNKNOWN;

import com.typesafe.config.Config;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.HttpRequest;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.ObservableLongCounter;
import io.opentelemetry.api.metrics.ObservableLongUpDownCounter;
import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow.Processor;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import javax.json.JsonObject;
import net.pincette.jes.tel.EventTrace;
import net.pincette.jes.tel.HttpMetrics;
import net.pincette.json.JsonUtil;
import net.pincette.kafka.json.JsonDeserializer;
import net.pincette.kafka.json.JsonSerializer;
import net.pincette.netty.http.HeaderHandler;
import net.pincette.netty.http.HttpServer;
import net.pincette.netty.http.Metrics;
import net.pincette.netty.http.RequestHandler;
import net.pincette.rs.Merge;
import net.pincette.rs.Source;
import net.pincette.rs.kafka.ConsumerEvent;
import net.pincette.rs.kafka.KafkaPublisher;
import net.pincette.util.State;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.admin.MemberToRemove;
import org.apache.kafka.clients.admin.RemoveMembersFromConsumerGroupOptions;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class Server implements AutoCloseable {
  private static final String ACCESS_TOKEN = "access_token";
  private static final Duration CLEAN_UP_INTERVAL = ofSeconds(60);
  private static final String CONSUMER_GPOUP_ID = "consumerGroupId";
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
  private static final Pattern INDEX_SUFFIX = compile("-\\d+");
  private static final String JWT_PUBLIC_KEY = "jwtPublicKey";
  private static final String KAFKA = "kafka";
  private static final int MAX_EXISTING_CONSUMER_GROUPS = 10;
  private static final String MIME_TYPE = "text/event-stream";
  private static final String SUBSCRIPTIONS_FIELD = "subscriptionsField";
  private static final String TOPIC = "topic";
  private static final String TRACES_TOPIC = "tracesTopic";
  private static final String USERNAME_FIELD = "usernameField";

  private long activeRequests;
  private ObservableLongUpDownCounter activeRequestsCounter;
  private Admin admin;
  private final Map<String, Object> adminConfig;
  private final Config config;
  private ObservableLongCounter eventCounter;
  private final BiConsumer<ConsumerEvent, KafkaConsumer<String, JsonObject>> eventHandler;
  private final EventTrace eventTrace;
  private long events;
  private HttpServer httpServer;
  private final String instance = ofNullable(getenv(INSTANCE_ENV)).orElse(randomUUID().toString());
  private final Attributes attributes = Attributes.of(stringKey(INSTANCE_ATTRIBUTE), instance);
  private final Map<String, String> attributesMap = map(pair(INSTANCE_ATTRIBUTE, instance));
  private final Map<String, Object> kafkaConfig;
  private Meter meter;
  private final int port;
  private Producer<String, JsonObject> producer;

  private Server(
      final int port,
      final Config config,
      final BiConsumer<ConsumerEvent, KafkaConsumer<String, JsonObject>> eventHandler) {
    this.port = port;
    this.config = config;
    this.eventHandler = eventHandler;
    kafkaConfig = config != null ? fromConfig(config, KAFKA) : null;
    adminConfig = kafkaConfig != null ? adminConfig(kafkaConfig) : null;
    eventTrace =
        config != null
            ? new EventTrace()
                .withServiceNamespace(namespace(config))
                .withServiceName(SSE)
                .withServiceVersion(VERSION)
                .withName(SSE)
            : null;
  }

  public Server() {
    this(-1, null, null);
  }

  private static boolean canRetry(final Throwable t) {
    return t.getCause() instanceof KafkaException kafkaException
        && kafkaException.getCause() instanceof TimeoutException;
  }

  private static CompletionStage<Void> cleanUpConsumerGroups(
      final String topic, final Supplier<Admin> admin, final Consumer<Throwable> adminError) {
    return admin
        .get()
        .listConsumerGroups()
        .all()
        .toCompletionStage()
        .thenApply(c -> emptySseConsumerGroups(c, topic))
        .thenApply(Server::logDeleteConsumerGroups)
        .thenComposeAsync(
            groupIds -> admin.get().deleteConsumerGroups(groupIds).all().toCompletionStage())
        .exceptionally(
            t -> {
              LOGGER.log(SEVERE, t.getMessage(), t);

              if (canRetry(t)) {
                adminError.accept(t);
              }

              return null; // By this time some groups may be active again.
            });
  }

  private static Function<String, KafkaConsumer<String, JsonObject>> consumer(
      final String groupInstanceId, final Map<String, Object> config) {
    return group ->
        new KafkaConsumer<>(
            merge(
                config,
                map(
                    pair(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class),
                    pair(VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class),
                    pair(GROUP_ID_CONFIG, group),
                    pair(GROUP_INSTANCE_ID_CONFIG, groupInstanceId),
                    pair(ENABLE_AUTO_COMMIT_CONFIG, false))));
  }

  private static JsonObject createRaceCheckMessage(final String consumerGroupId) {
    return createObjectBuilder().add(CONSUMER_GPOUP_ID, consumerGroupId).build();
  }

  private static Collection<String> emptySseConsumerGroups(
      final Collection<ConsumerGroupListing> consumerGroups, final String topic) {
    return consumerGroups.stream()
        .filter(l -> l.groupState().map(s -> s == EMPTY).orElse(false))
        .map(ConsumerGroupListing::groupId)
        .filter(id -> isSseConsumerGroup(id, topic))
        .toList();
  }

  private static Set<String> existingConsumerGroupsIds(final String username, final String topic) {
    return rangeExclusive(0, MAX_EXISTING_CONSUMER_GROUPS)
        .map(i -> groupId(username, topic, i))
        .collect(toSet());
  }

  private static Collection<String> extraMembers(final Collection<MemberDescription> members) {
    return tail(members.stream()
            .map(m -> m.groupInstanceId().orElse(null))
            .filter(Objects::nonNull)
            .sorted())
        .toList();
  }

  private static String fallbackCookie(final Config config) {
    return tryToGetSilent(() -> config.getString(FALLBACK_COOKIE)).orElse(ACCESS_TOKEN);
  }

  private static Function<JsonObject, String> getEventName(final Config config) {
    final String eventName =
        tryToGetSilent(() -> config.getString(EVENT_NAME)).orElse(DEFAULT_EVENT_NAME);
    final Function<JsonObject, String> fromName = json -> eventName;

    return tryToGetSilent(() -> config.getString(EVENT_NAME_FIELD))
        .map(JsonUtil::toJsonPointer)
        .map(p -> getEventNameFromMessage(p, eventName))
        .orElse(fromName);
  }

  private static Function<JsonObject, String> getEventNameFromMessage(
      final String pointer, final String defaultName) {
    return json -> getString(json, pointer).orElse(defaultName);
  }

  private static Function<JsonObject, Stream<String>> getSubscriptions(final Config config) {
    final String field = getSubscriptionsField(config);

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
        .flatMap(p -> getString(p, "/" + SUB))
        .map(
            u -> {
              LOGGER.info(() -> "SSE connection for user " + u);
              return u;
            });
  }

  private static Function<JsonObject, String> getUsername(final Config config) {
    final String field = getUsernameField(config);

    return json -> getString(json, field).orElse(null);
  }

  private static String getUsernameField(final Config config) {
    return toJsonPointer(
        configValue(config::getString, USERNAME_FIELD).orElse(DEFAULT_USERNAME_FIELD));
  }

  private static CompletionStage<Void> goToLatest(
      final String consumerGroupId,
      final Map<TopicPartition, OffsetAndMetadata> latestOffsets,
      final Admin admin) {
    return admin
        .alterConsumerGroupOffsets(consumerGroupId, latestOffsets)
        .all()
        .toCompletionStage();
  }

  private static String groupId(final String username, final String topic, final int index) {
    return username + "-" + topic + "-" + index;
  }

  private static HeaderHandler headerHandler(final Config config) {
    final String fallbackCookie = fallbackCookie(config);

    return tryToGetSilent(() -> config.getString(JWT_PUBLIC_KEY))
        .map(key -> verify(key, fallbackCookie))
        .orElse(h -> h);
  }

  private static boolean isFree(final GroupState state) {
    return state == DEAD || state == EMPTY || state == UNKNOWN;
  }

  private static boolean isSseConsumerGroup(final String groupId, final String topic) {
    final int index = groupId.indexOf(topic);

    return index != -1
        && groupId.substring(0, index).endsWith("-")
        && INDEX_SUFFIX.matcher(groupId.substring(index + topic.length())).matches();
  }

  private static Map<TopicPartition, OffsetAndMetadata> latestOffsets(
      final Map<String, Map<TopicPartition, Long>> groupOffsets,
      final Map<TopicPartition, Long> topicOffsets) {
    return map(
        groupOffsets.values().stream()
            .map(Map::entrySet)
            .flatMap(Set::stream)
            .collect(groupingBy(Entry::getKey, maxBy(comparingLong(Entry::getValue))))
            .entrySet()
            .stream()
            .map(
                e ->
                    pair(
                        e.getKey(),
                        e.getValue()
                            .map(Entry::getValue)
                            .map(OffsetAndMetadata::new)
                            .orElseGet(
                                () -> new OffsetAndMetadata(topicOffsets.get(e.getKey()))))));
  }

  private static Collection<String> logDeleteConsumerGroups(final Collection<String> groupIds) {
    groupIds.forEach(id -> LOGGER.info(() -> "Deleting consumer group " + id));

    return groupIds;
  }

  private static Collection<MemberToRemove> membersToRemove(
      final Collection<String> groupInstanceIds) {
    return groupInstanceIds.stream().map(MemberToRemove::new).toList();
  }

  private static Publisher<ByteBuf> messagePublisher(final String message) {
    return Source.of(wrappedBuffer(message.getBytes(UTF_8)));
  }

  private static Subscriber<Metrics> metricsSubscriber(final Meter meter, final String instance) {
    return HttpMetrics.subscriber(meter, path -> null, instance);
  }

  private static KafkaProducer<String, JsonObject> producer(final Config config) {
    return createReliableProducer(
        fromConfig(config, KAFKA), new StringSerializer(), new JsonSerializer());
  }

  private static CompletionStage<Void> runCleanUpSseConsumerGroups(
      final String topic,
      final Supplier<Admin> admin,
      final Consumer<Throwable> adminError,
      final State<Boolean> stop) {
    return composeAsyncAfter(
            () ->
                FALSE.equals(stop.get())
                    ? cleanUpConsumerGroups(topic, admin, adminError)
                    : completedFuture(null),
            CLEAN_UP_INTERVAL)
        .thenComposeAsync(
            v ->
                FALSE.equals(stop.get())
                    ? runCleanUpSseConsumerGroups(topic, admin, adminError, stop)
                    : completedFuture(null));
  }

  private static Set<String> selectActiveConsumerGroups(
      final Map<String, ConsumerGroupDescription> groups) {
    return groups.entrySet().stream()
        .filter(e -> e.getValue().groupState() != DEAD)
        .map(Entry::getKey)
        .collect(toSet());
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

  private BiConsumer<ConsumerEvent, KafkaConsumer<String, JsonObject>> addRaceCheck(
      final BiConsumer<ConsumerEvent, KafkaConsumer<String, JsonObject>> eventHandler,
      final String consumerGroupId) {
    final BiConsumer<ConsumerEvent, KafkaConsumer<String, JsonObject>> check =
        (event, consumer) -> {
          if (event == STARTED) {
            tryToDoWithRethrow(
                () -> producer(config),
                p ->
                    send(
                            p,
                            new ProducerRecord<>(
                                config.getString(TOPIC),
                                "0", // The consuming group member will always be the same.
                                trace(createRaceCheckMessage(consumerGroupId), consumerGroupId)))
                        .toCompletableFuture()
                        .get());
          }
        };

    return eventHandler != null ? eventHandler.andThen(check) : check;
  }

  public void close() {
    if (producer != null) {
      producer.close();
    }

    if (activeRequestsCounter != null) {
      tryToDoSilent(activeRequestsCounter::close);
    }

    if (eventCounter != null) {
      tryToDoSilent(eventCounter::close);
    }

    httpServer.close();
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
                            .map(m -> wrapMetrics(handler(), metricsSubscriber(m, instance)))
                            .orElseGet(this::handler)),
                LOGGER));

    if (eventTrace != null) {
      producer = producer(config);
    }

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

  private Processor<JsonObject, JsonObject> eventTracer() {
    return ofNullable(eventTrace)
        .flatMap(e -> configValue(config::getString, TRACES_TOPIC))
        .map(topic -> probeValue((JsonObject v) -> sendTrace(topic, v)))
        .orElse(null);
  }

  private CompletionStage<Boolean> evictExtraMembers(final String consumerGroupId) {
    return extraMembers(consumerGroupId)
        .thenComposeAsync(
            members ->
                members.isEmpty()
                    ? completedFuture(false)
                    : admin
                        .removeMembersFromConsumerGroup(
                            consumerGroupId,
                            new RemoveMembersFromConsumerGroupOptions(membersToRemove(members)))
                        .all()
                        .thenApply(v -> true)
                        .toCompletionStage());
  }

  private JsonObject evictExtraMembersIfRace(
      final JsonObject message, final String consumerGroupId) {
    return ofNullable(message.getString(CONSUMER_GPOUP_ID, null))
        .filter(consumerGroupId::equals)
        .map(
            id ->
                evictExtraMembers(id)
                    .thenApply(r -> trace(r, consumerGroupId, () -> "Extra consumer group members"))
                    .thenApply(r -> message)
                    .toCompletableFuture()
                    .join())
        .orElse(message);
  }

  private CompletionStage<Set<String>> existingConsumerGroups(
      final String username, final String topic) {
    return describeConsumerGroups(existingConsumerGroupsIds(username, topic), getAdmin())
        .thenApply(Server::selectActiveConsumerGroups)
        .thenApply(groups -> trace(groups, () -> "Existing consumer groups"));
  }

  private CompletionStage<Collection<String>> extraMembers(final String consumerGroupId) {
    return describeConsumerGroups(set(consumerGroupId), getAdmin())
        .thenApply(
            map ->
                extraMembers(
                    trace(
                        map.get(consumerGroupId).members(), consumerGroupId, () -> "All members")));
  }

  private Admin getAdmin() {
    if (admin == null) {
      admin = Admin.create(adminConfig);
    }

    return admin;
  }

  private CompletionStage<String> getConsumerGroup(
      final String username, final String topic, final int index) {
    final String group = groupId(username, topic, index);

    return describeConsumerGroups(set(group), getAdmin())
        .thenComposeAsync(
            map ->
                map.isEmpty() || isFree(map.get(group).groupState())
                    ? completedFuture(trace(group, group, () -> "Didn't exist yet or was free"))
                    : getConsumerGroup(username, topic, index + 1));
  }

  private RequestHandler handler() {
    final Function<String, KafkaConsumer<String, JsonObject>> consumer =
        consumer(randomUUID().toString(), kafkaConfig);
    final String fallbackCookie = fallbackCookie(config);
    final String topic = config.getString(TOPIC);

    return (request, requestBody, response) ->
        getUsername(request, fallbackCookie)
            .map(
                u ->
                    getConsumerGroup(u, topic, 0)
                        .thenComposeAsync(
                            group ->
                                latestOffsets(u, topic)
                                    .thenComposeAsync(
                                        latest -> goToLatest(group, latest, getAdmin()))
                                    .thenApply(latest -> group))
                        .thenComposeAsync(
                            group ->
                                simpleResponse(
                                    response, OK, MIME_TYPE, sseStream(u, consumer, group)))
                        .exceptionally(
                            t -> {
                              admin = null;
                              LOGGER.log(SEVERE, t.getMessage(), t);
                              response.setStatus(INTERNAL_SERVER_ERROR);
                              return messagePublisher(t.getMessage());
                            }))
            .orElseGet(() -> simpleResponse(response, UNAUTHORIZED, empty()));
  }

  private CompletionStage<Map<TopicPartition, OffsetAndMetadata>> latestOffsets(
      final String username, final String topic) {
    return existingConsumerGroups(username, topic)
        .thenComposeAsync(groups -> topicPartitions(topic, admin).thenApply(p -> pair(groups, p)))
        .thenComposeAsync(
            pair ->
                topicPartitionOffsets(pair.second, admin)
                    .thenComposeAsync(
                        topicOffsets ->
                            consumerGroupOffsets(pair.first, pair.second, admin)
                                .thenApply(
                                    groupOffsets -> latestOffsets(groupOffsets, topicOffsets))));
  }

  public CompletionStage<Boolean> run() {
    final State<Boolean> stop = new State<>(false);

    startCleanUpSseConsumerGroups(stop);
    createServer();

    return httpServer
        .run()
        .thenApply(
            r -> {
              stop.set(true);
              return r;
            });
  }

  private void sendTrace(final String topic, final JsonObject message) {
    traceMessage(message)
        .ifPresent(m -> producer.send(new ProducerRecord<>(topic, message.getString(CORR), m)));
  }

  private Publisher<ByteBuf> sseStream(
      final String username,
      final Function<String, KafkaConsumer<String, JsonObject>> consumer,
      final String consumerGroupId) {
    final State<Boolean> completed = new State<>(false);
    final Function<JsonObject, String> getEventName = getEventName(config);
    final Function<JsonObject, Stream<String>> getSubscriptions = getSubscriptions(config);
    final Function<JsonObject, String> getUsername = getUsername(config);
    final String topic = config.getString(TOPIC);
    final KafkaPublisher<String, JsonObject> source =
        new KafkaPublisher<String, JsonObject>()
            .withTopics(set(topic))
            .withConsumer(() -> consumer.apply(consumerGroupId))
            .withStopImmediately(true)
            .withStopWhenNothingLeft(true)
            .withEventHandler(addRaceCheck(eventHandler, consumerGroupId));

    new Thread(
            () -> {
              ++activeRequests;
              source.start();
              --activeRequests;
              completed.set(true);
              LOGGER.info(() -> "SSE stream for user " + username + " disconnected");
            })
        .start();

    return with(Merge.of(
            with(source.publishers().get(topic))
                .map(ConsumerRecord::value)
                .map(json -> evictExtraMembersIfRace(trace(json, consumerGroupId), consumerGroupId))
                .filter(json -> !json.containsKey(CONSUMER_GPOUP_ID))
                .filter(
                    json ->
                        username.equals(getUsername.apply(json))
                            || getSubscriptions.apply(json).anyMatch(s -> s.equals(username)))
                .map(eventTracer())
                .map(
                    json ->
                        "event: " + getEventName.apply(json) + "\ndata: " + string(json) + "\n\n")
                .map(probeValue(m -> ++events))
                .get(),
            with(generate(() -> ":\n")).throttle(1).get()))
        .buffer(100, ofSeconds(1))
        .map(s -> trace(s, consumerGroupId).getBytes(UTF_8))
        .map(Unpooled::wrappedBuffer)
        .until(b -> completed.get())
        .get();
  }

  public void start() {
    run().toCompletableFuture().join();
  }

  private void startCleanUpSseConsumerGroups(final State<Boolean> stop) {
    runCleanUpSseConsumerGroups(config.getString(TOPIC), this::getAdmin, t -> admin = null, stop);
  }

  private <T> T trace(final T v, final Supplier<String> message) {
    trace(v, null, message);

    return v;
  }

  private <T> T trace(final T v, final String consumerGroupId) {
    return trace(v, consumerGroupId, null);
  }

  private <T> T trace(final T v, final String consumerGroupId, final Supplier<String> message) {
    LOGGER.finest(
        () ->
            instance
                + (consumerGroupId != null ? (": " + consumerGroupId) : "")
                + ": "
                + v.toString()
                + (message != null ? (": " + message.get()) : ""));

    return v;
  }

  private Optional<JsonObject> traceMessage(final JsonObject json) {
    final Function<JsonObject, String> getUsername = getUsername(config);

    return getString(json, "/" + CORR)
        .map(
            corr ->
                eventTrace
                    .withTraceId(corr)
                    .withTimestamp(now())
                    .withAttributes(attributesMap)
                    .withUsername(getUsername.apply(json))
                    .toJson()
                    .build());
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
