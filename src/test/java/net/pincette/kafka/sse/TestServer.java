package net.pincette.kafka.sse;

import static com.typesafe.config.ConfigFactory.defaultApplication;
import static com.typesafe.config.ConfigValueFactory.fromAnyRef;
import static java.net.http.HttpClient.Version.HTTP_1_1;
import static java.net.http.HttpClient.newBuilder;
import static java.net.http.HttpResponse.BodyHandlers.ofPublisher;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.time.Instant.now;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.stream.Collectors.toMap;
import static net.pincette.io.StreamConnector.copy;
import static net.pincette.jes.util.Kafka.createReliableProducer;
import static net.pincette.jes.util.Kafka.send;
import static net.pincette.jes.util.Kafka.topicPartitions;
import static net.pincette.json.Factory.a;
import static net.pincette.json.Factory.f;
import static net.pincette.json.Factory.o;
import static net.pincette.json.Factory.v;
import static net.pincette.json.JsonUtil.from;
import static net.pincette.json.JsonUtil.getValue;
import static net.pincette.rs.Chain.with;
import static net.pincette.rs.FlattenList.flattenList;
import static net.pincette.rs.Util.lines;
import static net.pincette.rs.Util.onComplete;
import static net.pincette.rs.kafka.ConsumerEvent.STARTED;
import static net.pincette.rs.kafka.Util.createTopics;
import static net.pincette.rs.kafka.Util.deleteTopics;
import static net.pincette.util.Collections.map;
import static net.pincette.util.Collections.set;
import static net.pincette.util.Pair.pair;
import static net.pincette.util.StreamUtil.rangeExclusive;
import static net.pincette.util.Util.tryToDoRethrow;
import static net.pincette.util.Util.tryToDoWithRethrow;
import static net.pincette.util.Util.tryToGetRethrow;
import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.admin.Admin.create;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.auth0.jwt.JWT;
import com.typesafe.config.Config;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;
import javax.json.JsonValue;
import net.pincette.io.DevNullInputStream;
import net.pincette.json.JsonUtil;
import net.pincette.jwt.Signer;
import net.pincette.kafka.json.JsonSerializer;
import net.pincette.util.Pair;
import net.pincette.util.State;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.MemberToRemove;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.RemoveMembersFromConsumerGroupOptions;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class TestServer {
  private static final String BOOTSTRAP_SERVER = "localhost:9092";
  private static final Map<String, Object> COMMON_CONFIG =
      map(pair(BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER));
  private static final String SUBSCRIPTIONS = "subscriptions";
  private static final String TOPIC = "test-sse";
  private static final String USERNAME = "username";
  private static final String USER_PREFIX = "user";
  private static final String VALUE = "value";

  private static final Admin admin = create(COMMON_CONFIG);
  private static final HttpClient client = newBuilder().version(HTTP_1_1).build();
  private static final Map<String, byte[]> resources = new HashMap<>();
  private static final Signer signer = new Signer(readKey("rsa.priv"));
  private static final URI uri = URI.create("http://localhost:9000");

  private static Config config() {
    return defaultApplication()
        .withValue("topic", fromAnyRef(TOPIC))
        .withValue("jwtPublicKey", fromAnyRef(readKey("rsa.pub")))
        .withValue("usernameField", fromAnyRef(USERNAME))
        .withValue("subscriptionsField", fromAnyRef(SUBSCRIPTIONS))
        .withValue("abandonedMessageLag", fromAnyRef(-1))
        .withValue("kafka.bootstrap.servers", fromAnyRef(BOOTSTRAP_SERVER));
  }

  private static Map<String, Collection<MemberToRemove>> convertToMemberToRemove(
      final Map<String, ConsumerGroupDescription> map) {
    return map(
        map.entrySet().stream()
            .map(
                e ->
                    pair(
                        e.getKey(),
                        e.getValue().members().stream()
                            .flatMap(member -> member.groupInstanceId().stream())
                            .map(MemberToRemove::new)
                            .toList())));
  }

  private static void deleteConsumerGroups() {
    admin
        .listConsumerGroups()
        .all()
        .toCompletionStage()
        .thenApply(TestServer::selectGroupIds)
        .thenComposeAsync(TestServer::removeConsumerGroupMembers)
        .thenComposeAsync(groups -> admin.deleteConsumerGroups(groups).all().toCompletionStage())
        .toCompletableFuture()
        .join();
  }

  private static Map<String, CompletableFuture<Void>> getReady(final int numberOfUsers) {
    return usernames(numberOfUsers)
        .map(u -> u + "-" + TOPIC + "-0")
        .collect(toMap(u -> u, u -> new CompletableFuture<>()));
  }

  private static NewTopic newTopic(final String name) {
    return new NewTopic(name, 1, (short) 1);
  }

  private static <T> HttpResponse<T> ok(final HttpResponse<T> response) {
    assertEquals(200, response.statusCode());

    return response;
  }

  private static byte[] read(final String resource) {
    return resources.computeIfAbsent(
        resource,
        r ->
            read(
                () ->
                    tryToGetRethrow(() -> TestServer.class.getResourceAsStream(r))
                        .orElseGet(DevNullInputStream::new)));
  }

  private static byte[] read(final Supplier<InputStream> in) {
    final ByteArrayOutputStream out = new ByteArrayOutputStream(0xfffff);

    tryToDoRethrow(() -> copy(in.get(), out));

    return out.toByteArray();
  }

  private static String readKey(final String name) {
    return new String(read("/" + name), US_ASCII);
  }

  private static HttpRequest request(final String username) {
    return HttpRequest.newBuilder()
        .uri(uri)
        .header("Authorization", "Bearer " + token(username))
        .build();
  }

  private static CompletionStage<Collection<String>> removeConsumerGroupMembers(
      final Collection<String> groupIds) {
    return admin
        .describeConsumerGroups(groupIds)
        .all()
        .toCompletionStage()
        .thenApply(TestServer::convertToMemberToRemove)
        .thenComposeAsync(TestServer::removeConsumerGroupMembers);
  }

  private static CompletionStage<Collection<String>> removeConsumerGroupMembers(
      final Map<String, Collection<MemberToRemove>> toRemove) {
    return allOf(
            toRemove.entrySet().stream()
                .map(
                    e ->
                        admin
                            .removeMembersFromConsumerGroup(
                                e.getKey(), new RemoveMembersFromConsumerGroupOptions(e.getValue()))
                            .all()
                            .toCompletionStage()
                            .toCompletableFuture())
                .toArray(CompletableFuture[]::new))
        .thenApply(v -> toRemove.keySet());
  }

  private static CompletableFuture<Void> runUser(final String username, final int maxMessages) {
    final CompletableFuture<Void> done = new CompletableFuture<>();

    client
        .sendAsync(request(username), ofPublisher())
        .thenApply(TestServer::ok)
        .thenAccept(
            resp -> {
              final State<Integer> lastValue = new State<>(-1);

              with(resp.body())
                  .map(flattenList())
                  .map(lines())
                  .filter(line -> line.startsWith("data:"))
                  .map(line -> line.substring("data:".length()).trim())
                  .map(TestServer::value)
                  .filter(value -> value != -1)
                  .map(
                      value -> {
                        assertEquals(lastValue.get() + 1, value);
                        lastValue.set(value);
                        return value;
                      })
                  .until(value -> value == maxMessages - 1)
                  .get()
                  .subscribe(onComplete(() -> done.complete(null)));
            });

    return done;
  }

  private static Stream<CompletableFuture<Void>> runUsers(final int number, final int maxMessages) {
    return usernames(number).map(username -> runUser(username, maxMessages));
  }

  private static Collection<String> selectGroupIds(final Collection<ConsumerGroupListing> groups) {
    return groups.stream()
        .map(ConsumerGroupListing::groupId)
        .filter(s -> s.contains("-" + TOPIC + "-"))
        .toList();
  }

  private static void sendMessages(
      final int numberOfUsers,
      final int maxMessages,
      final Function<String, Pair<String, JsonValue>> user) {
    tryToDoWithRethrow(
        () -> createReliableProducer(COMMON_CONFIG, new StringSerializer(), new JsonSerializer()),
        p -> {
          for (int i = 0; i < maxMessages; ++i) {
            for (int j = 0; j < numberOfUsers; ++j) {
              send(
                  p,
                  new ProducerRecord<>(
                      TOPIC,
                      randomUUID().toString(),
                      o(user.apply(USER_PREFIX + j), f(VALUE, v(i)))));
            }
          }
        });
  }

  private static Server startServer(final Map<String, CompletableFuture<Void>> ready) {
    final Collection<TopicPartition> partitions =
        topicPartitions(TOPIC, admin).toCompletableFuture().join();
    final Server server =
        new Server()
            .withEventHandler(
                (event, consumer) -> {
                  final CompletableFuture<Void> future =
                      ready.get(consumer.groupMetadata().groupId());

                  if (event == STARTED && future != null) {
                    consumer.seekToBeginning(partitions);
                    future.complete(null);
                  }
                })
            .withPort(9000)
            .withConfig(config());

    new Thread(server::start).start();

    return server;
  }

  private static Pair<String, JsonValue> subscription(final String username) {
    return f(SUBSCRIPTIONS, a(v(username)));
  }

  private static void test(
      final Function<String, Pair<String, JsonValue>> user, final int messages, final int users) {
    final Map<String, CompletableFuture<Void>> ready = getReady(users);
    final Server server = startServer(ready);
    final List<CompletableFuture<Void>> running = runUsers(users, messages).toList();

    allOf(ready.values().toArray(CompletableFuture[]::new)).join();
    sendMessages(users, messages, user);
    allOf(running.toArray(CompletableFuture[]::new)).join();
    server.close();
  }

  private static String token(final String username) {
    return signer.sign(JWT.create().withSubject(username).withExpiresAt(now().plusSeconds(3600)));
  }

  private static Pair<String, JsonValue> username(final String username) {
    return f(USERNAME, v(username));
  }

  private static Stream<String> usernames(final int numberOfUsers) {
    return rangeExclusive(0, numberOfUsers).map(i -> USER_PREFIX + i);
  }

  private static int value(final String line) {
    return from(line)
        .flatMap(json -> getValue(json, "/" + VALUE))
        .flatMap(JsonUtil::intValue)
        .orElse(-1);
  }

  @AfterEach
  void afterEach() {
    deleteTopics(set(TOPIC), admin).toCompletableFuture().join();
    deleteConsumerGroups();
  }

  @BeforeEach
  void beforeEach() {
    afterEach();
    createTopics(set(newTopic(TOPIC)), admin).toCompletableFuture().join();
  }

  @Test
  @DisplayName("test subscriptions")
  void testSubscriptions() {
    test(TestServer::subscription, 10, 10);
  }

  @Test
  @DisplayName("test username")
  void testUsername() {
    test(TestServer::username, 100, 100);
  }
}
