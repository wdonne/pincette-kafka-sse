package net.pincette.kafka.sse;

import static com.typesafe.config.ConfigFactory.defaultOverrides;
import static java.lang.Integer.parseInt;
import static java.lang.System.exit;
import static net.pincette.jes.tel.OtelUtil.addOtelLogHandler;
import static net.pincette.jes.tel.OtelUtil.logRecordProcessor;
import static net.pincette.jes.tel.OtelUtil.otelLogHandler;
import static net.pincette.jes.util.Configuration.loadDefault;
import static net.pincette.kafka.sse.Common.LOGGER;
import static net.pincette.kafka.sse.Common.SSE;
import static net.pincette.kafka.sse.Common.VERSION;
import static net.pincette.kafka.sse.Common.namespace;
import static net.pincette.util.Util.initLogging;
import static net.pincette.util.Util.isInteger;

import com.typesafe.config.Config;

public class Application {
  private static void addOtelLogger(final Config config) {
    logRecordProcessor(config)
        .flatMap(p -> otelLogHandler(namespace(config), SSE, VERSION, p))
        .ifPresent(h -> addOtelLogHandler(LOGGER, h));
  }

  @SuppressWarnings("java:S106") // Not logging
  public static void main(final String[] args) {
    if (args.length != 1 || !isInteger(args[0])) {
      System.err.println("Usage: net.pincette.kafka.sse.Application port");
      exit(1);
    }

    final Config config = defaultOverrides().withFallback(loadDefault());

    initLogging();
    addOtelLogger(config);
    LOGGER.info(() -> "Version " + VERSION);
    new Server().withPort(parseInt(args[0])).withConfig(config).start();
  }
}
