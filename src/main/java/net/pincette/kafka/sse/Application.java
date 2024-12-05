package net.pincette.kafka.sse;

import static com.typesafe.config.ConfigFactory.defaultOverrides;
import static java.lang.Integer.parseInt;
import static java.lang.System.exit;
import static java.util.logging.Logger.getLogger;
import static net.pincette.jes.util.Configuration.loadDefault;
import static net.pincette.util.Util.initLogging;
import static net.pincette.util.Util.isInteger;

import java.util.logging.Logger;

public class Application {
  static final Logger LOGGER = getLogger("net.pincette.kafka.sse");
  private static final String VERSION = "1.2.0";

  @SuppressWarnings("java:S106") // Not logging
  public static void main(final String[] args) {
    if (args.length != 1 || !isInteger(args[0])) {
      System.err.println("Usage: net.pincette.kafka.sse.Application port");
      exit(1);
    }

    initLogging();
    LOGGER.info(() -> "Version " + VERSION);
    new Server()
        .withPort(parseInt(args[0]))
        .withConfig(defaultOverrides().withFallback(loadDefault()))
        .start();
  }
}
