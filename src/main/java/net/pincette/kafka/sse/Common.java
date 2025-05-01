package net.pincette.kafka.sse;

import static java.util.logging.Logger.getLogger;
import static net.pincette.config.Util.configValue;

import com.typesafe.config.Config;
import java.util.logging.Logger;

class Common {
  static final Logger LOGGER = getLogger("net.pincette.kafka.sse");
  private static final String NAMESPACE = "namespace";
  static final String SSE = "sse";
  static final String VERSION = "1.3.1";

  private Common() {}

  static String namespace(final Config config) {
    return configValue(config::getString, NAMESPACE).orElse(SSE);
  }
}
