import com.twitter.conversions.time._
import com.twitter.logging.config._
import com.twitter.ostrich.admin.config._
import com.twitter.kestrelthrift.config._
import com.twitter.conversions.storage._
import com.twitter.ostrich.admin.config._
import net.lag.kestrel.config._

new KestrelConfig {
  listenAddress = "0.0.0.0"
  memcacheListenPort = 22133
  textListenPort = 2222

  queuePath = "/var/spool/kestrel"

  clientTimeout = None

  expirationTimerFrequency = 1.second

  maxOpenTransactions = 100

  // default queue settings:
  default.defaultJournalSize = 16.megabytes
  default.maxMemorySize = 128.megabytes
  default.maxJournalSize = 1.gigabyte
  default.syncJournal = 20.milliseconds

  admin.httpPort = 2223

  admin.statsNodes = new StatsConfig {
    reporters = new JsonStatsLoggerConfig {
      loggerName = "stats"
      serviceName = "kestrel"
    } :: new TimeSeriesCollectorConfig
  }

  queues = new QueueBuilder {
    // keep items for no longer than a half hour, and don't accept any more if
    // the queue reaches 1.5M items.
    name = "weather_updates"
    maxAge = 1800.seconds
    maxItems = 1500000
  } :: new QueueBuilder {
    // don't keep a journal file for this queue. when kestrel exits, any
    // remaining contents will be lost.
    name = "transient_events"
    keepJournal = false
  }

  loggers = new LoggerConfig {
    level = Level.INFO
    handlers = new FileHandlerConfig {
      filename = "/var/log/kestrel/kestrel.log"
      roll = Policy.SigHup
    }
  } :: new LoggerConfig {
    node = "stats"
    level = Level.INFO
    useParents = false
    handlers = new ScribeHandlerConfig {
      hostname = "localhost"
      category = "cuckoo_json"
      maxMessagesPerTransaction = 100
      formatter = BareFormatterConfig
    }
  }
}

// production mode.
new KestrelthriftServiceConfig {

  // Add your own config here

  // Where your service will be exposed.
  thriftPort = 9999

  // Ostrich http admin port.  Curl this for stats, etc
  admin.httpPort = 9900

  // End user configuration

  // Expert-only: Ostrich stats and logger configuration.

  admin.statsNodes = new StatsConfig {
    reporters = new TimeSeriesCollectorConfig
  }

  loggers =
    new LoggerConfig {
      level = Level.INFO
      handlers = new FileHandlerConfig {
        filename = "/var/log/kestrelthrift/production.log"
        roll = Policy.SigHup
      }
    } :: new LoggerConfig {
      node = "stats"
      level = Level.INFO
      useParents = false
      handlers = new FileHandlerConfig {
        filename = "stats.log"
        formatter = BareFormatterConfig
      }
    }
}
