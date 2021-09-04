package org.apache.spark.deploy.shuffleService
import org.apache.spark.SparkConf
import org.apache.spark.util.{IntParam, Utils}
import scala.annotation.tailrec
import org.apache.spark.internal.config
import java.net.InetAddress

/**
 * Command-line parser for the shuffle service.
 */
private[shuffleService] class ExternalShuffleServiceArguments(args: Array[String], conf: SparkConf) {
  var host = Utils.localHostName()
  var port = 7777
  var webUiPort = 8080
  var propertiesFile: String = null
  var lookAheadCaching: Boolean = false
  var distributedCherry: Boolean = false
  var lookAheadCachingSize: Int = 0
  var lookAheadCachingPort = 7788
  var lookAheadCachingHost = "localhost"

  if (System.getenv("SPARK_REMOTE_SHUFFLE_SERVICE_HOST") != null) {
    host = System.getenv("SPARK_REMOTE_SHUFFLE_SERVICE_HOST")
  }
  if (System.getenv("SPARK_CHERRY_SHUFFLE_SERVICE_PORT") != null) {
    if (System.getenv("SPARK_CHERRY_SHUFFLE_SERVICE_PORT").contains(":")){
      port = System.getenv("SPARK_CHERRY_SHUFFLE_SERVICE_PORT").substring(System.getenv("SPARK_CHERRY_SHUFFLE_SERVICE_PORT").lastIndexOf(':') + 1).toInt
    }
    else{
      port = System.getenv("SPARK_CHERRY_SHUFFLE_SERVICE_PORT").toInt
    }
  }

  parse(args.toList)

  // This mutates the SparkConf, so all accesses to it must be made after this line
  propertiesFile = Utils.loadDefaultSparkProperties(conf, propertiesFile)

  @tailrec
  private def parse(args: List[String]): Unit = args match {
    case ("--host" | "-h") :: value :: tail =>
      Utils.checkHost(value)
      host = value
      parse(tail)

    case ("--port" | "-p") :: IntParam(value) :: tail =>
      port = value
      parse(tail)

    case ("--look.ahead.caching") :: value :: tail =>
      lookAheadCaching = value.toBoolean
      conf.set(config.LOOK_AHEAD_CACHING_ENABLED, lookAheadCaching)
      parse(tail)

    case ("--look.ahead.caching.size") :: value :: tail =>
      lookAheadCachingSize = value.toInt
      conf.set(config.LOOK_AHEAD_CACHING_SIZE, lookAheadCachingSize)
      parse(tail)

    case ("--look.ahead.caching.host") :: value :: tail =>
      lookAheadCachingHost = value
      parse(tail)

    case ("--look.ahead.caching.port") :: IntParam(value) :: tail =>
      lookAheadCachingPort = value
      conf.set(config.LOOK_AHEAD_CACHING_PORT, lookAheadCachingPort)
      parse(tail)

    case ("--distributed") :: value :: tail =>
      distributedCherry = value.toBoolean
      conf.set(config.DISTRIBUTED_CHERRY_ENABLED, distributedCherry)

      conf.set(config.METADATA_SERVICE_HOST, "spark-metadata-service")
      conf.set(config.METADATA_SERVICE_PORT, 5555)
      parse(tail)

    case ("--properties-file") :: value :: tail =>
      propertiesFile = value
      parse(tail)

    case ("--help") :: tail =>
      printUsageAndExit(0)

    case Nil => // No-op

    case _ =>
      printUsageAndExit(1)
  }

  /**
   * Print usage and exit JVM with the given exit code.
   */
  def printUsageAndExit(exitCode: Int): Unit = {
    // scalastyle:off println
    System.err.println(
      "Usage: Remote Shuffle Service [options] \n" +
        "\n" +
        "Options:\n" +
        "  -h HOST, --host HOST     Hostname to listen on\n" +
        "  -p PORT, --port PORT     Port to listen on (default: 7777)\n" +
        "  --look.ahead.caching ENABLED_BOOLEAN    Look Ahead Caching(default: false)\n" +
        "  --look.ahead.caching.port PORT    Port for Shuffle Service to listen on(default: 7788)\n" +
        "  --distributed ENABLED_BOOLEAN    Distributed Cherry(default: false)\n" +
        //"  --webui-port PORT        Port for web UI (default: 8080)\n" +
        "  --properties-file FILE   Path to a custom Spark properties file.\n" +
        "                           Default is conf/spark-defaults.conf.")
    // scalastyle:on println
    System.exit(exitCode)
  }

}


