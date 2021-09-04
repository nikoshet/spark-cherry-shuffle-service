package org.apache.spark.deploy.metadataService

import org.apache.spark.SparkConf
import org.apache.spark.util.{IntParam, Utils}
import scala.annotation.tailrec
import org.apache.spark.internal.config

/**
 * Command-line parser for the metadata service.
 */
private[metadataService] class MetadataServiceArguments(args: Array[String], conf: SparkConf) {
  var host = Utils.localHostName()
  var port = 5555
  var propertiesFile: String = null
  //var distributedCherry: Boolean = true
  var metadataServiceHost = "localhost"

  if (System.getenv("SPARK_METADATA_SERVICE_HOST") != null) {
    host = System.getenv("SPARK_METADATA_SERVICE_HOST")
  }
  if (System.getenv("SPARK_METADATA_SERVICE_PORT") != null) {
    if (System.getenv("SPARK_METADATA_SERVICE_PORT").contains(":")){
      port = System.getenv("SPARK_METADATA_SERVICE_PORT").substring(System.getenv("SPARK_METADATA_SERVICE_PORT").lastIndexOf(':') + 1).toInt
    }
    else{
      port = System.getenv("SPARK_METADATA_SERVICE_PORT").toInt
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
        "  -p PORT, --port PORT     Port to listen on (default: 5555)\n" +
        "  --properties-file FILE   Path to a custom Spark properties file.\n" +
        "                           Default is conf/spark-defaults.conf.")
    // scalastyle:on println
    System.exit(exitCode)
  }

}


