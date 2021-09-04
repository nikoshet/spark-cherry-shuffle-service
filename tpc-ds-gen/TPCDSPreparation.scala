import com.databricks.spark.sql.perf.tpcds.TPCDSTables
import org.apache.spark.sql._

// Set:
val rootDir: String = "hdfs://10.0.1.167:6666/home/ubuntu/tpcds" // root directory of location to create data in.
val databaseName: String = "tpcds" // name of database to create.
val scaleFactor: String = "40" // scaleFactor defines the size of the dataset to generate (in GB).
val format: String = "parquet" // valid spark format like parquet "parquet".
val sqlContext = new SQLContext(sc)
// Run:
val tables = new TPCDSTables(sqlContext,
  dsdgenDir = "/home/ubuntu/spark-sql-perf/tpcds-kit/tools", // location of dsdgen
  scaleFactor = scaleFactor,
  useDoubleForDecimal = false, // true to replace DecimalType with DoubleType
  useStringForDate = false) // true to replace DateType with StringType

tables.genData(
  location = rootDir,
  format = format,
  overwrite = true, // overwrite the data that is already there
  partitionTables = true, // create the partitioned fact tables
  clusterByPartitionColumns = true, // shuffle to get partitions coalesced into single files.
  filterOutNullPartitionValues = false, // true to filter out the partition with NULL key value
  tableFilter = "", // "" means generate all tables
  numPartitions = 2000) // how many dsdgen partitions to run - number of input tasks.

// Create the specified database
//sql(s"create database $databaseName")
// Create metastore tables in a specified database for your data.
// Once tables are created, the current database will be switched to the specified database.
tables.createExternalTables(rootDir, "parquet", databaseName, overwrite = true, discoverPartitions = true)
