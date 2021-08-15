import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.SaveMode

class ScalaSparkTest {

    def main(args: Array[String]*): Unit = {

        Logger.getLogger("org").setLevel(Level.ERROR)

        val spark = SparkSession
            .builder
            .appName("TestApp")
            .getOrCreate()

        import spark.implicits._

        // Defining transaction dataframe
        val transactionsDataframe = spark
            .read
            .parquet("/data/custom/rb/card/pa/txn")

        // Getting partition sequence to iterate over it when reading
        val partitionsRange = transactionsDataframe.select("trx_date").distinct()
        val partitionSeq = partitionsRange.collect().toSeq.map(value => value.getString(0))

        // Iterating over partition sequence to find independent aggregates
        partitionSeq.foreach(partitionDate => {
            val dateColumn = lit(partitionDate)
            val dataframe = transactionsDataframe
                .where($"trx_date" === dateColumn) // Select certain partition
                .withColumn("client_w4_id", $"client_w4_id".cast(StringType)) // Cast client_w4_id from decimal to string
                .select("client_w4_id", "mcc_code", "local_amt") // Not all of the columns of the dataframe needed

            // Check if dataframe is empty. Unlikely, but still worth checking
            if (!dataframe.head(1).isEmpty) {
                val aggregate = dataframe
                    .groupBy("client_w4_id", "mcc_code")
                    .agg(sum("local_amt").alias("sum_txn"))
                    .withColumn("report_dt", last_day(dateColumn)) // Adding end of the month date to the group for future partitioning
                aggregate.write.mode(SaveMode.Append).parquet("/data/stg/agg_stg.parquet") // Save intermediate results
            }
        })

        val dataframe = spark.read.parquet("/data/stg/agg_stg.parquet") // Read all the intermediate results to one dataframe

        val finalAggregate = dataframe
            .groupBy("client_w4_id", "mcc_code", "report_dt")
            .agg(sum("local_amt").alias("sum_txn"))

        val epkDataframe = spark.read
            .parquet("/data/custom/rb/epk/pa/epk_lnk_host_id")
            .where($"row_actual_to" >= current_date) // Select only rows actual to this date
            .where($"external_system" === "WAY4")
            .select("external_system_client_id", "epk_id") // Again, not all columns are needed
            .withColumnRenamed("external_system_client_id", "client_w4_id") // Rename column for future join

        val transactionsAggregate = finalAggregate
            .join(epkDataframe, "client_w4_id")
            .select("epk_id", "sum_txn", "mcc_code", "report_dt")

        transactionsAggregate.write
            .partitionBy("report_dt") // Define partitions for parquet file
            .mode(SaveMode.Overwrite)
            .parquet("/data/custom/rb/txn_aggr/pa/txn_aggr")
    }
}
