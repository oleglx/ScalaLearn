import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row

case class TestRecord(key: Int, value: String)

val spark = SparkSession
    .builder
    .appName("TestApp")
    .master("local")
    .getOrCreate()

val dataFrame = spark.createDataFrame(
    (1 to 10000).map(i => TestRecord(i, s"val_$i"))
)

dataFrame.repartition(10)

dataFrame.foreachPartition((partition: Iterator[Row]) => {
    val partitionList = partition.toList
    partitionList.foreach((value: Row) => {
        print(value)
    })
})

