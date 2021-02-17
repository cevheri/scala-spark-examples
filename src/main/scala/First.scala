import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object First {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder.appName("csv-scala-demo1").getOrCreate
    val data: DataFrame = spark.read.option("header", "true").csv("file:///home/cevher/Downloads/Fire_Incident_Dispatch_Data.csv")
    data.printSchema()

    data.head(10).foreach(f => println(f))



  }
}
