import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object First {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder.appName("csv-scala-demo").getOrCreate
    val data: DataFrame = spark.read.option("header","true").csv( "file:///home/cevher/projects/spark-scala-basic-sample/Popular_Baby_Names.csv")

    data.printSchema()

    val res: Array[Row] = data.head(10)
    println(res)
    res.foreach(f => println(f))

  }


}
