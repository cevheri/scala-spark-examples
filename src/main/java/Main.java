
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Main {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("csv-java-demo").getOrCreate();
        Dataset<Row> babies = spark.read().csv("file:///home/cevher/projects/spark-scala-basic-sample/Popular_Baby_Names.csv");
        babies.printSchema();
        babies.show();
        babies.write().csv("file:///home/cevher/projects/spark-scala-basic-sample/Popular_Baby_Names_write.csv");
    }
}