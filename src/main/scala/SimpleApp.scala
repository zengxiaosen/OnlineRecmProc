/**
  * Created by root on 18-3-31.
  */
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
object SimpleApp {
  def main(args: Array[String]) {
    val logFile = "/home/zengxiaosen/zengxiaose.txt"
    val sparkConf = new SparkConf().setAppName("aa").setMaster("local")
    val sparkContext = new SparkContext(sparkConf)
    val spark = SparkSession.builder.appName("SimpleApplication").master("local").getOrCreate()

    val logData = spark.read.textFile(logFile).cache()
    val rdd = spark.sparkContext.textFile(logFile)
    import spark.implicits._
    val dataSet = spark.read.textFile(logFile)
      .flatMap(x => x.split(" "))
      .map(x => (x, 1)).groupBy("_1").count()

    val lines = rdd.flatMap(x => x.split(" ")).collect().toList
    println(lines)
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs")

    val rdd1 = Array(1,2, 3, 4)
    sparkContext.parallelize(rdd1)




    spark.stop()

  }

}
