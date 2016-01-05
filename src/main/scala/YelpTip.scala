package main.scala
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._

object YelpTip {
  def main(args: Array[String]){
    val conf = new SparkConf().setAppName("SparkForYelpTip").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val strRDD = sc.textFile("data/yelp_academic_dataset_tip.json")
    val jsonDF = sqlContext.jsonRDD(strRDD)
    jsonDF.printSchema()
    jsonDF.show(20)
    jsonDF.registerTempTable("TipTable")
    val sample = sqlContext.sql("select text from TipTable where date > CAST('2012-03-14' AS TIMESTAMP)")
    sample.show(50)
  }
}