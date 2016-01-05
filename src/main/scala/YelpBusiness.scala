package main.scala
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.TimestampType

object YelpBusiness {
  def main(args: Array[String]){
    val conf = new SparkConf().setAppName("SparkForYelpBusiness").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val strRDD = sc.textFile("data/yelp_academic_dataset_business.json")
    val jsonDF = sqlContext.jsonRDD(strRDD)
    jsonDF.printSchema()
    jsonDF.show(20)
    jsonDF.registerTempTable("BusinessTable")
    val sample = sqlContext.sql("select business_id,categories from BusinessTable where city = 'Phoenix'")
    val example = sample.map(row => (row(0).toString,row(1).toString))
    example.take(20).foreach(println)
    val count = sample.distinct.count()
    println("The total number business in Phoenix is : " + count)
    val number = sqlContext.sql("select business_id from BusinessTable")
    val count2 = number.count()
    println("The total number of colum is" + count2)
  }
}