package main.scala
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._

object YelpCheckIn {
  def main(args: Array[String]){
    val conf = new SparkConf().setAppName("SparkForYelpCheckIn").setMaster("local[2]")
    val sc =new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    //read the json file
    val dFrameOfCheckIn = sqlContext.jsonFile("data/yelp_academic_dataset_checkin.json")
    dFrameOfCheckIn.printSchema()
    dFrameOfCheckIn.show()
    dFrameOfCheckIn.registerTempTable("CheckInTable")
    val sample = sqlContext.sql("select checkin_info from CheckInTable where business_id != ' '")
    sample.show(100)
  }
}