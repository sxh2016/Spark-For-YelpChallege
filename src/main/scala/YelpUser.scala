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
import org.apache.spark.graphx._
import scala.math.abs
import org.apache.spark.rdd._

object YelpUser {
  def main(args: Array[String]){
    val conf = new SparkConf().setAppName("SparkForYelpUser").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val strRDD = sc.textFile("data/yelp_academic_dataset_user.json")
    val jsonDF = sqlContext.jsonRDD(strRDD)
    jsonDF.printSchema()
    jsonDF.saveAsParquetFile("result/business")
    jsonDF.show(20)
    jsonDF.registerTempTable("UserTable")
    val sample = sqlContext.sql("select atrributes from UserTable where yelping_since > CAST('2012-11' AS TIMESTAMP)")
    val sample1 = sqlContext.sql("select user_id, friends from UserTable where fans >= 1")
    val TotalNum = sqlContext.sql("select * from UserTable where user_id != ''").count
    val count = sample1.count
    sample1.show(5)
    println(count)
    println(TotalNum)
    println("*********************")
    
    
    //build the graph
    val EdgeList = sample1.map{t =>
      val Identy = t(0).toString
      val Friends = t(1).toString.split(",").toSeq
      val it = Friends.toIterator
      Friends.map(it => (Identy,it))
      }
    EdgeList.take(2).foreach(println)
    
    
    
    val edges : RDD[Edge[Int]] = 
      EdgeList.map{line =>
        val pair = line.toIterator.toSeq
        val srcId = abs(pair(0).hashCode.toLong)
        val dstId = abs(pair(1).hashCode.toLong)
        Edge(srcId, dstId, 1)
    }
    val UserNetwork : Graph[Int,Int] = Graph.fromEdges(edges,0)
    println("##################")
    val num1 = edges.first
    println(num1)
  }
}