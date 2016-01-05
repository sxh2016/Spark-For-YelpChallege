package main.scala
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.mllib.linalg.{ SparseVector => SV }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.linalg.{ SparseVector => SV }
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.feature.IDF
import breeze.linalg._
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors
import breeze.numerics.pow


object YelpReview {
  val conf = new SparkConf().setAppName("SparkForYelpReview").setMaster("local[2]")
  val sc = new SparkContext(conf)
  def main(args: Array[String]){
    val sqlContext = new SQLContext(sc)
    val strRDD = sc.textFile("data/yelp_academic_dataset_review.json")
    val jsonDF = sqlContext.jsonRDD(strRDD)
    jsonDF.printSchema()
    jsonDF.show(20)
    jsonDF.registerTempTable("ReviewTable")
    val sample = sqlContext.sql("select business_id, review_id, text from ReviewTable where date > CAST('2014-03-14' AS TIMESTAMP) and business_id = 'cE27W9VPgO88Qxe4ol6y_g'")
    //val sample1 = sqlContext.sql("select review_id, text from ReviewTable where date > CAST('2014-03-14' AS TIMESTAMP)")
    val count = sample.distinct().count()
    sample.take(50).foreach(println)
    //val count1 = sample1.distinct().count()
    println("The total number is : " + count)
    //println("The total number is : " + count1)
    
    
    val IdAndTxt = sample.map(t => (t(0).toString,tokenFilter(t(1).toString)))
    //val IdAndTxt1 = sample1.map(t => (t(0).toString,tokenFilter(t(1).toString)))
    IdAndTxt.take(5).foreach(println)

    val dim = math.pow(2,18).toInt
    val hashingTF = new HashingTF(dim)
    
    val tf = sample.map(t=> (t(0).toString,hashingTF.transform(tokenFilter(t(1).toString))))
    //val tf1 = sample1.map(t=> (t(0).toString,hashingTF.transform(tokenFilter(t(1).toString))))
    //println(tf.first)
    tf.cache()
    //tf1.cache()
    
    val idf = new IDF().fit(tf.map(_._2))
    //val idf1 = new IDF().fit(tf1.map(_._2))
    val tfidf = sample.map(t => (t(0).toString,idf.transform(hashingTF.transform(tokenFilter(t(1).toString)))))
    //val tfidf1 = sample1.map(t => (t(0).toString,idf1.transform(hashingTF.transform(tokenFilter(t(1).toString)))))
    val v = tfidf.map(_._2).first.asInstanceOf[SV]
    //val v1 = tfidf1.map(_._2).first.asInstanceOf[SV]
   
    
    //Using K-means do clustering
    val numClusters = 10
    val numIterations = 10
    val ClusterModel = KMeans.train(tfidf.map(_._2),numClusters,numIterations)
    val WSSSE = ClusterModel.computeCost(tfidf.map(_._2))
    println("Within Set Sum of Squared Errors about tfidf = " + WSSSE)
    
    //println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
    
    
    //recommendation
    val textAssigned = tfidf.map{case(id,vector) =>
      val pred = ClusterModel.predict(vector)
      val clustercenter = ClusterModel.clusterCenters(pred)
      val dist = computeDistance(DenseVector(clustercenter.toArray), DenseVector(vector.toArray))
      (id, pred, dist)
      }
    val ClusterAssignments = textAssigned.groupBy{case(id,cluster,dist) => cluster}.collectAsMap
    
    //recomendation based on distance
    
    for((k,v) <- ClusterAssignments.toSeq.sortBy(_._1)){
      println(s"Cluster $k:")
      val m = v.toSeq.sortBy(_._3)
      val FinalId = m.take(1).map{case(id,_,_) => (id)}
      val core = FinalId(0)
      println(core)
      //val result = sqlContext.sql("select text from ReviewTable where review_id = core")
      //println(result)
      
      println("======\n")
    }
    //sc.parallelize(result).saveAsTextFile("s3://sunxiaohua-sentimentjob/mushroom/test")
  }
  
  def tokenFilter(line : String): Seq[String] = {
    val nonWordsSplit = line.split("""\W+""").map(_.toLowerCase)
    val regex = """[^0-9]*""".r
    val numFilter = nonWordsSplit.filter(token => regex.pattern.matcher(token).matches)
    val tokenCounts = sc.parallelize(numFilter).map(t => (t,1)).reduceByKey(_+_)
    //val rareTokens = tokenCounts.filter{ case (k, v) => v < 2 }.map { case (k, v) => k }.collect.toSet
    val stopwords = Seq("the","a","an","of","or","in","for","by","on","but", "is", "not", "with", "as", "was", "if", "they", "are", "this", "and", "it", "have", "from", "at", "my", "be", "that", "to")
    val stopwordsFilter = numFilter.filterNot(token => stopwords.contains(token))
    //val rarewordsFilter = stopwordsFilter.filterNot(token => rareTokens.contains(token))
    val sizeFilter = stopwordsFilter.filter(token => token.size >= 2)
    val sequenceAfterFilter = sizeFilter.toSeq
    return sequenceAfterFilter
  }
  
  def computeDistance(v1: DenseVector[Double], v2: DenseVector[Double]): Double = pow(v1 - v2, 2).sum
}