import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._

object PageRank {
  
  def main(args: Array[String]): Unit = {
    if(args.length != 3){
      println("Number of Arguments must be three, Input File, Iterations, and Output File")
    }
    val sc = new SparkContext(new SparkConf().setAppName("PageRank"))
    val spark = SparkSession
      .builder()
      .appName("PageRank")
      .getOrCreate()
    
    val inputFile = args(0)
    val iterations = args(1).toInt
    val outputFile =args(2)

    val data = spark
      .read
      .option("header", "true")
      .csv(inputFile)
      .select("ORIGIN", "DEST")

    //Reading the Input Data
    val airport_data = spark.read.option("header","true").option("inferSchema","true").csv(args(0))
    
    //Map the values and convert into rdd
    val airport_data_string = airport_data.rdd.map(x => (x(0).toString, x(1).toString))
    val distinct_airport = airport_data_string.map(x => x._1).distinct.collect.toList
    
    //Create the variables
    var inDegree = scala.collection.mutable.Map[String, Double]()
    val outDegree = scala.collection.mutable.Map[String, Double]()
    val origin_rdd : RDD[Row] = airport_data.select("ORIGIN").rdd
    val origin_array : RDD[String] = origin_rdd.map(x => x(0).toString)
    
    //Outlinks from Origin and merged into collect
    val outlinks = origin_array.map(x => (x,1)).reduceByKey((x,y) => x+y).collect()
    for ((k,v) <- outlinks){
      outDegree.put(k,v)
    }
    
    //InDegree to Destination
    for (a <- distinct_airport){
      inDegree.put(a,10)
    }

    //Create PageRank
    var PageRankValue = scala.collection.mutable.Map[String, Double]()
    
    //Calculate PageRank
    for (itr <- 1 to iterations) {
      for (a <- distinct_airport) {
        PageRankValue.put(a, 0)
      }
      for (a <- distinct_airport) {
        inDegree.put(a, inDegree.get(a).getOrElse(Double).asInstanceOf[Double] / outDegree.get(a).getOrElse(Double).asInstanceOf[Double])
      }
      for ((o, d) <- airport_data_string.collect().toList) {
        PageRankValue.put(d, PageRankValue.get(d).getOrElse(Double).asInstanceOf[Double] + inDegree.get(o).getOrElse(Double).asInstanceOf[Double])
      }
      for ((a, v) <- PageRankValue) {
        PageRankValue.put(a, ((0.15 / distinct_airport.size) + 0.85 * v))
      }
      inDegree = PageRankValue.clone()
    }

    //Sorting the PageRanks Value in Descending Order
    val ranks = inDegree.toSeq.sortBy(-_._2)
    sc.parallelize(ranks).saveAsTextFile(outputFile + "")
  }  
}