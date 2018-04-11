import org.apache.spark.{SparkConf, SparkContext}

object Test1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("WordCount")
      .setMaster("local")
    //      .set("spark.executor.memory", "6g")
    //      .set("spark.executor.cores","6")
          .set("spark.default.parallelism","4")
    val sc = new SparkContext(conf)
    val res1 = sc.textFile("C://Users/12933/Desktop/",10)
    val res2 = res1.flatMap(_.split(" "))
    val res3 = res2.map(word=>(word,1)).reduceByKey(_+_)
    res3.collect().foreach(println)
    println(res1.partitions.length)
    println(res2.partitions.length)
    println(res3.partitions.length)
    sc.stop()
  }
}
