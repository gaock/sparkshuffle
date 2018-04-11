import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("WordCount")
      .setMaster("spark://172.16.1.107:7077")
//      .set("spark.executor.memory", "6g")
//      .set("spark.executor.cores","6")
    //      .set("spark.default.parallelism","4")
    val sc = new SparkContext(conf)
    val res1 = sc.textFile("hdfs://172.16.1.107:9000/word/input/17.txt")
    val res2 = res1.flatMap(_.split(" "))
    val res3 = res2.map(word=>(word,1)).reduceByKey(_+_).top(10)
    res3.foreach(println)
    sc.stop()
  }
}
