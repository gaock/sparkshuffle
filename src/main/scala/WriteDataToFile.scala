import org.apache.spark.{SparkConf, SparkContext}

import scala.reflect.ClassTag

object WriteDataToFile {
  val properties = new Properties()
  properties.setProperties(Array(1000000000,4,12,20,10,10))
  val property = properties.getProperties()
  val bb = "2323408888".toSeq

  //ItemNum,keySize,valueSize,partitionNum,mapTask,reduceTask

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("DataInput")
      .setMaster("spark://172.18.11.04:7077")
      .set("spark.executor.memory", "4g")
//      .set("spark.default.parallelism",property(5).toString)
    val sc = new SparkContext(conf)
    val res1 = sc.makeRDD(bb,property(3))
      .flatMap(func(_))
      .saveAsTextFile("hdfs://172.16.1.107:9000/DataInput/")
  }
  def func[U:ClassTag](word: Char): List[(String,String)] ={
    return random(property(0),property(1),property(2))
  }
  def random(item:Int,keySize:Int,valueSize:Int): List[(String,String)] = {
    val chr1: Array[Char] = Array('0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z')
    val chr2: Array[Char] = Array('0', '1', '2', '3', '4', '5', '6', '7', '8', '9')
    var mm:List[(String,String)] = List()
    var key,value:String =""
    for( i <- 0 until item/bb.length){
      for (j <-  0 until keySize) {
        key += chr1(new util.Random().nextInt(36))
      }
      for (j <-  0 until valueSize) {
        value += chr2(new util.Random().nextInt(10))
      }
      mm = mm:+((key,value))
      key=""
      value=""
    }
    return mm
  }
}
