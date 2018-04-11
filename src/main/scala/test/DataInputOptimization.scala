package test

import org.apache.spark.{SparkConf, SparkContext}

import scala.reflect.ClassTag

object DataInputOptimization {
  val properties = new Properties()
  //ItemNum,keySize,valueSize,partitionNum,mapTask,reduceTask
  properties.setProperties(Array(1000000000,4,12,1000,10,500))
  val property = properties.getProperties()
  val bb = 0 to (property(3)-1)


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("DataInput")
      .setMaster("spark://172.18.11.4:7077")
//      .set("spark.executor.memory", "2g")
      .set("spark.default.parallelism",property(5).toString)

    val sc = new SparkContext(conf)
    val res1 = sc.makeRDD(bb,property(3)).flatMap(func(_)).cache()
    val res2 = res1.groupByKey(property(5))
    res1.saveAsTextFile("hdfs://172.18.11.4:9000/gaochuan/dataInput/")
    println("res1.partitions.length: "+res1.partitions.length)
    println("res2.partitions.length: "+res2.partitions.length)
    println("res1.length: "+res1.count())
    println("res2.length: "+res2.count())

  }
  def func[U:ClassTag](word:Int): Array[(String,String)] ={
    return random(property(0),property(1),property(2))
  }
  def random(item:Int,keySize:Int,valueSize:Int): Array[(String,String)] = {
    val chr1: Array[Char] = Array('0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z')
    val chr2: Array[Char] = Array('0', '1', '2', '3', '4', '5', '6', '7', '8', '9')
    val mm = new Array[(String,String)](item/bb.length)
    var key,value:String =""
    val arr1 = new Array[Byte](keySize)
    val arr2 = new Array[Byte](valueSize)
    val random = new util.Random()

    for( i <- 0 until item/bb.length){
      random.nextBytes(arr1)
      for(i <- arr1){
        key= key+chr1((i+128)%chr1.length)
      }
      random.nextBytes(arr2)
      for(i <- arr2){
        value= value+chr2((i+128)%chr2.length)
      }
      mm(i) = (key,value)
      key=""
      value=""
    }
    return mm
  }

}
