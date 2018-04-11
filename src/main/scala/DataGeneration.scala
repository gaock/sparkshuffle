import java.io.{File, FileWriter}

import scala.collection.mutable.ArrayBuffer

object DataGeneration {
  def writeFileToDisk(ItemSize:Int,keySize:Int,valueSize:Int,partitionNum:Int): Unit ={
//  random(ItemSize:Int,keySize:Int,valueSize:Int)
//  for(i <- 1 to partitionNum){
//    itemSize = new util.Random().nextInt(10)
//    write(i,random(ItemSize:Int,keySize:Int,valueSize:Int))
//  }
  for(i <- 1 to partitionNum){
    val eachNum = ItemSize/partitionNum
    val repeatTime = eachNum/100000
    var writeSize = 100000
    for(j <- 1 to repeatTime){
      if(j == repeatTime) writeSize = eachNum%100000
      write(i,random(writeSize,keySize:Int,valueSize:Int))
    }

  }

  }
  def write(num:Int,fun:Array[(String,String)]) {
    val out = new FileWriter(new File("C://Users/12933/Desktop/test/"+num+".txt" ),true)
    for (i <- 0 until  fun.length)
      out.write(fun(i)._1.toString+"  "+fun(i)._2.toString+"\r\n")
    out.close()
  }
  def random(item:Int,keySize:Int,valueSize:Int): Array[(String,String)] = {
    val chr1: Array[Char] = Array('0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z')
    val chr2: Array[Char] = Array('0', '1', '2', '3', '4', '5', '6', '7', '8', '9')
    val mm = new ArrayBuffer[(String,String)]
    var key,value:String =""
    for( i <- 0 until item){
      for (j <-  0 until keySize) {
        key += chr1(new util.Random().nextInt(36))
      }
      for (j <-  0 until valueSize) {
        value += chr2(new util.Random().nextInt(10))
      }
      mm.insert(i,(key,value))
      key=""
      value=""
    }
    return mm.toArray
  }

  def main(args: Array[String]): Unit = {
    writeFileToDisk(100000000,4,12,40)
  }

}
