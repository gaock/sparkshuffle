package test

class Properties {
  var ItemNum=0
  var keySize=0
  var valueSize=0
  var partitionNum=0
  var mapTask=partitionNum
  var reduceTask=0
  def setProperties(propertiesArray:Array[Int]):Unit={
    ItemNum = propertiesArray(0)
    keySize = propertiesArray(1)
    valueSize = propertiesArray(2)
    partitionNum = propertiesArray(3)
    mapTask = partitionNum
    reduceTask = propertiesArray(5)
  }
  def getProperties():Array[Int]={
    return Array(ItemNum,keySize,valueSize,partitionNum,mapTask,reduceTask)
  }

}
