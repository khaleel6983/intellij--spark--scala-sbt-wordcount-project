import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("sparkJob")


    val sc = new SparkContext(conf)
    //val input = args(0)
    //val output = args(1)
    //var file = sc.parallelize(List("ram","raju","ramu","rani","s","swathi","ram"));
    val file = sc.textFile("F:/words.txt")
    //val file = sc.textFile(input)
    var fmap = file.flatMap(x => x.split(" "))
    var ma = fmap.map(x => (x,1))
      var red = ma.reduceByKey(_+_)
    var sorData = red.sortBy(x => (x._1))
    //sorData.saveAsTextFile(output)
    sorData.collect().foreach(println)
    sc.stop()


  }
}