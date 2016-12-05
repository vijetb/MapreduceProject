import org.apache.spark.{SparkConf, SparkContext}

object PreProcess {
  def main(args: Array[String]){
    val conf = new SparkConf()
      .setAppName("eBird Project")
      .setMaster("local")

    val sc = new SparkContext(conf)
    val inputData = sc.textFile(args(0))//read the file
    print("count" + inputData.count());
  }
}
