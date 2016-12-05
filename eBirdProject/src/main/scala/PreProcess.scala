import org.apache.spark.{SparkConf, SparkContext}

object PreProcess {
  def main(args: Array[String]){
    val conf = new SparkConf()
      .setAppName("eBird Project")
      .setMaster("local")

    val sc = new SparkContext(conf)
    val rawData = sc.textFile("input/input.csv")

    val preprocessedData = rawData.map(data => {
      val values = data.split(",")
      (values(1), //LocationId - no change
       values(5), //Month - no change
       values(6), //day - no change
       values(7), //time - no change
       values(11), //count_type - no change
       if(values(955).charAt(0)=='?') 1070.58824764576 else values(955),  // POP
       if(values(956).charAt(0)=='?') 492.831733564141 else values(956),  // Housing Density
       if(values(957).charAt(0)=='?') 0.13829529929963 else values(957),  // Housing Vacent
       if(values(963).charAt(0)=='?') 5 else values(963),                 // Avg Temp
       if(values(964).charAt(0)=='?') 6 else values(964),               // Min Temp
       if(values(965).charAt(0)=='?') 6 else values(965),              // Max  Temp
       if(values(966).charAt(0)=='?') 6 else values(966),              // Precipitation
       values(967), // CAUS_SNOW - no change
       if(values(26).charAt(0)!='0') 1 else 0 // target value
      )
    })


    System.out.println(rawData.count())
    System.out.println(preprocessedData.count())

    preprocessedData.coalesce(1,true).saveAsTextFile("output")

  }
}
