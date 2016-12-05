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
      (values(0), //LocationId - no change
       values(1), //Month - no change
       values(2), //day - no change
       values(3), //time - no change
       values(4), //count_type - no change
       if(values(6).charAt(0)=='?') 1070.58824764576 else values(6),  // POP
       if(values(7).charAt(0)=='?') 492.831733564141 else values(7),  // Housing Density
       if(values(8).charAt(0)=='?') 0.13829529929963 else values(8),  // Housing Vacent
       if(values(9).charAt(0)=='?') 5 else values(9),                 // Avg Temp
       if(values(10).charAt(0)=='?') 6 else values(10),               // Min Temp
       if(values(11).charAt(0)=='?') 6 else values(11),              // Max  Temp
       if(values(12).charAt(0)=='?') 6 else values(12),              // Precipitation
       values(13), // CAUS_SNOW - no change
       if(values(5).charAt(0)!='0') 1 else 0 // target value
      )
    })


    System.out.println(rawData.count())
    System.out.println(preprocessedData.count())

    preprocessedData.coalesce(1,true).saveAsTextFile("output")

  }
}
