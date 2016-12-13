import org.apache.hadoop.yarn.util.RackResolver
import org.apache.spark.ml.feature._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.sql.types.{DataTypes, StructType}
object PreProcess {
  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setAppName("eBird Project")
      .setMaster("local[*]")
    //      .setMaster("yarn")

    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    // Input  : BzCompress File
    // Output : RDD - (Lines of CSV)
    // Purpose: Ignore the header of CSV
    val rawData = sc.textFile(args(0))
    val rawDataWithoutHeader = rawData.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }

    // Input  : RDD
    // Output : RDD - (String, ...)
    // Purpose: Filter the the required features
    val preprocessedData = rawDataWithoutHeader.map(data => {
      val values = data.split(",")
      (if (values(26).charAt(0) != '0') 1.0.toString else 0.0.toString, // target value
        values(2), //Lat - no change //
        values(3), //Long - no change //
        values(5), //Month - no change //
        values(6), //day - no change
        values(7), //time - no change
        if (values(955).charAt(0) == '?') null else values(955), // POP 6
        if (values(956).charAt(0) == '?') null else values(956), // Housing Density
        if (values(957).charAt(0) == '?') null else values(957), // Housing Vacent
        if (values(963).charAt(0) == '?') null else values(963), // Avg Temp
        if (values(964).charAt(0) == '?') null else values(964), // Min Temp
        if (values(965).charAt(0) == '?') null else values(965), // Max  Temp
        if (values(966).charAt(0) == '?') null else values(966), // Precipitation
        if (values(967).charAt(0) == '?') "0" else values(967) // CAUS_SNOW - if ? make it 0
        )
    })

    val colNames = Array("Label", "LAT", "LONG", "MONTH", "DAY", "TIME", "POP00_SQMI", "HOUSING_DENSITY", "HOUSING_PERCENT_VACANT", "CAUS_TEMP_AVG", "CAUS_TEMP_MIN", "CAUS_TEMP_MAX", "CAUS_PREC", "CAUS_SNOW")
    val colRequiredToBeImputed = Array("POP00_SQMI", "HOUSING_DENSITY", "HOUSING_PERCENT_VACANT", "CAUS_TEMP_AVG", "CAUS_TEMP_MIN", "CAUS_TEMP_MAX", "CAUS_PREC")
    val categoricalCol = Array("CAUS_TEMP_AVG", "CAUS_TEMP_MIN", "CAUS_TEMP_MAX", "CAUS_PREC")
    val featureList = Array("LAT", "LONG", "MONTH", "DAY", "TIME", "POP00_SQMI", "HOUSING_DENSITY", "HOUSING_PERCENT_VACANT", "CAUS_TEMP_AVG", "CAUS_TEMP_MIN", "CAUS_TEMP_MAX", "CAUS_PREC", "CAUS_SNOW")

    import sqlContext.implicits._

    // Input  : RDD
    // Output : DataFrame
    // Purpose: Converts the RDD to data frame
    val df = preprocessedData.toDF(colNames: _*)

    // Input  : DataFrame
    // Output : DataFrame
    // Purpose: Calculate the avg of the selected features
    val imputer = df.select(colRequiredToBeImputed.map(avg(_)): _*).toDF(colRequiredToBeImputed: _*)

    // Input  : DataFrame
    // Output : Map (String -> String)
    // Purpose: Converts the data frame to a map of Column and average
    val imputerMap = imputer.columns.zip(imputer.first().toSeq).
      map(a =>
        if (categoricalCol contains a._1)
          (a._1 -> a._2.toString.toDouble.ceil.toString)
        else
          (a._1 -> a._2)).toMap

    // Input  : DataFrame
    // Output : DataFrame
    // Purpose: Create a DataFrame with no nulls
    val imputedDataFrameStringRep = df.na.fill(imputerMap)

    // Input  : DataFrame
    // Output : DataFrame
    // Purpose: Converts the data frame double represenation for future calculation
    val completeData = imputedDataFrameStringRep.select(imputedDataFrameStringRep.columns.map
    (imputedDataFrameStringRep.col(_).cast(DataTypes.DoubleType)): _*).persist()

    // Input  : DataFrame
    // Output : Map (String -> String)
    // Purpose: Creates a vector from the given feature list
    val assembler = new VectorAssembler()
      .setInputCols(featureList)
      .setOutputCol("features")

    // Train a RandomForest model.
    val rf = new RandomForestClassifier()
      .setLabelCol("Label")
      .setFeaturesCol("features")
      .setNumTrees(125)
      .setMaxDepth(10)

    // Chain indexers and forest in a Pipeline.
    val pipeline = new Pipeline()
      .setStages(Array(assembler, rf))
    // Train model. This also runs the indexers.
    val model = pipeline.fit(completeData)
    val rfModel = model.stages(1).asInstanceOf[RandomForestClassificationModel]
    //    rfModel.save("s3://com.project/finalTrainedModel")
    rfModel.save("/Users/Shetty/Desktop/MapReduce/Proj/Programs/Train/model")
  }
}