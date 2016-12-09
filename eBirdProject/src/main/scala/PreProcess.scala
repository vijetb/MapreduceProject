import org.apache.spark.mllib.feature.Normalizer
import org.apache.spark.ml.feature.StandardScaler
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.ml.feature._
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.functions._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.MaxAbsScaler

import org.apache.spark.sql.types.{DataTypes, StructType}
object PreProcess {
  def main(args: Array[String]){
    val conf = new SparkConf()
      .setAppName("eBird Project")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val rawData = sc.textFile("input/labeledsmall.csv")

    val rawDataWithoutHeader =  rawData.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
        val preprocessedData = rawDataWithoutHeader.map(data => {
          val values = data.split(",")
          (if(values(26).charAt(0)!='0') 1.0.toString else 0.0.toString, // target value
           values(2), //Lat - no change //
           values(3), //Long - no change //
           values(5), //Month - no change //
           values(6), //day - no change
           values(7), //time - no change
           if(values(955).charAt(0)=='?') null else values(955),  // POP 6
           if(values(956).charAt(0)=='?') null else values(956),  // Housing Density
           if(values(957).charAt(0)=='?') null else values(957),  // Housing Vacent
           if(values(963).charAt(0)=='?') null else values(963),  // Avg Temp
           if(values(964).charAt(0)=='?') null else values(964),  // Min Temp
           if(values(965).charAt(0)=='?') null else values(965),  // Max  Temp
           if(values(966).charAt(0)=='?') null else values(966),  // Precipitation
           if(values(967).charAt(0)=='?') "0" else values(967)    // CAUS_SNOW - if ? make it 0
          )})

    val colNames = Array("Label","LAT", "LONG","MONTH","DAY","TIME","POP00_SQMI","HOUSING_DENSITY","HOUSING_PERCENT_VACANT","CAUS_TEMP_AVG","CAUS_TEMP_MIN","CAUS_TEMP_MAX","CAUS_PREC", "CAUS_SNOW")

    val colRequiredToBeImputed = Array("POP00_SQMI","HOUSING_DENSITY","HOUSING_PERCENT_VACANT","CAUS_TEMP_AVG","CAUS_TEMP_MIN","CAUS_TEMP_MAX","CAUS_PREC")

    val categoricalCol = Array("CAUS_TEMP_AVG","CAUS_TEMP_MIN","CAUS_TEMP_MAX","CAUS_PREC")

    val featureList = Array("LAT", "LONG","MONTH","DAY","TIME","POP00_SQMI","HOUSING_DENSITY","HOUSING_PERCENT_VACANT","CAUS_TEMP_AVG","CAUS_TEMP_MIN","CAUS_TEMP_MAX","CAUS_PREC", "CAUS_SNOW")

    import sqlContext.implicits._
    val df = preprocessedData.toDF(colNames: _*)


    val imputer = df.select(colRequiredToBeImputed.map(avg(_)):_*).toDF(colRequiredToBeImputed: _*)
    val imputerMap = imputer.columns.zip(imputer.first().toSeq).
      map(a =>
        if (categoricalCol contains  a._1)
          (a._1 -> a._2.toString.toDouble.ceil.toString)
        else
          (a._1 -> a._2)).toMap
    val imputedDataFrameStringRep = df.na.fill(imputerMap)
    val completeData= imputedDataFrameStringRep.select(imputedDataFrameStringRep.columns.map
    (imputedDataFrameStringRep.col(_).cast(DataTypes.DoubleType)) : _*)
    val assembler = new VectorAssembler()
      .setInputCols(featureList)
      .setOutputCol("features")
    val output = assembler.transform(completeData)
    val scaler = new StandardScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")
      .setWithStd(true)
      .setWithMean(false)
    // Compute summary statistics and generate MaxAbsScalerModel
    val scalerModel = scaler.fit(output)
    val scaledData = scalerModel.transform(output)


    //
//    output.printSchema
//
//
//    val featureIndexer = new VectorIndexer()
//      .setInputCol("features")
//      .setOutputCol("indexedFeatures")
//      .setMaxCategories(100)
//
//    val categoricalFeaturesTransformed = featureIndexer.fit(output).transform(output)
//
    val Array(trainingData, testData) = scaledData.randomSplit(Array(0.8, 0.2))
//
    // Train a RandomForest model.
    val rf = new RandomForestClassifier()
      .setLabelCol("Label")
      .setFeaturesCol("scaledFeatures")
      .setNumTrees(100)

    // Chain indexers and forest in a Pipeline.
    val pipeline = new Pipeline()
      .setStages(Array(rf))

    // Train model. This also runs the indexers.
    val model = pipeline.fit(trainingData)

    // Make predictions.
    val predictions = model.transform(testData)
    // Select (prediction, true label) and compute test error.
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("Label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val accuracy = evaluator.evaluate(predictions)
    println("Test Error = " + (1.0 - accuracy))

    val rfModel = model.stages(0).asInstanceOf[RandomForestClassificationModel]
    println("Learned classification forest model:\n" + rfModel.toDebugString)
  }
}
