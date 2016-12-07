import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel, SVMModel, SVMWithSGD}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.tree.{DecisionTree, RandomForest}
import org.apache.spark.mllib.tree.model.{DecisionTreeModel, RandomForestModel}
import org.apache.spark.mllib.util.MLUtils

object EnsembleLearning {
  def main(args: Array[String]){

    val conf = new SparkConf()
      .setAppName("eBird Project")
      .setMaster("local")

    val sc = new SparkContext(conf)
//    val rawData = MLUtils.loadLibSVMFile(sc,"input/norm_test")
    val rawData = sc.textFile("input/norm_test")

    //format the training data
    val formattedTrainData = rawData.map(x => {
      println(x)
      val values = x.toString().split(",")
      LabeledPoint(values(4).toDouble,Vectors.dense(values(0).toDouble,values(1).toDouble,values(2).toDouble,values(3).toDouble))
    })
    // format the testing data
    val testData = sc.textFile("input/test_data")
    val formattedTestData = testData.map(x => {
      val values = x.toString().split(",")
      (values(0),Vectors.dense(values(1).toDouble,values(2).toDouble,values(3).toDouble,values(4).toDouble))
    })

    // TRAINING-DECISION-TREE
    val numClasses = 2
    val categoricalFeaturesInfo = Map[Int, Int]()
    val impurity = "gini"
    val maxDepth = 5
    val maxBins = 32
    val modelDecisionTree = DecisionTree.trainClassifier(formattedTrainData, numClasses, categoricalFeaturesInfo,impurity, maxDepth, maxBins)

    // RANDOM-FOREST
    val numClassesRF = 2
    val categoricalFeaturesInfoRF = Map[Int, Int]()
    val numTreesRF = 101 // Use more in practice.
    val featureSubsetStrategyRF = "auto" // Let the algorithm choose.
    val impurityRF = "gini"
    val maxDepthRF = 4
    val maxBinsRF = 32

    val modelRF = RandomForest.trainClassifier(formattedTrainData, numClassesRF, categoricalFeaturesInfoRF,
      numTreesRF, featureSubsetStrategyRF, impurityRF, maxDepthRF, maxBinsRF)

    // NAIVE-BAYES
    val modelNB = NaiveBayes.train(formattedTrainData, lambda = 1.0, modelType = "multinomial")

    // SVM
    val modelSVM = SVMWithSGD.train(formattedTrainData, 100) //100 - no of iterations

    //Testing
    val predictedResults  = formattedTestData.map(dataPoint => {
      val dtValue = modelDecisionTree.predict(dataPoint._2)
      val rfValue = modelDecisionTree.predict(dataPoint._2)
      val nbValue = modelDecisionTree.predict(dataPoint._2)
      val svmValue = modelDecisionTree.predict(dataPoint._2)

      var vote = 0;
      if(dtValue==1.0) vote=vote+1
      if(rfValue==1.0) vote=vote+1
      if(nbValue==1.0) vote=vote+1
      if(svmValue==1.0) vote=vote+1
      if(vote>=2.0){
        (dataPoint._1,1)
      }else{
        (dataPoint._1,0)
      }
    })
    //dump the results
    predictedResults.coalesce(1,true).saveAsTextFile("output")

    // save the models
    modelDecisionTree.save(sc, "target/learntModel/decisionTree")
    modelRF.save(sc, "target/learntModel/RandomForest")
    modelNB.save(sc, "target/learntModel/NaiveBayes")
    modelSVM.save(sc, "target/learntModel/svm")



    //MODEL-1: Decision Tree
//        val numClasses = 2
//        val categoricalFeaturesInfo = Map[Int, Int]()
//        val impurity = "gini"
//        val maxDepth = 5
//        val maxBins = 32
//
//
//        val model = DecisionTree.trainClassifier(formattedData, numClasses, categoricalFeaturesInfo,impurity, maxDepth, maxBins)
//
//
//    val testValue = model.predict(formattedTestData)
//        println("Testing")
//        print(testValue)
//      testValue.map(x=>{
//        println(x+" Testing")
//        x
//      })
//    println(testValue.count())
//    testValue.coalesce(1,true).saveAsTextFile("output")

    // MODEL-2:


    // Train a RandomForest model.
//    // Empty categoricalFeaturesInfo indicates all features are continuous.
//        val numClassesRF = 2
//        val categoricalFeaturesInfoRF = Map[Int, Int]()
//        val numTreesRF = 101 // Use more in practice.
//        val featureSubsetStrategyRF = "auto" // Let the algorithm choose.
//        val impurityRF = "gini"
//        val maxDepthRF = 4
//        val maxBinsRF = 32
//
//        val modelRF = RandomForest.trainClassifier(formattedTrainData, numClassesRF, categoricalFeaturesInfoRF,
//          numTreesRF, featureSubsetStrategyRF, impurityRF, maxDepthRF, maxBinsRF)
//
//
//    val testValue = modelRF.predict(formattedTestData)
//            println("Testing")
//            print(testValue)
//          testValue.map(x=>{
//            println(x+" Testing")
//            x
//          })
//        println(testValue.count())
//        testValue.coalesce(1,true).saveAsTextFile("output")
//
//        // Save and load model
//        modelRF.save(sc, "target/tmp/myRandomForestClassificationModel")
//        val sameModel = RandomForestModel.load(sc, "target/tmp/myRandomForestClassificationModel")

//        // Evaluate model on test instances and compute test error
//        val labelAndPreds = testData.map { point =>
//          val prediction = model.predict(point.features)
//          (point.label, prediction)
//        }
//        val testErr = labelAndPreds.filter(r => r._1 != r._2).count().toDouble / testData.count()
//        println("Test Error = " + testErr)
//        println("Learned classification tree model:\n" + model.toDebugString)

        // Save and load model
//        model.save(sc, "target/tmp/myDecisionTreeClassificationModel")
//        val sameModel = DecisionTreeModel.load(sc, "target/tmp/myDecisionTreeClassificationModel")

  }
}
