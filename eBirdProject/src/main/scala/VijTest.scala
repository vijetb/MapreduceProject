import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel, SVMModel, SVMWithSGD}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.tree.{DecisionTree, RandomForest}
import org.apache.spark.mllib.tree.model.{DecisionTreeModel, RandomForestModel}
import org.apache.spark.mllib.util.MLUtils

object VijTest {
  def main(args: Array[String]){

    val conf = new SparkConf()
      .setAppName("eBird Project")
      .setMaster("local")

    val sc = new SparkContext(conf)
    // Load and parse the data file.
    val data = MLUtils.loadLibSVMFile(sc, "input/sample_libsvm_data.txt")
    // Split the data into training and test sets (30% held out for testing)
    val splits = data.randomSplit(Array(0.7, 0.3))
    val (trainingData, testData) = (splits(0), splits(1))

//    // Train a DecisionTree model.
//    //  Empty categoricalFeaturesInfo indicates all features are continuous.
//    val numClasses = 2
//    val categoricalFeaturesInfo = Map[Int, Int]()
//    val impurity = "gini"
//    val maxDepth = 5
//    val maxBins = 32
//
//    val model = DecisionTree.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
//      impurity, maxDepth, maxBins)
//
//    // Evaluate model on test instances and compute test error
//    val labelAndPreds = testData.map { point =>
//      val prediction = model.predict(point.features)
//      (point.label, prediction)
//    }
//    val testErr = labelAndPreds.filter(r => r._1 != r._2).count().toDouble / testData.count()
//    println("Test Error = " + testErr)
//    println("Learned classification tree model:\n" + model.toDebugString)
//
//    // Save and load model
//    model.save(sc, "target/tmp/myDecisionTreeClassificationModel")
//    val sameModel = DecisionTreeModel.load(sc, "target/tmp/myDecisionTreeClassificationModel")
    //----------------------------------RANDOM FORESTS--------------------------------------
    // Train a RandomForest model.
    // Empty categoricalFeaturesInfo indicates all features are continuous.
//    val numClasses = 2
//    val categoricalFeaturesInfo = Map[Int, Int]()
//    val numTrees = 3 // Use more in practice.
//    val featureSubsetStrategy = "auto" // Let the algorithm choose.
//    val impurity = "gini"
//    val maxDepth = 4
//    val maxBins = 32
//
//    val model = RandomForest.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
//      numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)
//
//    // Evaluate model on test instances and compute test error
//    val labelAndPreds = testData.map { point =>
//      val prediction = model.predict(point.features)
//      (point.label, prediction)
//    }
//    val testErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / testData.count()
//    println("Test Error = " + testErr)
//    println("Learned classification forest model:\n" + model.toDebugString)
//
//    // Save and load model
//    model.save(sc, "target/tmp/myRandomForestClassificationModel")
//    val sameModel = RandomForestModel.load(sc, "target/tmp/myRandomForestClassificationModel")

    //------------NAIVE BAYES

//    val model = NaiveBayes.train(trainingData, lambda = 1.0, modelType = "multinomial")
//
//    val predictionAndLabel = testData.map(p => (model.predict(p.features), p.label))
//    val accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / testData.count()
//    println("Accuracy = " + accuracy)
//    // Save and load model
//    model.save(sc, "target/tmp/myNaiveBayesModel")
//    val sameModel = NaiveBayesModel.load(sc, "target/tmp/myNaiveBayesModel")

    //----------------- SVM
    // Run training algorithm to build the model
    val numIterations = 100
    val model = SVMWithSGD.train(trainingData, numIterations)
    // Clear the default threshold.
    model.clearThreshold()

    // Compute raw scores on the test set.
    val scoreAndLabels = testData.map { point =>
      val score = model.predict(point.features)
      (score, point.label)
    }

    // Get evaluation metrics.
    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    val auROC = metrics.areaUnderROC()

    println("Area under ROC = " + auROC)

    // Save and load model
    model.save(sc, "target/tmp/scalaSVMWithSGDModel")
    val sameModel = SVMModel.load(sc, "target/tmp/scalaSVMWithSGDModel")
  }
}
