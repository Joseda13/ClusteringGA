package es.us.spark.mllib.clustering.validation

import java.util.Random

import es.us.spark.mllib.Utils
import org.apache.spark.internal.Logging
import org.apache.spark.ml.clustering.{BisectingKMeans, KMeans}
import org.apache.spark.mllib.linalg.Vectors
//import org.apache.spark.mllib.clustering.{KMeans}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * Created by Josem on 27/09/2017.
  */
object FeatureStatistics extends Logging {

  def getFitnessWithoutSpar(features: Array[Int]): Double = {
    val startTime = System.nanoTime

    val dataFile = "B:\\DataSets_Genetics\\dataset_101.csv"

    var testFile = Array("","")
    val bufferedSource = scala.io.Source.fromFile(dataFile)
    for (line <- bufferedSource.getLines) {
      val cols = line.split(",").map(_.trim)
      testFile = cols
    }
    bufferedSource.close

    logInfo("Genotype: " + features.foreach(println(_)))

    for (i <- 0 to features.length - 2){
      if (features(i) == 0){
        testFile = testFile.drop(i)
      }
    }

    val featureColumns = testFile.drop(features.length-1)
    val featureAssembler = new VectorAssembler().setInputCols(featureColumns).setOutputCol("features")
//    val df_kmeans = featureAssembler.transform(testFile).select("class", "features")
//
    val startClustering = System.nanoTime
    val clusteringResult = new BisectingKMeans()
      .setK(features(features.length-1))
      //      .setK(4)
      .setSeed(1L)
      .setMaxIter(100)
      .setFeaturesCol("features")

//    val model = clusteringResult.fit(df_kmeans)

    val timeClustering = (System.nanoTime - startClustering) / 1e9d
    logInfo("TIME TO CLUSTERING: " + timeClustering)

    val elapsed = (System.nanoTime - startTime) / 1e9d
    logInfo("TIME TO CALCULATE FITNESS: " + elapsed)

    0
  }

  /**
    * Calculate the Chi Square index.
    *
    * @param features Chromosome with the features and K clusters to the calculation of the Chi Square index.
    * @param pathToData String with the path to the DataSet.
    * @return Return Chi Square index.
    * @example getChiIndices(features, pathToData)
    */
  def getChiIndices(features: Array[Int], pathToData: String): (Double, Double) = {
    val K = features(features.length-1)
    val spark = SparkSession.builder()
      .appName(s"Featuring Clusters $K")
      .master("local[*]")
      .getOrCreate()

    //Set up the global variables
    val idIndex = -1
    val classIndex = 8
    val delimiter = ","

    //Load data
    val dataRead = spark.read
      .option("header", "false")
      .option("inferSchema", "true")
      .option("delimiter", delimiter)
      .csv(pathToData)
//      .repartition(1)
      .cache()

    //Delete the ID column and the class column
    var data = if (idIndex != -1) {
      dataRead.drop(s"_c$idIndex")
        .withColumnRenamed(dataRead.columns(classIndex), "class")
    } else {
      dataRead.withColumnRenamed(dataRead.columns(classIndex), "class")
    }

    //If the gen to the chromosome if == 0, then delete its column to the DataSet
    for (i <- 0 to features.length - 2){
      if (features(i) == 0){
        data = data.drop(s"_c$i")
      }
    }

    val featureColumns = data.drop("class").columns
    val featureAssembler = new VectorAssembler().setInputCols(featureColumns).setOutputCol("features")
    val df_kmeans = featureAssembler.transform(data).select("class", "features")

    //Create the clusters using the K-Means algorithm to Spark
    val startClustering = System.nanoTime

//    val clusteringResult = new BisectingKMeans()
    val clusteringResult = new KMeans()
      .setK(K)
      .setSeed(1L)
      .setMaxIter(100)
      .setFeaturesCol("features")

    val model = clusteringResult.fit(df_kmeans)

    val timeClustering = (System.nanoTime - startClustering) / 1e9d
    logInfo("TIME TO CLUSTERING: " + timeClustering)

    var predictionResult = model.transform(df_kmeans).select("class", "prediction")

    predictionResult = predictionResult.withColumn("prediction", predictionResult("prediction").cast(DoubleType))

    val chi = getTotalChiCross(List("class"), predictionResult, features(features.length-1), "B:\\DataSets_Genetics" + Utils.whatTimeIsIt())
      .replace("(", "").replace(")", "").split(",")

    spark.stop()

    (chi(0).toDouble, chi(1).toDouble)
  }

  /**
    * Calculate the fitness to a chromosome.
    *
    * @param features Chromosome with the features and K clusters to the calculation of the Chi Square index.
    * @param pathToData String with the path to the DataSet.
    * @return Return the fitness to a chromosome.
    * @example getFitnessChi(features, pathToData)
    */
  def getFitnessChi(features: Array[Int], pathToData: String): Double = {

    val startTime = System.nanoTime

    val chi = getChiIndices(features, pathToData)
    val res = (chi._1 - chi._2) // chi(0) - chi(1) = negative, chi(1) - chi(0) = positive

    val elapsed = (System.nanoTime - startTime) / 1e9d
    logInfo("TIME TO CALCULATE FITNESS: " + elapsed)

    val divided = math.max(chi._1,chi._2)
//    val divided = (features(features.length-1) - 1)*(df_kmeans.select("class").distinct().count() - 1)
    val result = Math.abs(res/divided)

    result
  }


  def getVarianceMeanByColumns(data: DataFrame): Double = {
    val columneo = data.columns.drop(0)

    columneo.map { colName =>

      val dfRDD = data.select(col(colName)).rdd.map(_ (0).asInstanceOf[Int])

      val total = dfRDD.count()
      val mean = dfRDD.mean()

      dfRDD.map(x => (x - mean) * (x - mean)).sum() / total
    }.sum / columneo.length

  }

  /**
    * Back ticks can't exist in DataFrame column names, therefore drop them. To be able to accept special keywords and `.`, wrap the column names in ``.
    *
    * @param name The name of the column which is going to be cleaned
    * @example getSquaredChi("1.0")
    * @return `1.0`
    */

  def cleanColumnName(name: String): String = {
    s"`$name`"
  }

  /**
    * Calculate the Squared Chi statistic
    *
    * @param data        DataFrame to which we are going to calculate the chi square
    * @param numClusters The number of clusters
    * @return Square Chi
    * @example getSquaredChi(data, 3)
    */
  def getSquaredChi(data: DataFrame, numClusters: Int): Double = {

    val dropedData = data.drop("prediction", "prediction_class", "class_prediction")

    val columnsNames = dropedData.columns

    val dfwithSum = dropedData.withColumn("sum", columnsNames.map(cleanColumnName(_)).map(col)
      .reduce((c1, c2) => c1 + c2)).cache()

    val spark = SparkSession.builder().getOrCreate()

    val totalDF = getTotal(dfwithSum)
    val bc_totalDF = spark.sparkContext.broadcast(totalDF)

    val totalColumn = getTotalColumn(dfwithSum)
    val bc_totalColumn = spark.sparkContext.broadcast(totalColumn)

    import spark.implicits._

    val res = dfwithSum.map { this_row =>
      val totalRow = getTotalRow(this_row)

      val x2 = for (i <- 0 until this_row.size) yield {

        val realValue = this_row.getDouble(i)

        val expected = (totalRow * bc_totalColumn.value.apply(i)) / bc_totalDF.value

        val x2aux = ((expected - realValue) * (expected - realValue)) / expected

        //println(x2aux)
        x2aux
      }

      x2.sum
    }.reduce(_ + _)

    dfwithSum.unpersist()

    res / (numClusters - 1)

  }

  /**
    * Return the value of the last column of the row
    *
    * @param rowValue Row
    * @example getTotalRow(rowValue)
    */
  def getTotalRow(rowValue: Row): Double = {
    rowValue.getDouble(rowValue.size - 1)
  }

  /**
    * Return an array with the sum of the columns of a DataFrame
    *
    * @param data DataFrame which is going to be calculated their sum of columns
    * @example getTotalColumn(data)
    */
  def getTotalColumn(data: DataFrame): Array[Double] = {

    val columnNames = data.columns

    columnNames.map { colName =>
      data.select(sum(cleanColumnName(colName)))
        .first()
        .getDouble(0)
    }

  }

  /**
    * Return the sum of the values of the column "sum"
    *
    * @param data DataFrame which is going to be calculated their sum s
    * @example getTotal(data)
    */
  def getTotal(data: DataFrame): Double = {

    data.select("sum")
      .rdd.map(_ (0).asInstanceOf[Double])
      .reduce(_ + _)


  }

  /**
    * Return a tuple with the values of the chi square of the contingencies tables
    *
    * @param dataRow     DataFrame with the contingency table by rows
    * @param dataColumn  DataFrame with the contingency table by columns
    * @param numClusters Total umber of clusters
    * @example calculateTotalChi(dataRow, dataColumn, 3)
    */
  def calculateTotalChi(dataRow: DataFrame, dataColumn: DataFrame, numClusters: Int): (Double, Double) = {
//    println(s"Calculating chi square by cluster..")
    val rowsChi = getSquaredChi(dataRow, numClusters)
//    println("Done!")

//    println(s"Calculating chi square by class..")
    val columnsChi = getSquaredChi(dataColumn, numClusters)
//    println("Done!")

//    println(s"Chi Result: $rowsChi + $columnsChi")
    (rowsChi.toDouble, columnsChi.toDouble)
  }

  /**
    * Return a String with the values of chi square of both contingency tables separated by an "+" symbol
    *
    * @param featureNameList List of feature names of the data
    * @param dfResults       DataFrame with "prediction" column and the columns of features
    * @param numClusters     Total umber of clusters
    * @param destino         Destiny path for the results
    * @example getTotalChiCross(featureNameList, dfResults, 3, "/tmp/testing")
    * @return 41.231 + 81.622
    */
  def getTotalChiCross(featureNameList: List[String], dfResults: DataFrame, numClusters: Int, destino: String): String = {

    featureNameList.map { featureName =>

      val dfResultsRenamed = dfResults.withColumnRenamed(featureName, "class")

      val dfContingencies = Feature.getContingencies(dfResultsRenamed)

      val dfByCluster = dfContingencies._1
      val dfByFeature = dfContingencies._2

      println("Crosstab by cluster")
      dfByCluster.show()
      println(s"Saving crosstab by cluster into $destino-DFClusters-$numClusters..")
      dfByCluster.repartition(1).write
        .option("header", "true")
        .option("delimiter", "\t")
        .csv(s"$destino/-DFClusters-$numClusters")
      println(s"Done")

      println("Crosstab by class")
      dfByFeature.show()
      println(s"Saving crosstab by class into $destino-DFFeatures-$numClusters..")
      dfByFeature.repartition(1).write
        .option("header", "true")
        .option("delimiter", "\t")
        .csv(s"$destino/-DFFeatures-$numClusters")
      println(s"Done")

      calculateTotalChi(dfByCluster, dfByFeature, numClusters).toString()

    }.reduce(_ + _)

  }

  def getConfusionMatrix(featureNameList: List[String], dfResults: DataFrame, numClusters: Int, destino: String): Unit = {

    featureNameList.map { featureName =>

      val dfResultsRenamed = dfResults.withColumnRenamed(featureName, "class")

      val dfContingencies = Feature.getContingencies(dfResultsRenamed)

      val dfByCluster = dfContingencies._1
      val dfByFeature = dfContingencies._2

      println("Crosstab by cluster")
      dfByCluster.show()
      println(s"Saving crosstab by cluster into $destino-DFClusters-$numClusters..")
      dfByCluster.repartition(1).write
        .option("header", "true")
        .option("delimiter", "\t")
        .csv(s"$destino/-DFClusters-$numClusters")
      println(s"Done")

      println("Crosstab by class")
      dfByFeature.show()
      println(s"Saving crosstab by class into $destino-DFFeatures-$numClusters..")
      dfByFeature.repartition(1).write
        .option("header", "true")
        .option("delimiter", "\t")
        .csv(s"$destino/-DFFeatures-$numClusters")
      println(s"Done")

    }

  }

  /*
    /**
      * Calculate the Square Chi statistic taking the columns as the total
      *
      * @param data DataFrame to which we are going to calculate the square chi
      * @return Square Chi by columns
      * @example getChiByColums(data)
      */
    def getChiByColums(data: DataFrame): Double = {

      val dropedData = data.drop("cluster")

      val columneo = dropedData.columns

      val celdas = columneo.length * dropedData.count()

      columneo.map { colName =>

        val dfRDD = dropedData.select(col(colName)).rdd.map(_ (0).asInstanceOf[Double])

        val total = 1.0 / dfRDD.count()
        dfRDD.map(x => (total - x) * (total - x)).reduce(_ + _)

      }.sum / celdas
    }
    */
  /*
    /**
      * Calculate the Square Chi statistic taking the rows as the total
      *
      * @param data DataFrame to which we are going to calculate the square chi
      * @return Square Chi by columns
      * @example getChiByRows(data)
      */
    def getChiByRows(data: DataFrame): Double = {

      val dropedData = data.drop("cluster")

      val columneo = dropedData.columns
      val totalColumnas = columneo.length

      val celdas = columneo.length * dropedData.count()

      columneo.map { colName =>

        val dfRDD = dropedData.select(col(colName)).rdd.map(_ (0).asInstanceOf[Double])

        val total = 1.0 / totalColumnas
        dfRDD.map(x => (total - x) * (total - x)).reduce(_ + _)

      }.sum / celdas
    }
  */


  /*
    def getTotalChi(featureNameList: List[String], delimiter: String, dfFeatures: DataFrame, dfJoin: DataFrame, numClusters: Int): String = {

      featureNameList.map { featureName =>
        val dfByCluster = Feature.getResultsByCluster(featureName, dfFeatures, dfJoin, numClusters)
        val dfByFeature = Feature.getResultsByFeature(featureName, dfFeatures, dfJoin, numClusters)

        println("dfByCluster")
        dfByCluster.show()

        println("dfByFeature")
        dfByFeature.show()

        calculateTotalChi(dfByCluster, dfByFeature, numClusters).toString()

      }.reduce(_ + _)

    }
    */
  /*
    def getTotalChi(featureNameList: List[String], delimiter: String, dfFeatures: DataFrame, dfJoin: DataFrame, numClusters: Int, destino: String): String = {

      featureNameList.map { featureName =>
        val dfByCluster = Feature.getResultsByCluster(featureName, dfFeatures, dfJoin, numClusters)
        val dfByFeature = Feature.getResultsByFeature(featureName, dfFeatures, dfJoin, numClusters)

        println("dfByCluster")
        dfByCluster.show()
        dfByCluster.repartition(1).write
          .option("header", "true")
          .option("delimiter", "\t")
          .csv(s"$destino-DFClusters-$numClusters")
        println("dfByFeature")
        dfByFeature.show()
        dfByFeature.repartition(1).write
          .option("header", "true")
          .option("delimiter", "\t")
          .csv(s"$destino-DFFeatures-$numClusters")

        calculateTotalChi(dfByCluster, dfByFeature, numClusters).toString()

      }.reduce(_ + _)

    }
  */


  /*
    /**
      * Calculate the Squared Chi statistic of a dataframe kmeans result
      *
      * @param dfClusteringResult DataFrame to which we are going to calculate the square chi
      * @example giveMeSquaredChi(dfClusteringResult,5)
      */
    def giveMeSquaredChi(dfClusteringResult: DataFrame, numClusters: Int): String = {

      val dfByCluster = Feature.getResultsByCluster(dfClusteringResult, numClusters)
      val dfByFeature = Feature.getResultsByFeature(dfClusteringResult, numClusters)

      calculateTotalChi(dfByCluster, dfByFeature, numClusters).toString()

    }
  */
  /*
    def calculateMatrixChi(dataRow: DataFrame, dataColum: DataFrame): (Double, Double) = {
      val v1 = getChiByRows(dataRow)
      val v2 = getChiByColums(dataColum)

      logInfo(s"RowChi: $v1\tColumnsChi: $v2")
      (v1.toDouble, v2.toDouble)

    }
  */
  /*
  def getMatrixChi(featureNameList: List[String], dfFeatures: DataFrame, dfJoin: DataFrame, numClusters: Int): DataFrame = {

    val res = featureNameList.map { featureName =>
      val dfByCluster = Feature.getResultsByCluster(featureName, dfFeatures, dfJoin, numClusters)
      val dfByFeature = Feature.getResultsByFeature(featureName, dfFeatures, dfJoin, numClusters)

      val chiRes = calculateMatrixChi(dfByCluster, dfByFeature)

      (featureName, chiRes._1, chiRes._2)

    }

    val spark = SparkSession.builder().getOrCreate()

    val columnaRDD = spark.sparkContext.parallelize(res)
    val filas = columnaRDD.map(Row.fromTuple(_))

    val schema = Array(
      StructField("feature", StringType, true),
      StructField("byCluster", DoubleType, true),
      StructField("byFeature", DoubleType, true))


    val customSchema = StructType(schema)

    spark.createDataFrame(filas, customSchema)

  }
*/
  /*
  def saveMatrixChi(featureNameList: List[String], dfFeatures: DataFrame, dfJoin: DataFrame, numClusters: Int): (Int, DataFrame) = {

    val dfResult = getMatrixChi(featureNameList, dfFeatures, dfJoin, numClusters)

    val momentum = Utils.whatTimeIsIt()
    val fileName = s"$momentum C$numClusters"

    dfResult.repartition(1).write
      .option("header", "true")
      .option("delimiter", "\t")
      .csv(fileName)

    (numClusters, dfResult)

  }
*/
}