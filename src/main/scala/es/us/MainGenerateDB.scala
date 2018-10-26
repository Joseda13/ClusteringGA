package es.us

import java.util.concurrent.ThreadLocalRandom

import org.apache.spark.sql.functions.{col, max, min, monotonically_increasing_id, split}
import es.us.MainGenerateDB.pairCombinationArrayInt

import scala.util.control.Breaks._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import es.us.spark.mllib.Utils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

object MainGenerateDB {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)

    val conf = new SparkConf()
      .setAppName("Generate DataSet")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)

    var features = 3   //Number of features (columns)
    var dummies = 20
    var tags = 5
    var K = 7       //Number of clusters
    var minimumPoints = 500    //Instances minimum per cluster
    var maximumPoints = 1000   //Instances maximum per cluster
    var desviation = 0.03f   //Standard deviation for the gaussian distribution
    var destination = ""
    val withTag = true   //True if the class have to be included

    val test = createDataBase(features, tags, K, desviation, dummies, minimumPoints, maximumPoints, destination, 0)
//    val test = allCombinations(features, tags, destination)

    sc.stop()
  }

  def allCombinations(features: Int, tags: Int, destination: String): Unit ={

    val spark = SparkSession.builder()
      .appName(s"CreateDataBase")
      .master("local[*]")
      .config("spark.driver.maxResultSize", "0")
      .getOrCreate()

    import spark.implicits._

    var data = spark.read
      .option("header", "false")
      .option("inferSchema", "true")
      .option("delimiter", ",")
      .csv(s"B:\\N=6.txt").cache()

//    val aux0_final = data.map{row =>
//      Array(row.getInt(0), row.getInt(1), row.getInt(2), row.getInt(3), row.getInt(4), row.getInt(5), row.getInt(6), row.getInt(7), row.getInt(8),
//        row.getInt(9), 0)
//    }
//    val aux1_final = data.map{row =>
//      Array(row.getInt(0), row.getInt(1), row.getInt(2), row.getInt(3), row.getInt(4), row.getInt(5), row.getInt(6), row.getInt(7), row.getInt(8),
//        row.getInt(9), 1)
//    }
//    val aux2_final = data.map{row =>
//      Array(row.getInt(0), row.getInt(1), row.getInt(2), row.getInt(3), row.getInt(4), row.getInt(5), row.getInt(6), row.getInt(7), row.getInt(8),
//        row.getInt(9), 2)
//    }
//    val aux3_final = data.map{row =>
//      Array(row.getInt(0), row.getInt(1), row.getInt(2), row.getInt(3), row.getInt(4), row.getInt(5), row.getInt(6), row.getInt(7), row.getInt(8),
//        row.getInt(9), 3)
//    }
//    val aux4_final = data.map{row =>
//      Array(row.getInt(0), row.getInt(1), row.getInt(2), row.getInt(3), row.getInt(4), row.getInt(5), row.getInt(6), row.getInt(7), row.getInt(8),
//        row.getInt(9), 4)
//    }
//
//    val aux0_initial = data.map{row =>
//      Array(0, row.getInt(0), row.getInt(1), row.getInt(2), row.getInt(3), row.getInt(4), row.getInt(5), row.getInt(6), row.getInt(7), row.getInt(8),
//        row.getInt(9))
//    }
//    val aux1_initial = data.map{row =>
//      Array(1, row.getInt(0), row.getInt(1), row.getInt(2), row.getInt(3), row.getInt(4), row.getInt(5), row.getInt(6), row.getInt(7), row.getInt(8),
//        row.getInt(9))
//    }
//    val aux2_initial = data.map{row =>
//      Array(2, row.getInt(0), row.getInt(1), row.getInt(2), row.getInt(3), row.getInt(4), row.getInt(5), row.getInt(6), row.getInt(7), row.getInt(8),
//        row.getInt(9))
//    }
//    val aux3_initial = data.map{row =>
//      Array(3, row.getInt(0), row.getInt(1), row.getInt(2), row.getInt(3), row.getInt(4), row.getInt(5), row.getInt(6), row.getInt(7), row.getInt(8),
//        row.getInt(9))
//    }
//    val aux4_initial = data.map{row =>
//      Array(4, row.getInt(0), row.getInt(1), row.getInt(2), row.getInt(3), row.getInt(4), row.getInt(5), row.getInt(6), row.getInt(7), row.getInt(8),
//        row.getInt(9))
//    }

    val auxTotal = data.map{row =>
      Array(row.getInt(0), row.getInt(1), row.getInt(2), row.getInt(3), row.getInt(4))
    }

    val aux = Array.fill(2)(0 to tags-1).flatten.combinations(2).flatMap(_.permutations).toArray

    val testwtwtrt = auxTotal.collect().flatMap{
      row =>

        val auxArray = for (index <- aux) yield{
          val auxZip = row.union(index)
          auxZip
        }

        auxArray
    }

//    println("el numero: " + spark.sparkContext.parallelize(testwtwtrt).toDF().count())
//    val savedDataFrame = spark.sparkContext.parallelize(prueba2).toDF().cache()
    val prueba2 = Random.shuffle(testwtwtrt.toSeq).toArray

//    val testeret = auxTotal.collect().zip(prueba2).map(value => value._1.union(value._2))
//    spark.sparkContext.parallelize(testeret).toDF().show(25, false)

//    val auxTotal = aux0_final.union(aux0_initial).union(aux1_final).union(aux1_initial).union(aux2_final).union(aux2_initial)
//      .union(aux3_final).union(aux3_initial).union(aux4_final).union(aux4_initial)

//    val auxArrayF = data.map(row => Array(row.getInt(0), row.getInt(1), row.getInt(2), row.getInt(3),row.getInt(4),row.getInt(5)) )

    val testPer = spark.sparkContext.parallelize(prueba2).flatMap(row => row.permutations).collect()
//    val aux = Array.fill(features)(0 to tags-1).flatten.combinations(features).flatMap(_.permutations).toArray
//    val testAux = spark.sparkContext.parallelize(aux).toDF()
//    testAux.show(25, false)

//    println("Tamano : " + testPer.collect().length)
//    spark.sparkContext.parallelize(testPer.collect()).toDF().distinct().rdd.map(x => x.toString.replace("[", "").replace("]", "")
//      .replace("WrappedArray(", "").replace(")", "").replace(" ", ""))
//      .coalesce(1, shuffle = true)
//      .saveAsTextFile(s"V$features-T$tags" + Utils.whatTimeIsIt())
//    println("long: " + testPer.length)
//    auxArray.show(10, false)

//    val prueba = Random.shuffle(testeret.toSeq).toArray

    val index = ThreadLocalRandom.current.nextInt(0, testPer.length - 1)
    val elementoRandom = testPer.apply(index)
//    elementoRandom.updated(0,4)
//    elementoRandom.updated(1,4)
//    elementoRandom.updated(2,4)
//    elementoRandom.updated(3,4)

    val resultTotal = Array.ofDim[Int](30,features)

    for (in <- 0 to resultTotal.length - 1){
      resultTotal.update(in, elementoRandom)
    }

    var indexResult = 1

    for (arrayAux <- testPer){

      val arrayAuxCombinations = pairCombinationArrayInt(arrayAux)

      if (!resultTotal.contains(arrayAux)) {

        var contRepeat = 0

        for (indexRes <- resultTotal) {

          val arrayAuxResultCombinations = pairCombinationArrayInt(indexRes)

          val zipArray = arrayAuxResultCombinations.zip(arrayAuxCombinations)

          zipArray.map {
            x =>
              if (x._1 == x._2) {
                contRepeat += 1
              }
          }
        }

        if (contRepeat < 1) {
          resultTotal.update(indexResult, arrayAux)
          indexResult += 1
        }

      }
    }

    println(s"Different valid combinations number with $tags tags and $features features: " + (indexResult) )

    val testResultTotal = spark.sparkContext.parallelize(resultTotal).toDF().distinct()
//    testResultTotal.show(30, false)
    if (testResultTotal.count() < 25){
//      testResultTotal.show(30, false)
      allCombinations(features, tags, destination)
//      createDataBase(features, tags, number_cluster, desviation, dummies, minimumPoints, maximumPoints, destination, index + 1)
    }else {
      testResultTotal.show(30, false)
      testResultTotal.rdd.map(x => x.toString.replace("[", "").replace("]", "")
        .replace("WrappedArray(", "").replace(")", "").replace(" ", ""))
        .coalesce(1, shuffle = true)
        .saveAsTextFile(s"V$features-T$tags-$indexResult" + Utils.whatTimeIsIt())
      println("Save file...")
    }

  }

  def createDataBase(features: Int, tags: Int, number_cluster: Int, desviation: Float, dummies: Int, minimumPoints: Int, maximumPoints: Int, destination: String, index: Int): Unit = {

    println("*******************************")
    println("*******DATASET GENERATOR*******")
    println("*******************************")
    println("Configuration:")
    println("\tClusters: " + number_cluster)
    println("\tInstances per cluster between: " + minimumPoints + " - " + maximumPoints)
    println("\tClasses: " + tags)
    println("\tFeatures: " + features)
    println("\tSave directory: " + destination)
    println("Running...\n")

    val spark = SparkSession.builder()
      .appName(s"CreateDataBase")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

//    var data = spark.read
//      .option("header", "false")
//      .option("inferSchema", "true")
//      .option("delimiter", ",")
//      .csv(s"B:\\N_10.txt").distinct()

//    println("Hay diferentes: " + data.count())
//    val aux0 = data.map{row =>
//      Array(row.getInt(0), row.getInt(1), row.getInt(2), row.getInt(3), row.getInt(4), row.getInt(5), row.getInt(6), row.getInt(7), row.getInt(8)
//        , row.getInt(9)
//        , row.getInt(10), row.getInt(11), row.getInt(12), row.getInt(13), row.getInt(14), row.getInt(15), row.getInt(16), row.getInt(17)
//      ,row.getInt(18), row.getInt(19))
//      )
//    }.collect()

    //Create all permutations between the tags into the number of features
    val aux = Array.fill(features)(0 to tags-1).flatten.combinations(features).flatMap(_.permutations).toArray
//    val aux = Array.fill(2)(0 to tags-1).flatten.combinations(2).flatMap(_.permutations).toArray

//    val auxTotal = aux.map{row =>
//      (row.apply(0), row.apply(1))
//    }

//    val savedDataFrame = spark.sparkContext.parallelize(auxTotal.toSeq).toDF().cache()
//    savedDataFrame.show(30, false)
//    savedDataFrame.rdd.map(x => x.toString.replace("[", "").replace("]", "")
//      .replace("WrappedArray(", "").replace(")", "").replace(" ", ""))
//      .coalesce(1, shuffle = true)
//      .saveAsTextFile(s"V$features-T$tags" + Utils.whatTimeIsIt())
    val prueba = Random.shuffle(aux.toSeq)

//    val index = ThreadLocalRandom.current.nextInt(0, prueba.length - 1)
//    val elementoRandom = prueba.apply(index)

    val resultTotal = Array.ofDim[Int](30,features)

//    for (in <- 0 to resultTotal.length - 1){
//      resultTotal.update(in, elementoRandom)
//    }

//    var resultTotal = Array.empty[Array[Int]]
    var result = Array.ofDim[Int](number_cluster,features)
    var indexResult = 1

    for (arrayAux <- prueba){

      val arrayAuxCombinations = pairCombinationArrayInt(arrayAux)

      if (!resultTotal.contains(arrayAux)) {

        var contRepeat = 0

        for (indexRes <- resultTotal) {

          val arrayAuxResultCombinations = pairCombinationArrayInt(indexRes)

          val zipArray = arrayAuxResultCombinations.zip(arrayAuxCombinations)

          zipArray.map {
            x =>
              if (x._1 == x._2) {
                contRepeat += 1
              }
          }
        }

        if (contRepeat < 1) {
          resultTotal.update(indexResult, arrayAux)
          indexResult += 1
        }
      }

    }

    println(s"Different valid combinations number with $tags tags and $features features: " + (indexResult) )

    result = Random.shuffle(resultTotal.take(indexResult).toSeq).toArray.take(number_cluster)
//    val testResultTotal = spark.sparkContext.parallelize(resultTotal).toDF()
//    testResultTotal.show(30)
//    result = Random.shuffle(aux0.take(7).toSeq).toArray.take(number_cluster)

    val testResult = spark.sparkContext.parallelize(result).toDF()
    println("Combinations choosen:")
    testResult.show(number_cluster, false)

    //Normalized the value of each tag between the range [0,1]
    val resultNormalized = result.map(v => v.map(value => ( value.toFloat / (tags-1)) ))

    //Add the cluster id of each features array normalized
    val resultClusterAndNormalized = for (cluster <- 0 to number_cluster-1) yield {
      (cluster, resultNormalized.apply(cluster))
    }

    //Create a RDD with the cluster id and the features array normalized
    val RDDDataBase = spark.sparkContext.parallelize(resultClusterAndNormalized)

    //Create the DataBase with the gaussian value in the features values
    val dataBase = RDDDataBase.flatMap { x =>
      val points = ThreadLocalRandom.current.nextInt(minimumPoints, maximumPoints)
      val clusterNumber = x._1
      println(s"Number of points to the cluster $clusterNumber: " + points)

      for {indexPoint <- 0 until points} yield {

        val arrayGaussian = for {indexGaussian <- 0 until features} yield {
          var valueGaussian = getGaussian(x._2(indexGaussian), desviation)

          valueGaussian
        }

        (x._1, arrayGaussian)
      }
    }.cache()

    val dataHomotecia = dataBase.map{
      row =>

        val arrayFeautues = for (index <- 0 until features) yield {
          val value = row._2.apply(index)
          value
        }

        (arrayFeautues.toString().replace("Vector(", "").replace(")", "").replace(" ", ""))
    }.toDF().withColumn("temp", split(col("value"), "\\,"))
      .select((0 until features).map(i => col("temp").getItem(i).cast("Float").as(s"col$i")): _*).cache()

    var resultData = Seq((0.0f, 0.0f)).toDF("min", "max")

    for (index <- 0 until features) {

      val auxMin_Max = dataHomotecia.agg(min(s"col$index"), max(s"col$index"))
      resultData = resultData.union(auxMin_Max)
    }

    val resultHomotecia = resultData.select("min", "max").withColumn("index", monotonically_increasing_id())
      .filter(value => value.getLong(2) > 0).collect()

    val resultDataBase = dataBase.map{
      row =>

        val arrayFeautues = for (index <- 0 until features) yield {
          val testFilter = filterDatasetByIndex(resultHomotecia, index)
          val col_min = testFilter.getFloat(0)
          val col_max = testFilter.getFloat(1)

          val valueAux = (row._2.apply(index) - col_min) / (col_max - col_min)

          valueAux
        }

        val arrayDummies = for {indexDummie <- 0 until dummies} yield {
          Math.random().toFloat
        }

        (row._1, arrayFeautues.union(arrayDummies))

    }

    println("Saving DataBase ...\n")

    //Save the DataBase
    resultDataBase.map(x => x._1 + "," + x._2.toString.replace("(", "").replace(")", "").replace("Vector", "").replace(" ",""))
      .coalesce(1, shuffle = true)
      .saveAsTextFile(s"K$number_cluster-N$features-D$dummies-I($minimumPoints-$maximumPoints)-${Utils.whatTimeIsIt()}")

    println("DataBase saved!")
  }

  def filterDatasetByIndex (data: Array[Row], id: Long): Row = {
    val result = data.filter(row => row.getLong(2) == (8589934592l * (id + 1))).apply(0)

    result
  }

  def pairCombinationArrayInt (base: Array[Int]): Array[(Int,Int)] = {

    var result = new Array[(Int,Int)]((base.length * (base.length - 1)) / 2 )

    var cont = 0

    for (value <- base.indices){
      base.indices.map{
        case otherValue =>
          if (value != otherValue && value < otherValue){
            result.update(cont,(base.apply(value),base.apply(otherValue)))
            cont+=1
          }
      }
    }

    result
  }

  /**
    * It generates a random number in a gaussian distribution with the given mean and standard deviation
    *
    * @param average The start point
    * @param desv The last point
    * @example getGaussian(0.5, 0.05)
    */
  def getGaussian(average: Float, desv: Float): Float = {
    val rnd = new Random()
    rnd.nextGaussian().toFloat * desv + average
  }
}
