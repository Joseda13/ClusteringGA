package es.us

import java.util.concurrent.ThreadLocalRandom

import es.us.MainGenerateDB.pairCombinationArrayInt

import scala.util.control.Breaks._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
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

    var features = 5      //Number of features (columns)
    var dummies = 2
    var tags = 5
    var K = 7        //Number of clusters
    var minimumPoints = 500    //Instances minimum per cluster
    var maximumPoints = 1000   //Instances maximum per cluster
    var desviation = 0.05f    //Standard deviation for the gaussian distribution
    val withTag = true   //True if the class have to be included

    val test = createDataBase(features, tags, K, desviation, dummies, minimumPoints, maximumPoints)

    sc.stop()
  }

  def createDataBase(features: Int, tags: Int, number_cluster: Int, desviation: Float, dummies: Int, minimumPoints: Int, maximumPoints: Int): Unit = {

    val spark = SparkSession.builder()
      .appName(s"CreateDataBase")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    //Create all permutations between the tags into the number of features
    val aux = Array.fill(features)(0 to tags-1).flatten.combinations(features).flatMap(_.permutations).toArray

//    println("tamanoy combinaciones: " + aux.length)

    val result = Array.ofDim[Int](number_cluster,features)
    var indexResult = 0

    for (arrayAux <- aux){

      val arrayAuxCombinations = pairCombinationArrayInt(arrayAux)

      if (!result.contains(arrayAux) && indexResult < number_cluster) {

        var contRepeat = 0

        for (indexRes <- result) {

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
          result.update(indexResult, arrayAux)
          indexResult += 1
        }
      }

    }

//    val testCombinations = spark.sparkContext.parallelize(aux).toDF()
//    testCombinations.show(30, false)
//
//    val testResult = spark.sparkContext.parallelize(result).toDF()
//    testResult.show(100, false)
//
//    println("Tamaño de todas las combinaciones: ")

    //Normalized the value of each tag between the range [0,1]
    val resultNormalized = result.map(v => v.map(value => ( value.toFloat / (tags-1)) ))

    //Add the cluster id of each features array normalized
    val resultClusterAndNormalized = for (cluster <- 0 to number_cluster-1) yield {
      (cluster, resultNormalized.apply(cluster))
    }

    //Create a RDD with the cluster id and the features array normalized
    val RDDDataBase = spark.sparkContext.parallelize(resultClusterAndNormalized)

//    val dataset = RDDDataBase.flatMapValues(x =>
//      for {i <- 0 until instances } yield {
//        features match {
//          case 2 => (getGaussian(x(0), desviation), getGaussian(x(1), desviation))
//          case 3 => (getGaussian(x(0), desviation), getGaussian(x(1), desviation), getGaussian(x(2), desviation))
//          case 4 => (getGaussian(x(0), desviation), getGaussian(x(1), desviation), getGaussian(x(2), desviation), getGaussian(x(3), desviation))
//          case 5 => (getGaussian(x(0), desviation), getGaussian(x(1), desviation), getGaussian(x(2), desviation), getGaussian(x(3), desviation), getGaussian(x(4), desviation))
//          case 6 => (getGaussian(x(0), desviation), getGaussian(x(1), desviation), getGaussian(x(2), desviation), getGaussian(x(3), desviation), getGaussian(x(4), desviation), getGaussian(x(5), desviation))
//          case 7 => (getGaussian(x(0), desviation), getGaussian(x(1), desviation), getGaussian(x(2), desviation), getGaussian(x(3), desviation), getGaussian(x(4), desviation), getGaussian(x(5), desviation), getGaussian(x(6), desviation))
//          case 8 => (getGaussian(x(0), desviation), getGaussian(x(1), desviation), getGaussian(x(2), desviation), getGaussian(x(3), desviation), getGaussian(x(4), desviation), getGaussian(x(5), desviation), getGaussian(x(6), desviation), getGaussian(x(7), desviation))
//          case 9 => (getGaussian(x(0), desviation), getGaussian(x(1), desviation), getGaussian(x(2), desviation), getGaussian(x(3), desviation), getGaussian(x(4), desviation), getGaussian(x(5), desviation), getGaussian(x(6), desviation), getGaussian(x(7), desviation), getGaussian(x(8), desviation))
//          case 10 => (getGaussian(x(0), desviation), getGaussian(x(1), desviation), getGaussian(x(2), desviation), getGaussian(x(3), desviation), getGaussian(x(4), desviation), getGaussian(x(5), desviation), getGaussian(x(6), desviation), getGaussian(x(7), desviation), getGaussian(x(8), desviation), getGaussian(x(9), desviation))
//          case 11 => (getGaussian(x(0), desviation), getGaussian(x(1), desviation), getGaussian(x(2), desviation), getGaussian(x(3), desviation), getGaussian(x(4), desviation), getGaussian(x(5), desviation), getGaussian(x(6), desviation), getGaussian(x(7), desviation), getGaussian(x(8), desviation), getGaussian(x(9), desviation), getGaussian(x(10), desviation))
//          case 12 => (getGaussian(x(0), desviation), getGaussian(x(1), desviation), getGaussian(x(2), desviation), getGaussian(x(3), desviation), getGaussian(x(4), desviation), getGaussian(x(5), desviation), getGaussian(x(6), desviation), getGaussian(x(7), desviation), getGaussian(x(8), desviation), getGaussian(x(9), desviation), getGaussian(x(10), desviation), getGaussian(x(11), desviation))
//          case 13 => (getGaussian(x(0), desviation), getGaussian(x(1), desviation), getGaussian(x(2), desviation), getGaussian(x(3), desviation), getGaussian(x(4), desviation), getGaussian(x(5), desviation), getGaussian(x(6), desviation), getGaussian(x(7), desviation), getGaussian(x(8), desviation), getGaussian(x(9), desviation), getGaussian(x(10), desviation), getGaussian(x(11), desviation), getGaussian(x(12), desviation))
//          case 14 => (getGaussian(x(0), desviation), getGaussian(x(1), desviation), getGaussian(x(2), desviation), getGaussian(x(3), desviation), getGaussian(x(4), desviation), getGaussian(x(5), desviation), getGaussian(x(6), desviation), getGaussian(x(7), desviation), getGaussian(x(8), desviation), getGaussian(x(9), desviation), getGaussian(x(10), desviation), getGaussian(x(11), desviation), getGaussian(x(12), desviation), getGaussian(x(13), desviation))
//          case 15 => (getGaussian(x(0), desviation), getGaussian(x(1), desviation), getGaussian(x(2), desviation), getGaussian(x(3), desviation), getGaussian(x(4), desviation), getGaussian(x(5), desviation), getGaussian(x(6), desviation), getGaussian(x(7), desviation), getGaussian(x(8), desviation), getGaussian(x(9), desviation), getGaussian(x(10), desviation), getGaussian(x(11), desviation), getGaussian(x(12), desviation), getGaussian(x(13), desviation), getGaussian(x(14), desviation))
//          case 16 => (getGaussian(x(0), desviation), getGaussian(x(1), desviation), getGaussian(x(2), desviation), getGaussian(x(3), desviation), getGaussian(x(4), desviation), getGaussian(x(5), desviation), getGaussian(x(6), desviation), getGaussian(x(7), desviation), getGaussian(x(8), desviation), getGaussian(x(9), desviation), getGaussian(x(10), desviation), getGaussian(x(11), desviation), getGaussian(x(12), desviation), getGaussian(x(13), desviation), getGaussian(x(14), desviation), getGaussian(x(15), desviation))
//          case 17 => (getGaussian(x(0), desviation), getGaussian(x(1), desviation), getGaussian(x(2), desviation), getGaussian(x(3), desviation), getGaussian(x(4), desviation), getGaussian(x(5), desviation), getGaussian(x(6), desviation), getGaussian(x(7), desviation), getGaussian(x(8), desviation), getGaussian(x(9), desviation), getGaussian(x(10), desviation), getGaussian(x(11), desviation), getGaussian(x(12), desviation), getGaussian(x(13), desviation), getGaussian(x(14), desviation), getGaussian(x(15), desviation), getGaussian(x(16), desviation))
//          case 18 => (getGaussian(x(0), desviation), getGaussian(x(1), desviation), getGaussian(x(2), desviation), getGaussian(x(3), desviation), getGaussian(x(4), desviation), getGaussian(x(5), desviation), getGaussian(x(6), desviation), getGaussian(x(7), desviation), getGaussian(x(8), desviation), getGaussian(x(9), desviation), getGaussian(x(10), desviation), getGaussian(x(11), desviation), getGaussian(x(12), desviation), getGaussian(x(13), desviation), getGaussian(x(14), desviation), getGaussian(x(15), desviation), getGaussian(x(16), desviation), getGaussian(x(17), desviation))
//          case 19 => (getGaussian(x(0), desviation), getGaussian(x(1), desviation), getGaussian(x(2), desviation), getGaussian(x(3), desviation), getGaussian(x(4), desviation), getGaussian(x(5), desviation), getGaussian(x(6), desviation), getGaussian(x(7), desviation), getGaussian(x(8), desviation), getGaussian(x(9), desviation), getGaussian(x(10), desviation), getGaussian(x(11), desviation), getGaussian(x(12), desviation), getGaussian(x(13), desviation), getGaussian(x(14), desviation), getGaussian(x(15), desviation), getGaussian(x(16), desviation), getGaussian(x(17), desviation), getGaussian(x(18), desviation))
//        }
//      }
//    )

    //Create the DataBase with the gaussian value in the features values and the dummies values
    val dataBase = RDDDataBase.flatMapValues { x =>
      val points = ThreadLocalRandom.current.nextInt(minimumPoints, maximumPoints)
      for {i <- 0 until points} yield {
        val arrayGaussian = for {indexGaussian <- 0 until features} yield {
          getGaussian(x(indexGaussian), desviation)
        }

        val arrayDummies = for {indexDummie <- 0 until dummies} yield {
          Math.random().toFloat
        }

        arrayGaussian.union(arrayDummies)
      }
    }

    //Save the DataBase
    dataBase.map(x => x._1 + "," + x._2.toString.replace("(", "").replace(")", "").replace("Vector", "").replace(" ",""))
      .coalesce(1, shuffle = true)
      .saveAsTextFile(s"C$number_cluster-V$features-D$dummies-I($minimumPoints-$maximumPoints)-${Utils.whatTimeIsIt()}")

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
