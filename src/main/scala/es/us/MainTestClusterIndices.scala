package es.us

import es.us.spark.mllib.Utils
import es.us.spark.mllib.clustering.validation.Indices
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.sql.SparkSession

object MainTestClusterIndices {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName(s"Datasets")
      .master("local[*]")
      .getOrCreate()

    var numVariables = 0
    var numCluster = 0
    var numPoints = 0
    var dataFile = ""

    val delimiter = ","

    val minimunCluster = 2
    val maximumCluster = 6
    val minimunVariable = 3
    val maximumVariable = 10
    val limitNumber = 5

    var arguments = List(Array[String]())

    for (k <- minimunCluster to maximumCluster){
      for (nv <- minimunVariable to maximumVariable){
        val auxList = Array[String](s"$k", s"$nv")
        arguments = auxList :: arguments
      }
    }

    arguments = arguments.take(arguments.length - 1).reverse

    for (i <- 1 to limitNumber) {

      val result = for (data <- arguments) yield {
        numCluster = data.apply(0).toInt
        numVariables = data.apply(1).toInt
        numPoints = 1200 / numCluster

        dataFile = s"B:\\DataSets_Internos\\C$numCluster-D$numVariables-I$numPoints" + s"_$i"

        val dataRead = spark.read
          .option("header", "false")
          .option("inferSchema", "true")
          .option("delimiter", delimiter)
          .csv(dataFile)
          .cache()


        val columsDataSet = dataRead.columns.tail
        val dataRDD = dataRead.rdd.map { r =>

          val vectorValues = for (co <- columsDataSet) yield{
            r.getDouble(co.charAt(2).toInt - 48)
          }

          val auxVector = Vectors.dense(vectorValues)

          (r.getInt(0), auxVector)
        }.groupByKey()

        println("*** K = " + numCluster + " ***")
        println("*** NV = " + numVariables + "***")
        println("Executing Indices")
        val siloutes = Indices.getSilhouette(dataRDD.collect().toList)
        val dunns = Indices.getDunn(dataRDD.collect().toList)
        println("VALUES:")
        println("\tSilhouette (b): " + siloutes._1)
        println("\tSilhouette (a): " + siloutes._2)
        println("\tSilhouette: " + siloutes._3)
        println("\tDunn (inter): " + dunns._1)
        println("\tDunn (intra): " + dunns._2)
        println("\tDunn: " + dunns._3)
        println("\n")

        (s"$numCluster-$numVariables", siloutes, dunns)

      }

      val stringRdd = spark.sparkContext.parallelize(result)

      stringRdd.repartition(1)
        .map(_.toString().replace("(", "").replace(")", ""))
        .saveAsTextFile(s"-Results-$i-" + Utils.whatTimeIsIt())
    }

    spark.stop()

  }

  def calculateMean(vectors: Iterable[Vector]): Vector = {

    val vectorsCalculateMean = vectors.map(v => v.toArray.map(d => (d/vectors.size)))

    val sumArray = new Array[Double](vectorsCalculateMean.head.size)
    val auxSumArray = vectorsCalculateMean.map{
      case va =>
        var a = 0
        while (a < va.size){
          sumArray(a) += va.apply(a)
          a += 1
        }
        sumArray
    }.head

    Vectors.dense(auxSumArray)
  }
}
