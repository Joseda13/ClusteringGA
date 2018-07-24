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

    var numVariables = 10
    var numCluster = 2
    var numPoints = 1200/numCluster

    val arguments = List(
      Array[String]("2","3"),
      Array[String]("2","4"),
      Array[String]("2","5"),
      Array[String]("2","6"),
      Array[String]("2","7"),
      Array[String]("2","8"),
      Array[String]("2","9"),
      Array[String]("2","10"),
      Array[String]("3","3"),
      Array[String]("3","4"),
      Array[String]("3","5"),
      Array[String]("3","6"),
      Array[String]("3","7"),
      Array[String]("3","8"),
      Array[String]("3","9"),
      Array[String]("3","10"),
      Array[String]("4","3"),
      Array[String]("4","4"),
      Array[String]("4","5"),
      Array[String]("4","6"),
      Array[String]("4","7"),
      Array[String]("4","8"),
      Array[String]("4","9"),
      Array[String]("4","10"),
      Array[String]("5","3"),
      Array[String]("5","4"),
      Array[String]("5","5"),
      Array[String]("5","6"),
      Array[String]("5","7"),
      Array[String]("5","8"),
      Array[String]("5","9"),
      Array[String]("5","10"),
      Array[String]("6","3"),
      Array[String]("6","4"),
      Array[String]("6","5"),
      Array[String]("6","6"),
      Array[String]("6","7"),
      Array[String]("6","8"),
      Array[String]("6","9"),
      Array[String]("6","10")
    )

    var dataFile = ""
    val delimiter = ","
    val limitNumber = 3

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

        val auxRDD = numVariables match {
          case 3 =>
            dataRead.rdd.map(r => (r.getInt(0),
              Vectors.dense(r.getDouble(1)
                , r.getDouble(2)
                , r.getDouble(3)
              ))).groupByKey()
          case 4 =>
            dataRead.rdd.map(r => (r.getInt(0),
              Vectors.dense(r.getDouble(1)
                , r.getDouble(2)
                , r.getDouble(3)
                , r.getDouble(4)
              ))).groupByKey()
          case 5 =>
            dataRead.rdd.map(r => (r.getInt(0),
              Vectors.dense(r.getDouble(1)
                , r.getDouble(2)
                , r.getDouble(3)
                , r.getDouble(4)
                , r.getDouble(5)
              ))).groupByKey()
          case 6 =>
            dataRead.rdd.map(r => (r.getInt(0),
              Vectors.dense(r.getDouble(1)
                , r.getDouble(2)
                , r.getDouble(3)
                , r.getDouble(4)
                , r.getDouble(5)
                , r.getDouble(6)
              ))).groupByKey()
          case 7 =>
            dataRead.rdd.map(r => (r.getInt(0),
              Vectors.dense(r.getDouble(1)
                , r.getDouble(2)
                , r.getDouble(3)
                , r.getDouble(4)
                , r.getDouble(5)
                , r.getDouble(6)
                , r.getDouble(7)
              ))).groupByKey()
          case 8 =>
            dataRead.rdd.map(r => (r.getInt(0),
              Vectors.dense(r.getDouble(1)
                , r.getDouble(2)
                , r.getDouble(3)
                , r.getDouble(4)
                , r.getDouble(5)
                , r.getDouble(6)
                , r.getDouble(7)
                , r.getDouble(8)
              ))).groupByKey()
          case 9 =>
            dataRead.rdd.map(r => (r.getInt(0),
              Vectors.dense(r.getDouble(1)
                , r.getDouble(2)
                , r.getDouble(3)
                , r.getDouble(4)
                , r.getDouble(5)
                , r.getDouble(6)
                , r.getDouble(7)
                , r.getDouble(8)
                , r.getDouble(9)
              ))).groupByKey()
          case 10 =>
            dataRead.rdd.map(r => (r.getInt(0),
              Vectors.dense(r.getDouble(1)
                , r.getDouble(2)
                , r.getDouble(3)
                , r.getDouble(4)
                , r.getDouble(5)
                , r.getDouble(6)
                , r.getDouble(7)
                , r.getDouble(8)
                , r.getDouble(9)
                , r.getDouble(10)
              ))).groupByKey()
        }

        println("*** K = " + numCluster + " ***")
        println("*** NV = " + numVariables + "***")
        println("Executing Indices")
        val siloutes = Indices.getSilhouette(auxRDD.collect().toList)
        val dunns = Indices.getDunn(auxRDD.collect().toList)
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
      //    stringRdd.collect.foreach(println(_))

      stringRdd.repartition(1)
        .map(_.toString().replace("(", "").replace(")", ""))
        .saveAsTextFile("" + s"-Results-$i-" + Utils.whatTimeIsIt())
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
