package es.us

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

    val dataFile = "B:\\DataSets_Genetics\\dataset_c10_4_2000p.csv"
    val delimiter = ","

    val dataRead = spark.read
      .option("header", "false")
      .option("inferSchema", "true")
      .option("delimiter", delimiter)
      .csv(dataFile)
      .cache()

    val rddTest = dataRead.rdd.map(r => (r.getString(10),
      Vectors.dense(r.getDouble(0),r.getDouble(1),r.getDouble(2),r.getDouble(3),r.getDouble(4),r.getDouble(5),r.getDouble(6),r.getDouble(7),r.getDouble(8),r.getDouble(9))
    ))
    val auxRDD = dataRead.rdd.map(r => (r.getString(10),
      Vectors.dense(r.getDouble(0),r.getDouble(1),r.getDouble(2),r.getDouble(3),r.getDouble(4),r.getDouble(5),r.getDouble(6),r.getDouble(7),r.getDouble(8),r.getDouble(9))
    )).groupByKey()
    auxRDD.collect().foreach(x => println(x._2.size))

    val auxTest = auxRDD.map(x => (x._1,calculateMean(x._2)))

    val intras = rddTest.join(auxTest).map(x => (x._1,Vectors.sqdist(x._2._1,x._2._2))).groupByKey().mapValues(_.sum)
    println("Intra clusters:")
    intras.collect().foreach(x=>println(x._2))

    val inters = auxTest.cartesian(auxTest).filter(x => x._1 != x._2).map(x => Vectors.sqdist(x._1._2,x._2._2)).distinct()
    println("Inter clusters:")
    inters.collect().foreach(println(_))
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
