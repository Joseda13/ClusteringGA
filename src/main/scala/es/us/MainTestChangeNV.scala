package es.us

import es.us.ga.Chromosome_Clustering
import es.us.spark.mllib.clustering.validation.Indices
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.SparkSession

object MainTestChangeNV {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName("Genetic Algorithm")
      .master("local[*]")
      .getOrCreate()

    val pathDataSets = "B:\\Genetic_test\\"

    val arguments = List(
      //Array[String](pathDataSets+"K3-N20-D20-DES0_03", "40")
      Array[String](pathDataSets+"K5-N20-D10-DES0_03", "40")
    )

    val baseTest = pathDataSets + "K3-N20-D15-DES0_03"
    val dimension = 35
    val K = 3

    val chromoTest = new Chromosome_Clustering(dimension, K)
//    val aux = Array.fill(35)(0 to 1).flatten.combinations(35).flatMap(_.permutations).toArray
    chromoTest.getGenes().update(0,1)
    chromoTest.getGenes().update(1,1)
    chromoTest.getGenes().update(2,1)
    chromoTest.getGenes().update(3,1)
    chromoTest.getGenes().update(4,1)
    chromoTest.getGenes().update(5,1)
    chromoTest.getGenes().update(6,1)
    chromoTest.getGenes().update(7,1)
    chromoTest.getGenes().update(8,1)
    chromoTest.getGenes().update(9,1)
    chromoTest.getGenes().update(10,1)
    chromoTest.getGenes().update(11,1)
    chromoTest.getGenes().update(12,1)
    chromoTest.getGenes().update(13,1)
    chromoTest.getGenes().update(14,1)
    chromoTest.getGenes().update(15,1)
    chromoTest.getGenes().update(16,1)
    chromoTest.getGenes().update(17,1)
    chromoTest.getGenes().update(18,1)
    chromoTest.getGenes().update(19,1)
    chromoTest.getGenes().update(20,1)
    chromoTest.getGenes().update(21,1)
    chromoTest.getGenes().update(22,1)
    chromoTest.getGenes().update(23,1)
    chromoTest.getGenes().update(24,1)
    chromoTest.getGenes().update(25,1)
    chromoTest.getGenes().update(26,1)
    chromoTest.getGenes().update(27,1)
    chromoTest.getGenes().update(28,1)
    chromoTest.getGenes().update(29,1)
    chromoTest.getGenes().update(30,1)
    chromoTest.getGenes().update(31,1)
    chromoTest.getGenes().update(32,1)
    chromoTest.getGenes().update(33,1)
    chromoTest.getGenes().update(34,1)
//    chromoTest.getGenes().update(35,1)
//    chromoTest.getGenes().update(36,1)
//    chromoTest.getGenes().update(37,1)
//    chromoTest.getGenes().update(38,0)
//    chromoTest.getGenes().update(39,1)

    println("Chromo initial: " + chromoTest.toSpecialString)
    println("Result Silhoutte: " + Indices.getFitnessSilhouette(chromoTest.getGenes,baseTest))

    chromoTest.improbeNumberVariables(baseTest)
//    println("Chromo final: " + chromoTest.toSpecialString)

    spark.stop()
  }
}
