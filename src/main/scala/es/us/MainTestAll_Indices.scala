package es.us

import java.util

import es.us.ga.GeneticAlgorithm_Example
import es.us.spark.mllib.clustering.validation.{FeatureStatistics, Indices}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.sql.{Column, SparkSession, functions}

object MainTestAll_Indices {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val startTime = System.nanoTime

    val dataFile = "B:\\DataSets_Genetics\\dataset_104.csv"

    val geneticAlgorithm = new GeneticAlgorithm_Example

    var population = geneticAlgorithm.randomPopulation()
    population.getChromosomes.toArray().foreach(println(_))

    for (i <- 0 until population.getChromosomes.size()){
//      var indicesChi = FeatureStatistics.getChiIndices(population.getChromosomes.get(i).getGenes, dataFile)
      var indicesOld = Indices.getInternalIndices(population.getChromosomes.get(i).getGenes, dataFile)
      var indicesInternal = Indices.getInternalIndicesNewVersion(population.getChromosomes.get(i).getGenes, dataFile)

      println("Chromosome: " + population.getChromosomes.get(i).toSpecialString + "=>"
//        + " -> Indices => Chi Row: " + indicesChi._1 + ", Chi Column: " + indicesChi._2
       + ", Silhouette: " + indicesInternal._1 + ", Dunn: " + indicesInternal._2
      + ", Davies-Bouldin: " + indicesInternal._3 + ", Compute-Cost: " + indicesInternal._4
        + ", Old Silhouette: " + indicesOld._1 + ", Old Dunn: " + indicesOld._2
        + ", Old Davies-Bouldin: " + indicesOld._3 + ", Old Compute-Cost: " + indicesOld._4)
    }

    val duration = (System.nanoTime - startTime) / 1e9d
    println(s"TIME TOTAL: $duration")
  }

}
