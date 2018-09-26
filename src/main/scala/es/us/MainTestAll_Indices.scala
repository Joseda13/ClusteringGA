package es.us

import java.util

import es.us.ga.{Chromosome_Clustering, GeneticAlgorithm_Example}
import es.us.spark.mllib.clustering.validation.{FeatureStatistics, Indices}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.sql.{Column, SparkSession, functions}

object MainTestAll_Indices {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val startTime = System.nanoTime

//    val spark = SparkSession.builder()
//      .appName(s"VariablesIndices-7")
//      .master("local[*]")
//      .getOrCreate()
//
//    import spark.implicits._
//
//    val pathToData = "C:\\Users\\Jose David\\IdeaProjects\\CreateRandomDataset\\C7-D5-I1000-201809130912\\part-00000"
//
//    var dataRead = spark.read
//      .option("header", "false")
//      .option("inferSchema", "true")
//      .option("delimiter", ",")
//      .csv(pathToData)
//      .cache()
//
//    val auxD = dataRead.map(x => (x.getInt(0), x.getDouble(1), x.getDouble(2), x.getDouble(3), x.getDouble(4), x.getDouble(5)
//    , Math.random(), Math.random(), Math.random(), Math.random(), Math.random()))

//    val auxDF = dataRead.withColumn("atribute0", functions.lit(Math.random()))
//    val auxDF2 = auxDF.withColumn("atribute1", functions.lit(Math.random()))
//    val auxDF3 = auxDF2.withColumn("atribute2", functions.lit(Math.random()))
//    val auxDF4 = auxDF3.withColumn("atribute3", functions.lit(Math.random()))
//    val auxDF5 = auxDF4.withColumn("atribute4", functions.lit(Math.random()))
//
//    auxD.write.csv("B:\\K7-10D-5V-5DU")

    val geneticAlgorithm = new GeneticAlgorithm_Example

    var population = geneticAlgorithm.randomPopulation()
    population.sortChromosomesByFitness()
//    population.getChromosomes.toArray().foreach(println(_))

    for (i <- 0 until population.getChromosomes.size()){
    //      var indicesChi = FeatureStatistics.getChiIndices(population.getChromosomes.get(i).getGenes, dataFile)
          var indicesOld = Indices.getInternalIndices(population.getChromosomes.get(i).getGenes, GeneticAlgorithm_Example.PATHTODATA)
//          var fitnessDunn = Indices.getFitnessDunn(population.getChromosomes.get(i).getGenes, GeneticAlgorithm_Example.PATHTODATA)
      var fitnessDunn = population.getChromosomes.get(i).getFitness
      var fitnessSilhoutte = Indices.getFitnessSilhouette(population.getChromosomes.get(i).getGenes, GeneticAlgorithm_Example.PATHTODATA)
    //      var indicesInternal = Indices.getInternalIndicesNewVersion(population.getChromosomes.get(i).getGenes, dataFile)

          println(
//            "Chromosome: " + population.getChromosomes.get(i).toSpecialString + "=>"
     population.getChromosomes.get(i).toSpecialString + ";" + population.getChromosomes.get(i).contAttributeGood() + ";"
       + population.getChromosomes.get(i).contDummies() + ";"
    //        + " -> Indices => Chi Row: " + indicesChi._1 + ", Chi Column: " + indicesChi._2
    //       + ", Silhouette: " + indicesInternal._1 + ", Dunn: " + indicesInternal._2
    //      + ", Davies-Bouldin: " + indicesInternal._3 + ", Compute-Cost: " + indicesInternal._4
    //        + ", Old Silhouette: " + indicesOld._1 + ", Old Dunn: " + indicesOld._2
//            + ", Dunn: " + fitnessDunn + ", Silhoutte: " + fitnessSilhoutte
//            + ", Old Davies-Bouldin: " + indicesOld._3 + ", Old Compute-Cost: " + indicesOld._4
       + fitnessDunn + ";" + fitnessSilhoutte
       + ";" + indicesOld._3
           )
        }


    val duration = (System.nanoTime - startTime) / 1e9d
    println(s"TIME TOTAL: $duration")
  }

}
