package es.us

import es.us.ga.{GeneticAlgorithm, Population_Clustering}
import es.us.ga.MainTestGA.logInfo
import es.us.spark.mllib.Utils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

object MainTestDummies extends Logging{

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName("Genetic Algorithm")
      .master("local[*]")
      .getOrCreate()

    val startTime = System.nanoTime

    var origen = ""
    var destination = ""
    var dimension = 0

    val pathDataSets = "B:\\Genetic_test\\"

    val arguments = List(

//      Array[String](pathDataSets+"K3-N20-D20-DES0_03", "40")

      Array[String](pathDataSets+"K5-N20-D20-DES0_03", "40")

    )

    for (data <- arguments){

      origen = data.apply(0)
      dimension = data.apply(1).toInt

      val geneticAlgorithm = new GeneticAlgorithm
      geneticAlgorithm.setDimension(dimension)
      geneticAlgorithm.setPATHTODATA(origen)

      println("*******************************")
      println("*********GA CLUSTERING*********")
      println("*******************************")
      println("Configuration:")
      println("\tPOPULATION SIZE: " + GeneticAlgorithm.POPULATION_SIZE)
      println("\tNUMBER GENERATIONS: " + GeneticAlgorithm.NUM_GENERATIONS)
      println("\tDIMENSION CHROMOSOMES: " + GeneticAlgorithm.DIMENSION)
      println("Running...\n")

      var generationNumber = 0

      while (generationNumber < GeneticAlgorithm.NUM_GENERATIONS) {
        generationNumber += 1

        val startIter = System.nanoTime

        var population = geneticAlgorithm.randomPopulationTestDummies()
        population.sortChromosomesByFitness()

        println(s"Chromosomes of Generaton $generationNumber:")

        for (i <- 0 until population.getChromosomes.size()) {

          println(
            population.getChromosomes.get(i).toString + ";" + population.getChromosomes.get(i).contAttributeGood() + ";"
              + population.getChromosomes.get(i).contDummies() + ";"
          )
        }

        val elapsedIter = (System.nanoTime - startIter) / 1e9d
        logInfo(s"Time for iteration $generationNumber: " + elapsedIter)

      }

      val elapsed = (System.nanoTime - startTime) / 1e9d
      logInfo("TOTAL TIME: " + elapsed)

    }

    spark.stop()
  }
}
