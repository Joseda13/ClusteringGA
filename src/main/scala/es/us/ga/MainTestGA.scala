package es.us.ga

import es.us.spark.mllib.Utils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

object MainTestGA extends Logging{

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName("Genetic Algorithm")
//      .master("local[*]")
      .getOrCreate()

    val startTime = System.nanoTime

    val pathDataSets = "hdfs://hdfs/jdmartin/DataSets/"
    val pathResults = "hdfs://hdfs/jdmartin/Results/Silhoutte/"

    var origen = ""
    var destination = ""
    var dimension = 0

//    val pathDataSets = "B:\\Genetic_test\\"
//    val pathResults = "B:\\Results\\Dunn\\"

    val arguments = List(
      Array[String](pathDataSets+"K3-N3-D3-DES0_03", pathResults+"K3-N3-D3-DES0_03", "6"),
      Array[String](pathDataSets+"K3-N3-D5-DES0_03", pathResults+"K3-N3-D5-DES0_03", "8"),
      Array[String](pathDataSets+"K3-N3-D10-DES0_03", pathResults+"K3-N3-D10-DES0_03", "13"),

      Array[String](pathDataSets+"K5-N3-D3-DES0_03", pathResults+"K5-N3-D3-DES0_03", "6"),
      Array[String](pathDataSets+"K5-N3-D5-DES0_03", pathResults+"K5-N3-D5-DES0_03", "8"),
      Array[String](pathDataSets+"K5-N3-D10-DES0_03", pathResults+"K5-N3-D10-DES0_03", "13"),

      Array[String](pathDataSets+"K7-N3-D3-DES0_03", pathResults+"K7-N3-D3-DES0_03", "6"),
      Array[String](pathDataSets+"K7-N3-D5-DES0_03", pathResults+"K7-N3-D5-DES0_03", "8"),
      Array[String](pathDataSets+"K7-N3-D10-DES0_03", pathResults+"K7-N3-D10-DES0_03", "13"),

      Array[String](pathDataSets+"K3-N5-D3-DES0_03", pathResults+"K3-N5-D3-DES0_03", "8"),
      Array[String](pathDataSets+"K3-N5-D5-DES0_03", pathResults+"K3-N5-D5-DES0_03", "10"),
      Array[String](pathDataSets+"K3-N5-D10-DES0_03", pathResults+"K3-N5-D10-DES0_03", "15"),

      Array[String](pathDataSets+"K5-N5-D3-DES0_03", pathResults+"K5-N5-D3-DES0_03", "8"),
      Array[String](pathDataSets+"K5-N5-D5-DES0_03", pathResults+"K5-N5-D5-DES0_03", "10"),
      Array[String](pathDataSets+"K5-N5-D10-DES0_03", pathResults+"K5-N5-D10-DES0_03", "15"),

      Array[String](pathDataSets+"K7-N5-D3-DES0_03", pathResults+"K7-N5-D3-DES0_03", "8"),
      Array[String](pathDataSets+"K7-N5-D5-DES0_03", pathResults+"K7-N5-D5-DES0_03", "10"),
      Array[String](pathDataSets+"K7-N5-D10-DES0_03", pathResults+"K7-N5-D10-DES0_03", "15"),

      Array[String](pathDataSets+"K3-N10-D3-DES0_03", pathResults+"K3-N10-D3-DES0_03", "13"),
      Array[String](pathDataSets+"K3-N10-D5-DES0_03", pathResults+"K3-N10-D5-DES0_03", "15"),
      Array[String](pathDataSets+"K3-N10-D10-DES0_03", pathResults+"K3-N10-D10-DES0_03", "20"),

      Array[String](pathDataSets+"K5-N10-D3-DES0_03", pathResults+"K5-N10-D3-DES0_03", "13"),
      Array[String](pathDataSets+"K5-N10-D5-DES0_03", pathResults+"K5-N10-D5-DES0_03", "15"),
      Array[String](pathDataSets+"K5-N10-D10-DES0_03", pathResults+"K5-N10-D10-DES0_03", "20"),

      Array[String](pathDataSets+"K7-N10-D3-DES0_03", pathResults+"K7-N10-D3-DES0_03", "13"),
      Array[String](pathDataSets+"K7-N10-D5-DES0_03", pathResults+"K7-N10-D5-DES0_03", "15"),
      Array[String](pathDataSets+"K7-N10-D10-DES0_03", pathResults+"K7-N10-D10-DES0_03", "20")
    )

    for (data <- arguments){

      origen = data.apply(0)
      destination = data.apply(1)
      dimension = data.apply(2).toInt

      val geneticAlgorithm = new GeneticAlgorithm_Example
      geneticAlgorithm.setDimension(dimension)
      geneticAlgorithm.setPATHTODATA(origen)

      val resultGenetic = new Array[String](GeneticAlgorithm_Example.NUM_GENERATIONS + 1)

      println("*******************************")
      println("*********GA CLUSTERING*********")
      println("*******************************")
      println("Configuration:")
      println("\tPOPULATION SIZE: " + GeneticAlgorithm_Example.POPULATION_SIZE)
      println("\tNUMBER GENERATIONS: " + GeneticAlgorithm_Example.NUM_GENERATIONS)
      println("\tDIMENSION CHROMOSOMES: " + GeneticAlgorithm_Example.DIMENSION)
      println("\tMUTATION RATE: " + GeneticAlgorithm_Example.MUTATION_RATE)
      println("\tMUTATION WEIGHTS: " + GeneticAlgorithm_Example.MUTATION_WEIGHTS)
      println("\tMUTATION K: " + GeneticAlgorithm_Example.MUTATION_K)
      println("\tCROSSOVER RATE: " + GeneticAlgorithm_Example.CROSSOVER_RATE)
      println("\tNUMBER ELITE CHROMOSOMES: " + GeneticAlgorithm_Example.NUM_ELIT_CHROMOSOMES)
      println("\tTOURNAMENT SIZE: " + GeneticAlgorithm_Example.TOURNAMENT_SIZE)
      println("Running...\n")

      var population = geneticAlgorithm.randomPopulation()
      population.sortChromosomesByFitness()
      val resultInitial = "Initial Generation => " + "Fittest chromosome: " + population.getChromosomes.get(0).toString
      println(resultInitial)

      resultGenetic.update(0, resultInitial)

      //    println("Chromosomes of Initial Generaton:")
      //    population.getChromosomes.toArray().foreach(println(_))

      val elapsedIter0 = (System.nanoTime - startTime) / 1e9d
      logInfo("Time for iteration 0: " + elapsedIter0)

      var generationNumber = 0
      while ( {generationNumber < GeneticAlgorithm_Example.NUM_GENERATIONS} ) {
        val startIter = System.nanoTime

        generationNumber += 1

        population = geneticAlgorithm.evolve(population)
        population.sortChromosomesByFitness()
        var resultGeneration = "Generation # " + generationNumber + " => Fittest chromosome: " + population.getChromosomes.get(0).toString
        println(resultGeneration)

        resultGenetic.update(generationNumber, resultGeneration)
        //      println(s"Chromosomes of Generaton $generationNumber:")
        //      population.getChromosomes.toArray().foreach(println(_))

        val elapsedIter = (System.nanoTime - startIter) / 1e9d
        logInfo(s"Time for iteration $generationNumber: " + elapsedIter)
      }

      val resultRDD = spark.sparkContext.parallelize(resultGenetic)

      //Save the result
      resultRDD.coalesce(1)
        .map(_.toString().replace("(", "").replace(")", "").replace(" ", ""))
        .saveAsTextFile(destination + "-Results-" + Utils.whatTimeIsIt())

      val elapsed = (System.nanoTime - startTime) / 1e9d
      logInfo("TOTAL TIME: " + elapsed)

    }

    spark.stop()
  }
}