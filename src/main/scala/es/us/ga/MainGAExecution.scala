package es.us.ga

import es.us.spark.mllib.Utils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

object MainGAExecution extends Logging{

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName("Genetic Algorithm")
      .master("local[*]")
      .getOrCreate()

    val startTime = System.nanoTime

    var origen = "data/K3-N3-D3-DES0_03"
    var destination = "Results/Silhoutte_75COR/K3-N3-D3-DES0_03"
    var dimension = 6
    var delimiter = ","

    if (args.length > 2){
      origen = args.apply(0)
      destination = args.apply(1)
      dimension = args.apply(2).toInt
      delimiter = args.apply(3)
    }

    //Load the data
    val dataRead = spark.read
      .option("header", "false")
      .option("inferSchema", "true")
      .option("delimiter", delimiter)
      .csv(origen)
      .cache()

    //Initialization the GA
    val geneticAlgorithm = new GeneticAlgorithm
    geneticAlgorithm.setDATABASE(dataRead)
    geneticAlgorithm.setDimension(dimension)

    val resultGenetic = new Array[String](GeneticAlgorithm.NUM_GENERATIONS + 1)

    for (in <- 0 to resultGenetic.length - 1) {
      resultGenetic.update(in, "")
    }

    println("*******************************")
    println("*********GA CLUSTERING*********")
    println("*******************************")
    println("Configuration:")
    println("\tPOPULATION SIZE: " + GeneticAlgorithm.POPULATION_SIZE)
    println("\tNUMBER GENERATIONS: " + GeneticAlgorithm.NUM_GENERATIONS)
    println("\tDIMENSION CHROMOSOMES: " + GeneticAlgorithm.DIMENSION)
    println("\tMUTATION RATE: " + GeneticAlgorithm.MUTATION_RATE)
    println("\tMUTATION WEIGHTS: " + GeneticAlgorithm.MUTATION_WEIGHTS)
    println("\tMUTATION K: " + GeneticAlgorithm.MUTATION_K)
    println("\tCROSSOVER RATE: " + GeneticAlgorithm.CROSSOVER_RATE)
    println("\tNUMBER ELITE CHROMOSOMES: " + GeneticAlgorithm.NUM_ELIT_CHROMOSOMES)
    println("\tTOURNAMENT SIZE: " + GeneticAlgorithm.TOURNAMENT_SIZE)
    println("Running...\n")

    //Create the initial population and ordered by fitness
    var population = geneticAlgorithm.randomPopulation()
    population.sortChromosomesByFitness()

    //Show the winner of the initial population
    val resultInitial = "Initial Generation => " + "Fittest chromosome: " + population.getChromosomes.get(0).toString
    println(resultInitial)

    resultGenetic.update(0, resultInitial)

    val elapsedIter0 = (System.nanoTime - startTime) / 1e9d
    logInfo("Time for iteration 0: " + elapsedIter0)

    //Create and initialize the control variables
    var generationNumber = 0

    var fitnessGeneration = population.getChromosomes.get(0).getFitness
    var fitnessNextGeneration = 0d

    var generationNotChangeFitness = 0
    var numberGenerationsWithoutChange = 10

    var conditionStop = true
    var enabledSubstitution = true

    //GA evolution process
    while ((generationNumber < GeneticAlgorithm.NUM_GENERATIONS) && conditionStop) {
      generationNumber += 1

      val startIter = System.nanoTime

      //Change generational
      population = geneticAlgorithm.evolve(population)
      population.sortChromosomesByFitness()

      var resultGeneration = "Generation # " + generationNumber + " => Fittest chromosome: " + population.getChromosomes.get(0).toString
      println(resultGeneration)

      resultGenetic.update(generationNumber, resultGeneration)

      if (generationNumber == 0) {
        fitnessGeneration = population.getChromosomes.get(0).getFitness
        fitnessNextGeneration = population.getChromosomes.get(0).getFitness
      } else if (generationNumber > 0) {
        fitnessGeneration = fitnessNextGeneration
        fitnessNextGeneration = population.getChromosomes.get(0).getFitness
      }

      if (fitnessGeneration == fitnessNextGeneration) {
        generationNotChangeFitness += 1
      } else {
        generationNotChangeFitness = 0
      }

      if (generationNotChangeFitness == numberGenerationsWithoutChange) {

        if (!enabledSubstitution) {
          conditionStop = false
        } else {
          population = geneticAlgorithm.substitutionPoblation(new Population_Clustering(population.getChromosomes.subList(0, (GeneticAlgorithm.POPULATION_SIZE / 2))))
        }

        enabledSubstitution = false
        generationNotChangeFitness = 0
      }

      val elapsedIter = (System.nanoTime - startIter) / 1e9d
      logInfo(s"Time for iteration $generationNumber: " + elapsedIter)

    }

    val resultRDD = spark.sparkContext.parallelize(resultGenetic)

    //Save the result
    resultRDD.coalesce(1)
      .map(_.toString().replace("(", "").replace(")", "").replace(" ", ""))
      .saveAsTextFile(destination +"-Results-" + Utils.whatTimeIsIt())

    val elapsed = (System.nanoTime - startTime) / 1e9d
    logInfo("TOTAL TIME: " + elapsed)

    spark.stop()

  }

}
