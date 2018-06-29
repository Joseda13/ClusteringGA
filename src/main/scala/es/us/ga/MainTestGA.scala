package es.us.ga

import java.util
import java.util.Arrays

import org.apache.commons.math3.genetics._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.internal.Logging

object MainTestGA extends Logging{ // parameters for the GA

  private val NUM_GENERATIONS = 5
//  private val ELITISM_RATE = 0.2

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

//    var testFile = null
//    val bufferedSource = scala.io.Source.fromFile("B:\\DataSets_Genetics\\dataset_101.csv")
//    var testFile = for (line <- bufferedSource.getLines) {
//      var cols = line.split(",").map(_.trim)
//      // do whatever you want with the columns here
//      cols = cols.drop(1)
//      println(s"${cols(0)}|${cols(1)}|${cols(2)}|${cols(3)}")
//
//    }
//    bufferedSource.close
//    testFile.

    val startTime = System.nanoTime

    // initialize a new genetic algorithm
//    val ga = new GeneticAlgorithm(new UniformCrossover[Integer](0.5), CROSSOVER_RATE, new BinaryMutation(),
//      MUTATION_RATE, new TournamentSelection(TOURNAMENT_ARITY))
//
//    val testPoblation = new Population_Clustering(POPULATION_SIZE).inicialicePopulation(DIMENSION, K_MAX)
//
//    val initial = MainTestGAJava.randomPopulation()
//    logInfo("Initial Poblation: " + initial.getChromosomes)
//
//    val stopCond = new FixedGenerationCount(NUM_GENERATIONS)
//
//    val finalPopulation_Clustering = ga.evolve(initial, stopCond)
//    logInfo("Final Poblation: " + finalPopulation_Clustering.toString)


    val geneticAlgorithm = new GeneticAlgorithm_Example

    var population = geneticAlgorithm.randomPopulation()
    population.sortChromosomesByFitness()
    System.out.println("--------------------------------")
    System.out.println("Generation # 0 " + " | Fittest chromosome fitness:" + population.getChromosomes.get(0).getFitness + " Numbers of genes: " + population.getChromosomes.size())
    val elapsedIter0 = (System.nanoTime - startTime) / 1e9d
    logInfo("Time for iteration 0: " + elapsedIter0)
//    Test_GA.printPopulation(population)

    var generationNumber: Int = 0
    while ( {
      generationNumber < NUM_GENERATIONS
    }) {
      val startIter = System.nanoTime
      generationNumber += 1
      System.out.println("--------------------------------")
      population = geneticAlgorithm.evolve(population)
      population.sortChromosomesByFitness()
      System.out.println("Generation # " + generationNumber + " | Fittest chromosome fitness:" + population.getChromosomes.get(0).getFitness + " Numbers of genes: " + population.getChromosomes.size())
      val elapsedIter = (System.nanoTime - startIter) / 1e9d
      logInfo(s"Time for iteration $generationNumber: " + elapsedIter)
//      Test_GA.printPopulation(population)
    }

    val elapsed = (System.nanoTime - startTime) / 1e9d
    logInfo("TOTAL TIME: " + elapsed)
  }
}