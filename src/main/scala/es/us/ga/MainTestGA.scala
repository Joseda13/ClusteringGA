package es.us.ga

import java.util
import java.util.Arrays

import org.apache.commons.math3.genetics._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.internal.Logging

object MainTestGA extends Logging{

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val startTime = System.nanoTime

    val geneticAlgorithm = new GeneticAlgorithm_Example

    println("*******************************")
    println("*********GA CLUSTERING*********")
    println("*******************************")
    println("Configuration:")
    println("\tPOPULATION SIZE: " + GeneticAlgorithm_Example.POPULATION_SIZE)
    println("\tNUMBER GENERATIONS: " + GeneticAlgorithm_Example.NUM_GENERATIONS)
    println("\tMUTATION RATE: " + GeneticAlgorithm_Example.MUTATION_RATE)
    println("\tMUTATION WEIGHTS: " + GeneticAlgorithm_Example.MUTATION_WEIGHTS)
    println("\tMUTATION K: " + GeneticAlgorithm_Example.MUTATION_K)
    println("\tCROSSOVER RATE: " + GeneticAlgorithm_Example.CROSSOVER_RATE)
    println("\tNUMBER ELITE CHROMOSOMES: " + GeneticAlgorithm_Example.NUM_ELIT_CHROMOSOMES)
    println("\tTOURNAMENT SIZE: " + GeneticAlgorithm_Example.TOURNAMENT_SIZE)
    println("Running...\n")

    var population = geneticAlgorithm.randomPopulation()
    population.sortChromosomesByFitness()
    println("Initial Generation => " + " Numbers of chromosomes: " + population.getChromosomes.size() + " | Fittest chromosome: " + population.getChromosomes.get(0).toString)
    println("Chromosomes of Initial Generaton:")
    population.getChromosomes.toArray().foreach(println(_))

    val elapsedIter0 = (System.nanoTime - startTime) / 1e9d
    logInfo("Time for iteration 0: " + elapsedIter0)

    var generationNumber = 0
    while ( {generationNumber < GeneticAlgorithm_Example.NUM_GENERATIONS} ) {
      val startIter = System.nanoTime

      generationNumber += 1

      population = geneticAlgorithm.evolve(population)
      population.sortChromosomesByFitness()
      println("Generation # " + generationNumber + " => Numbers of chromosomes: " + population.getChromosomes.size() + " | Fittest chromosome: " + population.getChromosomes.get(0).toString)
      println(s"Chromosomes of Generaton $generationNumber:")
      population.getChromosomes.toArray().foreach(println(_))

      val elapsedIter = (System.nanoTime - startIter) / 1e9d
      logInfo(s"Time for iteration $generationNumber: " + elapsedIter)
    }

    val elapsed = (System.nanoTime - startTime) / 1e9d
    logInfo("TOTAL TIME: " + elapsed)
  }
}