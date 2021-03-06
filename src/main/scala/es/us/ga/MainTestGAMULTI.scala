package es.us.ga

import es.us.spark.mllib.Utils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

object MainTestGAMULTI extends Logging{

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName("Genetic Multi Algorithm")
      .master("local[*]")
      .getOrCreate()

    val startTime = System.nanoTime

    val pathDataSets = "data/DataSets/"
    val pathResults = "Results/Multi_New_N_New_Dist/"

    var origen = ""
    var destination = ""
    var dimension = 0

//    val pathDataSets = "B:\\Genetic_test\\"
//    val pathResults = "B:\\Results\\Multi_New_N_New_Corr\\"

    val arguments = List(
      Array[String](pathDataSets+"K3-N3-D3-DES0_03", pathResults+"K3-N3-D3-DES0_03", "6"),
      Array[String](pathDataSets+"K3-N3-D5-DES0_03", pathResults+"K3-N3-D5-DES0_03", "8"),
      Array[String](pathDataSets+"K3-N3-D10-DES0_03", pathResults+"K3-N3-D10-DES0_03", "13"),
      Array[String](pathDataSets+"K3-N3-D15-DES0_03", pathResults+"K3-N3-D15-DES0_03", "18"),
      Array[String](pathDataSets+"K3-N3-D20-DES0_03", pathResults+"K3-N3-D20-DES0_03", "23"),

      Array[String](pathDataSets+"K5-N3-D3-DES0_03", pathResults+"K5-N3-D3-DES0_03", "6"),
      Array[String](pathDataSets+"K5-N3-D5-DES0_03", pathResults+"K5-N3-D5-DES0_03", "8"),
      Array[String](pathDataSets+"K5-N3-D10-DES0_03", pathResults+"K5-N3-D10-DES0_03", "13"),
      Array[String](pathDataSets+"K5-N3-D15-DES0_03", pathResults+"K5-N3-D15-DES0_03", "18"),
      Array[String](pathDataSets+"K5-N3-D20-DES0_03", pathResults+"K5-N3-D20-DES0_03", "23"),

      Array[String](pathDataSets+"K7-N3-D3-DES0_03", pathResults+"K7-N3-D3-DES0_03", "6"),
      Array[String](pathDataSets+"K7-N3-D5-DES0_03", pathResults+"K7-N3-D5-DES0_03", "8"),
      Array[String](pathDataSets+"K7-N3-D10-DES0_03", pathResults+"K7-N3-D10-DES0_03", "13"),
      Array[String](pathDataSets+"K7-N3-D15-DES0_03", pathResults+"K7-N3-D15-DES0_03", "18"),
      Array[String](pathDataSets+"K7-N3-D20-DES0_03", pathResults+"K7-N3-D20-DES0_03", "23"),

      Array[String](pathDataSets+"K3-N5-D3-DES0_03", pathResults+"K3-N5-D3-DES0_03", "8"),
      Array[String](pathDataSets+"K3-N5-D5-DES0_03", pathResults+"K3-N5-D5-DES0_03", "10"),
      Array[String](pathDataSets+"K3-N5-D10-DES0_03", pathResults+"K3-N5-D10-DES0_03", "15"),
      Array[String](pathDataSets+"K3-N5-D15-DES0_03", pathResults+"K3-N5-D15-DES0_03", "20"),
      Array[String](pathDataSets+"K3-N5-D20-DES0_03", pathResults+"K3-N5-D20-DES0_03", "25"),

      Array[String](pathDataSets+"K5-N5-D3-DES0_03", pathResults+"K5-N5-D3-DES0_03", "8"),
      Array[String](pathDataSets+"K5-N5-D5-DES0_03", pathResults+"K5-N5-D5-DES0_03", "10"),
      Array[String](pathDataSets+"K5-N5-D10-DES0_03", pathResults+"K5-N5-D10-DES0_03", "15"),
      Array[String](pathDataSets+"K5-N5-D15-DES0_03", pathResults+"K5-N5-D15-DES0_03", "20"),
      Array[String](pathDataSets+"K5-N5-D20-DES0_03", pathResults+"K5-N5-D20-DES0_03", "25"),

      Array[String](pathDataSets+"K7-N5-D3-DES0_03", pathResults+"K7-N5-D3-DES0_03", "8"),
      Array[String](pathDataSets+"K7-N5-D5-DES0_03", pathResults+"K7-N5-D5-DES0_03", "10"),
      Array[String](pathDataSets+"K7-N5-D10-DES0_03", pathResults+"K7-N5-D10-DES0_03", "15"),
      Array[String](pathDataSets+"K7-N5-D15-DES0_03", pathResults+"K7-N5-D15-DES0_03", "20"),
      Array[String](pathDataSets+"K7-N5-D20-DES0_03", pathResults+"K7-N5-D20-DES0_03", "25"),

      Array[String](pathDataSets+"K3-N10-D3-DES0_03", pathResults+"K3-N10-D3-DES0_03", "13"),
      Array[String](pathDataSets+"K3-N10-D5-DES0_03", pathResults+"K3-N10-D5-DES0_03", "15"),
      Array[String](pathDataSets+"K3-N10-D10-DES0_03", pathResults+"K3-N10-D10-DES0_03", "20"),
      Array[String](pathDataSets+"K3-N10-D15-DES0_03", pathResults+"K3-N10-D15-DES0_03", "25"),
      Array[String](pathDataSets+"K3-N10-D20-DES0_03", pathResults+"K3-N10-D20-DES0_03", "30"),

      Array[String](pathDataSets+"K5-N10-D3-DES0_03", pathResults+"K5-N10-D3-DES0_03", "13"),
      Array[String](pathDataSets+"K5-N10-D5-DES0_03", pathResults+"K5-N10-D5-DES0_03", "15"),
      Array[String](pathDataSets+"K5-N10-D10-DES0_03", pathResults+"K5-N10-D10-DES0_03", "20"),
      Array[String](pathDataSets+"K5-N10-D15-DES0_03", pathResults+"K5-N10-D15-DES0_03", "25"),
      Array[String](pathDataSets+"K5-N10-D20-DES0_03", pathResults+"K5-N10-D20-DES0_03", "30"),

      Array[String](pathDataSets+"K7-N10-D3-DES0_03", pathResults+"K7-N10-D3-DES0_03", "13"),
      Array[String](pathDataSets+"K7-N10-D5-DES0_03", pathResults+"K7-N10-D5-DES0_03", "15"),
      Array[String](pathDataSets+"K7-N10-D10-DES0_03", pathResults+"K7-N10-D10-DES0_03", "20"),
      Array[String](pathDataSets+"K7-N10-D15-DES0_03", pathResults+"K7-N10-D15-DES0_03", "25"),
      Array[String](pathDataSets+"K7-N10-D20-DES0_03", pathResults+"K7-N10-D20-DES0_03", "30"),

      Array[String](pathDataSets+"K3-N15-D3-DES0_03", pathResults+"K3-N15-D3-DES0_03", "18"),
      Array[String](pathDataSets+"K3-N15-D5-DES0_03", pathResults+"K3-N15-D5-DES0_03", "20"),
      Array[String](pathDataSets+"K3-N15-D10-DES0_03", pathResults+"K3-N15-D10-DES0_03", "25"),
      Array[String](pathDataSets+"K3-N15-D15-DES0_03", pathResults+"K3-N15-D15-DES0_03", "30"),
      Array[String](pathDataSets+"K3-N15-D20-DES0_03", pathResults+"K3-N15-D20-DES0_03", "35"),

      Array[String](pathDataSets+"K5-N15-D3-DES0_03", pathResults+"K5-N15-D3-DES0_03", "18"),
      Array[String](pathDataSets+"K5-N15-D5-DES0_03", pathResults+"K5-N15-D5-DES0_03", "20"),
      Array[String](pathDataSets+"K5-N15-D10-DES0_03", pathResults+"K5-N15-D10-DES0_03", "25"),
      Array[String](pathDataSets+"K5-N15-D15-DES0_03", pathResults+"K5-N15-D15-DES0_03", "30"),
      Array[String](pathDataSets+"K5-N15-D20-DES0_03", pathResults+"K5-N15-D20-DES0_03", "35"),

      Array[String](pathDataSets+"K3-N20-D3-DES0_03", pathResults+"K3-N20-D3-DES0_03", "23"),
      Array[String](pathDataSets+"K3-N20-D5-DES0_03", pathResults+"K3-N20-D5-DES0_03", "25"),
      Array[String](pathDataSets+"K3-N20-D10-DES0_03", pathResults+"K3-N20-D10-DES0_03", "30"),
      Array[String](pathDataSets+"K3-N20-D15-DES0_03", pathResults+"K3-N20-D15-DES0_03", "35"),
      Array[String](pathDataSets+"K3-N20-D20-DES0_03", pathResults+"K3-N20-D20-DES0_03", "40"),

      Array[String](pathDataSets+"K5-N20-D3-DES0_03", pathResults+"K5-N20-D3-DES0_03", "23"),
      Array[String](pathDataSets+"K5-N20-D5-DES0_03", pathResults+"K5-N20-D5-DES0_03", "25"),
      Array[String](pathDataSets+"K5-N20-D10-DES0_03", pathResults+"K5-N20-D10-DES0_03", "30"),
      Array[String](pathDataSets+"K5-N20-D15-DES0_03", pathResults+"K5-N20-D15-DES0_03", "35"),
      Array[String](pathDataSets+"K5-N20-D20-DES0_03", pathResults+"K5-N20-D20-DES0_03", "40")
    )

    for (data <- arguments){

      origen = data.apply(0)
      destination = data.apply(1)
      dimension = data.apply(2).toInt

      val geneticAlgorithm = new GeneticAlgorithm_Multi
      geneticAlgorithm.setDimension(dimension)
      geneticAlgorithm.setPATHTODATA(origen)

      val resultGenetic = new Array[String](GeneticAlgorithm_Multi.NUM_GENERATIONS + 1)

      for (in <- 0 to resultGenetic.length - 1){
        resultGenetic.update(in, "")
      }

      println("*******************************")
      println("*******GA MULTI CLUSTERING*****")
      println("*******************************")
      println("Configuration:")
      println("\tPOPULATION SIZE: " + GeneticAlgorithm_Multi.POPULATION_SIZE)
      println("\tNUMBER GENERATIONS: " + GeneticAlgorithm_Multi.NUM_GENERATIONS)
      println("\tDIMENSION CHROMOSOMES: " + GeneticAlgorithm_Multi.DIMENSION)
      println("\tMUTATION RATE: " + GeneticAlgorithm_Multi.MUTATION_RATE)
      println("\tMUTATION WEIGHTS: " + GeneticAlgorithm_Multi.MUTATION_WEIGHTS)
      println("\tMUTATION K: " + GeneticAlgorithm_Multi.MUTATION_K)
      println("\tCROSSOVER RATE: " + GeneticAlgorithm_Multi.CROSSOVER_RATE)
      println("\tNUMBER ELITE CHROMOSOMES: " + GeneticAlgorithm_Multi.NUM_ELIT_CHROMOSOMES)
      println("\tTOURNAMENT SIZE: " + GeneticAlgorithm_Multi.TOURNAMENT_SIZE)
      println("Running...\n")

      var population = geneticAlgorithm.randomPopulation()
      population.updateChromosomes()
//      population.calculatedObjetivesValues()
      population.calculatedObjetivesValuesStack()
      population.sortChromosomesByFitness()

      val resultInitial = "Initial Generation => " + "Fittest chromosome: " + population.getChromosomes.get(0).toString
      println(resultInitial)

      resultGenetic.update(0, resultInitial)

//      println("Chromosomes of Initial Generaton:")
//      population.getChromosomes.toArray().foreach(println(_))

      val elapsedIter0 = (System.nanoTime - startTime) / 1e9d
      logInfo("Time for iteration 0: " + elapsedIter0)

      var generationNumber = 0

      var fitnessGeneration: Chromosome_Multi = population.getChromosomes.get(0)
      var fitnessNextGeneration: Chromosome_Multi = null

      var generationNotChangeFitness = 0
      var numberGenerationsWithoutChange = 10

      var conditionStop = true
      var enabledSubstitution = true

      while ( (generationNumber < GeneticAlgorithm_Multi.NUM_GENERATIONS) && conditionStop) {
        generationNumber += 1

        val startIter = System.nanoTime

        population = geneticAlgorithm.evolve(population)
        population.updateChromosomes()
//        population.calculatedObjetivesValues()
        population.calculatedObjetivesValuesStack()
        population.sortChromosomesByFitness()

        var resultGeneration = "Generation # " + generationNumber + " => Fittest chromosome: " + population.getChromosomes.get(0).toString
        println(resultGeneration)

        resultGenetic.update(generationNumber, resultGeneration)
        //      println(s"Chromosomes of Generaton $generationNumber:")
        //      population.getChromosomes.toArray().foreach(println(_))

        if (generationNumber == 0){
          fitnessGeneration = population.getChromosomes.get(0)
          fitnessNextGeneration = population.getChromosomes.get(0)
        }else if (generationNumber > 0){
          fitnessGeneration = fitnessNextGeneration
          fitnessNextGeneration = population.getChromosomes.get(0)
        }

        if(fitnessGeneration == fitnessNextGeneration){
          generationNotChangeFitness +=1
        }else{
          generationNotChangeFitness = 0
        }

        if (generationNotChangeFitness == numberGenerationsWithoutChange){

          if (!enabledSubstitution){
            conditionStop = false
          }else {
            population = geneticAlgorithm.substitutionPoblation(new Population_Multi(population.getChromosomes.subList(0, (GeneticAlgorithm_Multi.POPULATION_SIZE / 2))))
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
        .saveAsTextFile(destination + "-Results-" + Utils.whatTimeIsIt())

      val elapsed = (System.nanoTime - startTime) / 1e9d
      logInfo("TOTAL TIME: " + elapsed)

    }

    spark.stop()
  }
}
