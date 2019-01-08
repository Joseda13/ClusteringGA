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
      .master("local[*]")
      .getOrCreate()

    val startTime = System.nanoTime

//    val pathDataSets = "hdfs://hdfs/jdmartin/DataSets/"
//    val pathResults = "hdfs://hdfs/jdmartin/Results/Dunn/"

//    for (iter <- 2 to 10) {

//      val pathDataSets = "data/Real_DB/"
//      val pathResults = s"Results/Sil_Final_75_Real_DB/"

      var origen = ""
      var destination = ""
      var dimension = 0

    val pathDataSets = "B:\\DataSets_Real\\"
    val pathResults = "B:\\Results\\Sil_Final_75_Real_DB\\"

      val arguments = List(

//        Array[String](pathDataSets + "dim3.txt", pathResults + "dim3.txt", "3"),
//        Array[String](pathDataSets + "dim5.txt", pathResults + "dim5.txt", "5"),
//        Array[String](pathDataSets + "dim10.txt", pathResults + "dim10.txt", "10"),
//        Array[String](pathDataSets + "dim15.txt", pathResults + "dim15.txt", "15"),
//        Array[String](pathDataSets + "dim32.txt", pathResults + "dim32.txt", "32"),
//          Array[String](pathDataSets + "dim64.txt", pathResults + "dim64.txt", "64"),
//          Array[String](pathDataSets + "dim128.txt", pathResults + "dim128.txt", "128"),
//          Array[String](pathDataSets + "dim256.txt", pathResults + "dim256.txt", "256"),
//          Array[String](pathDataSets + "dim512.txt", pathResults + "dim512.txt", "512"),
//          Array[String](pathDataSets + "g2-4-30.txt", pathResults + "g2-4-30.txt", "4"),
//          Array[String](pathDataSets + "g2-8-30.txt", pathResults + "g2-8-30.txt", "8"),
//          Array[String](pathDataSets + "g2-16-30.txt", pathResults + "g2-16-30.txt", "16"),
//          Array[String](pathDataSets + "g2-32-30.txt", pathResults + "g2-32-30.txt", "32"),
//          Array[String](pathDataSets + "g2-64-30.txt", pathResults + "g2-64-30.txt", "64"),
//          Array[String](pathDataSets + "g2-128-30.txt", pathResults + "g2-128-30.txt", "128"),
//          Array[String](pathDataSets + "g2-256-30.txt", pathResults + "g2-256-30.txt", "256"),
          Array[String](pathDataSets + "g2-512-30.txt", pathResults + "g2-512-30.txt", "512")

//        Array[String](pathDataSets + "K3-N20-D3-DES0_03", pathResults + "K3-N20-D3-DES0_03", "23"),
//        Array[String](pathDataSets + "K3-N20-D5-DES0_03", pathResults + "K3-N20-D5-DES0_03", "25"),
//        Array[String](pathDataSets + "K3-N20-D10-DES0_03", pathResults + "K3-N20-D10-DES0_03", "30"),
//        Array[String](pathDataSets + "K3-N20-D15-DES0_03", pathResults + "K3-N20-D15-DES0_03", "35"),
//        Array[String](pathDataSets + "K3-N20-D20-DES0_03", pathResults + "K3-N20-D20-DES0_03", "40"),
//
//        Array[String](pathDataSets + "K5-N20-D3-DES0_03", pathResults + "K5-N20-D3-DES0_03", "23"),
//        Array[String](pathDataSets + "K5-N20-D5-DES0_03", pathResults + "K5-N20-D5-DES0_03", "25"),
//        Array[String](pathDataSets + "K5-N20-D10-DES0_03", pathResults + "K5-N20-D10-DES0_03", "30"),
//        Array[String](pathDataSets + "K5-N20-D15-DES0_03", pathResults + "K5-N20-D15-DES0_03", "35"),
//        Array[String](pathDataSets + "K5-N20-D20-DES0_03", pathResults + "K5-N20-D20-DES0_03", "40"),
//
//        Array[String](pathDataSets + "K3-N3-D3-DES0_03", pathResults + "K3-N3-D3-DES0_03", "6"),
//        Array[String](pathDataSets + "K3-N3-D5-DES0_03", pathResults + "K3-N3-D5-DES0_03", "8"),
//        Array[String](pathDataSets + "K3-N3-D10-DES0_03", pathResults + "K3-N3-D10-DES0_03", "13"),
//        Array[String](pathDataSets + "K3-N3-D15-DES0_03", pathResults + "K3-N3-D15-DES0_03", "18"),
//        Array[String](pathDataSets + "K3-N3-D20-DES0_03", pathResults + "K3-N3-D20-DES0_03", "23"),
//
//        Array[String](pathDataSets + "K5-N3-D3-DES0_03", pathResults + "K5-N3-D3-DES0_03", "6"),
//        Array[String](pathDataSets + "K5-N3-D5-DES0_03", pathResults + "K5-N3-D5-DES0_03", "8"),
//        Array[String](pathDataSets + "K5-N3-D10-DES0_03", pathResults + "K5-N3-D10-DES0_03", "13"),
//        Array[String](pathDataSets + "K5-N3-D15-DES0_03", pathResults + "K5-N3-D15-DES0_03", "18"),
//        Array[String](pathDataSets + "K5-N3-D20-DES0_03", pathResults + "K5-N3-D20-DES0_03", "23"),
//
//        Array[String](pathDataSets + "K7-N3-D3-DES0_03", pathResults + "K7-N3-D3-DES0_03", "6"),
//        Array[String](pathDataSets + "K7-N3-D5-DES0_03", pathResults + "K7-N3-D5-DES0_03", "8"),
//        Array[String](pathDataSets + "K7-N3-D10-DES0_03", pathResults + "K7-N3-D10-DES0_03", "13"),
//        Array[String](pathDataSets + "K7-N3-D15-DES0_03", pathResults + "K7-N3-D15-DES0_03", "18"),
//        Array[String](pathDataSets + "K7-N3-D20-DES0_03", pathResults + "K7-N3-D20-DES0_03", "23"),
//
//        Array[String](pathDataSets + "K3-N5-D3-DES0_03", pathResults + "K3-N5-D3-DES0_03", "8"),
//        Array[String](pathDataSets + "K3-N5-D5-DES0_03", pathResults + "K3-N5-D5-DES0_03", "10"),
//        Array[String](pathDataSets + "K3-N5-D10-DES0_03", pathResults + "K3-N5-D10-DES0_03", "15"),
//        Array[String](pathDataSets + "K3-N5-D15-DES0_03", pathResults + "K3-N5-D15-DES0_03", "20"),
//        Array[String](pathDataSets + "K3-N5-D20-DES0_03", pathResults + "K3-N5-D20-DES0_03", "25"),
//
//        Array[String](pathDataSets + "K5-N5-D3-DES0_03", pathResults + "K5-N5-D3-DES0_03", "8"),
//        Array[String](pathDataSets + "K5-N5-D5-DES0_03", pathResults + "K5-N5-D5-DES0_03", "10"),
//        Array[String](pathDataSets + "K5-N5-D10-DES0_03", pathResults + "K5-N5-D10-DES0_03", "15"),
//        Array[String](pathDataSets + "K5-N5-D15-DES0_03", pathResults + "K5-N5-D15-DES0_03", "20"),
//        Array[String](pathDataSets + "K5-N5-D20-DES0_03", pathResults + "K5-N5-D20-DES0_03", "25"),
//
//        Array[String](pathDataSets + "K7-N5-D3-DES0_03", pathResults + "K7-N5-D3-DES0_03", "8"),
//        Array[String](pathDataSets + "K7-N5-D5-DES0_03", pathResults + "K7-N5-D5-DES0_03", "10"),
//        Array[String](pathDataSets + "K7-N5-D10-DES0_03", pathResults + "K7-N5-D10-DES0_03", "15"),
//        Array[String](pathDataSets + "K7-N5-D15-DES0_03", pathResults + "K7-N5-D15-DES0_03", "20"),
//        Array[String](pathDataSets + "K7-N5-D20-DES0_03", pathResults + "K7-N5-D20-DES0_03", "25"),
//
//        Array[String](pathDataSets + "K3-N10-D3-DES0_03", pathResults + "K3-N10-D3-DES0_03", "13"),
//        Array[String](pathDataSets + "K3-N10-D5-DES0_03", pathResults + "K3-N10-D5-DES0_03", "15"),
//        Array[String](pathDataSets + "K3-N10-D10-DES0_03", pathResults + "K3-N10-D10-DES0_03", "20"),
//        Array[String](pathDataSets + "K3-N10-D15-DES0_03", pathResults + "K3-N10-D15-DES0_03", "25"),
//        Array[String](pathDataSets + "K3-N10-D20-DES0_03", pathResults + "K3-N10-D20-DES0_03", "30"),
//
//        Array[String](pathDataSets + "K5-N10-D3-DES0_03", pathResults + "K5-N10-D3-DES0_03", "13"),
//        Array[String](pathDataSets + "K5-N10-D5-DES0_03", pathResults + "K5-N10-D5-DES0_03", "15"),
//        Array[String](pathDataSets + "K5-N10-D10-DES0_03", pathResults + "K5-N10-D10-DES0_03", "20"),
//        Array[String](pathDataSets + "K5-N10-D15-DES0_03", pathResults + "K5-N10-D15-DES0_03", "25"),
//        Array[String](pathDataSets + "K5-N10-D20-DES0_03", pathResults + "K5-N10-D20-DES0_03", "30"),
//
//        Array[String](pathDataSets + "K7-N10-D3-DES0_03", pathResults + "K7-N10-D3-DES0_03", "13"),
//        Array[String](pathDataSets + "K7-N10-D5-DES0_03", pathResults + "K7-N10-D5-DES0_03", "15"),
//        Array[String](pathDataSets + "K7-N10-D10-DES0_03", pathResults + "K7-N10-D10-DES0_03", "20"),
//        Array[String](pathDataSets + "K7-N10-D15-DES0_03", pathResults + "K7-N10-D15-DES0_03", "25"),
//        Array[String](pathDataSets + "K7-N10-D20-DES0_03", pathResults + "K7-N10-D20-DES0_03", "30"),
//
//        Array[String](pathDataSets + "K3-N15-D3-DES0_03", pathResults + "K3-N15-D3-DES0_03", "18"),
//        Array[String](pathDataSets + "K3-N15-D5-DES0_03", pathResults + "K3-N15-D5-DES0_03", "20"),
//        Array[String](pathDataSets + "K3-N15-D10-DES0_03", pathResults + "K3-N15-D10-DES0_03", "25"),
//        Array[String](pathDataSets + "K3-N15-D15-DES0_03", pathResults + "K3-N15-D15-DES0_03", "30"),
//        Array[String](pathDataSets + "K3-N15-D20-DES0_03", pathResults + "K3-N15-D20-DES0_03", "35"),
//
//        Array[String](pathDataSets + "K5-N15-D3-DES0_03", pathResults + "K5-N15-D3-DES0_03", "18"),
//        Array[String](pathDataSets + "K5-N15-D5-DES0_03", pathResults + "K5-N15-D5-DES0_03", "20"),
//        Array[String](pathDataSets + "K5-N15-D10-DES0_03", pathResults + "K5-N15-D10-DES0_03", "25"),
//        Array[String](pathDataSets + "K5-N15-D15-DES0_03", pathResults + "K5-N15-D15-DES0_03", "30"),
//        Array[String](pathDataSets + "K5-N15-D20-DES0_03", pathResults + "K5-N15-D20-DES0_03", "35")
      )

      for (data <- arguments) {

        origen = data.apply(0)
        destination = data.apply(1)
        dimension = data.apply(2).toInt

        val geneticAlgorithm = new GeneticAlgorithm_Example
        geneticAlgorithm.setDimension(dimension)
        geneticAlgorithm.setPATHTODATA(origen)

        val resultGenetic = new Array[String](GeneticAlgorithm_Example.NUM_GENERATIONS + 1)

        for (in <- 0 to resultGenetic.length - 1) {
          resultGenetic.update(in, "")
        }

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

        //      println("Chromosomes of Initial Generaton:")
        //      population.getChromosomes.toArray().foreach(println(_))

        val elapsedIter0 = (System.nanoTime - startTime) / 1e9d
        logInfo("Time for iteration 0: " + elapsedIter0)

        var generationNumber = 0

        var fitnessGeneration = population.getChromosomes.get(0).getFitness
        var fitnessNextGeneration = 0d

        var generationNotChangeFitness = 0
        var numberGenerationsWithoutChange = 10

        var conditionStop = true
        var enabledSubstitution = true

        while ((generationNumber < GeneticAlgorithm_Example.NUM_GENERATIONS) && conditionStop) {
          generationNumber += 1

          val startIter = System.nanoTime

          population = geneticAlgorithm.evolve(population)
          population.sortChromosomesByFitness()

          var resultGeneration = "Generation # " + generationNumber + " => Fittest chromosome: " + population.getChromosomes.get(0).toString
          println(resultGeneration)

          resultGenetic.update(generationNumber, resultGeneration)
          //      println(s"Chromosomes of Generaton $generationNumber:")
          //      population.getChromosomes.toArray().foreach(println(_))

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
              population = geneticAlgorithm.substitutionPoblation(new Population_Clustering(population.getChromosomes.subList(0, (GeneticAlgorithm_Example.POPULATION_SIZE / 2))))
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

//      }

    }

    spark.stop()
  }
}