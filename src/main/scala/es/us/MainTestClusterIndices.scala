package es.us

import es.us.spark.mllib.Utils
import es.us.spark.mllib.clustering.validation.Indices
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.SparkSession

object MainTestClusterIndices {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName(s"VariablesIndices")
      .master("local[*]")
      .getOrCreate()

    //Set up the global variables
    var numVariables = 0
    var numCluster = 0
    var numPoints = 0
    var dataFile = ""

    val delimiter = ","
    var classIndex = 0

    //Set up the limits of the algorithm
    val minimumCluster = 4
    val maximumCluster = 5
    val minimumVariable = 1
    val maximumVariable = 10
    val limitNumber = 1

    //Create an Array with each DataSet posibility
    var arguments = List(Array[String]())

    for (k <- minimumCluster to maximumCluster){
      for (nv <- minimumVariable to maximumVariable){
        val auxList = Array[String](s"$k", s"$nv")
        arguments = auxList :: arguments
      }
    }

    arguments = arguments.take(arguments.length - 1).reverse

    for (i <- 1 to limitNumber) {

      val result = for (data <- arguments) yield {
        numCluster = data.apply(0).toInt
        numVariables = data.apply(1).toInt
//        numPoints = 1200 / numCluster
        numPoints = 1500
        classIndex = numVariables

//        dataFile = s"B:\\DataSets_Internos\\C$numCluster-D$numVariables-I$numPoints" + s"_$i"
        dataFile = s"B:\\DataSets_Genetics\\dataset_c$numVariables" + s"_$numCluster" + s"_$numPoints" + "p.csv"

        //Load data
        val dataRead = spark.read
          .option("header", "false")
          .option("inferSchema", "true")
          .option("delimiter", delimiter)
          .csv(dataFile)
          .cache()

        //Save all columns less the class column
        val columnsDataSet = dataRead.drop(s"_c$classIndex").columns

        //Create a RDD with each cluster and they points
        val dataRDD = dataRead.rdd.map { r =>

          //Create a Array[Double] with the values of each column to the DataSet read
          val vectorValues = for (co <- columnsDataSet) yield{

            //If the column number have two digits
            if(co.length == 4) {
             r.getDouble((co.charAt(2).toInt + co.charAt(3).toInt) - 87)
            }
            //If the column number have one digit
            else {
              r.getDouble(co.charAt(2).toInt - 48)
            }
          }

          //Create a Vector with the Array[Vector] of each row in the DataSet read
          val auxVector = Vectors.dense(vectorValues)

          //Return the Cluster ID and the Vector for each row in the DataSet read
          (r.getString(classIndex).hashCode, auxVector)
        }.groupByKey()

        println("*** K = " + numCluster + " ***")
        println("*** NV = " + numVariables + "***")
        println("Executing Indices")
        val silhouetteValues = Indices.getSilhouette(dataRDD.collect())
        val dunnValues = Indices.getDunn(dataRDD.collect())
        println("VALUES:")
        println("\tSilhouette (b average): " + silhouetteValues._1)
        println("\tSilhouette (a average): " + silhouetteValues._2)
        println("\tSilhouette: " + silhouetteValues._3)
        println("\tDunn (inter): " + dunnValues._1)
        println("\tDunn (intra): " + dunnValues._2)
        println("\tDunn: " + dunnValues._3)
        println("\n")

        (s"$numCluster-$numVariables", silhouetteValues, dunnValues)

      }

      //Save the results
      val stringRdd = spark.sparkContext.parallelize(result)

      stringRdd.repartition(1)
        .map(_.toString().replace("(", "").replace(")", ""))
        .saveAsTextFile(s"-Results-$i-" + Utils.whatTimeIsIt())
    }

    spark.stop()

  }

}
