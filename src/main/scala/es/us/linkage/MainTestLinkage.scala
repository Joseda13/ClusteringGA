package es.us.linkage

import es.us.spark.mllib.Utils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.monotonically_increasing_id
import org.apache.spark.{SparkConf, SparkContext}

object MainTestLinkage {

  def main(args: Array[String]): Unit = {
    val start = System.nanoTime
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName("Spark Cluster")
      //      .config("spark.mesos.constraints", "enable_gpu:0")
      .master("local[*]")
      .getOrCreate()
    //      .set("spark.executor.instances", "19")
    //      .set("spark.yarn.executor.memoryOverhead", "1024")
    //      .set("spark.executor.memory", "4G")
    //      .set("spark.yarn.driver.memoryOverhead", "1024")
    //      .set("spark.driver.memory", "4G")
    //      .set("spark.executor.cores", "3")
    //      .set("spark.driver.cores", "3")
    //      .set("spark.default.parallelism", "114")

//    val sc = new SparkContext(conf)

    import spark.implicits._

    val  fileTest = "B:\\Datasets\\C11-D20-I100-Class"

    var origen: String = fileTest
    var destino: String = Utils.whatTimeIsIt()
    var numPartitions = 16 // cluster has 25 nodes with 4 cores. You therefore need 4 x 25 = 100 partitions.
    var numPoints = 1100
    var numClusters = 1
    var strategyDistance = "avg"
    var checkPointDir = "B:\\checkPoints"
    var distanceMethod = "Euclidean"
    var typDataSet = 1
    var classIndex = 0
    var checkMatrix = 20
    var checkTotal = 200

    if (args.length > 2) {
      origen = args(0)
      destino = args(1)
      numPartitions = args(2).toInt
      numPoints = args(3).toInt
      numClusters = args(4).toInt
      strategyDistance = args(5)
      checkPointDir = args(6)
      distanceMethod = args(7)
      typDataSet = args(8).toInt
      classIndex = args(9).toInt
      checkMatrix = args(10).toInt
      checkTotal = args(11).toInt
    }

    spark.sparkContext.setCheckpointDir(checkPointDir)

    //Load data from csv
    val dataDF = spark.read
      .option("header", "false")
      .option("inferSchema", "true")
      .option("delimiter", ",")
      .csv(origen)


    //Filtering DataFrame
    val dataDFFiltered = typDataSet match {
      //It is not necessary to remove any column
      case 0 =>
        dataDF.map(_.toSeq.asInstanceOf[Seq[Double]])
      //It is necessary to delete the class column
      case 1 =>
        dataDF.drop(s"_c$classIndex").map(_.toSeq.asInstanceOf[Seq[Double]])
//      //It is necessary to eliminate both the identifier and the class
//      case 2 =>
//        dataDF.drop(idIndex,classIndex).map(_.toSeq.asInstanceOf[Seq[Double]])
    }

    //Generate automatically an index for each row
    val dataAux = dataDFFiltered.withColumn("index", monotonically_increasing_id()+1)

    //Rename the columns and generate a new DataFrame copy of the previous to be able to do the subsequent filtered out in the join
    val newColumnsNames = Seq("valueAux", "indexAux")
    val dataAuxRenamed = dataAux.toDF(newColumnsNames: _*)

    //Calculate the distance between all points
    val distances = dataAux.crossJoin(dataAuxRenamed)
      .filter(r => r.getLong(1) < r.getLong(3))
      .map{r =>
        //Depending on the method chosen one to perform the distance, the value of the same will change
        val dist = distanceMethod match {

          case "Euclidean" =>
            distEuclidean(r.getSeq[Double](0), r.getSeq[Double](2))
        }

        //Return the result saving: (point 1, point 2, the distance between both)
        (r.getLong(1), r.getLong(3), dist)
      }

    //Save the distances between all points in a RDD[Distance]
    val distancesRDD =
    distances
      .rdd.map(_.toString().replace("(", "").replace(")", ""))
      .map(s => s.split(',').map(_.toFloat))
      .map { case x =>
        new Distance(x(0).toInt, x(1).toInt, x(2))
      }.filter(x => x.getIdW1 < x.getIdW2).repartition(numPartitions)

    //min,max,avg
    val linkage = new Linkage(numClusters, strategyDistance)
    println("New Linkage with strategy: " + strategyDistance)
    val model = linkage.runAlgorithm(distancesRDD, numPoints, checkMatrix, checkTotal)

    println("SCHEMA RESULT: ")
    model.printSchema(";")

    println("Saving schema: ")
    model.saveSchema(destino)
    println("--Saved schema--")

    val duration = (System.nanoTime - start) / 1e9d
    println(s"TIME TOTAL: $duration")

    spark.stop()
  }

  /**
    * Calculate the Euclidena distance between tow points
    * @param v1    First Vector in Sequence to Double
    * @param v2 Second Vector in Sequence to Double
    * @return Return the Euclidena distance between tow points
    * @example distEuclidean(v1, v2)
    */
  def distEuclidean(v1: Seq[Double], v2: Seq[Double]): Double = {
    require(v1.size == v2.size, s"Vector dimensions do not match: Dim(v1)=${v1.size} and Dim(v2)" +
      s"=${v2.size}.")
    var squaredDistance = 0.0

    var kv = 0
    val sz = v1.size
    while (kv < sz) {
      val score = v1.apply(kv) - v2.apply(kv)
      squaredDistance += Math.abs(score*score)
      kv += 1
    }
    math.sqrt(squaredDistance)
  }

}
