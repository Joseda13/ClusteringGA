package es.us.spark.mllib.clustering.validation

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.mllib.clustering.{BisectingKMeans, KMeans}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.SparkSession

object Indices {

  def getInternalIndices(features: Array[Int]) :(Double, Double, Double, Double) = {
    var i = 1
    var s = ""
    val test = features(features.length-1)
    val spark = SparkSession.builder()
      .appName(s"Featuring Clusters $test")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val sc = spark.sparkContext

    val dataFile = "B:\\DataSets_Genetics\\dataset_104.csv"

    val idIndex = -1
    val classIndex = 10
    val delimiter = ","

    val dataRead = spark.read
      .option("header", "false")
      .option("inferSchema", "true")
      .option("delimiter", delimiter)
      .csv(dataFile)
//      .repartition(1)
      .cache()

    var data = dataRead.drop("_c10")

    for (i <- 0 to features.length - 2){
      if (features(i) == 0){
        data = data.drop(s"_c$i")
      }
    }

    val parsedData = data.map(_.toSeq.asInstanceOf[Seq[Double]]).rdd.map(s => Vectors.dense(s.toArray)).cache()

    //val clusters = KMeans.train(parsedData, numClusters, numIterations, 1, "k-means||", Utils.giveMeTime())
    val clusters = new KMeans()
      .setK(features(features.length-1))
      .setMaxIterations(100)
      .setSeed(1L)
      .run(parsedData)

    //Global Center
    val centroides = sc.parallelize(clusters.clusterCenters)
    val centroidesCartesian = centroides.cartesian(centroides).filter(x => x._1 != x._2).cache()

    var startTimeK = System.currentTimeMillis

    val intraMean = clusters.computeCost(parsedData) / parsedData.count()
    val interMeanAux = centroidesCartesian.map(x => Vectors.sqdist(x._1, x._2)).reduce(_ + _)
    val interMean = interMeanAux / centroidesCartesian.count()
    /*val clusterCentroides = KMeans.train(centroides, 1, numIterations)
    val interMean = clusterCentroides.computeCost(centroides) / centroides.count()
*/
    //Get Silhoutte index: (intercluster - intracluster)/Max(intercluster,intracluster)
    val silhoutte = (interMean - intraMean) / (if (interMean > intraMean) interMean else intraMean)
    s += i + ";" + silhoutte + ";"

    var stopTimeK = System.currentTimeMillis
    val elapsedTimeSil = (stopTimeK - startTimeK)


    //DUNN
    startTimeK = System.currentTimeMillis

    //Min distance between centroids
    val minA = centroidesCartesian.map(x => Vectors.sqdist(x._1, x._2)).min()

    /*
    //Min distance from centroids to global centroid
    val minA = centroides.map { x =>
      Vectors.sqdist(x, clusterCentroides.clusterCenters.head)
    }.min()
*/
    //Max distance from points to its centroid
    val maxB = parsedData.map { x =>
      Vectors.sqdist(x, clusters.clusterCenters(clusters.predict(x)))
    }.max

    //Get Dunn index: Mín(Dist centroides al centroide)/Max(dist punto al centroide)
    val dunn = minA / maxB

    stopTimeK = System.currentTimeMillis
    val elapsedTime = (stopTimeK - startTimeK)

    //DAVIES-BOULDIN
    startTimeK = System.currentTimeMillis

    val avgCentroid = parsedData.map { x =>
      //Vectors.sqdist(x, clusters.clusterCenters(clusters.predict(x)))
      (clusters.predict(x), x)
    }.map(x => (x._1, (Vectors.sqdist(x._2, clusters.clusterCenters(x._1)))))
      .mapValues(x => (x, 1))
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .mapValues(y => 1.0 * y._1 / y._2)
      .collectAsMap()

    val bcAvgCentroid = sc.broadcast(avgCentroid)

    val centroidesWithId = centroides.zipWithIndex()
      .map(_.swap).cache()

    val cartesianCentroides = centroidesWithId.cartesian(centroidesWithId).filter(x => x._1._1 != x._2._1)

    val davis = cartesianCentroides.map { case (x, y) => (x._1.toInt, (bcAvgCentroid.value(x._1.toInt) + bcAvgCentroid.value(y._1.toInt)) / Vectors.sqdist(x._2, y._2)) }
      .groupByKey()
      .map(_._2.max)
      .reduce(_ + _)

    val bouldin = davis / features(features.length-1)

    stopTimeK = System.currentTimeMillis
    val elapsedTimeDavies = (stopTimeK - startTimeK)

    //WSSSE
    startTimeK = System.currentTimeMillis
    val wssse = clusters.computeCost(parsedData)

    stopTimeK = System.currentTimeMillis
    val elapsedTimeW = (stopTimeK - startTimeK)

    spark.stop()

    (silhoutte, dunn, bouldin, wssse)
  }

  def getInternalIndicesNewVersion(features: Array[Int]) :(Double, Double, Double, Double) = {
    var i = 1
    var s = ""
    val test = features(features.length-1)
    val spark = SparkSession.builder()
      .appName(s"Featuring Clusters $test")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val sc = spark.sparkContext

    val dataFile = "B:\\DataSets_Genetics\\dataset_104.csv"

    val idIndex = -1
    val classIndex = 10
    val delimiter = ","

    val dataRead = spark.read
      .option("header", "false")
      .option("inferSchema", "true")
      .option("delimiter", delimiter)
      .csv(dataFile)
      //      .repartition(1)
      .cache()

    var data = dataRead.drop("_c10")

    for (i <- 0 to features.length - 2){
      if (features(i) == 0){
        data = data.drop(s"_c$i")
      }
    }

    val parsedData = data.map(_.toSeq.asInstanceOf[Seq[Double]]).rdd.map(s => Vectors.dense(s.toArray)).cache()

    //val clusters = KMeans.train(parsedData, numClusters, numIterations, 1, "k-means||", Utils.giveMeTime())
//    val clusters = new BisectingKMeans()
    val clusters = new KMeans()
      .setK(features(features.length-1))
      .setMaxIterations(100)
      .setSeed(1L)
      .run(parsedData)

    //Global Center
    val centroides = sc.parallelize(clusters.clusterCenters)
    val centroidesCartesian = centroides.cartesian(centroides).filter(x => x._1 != x._2).cache()

    val intraMean = (((6*clusters.computeCost(parsedData))-1) /(parsedData.first().size-1)) / parsedData.count()
    val interMeanAux = centroidesCartesian.map(x => (((6*Vectors.sqdist(x._1, x._2))-1) / (x._1.size-1)) ).reduce(_ + _)
    val interMean = interMeanAux / centroidesCartesian.count()
    /*val clusterCentroides = KMeans.train(centroides, 1, numIterations)
    val interMean = clusterCentroides.computeCost(centroides) / centroides.count()
*/
    //Get Silhoutte index: (intercluster - intracluster)/Max(intercluster,intracluster)
    val silhoutte = (interMean - intraMean) / (if (interMean > intraMean) interMean else intraMean)
    s += i + ";" + silhoutte + ";"

    //DUNN
    //Min distance between centroids
    val minA = centroidesCartesian.map(x => (((6*Vectors.sqdist(x._1, x._2))-1) / (x._1.size - 1)) ).min()

    /*
    //Min distance from centroids to global centroid
    val minA = centroides.map { x =>
      Vectors.sqdist(x, clusterCentroides.clusterCenters.head)
    }.min()
*/
    //Max distance from points to its centroid
    val maxB = parsedData.map { x =>
      ((((6*Vectors.sqdist(x, clusters.clusterCenters(clusters.predict(x)))) - 1)) / (x.size - 1))
    }.max

    //Get Dunn index: Mín(Dist centroides al centroide)/Max(dist punto al centroide)
    val dunn = minA / maxB

    //DAVIES-BOULDIN
    val avgCentroid = parsedData.map { x =>
      //Vectors.sqdist(x, clusters.clusterCenters(clusters.predict(x)))
      (clusters.predict(x), x)
    }.map(x => (x._1, (((6*Vectors.sqdist(x._2, clusters.clusterCenters(x._1))) - 1) / (x._2.size - 1)) ))
      .mapValues(x => (x, 1))
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .mapValues(y => 1.0 * y._1 / y._2)
      .collectAsMap()

    val bcAvgCentroid = sc.broadcast(avgCentroid)

    val centroidesWithId = centroides.zipWithIndex()
      .map(_.swap).cache()

    val cartesianCentroides = centroidesWithId.cartesian(centroidesWithId).filter(x => x._1._1 != x._2._1)

    val davis = cartesianCentroides.map { case (x, y) => (x._1.toInt, (bcAvgCentroid.value(x._1.toInt) + bcAvgCentroid.value(y._1.toInt))
      / (((6*Vectors.sqdist(x._2, y._2)) - 1) / (x._2.size - 1)) ) }
      .groupByKey()
      .map(_._2.max)
      .reduce(_ + _)

    val bouldin = davis / features(features.length-1)

    //WSSSE
    val wssse = (((6*clusters.computeCost(parsedData))-1) / (parsedData.first().size-1))

    spark.stop()

    (silhoutte, dunn, bouldin, wssse)
  }

}
