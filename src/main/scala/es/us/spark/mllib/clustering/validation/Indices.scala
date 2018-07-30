package es.us.spark.mllib.clustering.validation

import breeze.linalg.min
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.mllib.clustering.{BisectingKMeans, KMeans}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
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

  /**
    * Calculate Silhouette index.
    *
    * @param data Array with the Vector for each cluster.
    * @return Return B average, A average and Silhouette value.
    * @example getSilhouette(data)
    */
  def getSilhouette(data: Array[(Int, scala.Iterable[Vector])]): (Double, Double, Double) = {
    //Set up the global variables
    var b = 0.0
    var a = 0.0
    var silhouette = 0.0

    //Set up each iteration variables
    var b_i_j = 0.0
    var a_i_p = 0.0
    var b_i = 0.0
    var a_i = 0.0
    var s_i = 0.0

    //For each cluster K
    for (k <- data.map(_._1)){
      val points_K = data.filter(_._1 == k)

      //For each i point into the cluster K
      for (i <- points_K.flatMap(_._2)){
        //Reset the variables to each i point
        b_i = 0
        a_i = 0
        s_i = 0

        a_i_p = 0.0

        //For each cluster M distinct to K
        val points_M = data.filter(_._1 != k)
        for (m <- points_M.map(_._1)){
          b_i_j = 0.0

          //For each j point into the cluster M
          for (j <- points_M.filter(_._1 == m).flatMap(_._2)){
            //Add the distance between the i point and j point
            b_i_j += Vectors.sqdist(i,j)
          }

          //Calculate the average distance between the i point and the cluster M
          b_i_j = b_i_j / (points_M.filter(_._1 == m).map(_._2.size).head)

          //Save the minimum average distance between the i point and the cluster M
          if (b_i != 0){
            if (b_i_j < b_i){
              b_i = b_i_j
            }
          } else{
            b_i = b_i_j
          }

        }

        //For each p point into the cluster K distinct to i point
        for (p <- points_K.flatMap(_._2) if p != i){
          //Add the distance between the i point and the p point
          a_i_p += Vectors.sqdist(i,p)
        }

        //Calculate the average distance between the i point and the rest of point in the cluster K
        a_i = a_i_p / (points_K.map(_._2.size).head - 1)

        //Calculate the silhouette to the i point
        s_i = (b_i - a_i) / Math.max(b_i,a_i)

        //Update the global variables
        b += b_i
        a += a_i
        silhouette += s_i
      }
    }

    //Calculate the average global variables
    b = b / data.map(_._2.size).sum
    a = a / data.map(_._2.size).sum
    silhouette = silhouette / data.map(_._2.size).sum

    (b, a, silhouette)
  }

  /**
    * Calculate Dunn index with minimum inter-cluster and maximum intra-cluster average distances.
    *
    * @param data Array with the Vector for each cluster.
    * @return Return minimum inter-cluster average distance, maximum intra-cluster average distance and Dunn value.
    * @example getDunn(data)
    */
  def getDunn(data: Array[(Int, scala.Iterable[Vector])]): (Double, Double, Double) = {
    //Set up the global variables
    var intra = 0.0
    var inter = 0.0
    var dunn = 0.0

    //Set up the intra-cluster distance variables
    var sum_intra = 0.0
    var aux_intra = 0.0

    //Set up the inter-cluster distance variables
    var sum_inter = 0.0
    var aux_inter = 0.0

    //For each cluster K
    for (k <- data.map(_._1)){
      sum_intra = 0.0
      val points_K = data.filter(_._1 == k)

      //For i point into the cluster K
      for (i <- points_K.flatMap(_._2)){

        //Calculate the average inter-cluster distance only one time, when i point it's the first element to cluster K
        if (i == points_K.flatMap(_._2).head) {

          //For each cluster M distinct to K
          val points_M = data.filter(_._1 != k)
          for (m <- points_M.map(_._1)) {
            sum_inter = 0.0

            //For each j point into the cluster M
            for (j <- points_M.filter(_._1 == m).flatMap(_._2)) {
              //Add the distance between the i point and the j point
              sum_inter += Vectors.sqdist(i, j)

              //For each z point in the cluster K distinct to i
              for (z <- points_K.flatMap(_._2) if z != i) {
                //Add the distance between the i point and the p point
                sum_inter += Vectors.sqdist(z, j)
              }
            }

            //Calculate the average inter-cluster distance between the cluster K and the cluster M
            aux_inter = sum_inter / (points_K.map(_._2.size).head * points_M.filter(_._1 == m).map(_._2.size).head)

            //Save the minimum average inter-cluster distance
            if (inter != 0) {
              if (aux_inter < inter) {
                inter = aux_inter
              }
            } else {
              inter = aux_inter
            }
          }
        }

        //For each p point in the cluster K distinct to i
        for (p <- points_K.flatMap(_._2) if p != i){
          //Add the distance between the i point and the p point
          sum_intra += Vectors.sqdist(i,p)
        }
      }

      //Calculate the average intra-cluster distance in the cluster K
      aux_intra = sum_intra / (points_K.map(_._2.size).head * (points_K.map(_._2.size).head - 1))

      //Save the maximum average intra-cluster distance
      if (intra != 0){
        if (aux_intra > intra){
          intra = aux_intra
        }
      } else {
        intra = aux_intra
      }
    }

    //Calculate the dunn measure = minimum average inter-cluster distance / maximum average intra-cluster distance
    dunn = inter / intra

    (inter, intra, dunn)
  }

  /**
    * Calculate Dunn index with average inter-cluster distance and average intra-cluster distances.
    *
    * @param data Array with the Vector for each cluster.
    * @return Return inter-cluster average distance, intra-cluster average distance and Dunn value.
    * @example getDunnWithAverage(data)
    */
  def getDunnWithAverage(data: Array[(Int, scala.Iterable[Vector])]): (Double, Double, Double) = {
    //Set up the global variables
    var intra = 0.0
    var inter = 0.0
    var dunn = 0.0

    //Set up the intra-cluster distance variables
    var sum_intra = 0.0
    var aux_intra = 0.0

    //Set up the inter-cluster distance variables
    var sum_inter = 0.0
    var aux_inter = 0.0

    //For each cluster K
    for (k <- data.map(_._1)){
      sum_intra = 0.0
      val points_K = data.filter(_._1 == k)

      //For i point into the cluster K
      for (i <- points_K.flatMap(_._2)){

        //Calculate the average inter-cluster distance only one time, when i point it's the first element to cluster K
        if (i == points_K.flatMap(_._2).head) {

          //For each cluster M distinct to K
          val points_M = data.filter(_._1 != k)
          for (m <- points_M.map(_._1)) {
            sum_inter = 0.0

            //For each j point into the cluster M
            for (j <- points_M.filter(_._1 == m).flatMap(_._2)) {
              //Add the distance between the i point and the j point
              sum_inter += Vectors.sqdist(i, j)

              //For each z point in the cluster K distinct to i
              for (z <- points_K.flatMap(_._2) if z != i) {
                //Add the distance between the i point and the p point
                sum_inter += Vectors.sqdist(z, j)
              }
            }

            //Calculate the average inter-cluster distance between the cluster K and the cluster M and add to aux_inter
            aux_inter += sum_inter / (points_K.map(_._2.size).head * points_M.filter(_._1 == m).map(_._2.size).head)

          }
        }

        //For each p point in the cluster K distinct to i
        for (p <- points_K.flatMap(_._2) if p != i){
          //Add the distance between the i point and the p point
          sum_intra += Vectors.sqdist(i,p)
        }
      }

      //Calculate the average intra-cluster distance in the cluster K and add to aux_intra
      aux_intra += sum_intra / (points_K.map(_._2.size).head * (points_K.map(_._2.size).head - 1))

    }

    //Calculate the average inter-cluster distance
    inter = aux_inter / (data.length * (data.length - 1))

    //Calculate the average intra-cluster distance
    intra = aux_intra / data.length

    //Calculate the dunn measure = average inter-cluster distance / average intra-cluster distance
    dunn = inter / intra

    (inter, intra, dunn)
  }

  /**
    * Calculate Dunn index with minimum inter-cluster distance and maximum intra-cluster distance.
    *
    * @param data Array with the Vector for each cluster.
    * @return Return minimum inter-cluster distance, maximum intra-cluster distance and Dunn value.
    * @example getDunnMinimumMaximum(data)
    */
  def getDunnMinimumMaximum(data: Array[(Int, scala.Iterable[Vector])]): (Double, Double, Double) = {
    //Set up the global variables
    var intra = 0.0
    var inter = 0.0
    var dunn = 0.0

    //Set up intra-cluster distance variable
    var aux_intra = 0.0

    //Set up inter-cluster distance variable
    var aux_inter = 0.0

    //For each cluster K
    for (k <- data.map(_._1)){

      val points_K = data.filter(_._1 == k)

      //For i point into the cluster K
      for (i <- points_K.flatMap(_._2)){

          //For each cluster M distinct to K
          val points_M = data.filter(_._1 != k)
          for (m <- points_M.map(_._1)) {

            //For each j point into the cluster M
            for (j <- points_M.filter(_._1 == m).flatMap(_._2)) {
              //Calculate the distance between the i point and the j point
              aux_inter = Vectors.sqdist(i, j)

              //Save the minimum inter-cluster distance
              if (inter != 0) {
                if (aux_inter < inter) {
                  inter = aux_inter
                }
              } else {
                inter = aux_inter
              }
            }
        }

        //For each p point in the cluster K distinct to i
        for (p <- points_K.flatMap(_._2) if p != i){
          //Calculate the distance between the i point and the p point
          aux_intra = Vectors.sqdist(i,p)

          //Save the maximum intra-cluster distance
          if (intra != 0){
            if (aux_intra > intra){
              intra = aux_intra
            }
          } else {
            intra = aux_intra
          }
        }
      }
    }

    //Calculate the dunn measure = minimum inter-cluster distance / maximum intra-cluster distance
    dunn = inter / intra

    (inter, intra, dunn)
  }

  /**
    * Calculate Dunn index with minimum inter-cluster centroid distance and maximum intra-cluster centroid average distance.
    *
    * @param data Array with the Vector for each cluster.
    * @return Return minimum inter-cluster centroid distance, maximum intra-cluster centroid average distance and Dunn value.
    * @example getDunnCentroids(data)
    */
  def getDunnCentroids(data: Array[(Int, scala.Iterable[Vector])]): (Double, Double, Double) = {
    //Set up the global variables
    var intra = 0.0
    var inter = 0.0
    var dunn = 0.0

    //Set up the intra-cluster distance variables
    var sum_intra = 0.0
    var aux_intra = 0.0

    //Set up inter-cluster distance variable
    var aux_inter = 0.0

    //Calculate the centroids of the cluster in the DataSet
    val centroids = data.map(cluster => (cluster._1,calculateMean(cluster._2)))

    //For each cluster K
    for (k <- data.map(_._1)){
      sum_intra = 0.0
      val points_K = data.filter(_._1 == k)

      //Save the centroid of the cluster K
      val centroid_k = centroids.filter(_._1 == k).head._2

      //Search all distinct centroids of cluster K
      val centroids_M = centroids.filter(_._1 != k)

      //For each centroid_m of cluster M
      for (centroid_m <- centroids_M.map(_._2)){

        //Calculate the distance between the centroid of the cluster K and the centroid of the cluster M
        aux_inter = Vectors.sqdist(centroid_k,centroid_m)

        //Save the minimum inter-cluster centroid distance
        if (inter != 0) {
          if (aux_inter < inter) {
            inter = aux_inter
          }
        } else {
          inter = aux_inter
        }
      }

      //For i point into the cluster K
      for (i <- points_K.flatMap(_._2)){

        //Add the distance between the i point and the centroid of cluster K
        sum_intra += Vectors.sqdist(i,centroid_k)
      }

      //Calculate the intra-cluster centroid average distance in the cluster K
      aux_intra = sum_intra / (points_K.map(_._2.size).head)

      //Save the maximum intra-cluster centroid average distance
      if (intra != 0){
        if (aux_intra > intra){
          intra = aux_intra
        }
      } else {
        intra = aux_intra
      }
    }

    //Calculate the dunn measure = minimum inter-cluster centroid distance / maximum intra-cluster centroid average distance
    dunn = inter / intra

    (inter, intra, dunn)
  }

  /**
    * Calculate Centroid to Iterable[Vector].
    *
    * @param vectors Array with the Vector values.
    * @return Return the centroid of the Iterable[Vector].
    * @example calculateMean(vectors)
    */
  def calculateMean(vectors: Iterable[Vector]): Vector = {

    val vectorsCalculateMean = vectors.map(v => v.toArray.map(d => (d/vectors.size)))

    val sumArray = new Array[Double](vectorsCalculateMean.head.length)
    val auxSumArray = vectorsCalculateMean.map{
      case va =>
        var a = 0
        while (a < va.length){
          sumArray(a) += va.apply(a)
          a += 1
        }
        sumArray
    }.head

    Vectors.dense(auxSumArray)
  }

}
