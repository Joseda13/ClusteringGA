package es.us

import es.us.ga.Chromosome_Clustering
import es.us.spark.mllib.clustering.validation.FeatureStatistics
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.types.IntegerType
//import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.monotonically_increasing_id

object MainConfusionMatrix {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val spark = SparkSession
      .builder()
      .appName("Confusion Matrix")
      .master("local[*]")
      .getOrCreate()

    var origen = "B:\\DataSets_Real\\g2-512-30.txt"
    var origenTest = "B:\\DataSets_Real\\g2-512-30-gt.pa"
    var chromo_Result = "[1,1,1,1,1,1,1,0,1,1,1,1,0,0,1,1,1,0,1,1,1,1,1,0,1,0,1,0,0,1,1,1,0,1,1,1,1,1,1,1,1,1,1,1,0,0,1,1,0,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,0,1,1,1,1,1,1,1,1,0,1,1,1,1,1,1,1,0,1,1,0,1,1,1,0,0,1,0,0,1,1,0,1,1,1,1,1,0,1,0,1,0,0,1,0,0,1,1,1,0,1,0,1,1,0,0,1,0,1,0,0,0,0,1,0,1,1,1,0,1,1,0,0,0,0,1,0,0,1,1,0,0,0,1,0,0,1,1,1,1,0,0,0,1,0,0,1,1,1,1,1,0,1,1,0,0,1,1,1,1,1,1,0,0,0,0,1,0,0,1,1,0,0,0,0,0,1,0,1,1,1,0,0,0,1,0,1,1,1,1,1,0,0,1,0,1,1,0,0,1,1,0,0,0,0,1,1,1,0,1,1,0,1,1,0,0,1,1,0,1,0,0,0,1,0,1,1,1,1,0,0,0,1,1,1,0,1,1,1,1,1,0,1,0,0,1,1,0,1,1,0,0,1,1,0,1,1,0,0,1,1,0,1,1,0,1,1,0,1,1,1,0,0,1,0,1,1,1,1,1,0,0,1,0,0,1,1,1,0,1,0,0,1,1,0,0,1,1,1,1,1,0,0,0,0,0,1,1,1,0,1,0,0,1,0,1,0,0,1,1,1,1,0,0,0,1,0,0,1,0,1,1,0,1,0,0,1,1,1,1,1,0,0,0,0,0,1,1,0,1,1,0,0,1,0,1,1,0,1,1,0,0,1,1,0,0,1,1,0,0,0,1,0,0,0,0,1,0,1,1,1,0,1,0,0,1,0,0,1,1,1,1,0,1,0,1,1,0,1,1,0,0,1,0,0,1,1,0,1,1,0,0,0,1,1,1,1,1,0,1,0,0,0,1,0,0,1,0,1,1,1,0,1,1,1,0,1,1,1,1,1,0,0,0,1,1,1,1,0,1,0,0,0,1,0,1,1,1,0,1,0,0,1,1,1,1,1,1,0,1,0,0,0,0,0,0,1,1,0,0,1,0,0,1,0,0,2]"
    var K = 2
    var dimension = 512
    var idIndex = -1
    var classIndex = -1
    var delimiter = ","
    var destination = "B:\\Results\\Confusion_Matrix_Real_DB\\g2-512-30.txt"

    val chromosome = Chromosome_Clustering.create_Chromosome_Clustering_From_String(chromo_Result)
    chromosome.setNV(dimension)
    println("Chromosome Result: " + chromosome.toSpecialString + ", number of features: " + chromosome.contAttributeGood())

    val dataRead = spark.read
      .option("header", "false")
      .option("inferSchema", "true")
      .option("delimiter", delimiter)
      .csv(origen)

    val dataReadTest = spark.read
      .option("header", "false")
      .option("inferSchema", "true")
      .option("delimiter", delimiter)
      .csv(origenTest)

    val dataIndex = dataRead.withColumn("id",monotonically_increasing_id())

    val dataC = dataReadTest.withColumn("id_2",monotonically_increasing_id()).withColumnRenamed("_c0","class")

    var data = dataC.crossJoin(dataIndex).filter(r => r.getLong(1) == r.getLong(dimension+2)).drop("id").drop("id_2")
//    data.show(10)
    //Si el fichero tiene indice, es eliminado, si no simplemente cambiamos el nombre a la columna
//    var data = if (idIndex != -1) {
//      dataRead.drop(s"_c$idIndex")
//        .withColumnRenamed(dataRead.columns(classIndex), "class")
//    } else {
//      dataRead.withColumnRenamed(dataRead.columns(classIndex), "class")
//    }

    //If the gen to the chromosome if == 0, then delete its column to the DataSet
    for (i <- 0 to chromosome.getGenes.length - 2){
      if (chromosome.getGenes.apply(i) == 0){
//        val index = i + 1
        val index = i
        data = data.drop(s"_c$index")
      }
    }

//    val featureColumns = data.drop("class").drop("_c7").drop("_c6").drop("_c5").drop("_c4")
//      .drop("_c2").drop("_c1").columns
    val featureColumns = data.drop("class").columns
    val featureAssembler = new VectorAssembler().setInputCols(featureColumns).setOutputCol("features")
    val df_kmeans = featureAssembler.transform(data).select("class", "features")
//    df_kmeans.show(20,false)

    val clusteringResult = new KMeans()
      .setK(K)
      .setMaxIter(100)
      .setSeed(K)
      .setFeaturesCol("features")

    val model = clusteringResult.fit(df_kmeans)

    var predictionResult = model.transform(df_kmeans)
      .select("class", "prediction", "features")

    predictionResult = predictionResult.withColumn("prediction", predictionResult("prediction").cast(IntegerType))
//    predictionResult.show(40)

//    val predictionWithIndex = predictionResult.select("prediction", "class").withColumn("index", monotonically_increasing_id() + 1)
//    predictionWithIndex.show(30)

    val result = FeatureStatistics.getConfusionMatrix(List("class"), predictionResult, K, destination)

//    predictionWithIndex.rdd.map(x => x.toString.replace("[", "").replace("]", ""))
//      .coalesce(1, shuffle = true)
//      .saveAsTextFile(s"Copia_test")

//    val auxPredictionRDD = predictionRDD.map{row =>
//
//      if (row.getInt(0) != row.getInt(1)) {
//        contError += 1
//      }
////      println(s"Number of points with error in the clustering: $contError")
//      row
//    }.count()

//    println(s"Number of points with error in the clustering: $contError")

//    var dataFeatures = dataRead.drop(s"_c$classIndex")
//
//    //Save all columns less the class column
//    val columnsDataSet = dataFeatures.columns
//
//    val parsedData = dataRead.rdd.map { r =>
//
//      //Create a Array[Double] with the values of each column to the DataSet read
//      val vectorValues = for (co <- columnsDataSet) yield{
//
//        //If the column number have two digits
//        if(co.length == 4) {
//          r.getDouble((co.charAt(2).toInt + co.charAt(3).toInt) - 87)
//        }
//        //If the column number have one digit
//        else {
//          r.getDouble(co.charAt(2).toInt - 48)
//        }
//      }
//
//      //Create a Vector with the Array[Vector] of each row in the DataSet read
//      val auxVector = Vectors.dense(vectorValues)
//
//      //Return the Cluster ID and the Vector for each row in the DataSet read
//      (auxVector)
//    }
//
//    val clusters = new KMeans()
//      .setK(7)
//      .setMaxIterations(100)
//      .setSeed(7)
//      .run(parsedData)
//
//    val dataDF = parsedData.map(v => (v , clusters.predict(v)) ).toDF()
//
//    dataDF.show(100,false)

  }
}
