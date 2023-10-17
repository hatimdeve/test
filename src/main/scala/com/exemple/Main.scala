package com.exemple

import java.io.File
import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.api.java.JavaRDD.fromRDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
object Main {

  def build():SparkSession={
    val spark=SparkSession.builder().appName("test").master("local[*]")
      .config("spark.eventLog.enabled", "true")
      .config("spark.eventLog.dir", "file:///C:/Users/Lenovo/Desktop/spark_events")
      .config(" spark.history.fs.logDirectory", "file:///C:/Users/Lenovo/Desktop/spark_events")
      .getOrCreate()
    return spark
  }
  def getfile(sc:SparkSession):Unit={
    val licence_file=sc.sparkContext.textFile("file:///C:/spark/spark/licenses/LICENSE-slf4j.txt",4)
    println(licence_file.getNumPartitions)
    println(licence_file.count())
  }
  def getallfile(sc:SparkSession): Unit = {
    val all_licence_file = sc.sparkContext.textFile("file:///C:/spark/spark/licenses/*.txt", 4)
    println(all_licence_file.getNumPartitions)
    println(all_licence_file.count())
  }
  def createrdd(sc:SparkSession): Unit = {
    val listofstrs = List("On manipule", "un RDD de type", "chaine de caractères")
    val chaines = sc.sparkContext.parallelize(listofstrs)
  }
  def rdd_sample_takesample(sc:SparkSession):Unit={
    val rangerdd = sc.sparkContext.range(0, 20, 1, 2)
    rangerdd.sample(false,0.6).foreach(l => println(l))
    rangerdd.takeSample(false,4).foreach(l => println(l))
  }
  def rdd_filter(sc:SparkSession): Unit = {
    val log_file_rdd = sc.sparkContext.textFile("file:///C:/Users/Lenovo/Desktop/big data analyst/big data 2/tp/datasets/spark_log.txt")
    System.out.println("nombre de lignes de log : " + log_file_rdd.count())

    //filter les enregistrements pour les erreurs / warnings et info
    val errors_only = log_file_rdd.filter(s => s.contains("ERROR"))
    println("nombre de lignes de log : " + errors_only.count())

    val warn_only = log_file_rdd.filter(s => s.contains("WARN"))
    warn_only.persist(StorageLevel.MEMORY_AND_DISK_2)
    warn_only.coalesce(1, true).saveAsTextFile("C:/Users/Lenovo/Desktop/big data analyst/big data 2/tp/datasets/Warnings_2")

  }
  def rdd_map(sc:SparkSession):Unit={
    val chaines = sc.sparkContext.parallelize(List("On manipule", "un RDD de type", "chaine de caractères"))
    val CHAINES = chaines.map(s => s.toUpperCase()) //lambda
    CHAINES.foreach(println)

  }
  def rdd_flatmap(sc:SparkSession):Unit={
    val chaines = sc.sparkContext.parallelize(List("On manipule", "un RDD de type", "chaine de caractères"))
    val chaines_map = chaines.map(s => s.split(" ").toList)
    val chaines_flatmap = chaines.flatMap(s => s.split(" "))
    chaines_map.foreach(println)
    chaines_flatmap.foreach(println)

  }
  def rdd_distinct(sc:SparkSession):Unit={
    val names = sc.sparkContext.parallelize(List("Tom", "Lenevo", "Anvisha", "John", "Jimmy", "Jacky", "John", "Jimmy", "Jimmy"))
    val distRdd = names.distinct()
    distRdd.foreach(println)

  }
  def rdd_groupby(sc:SparkSession):Unit={
    val names = sc.sparkContext.parallelize(List("Tom", "Lenevo", "Anvisha", "John", "Jimmy", "Jacky", "John", "Jimmy", "Jimmy"))
    val groupedNames = names.groupBy(s => s.length())
    groupedNames.foreach(tuple => println(tuple._1 + "--->" + tuple._2))

  }
  def rdd_sortby(sc:SparkSession): Unit = {
    val names = sc.sparkContext.parallelize(List("Tom", "Lenevo", "Anvisha", "John", "Jimmy", "Jacky", "John", "Jimmy", "Jimmy"))
    val sortedRdd = names.sortBy(s => s.length(), false, 1)
    sortedRdd.foreach(println)

  }
  def rdd_union(sc:SparkSession):Unit={
    val impairs = sc.sparkContext.parallelize(List(1, 3, 5, 7, 9))
    val fibonacci = sc.sparkContext.parallelize(List(0, 1, 2, 3, 5, 8))
    impairs.union(fibonacci).foreach(println)

  }
  def rdd_substract(sc:SparkSession): Unit = {
    val impairs = sc.sparkContext.parallelize(List(1, 3, 5, 7, 9))
    val fibonacci = sc.sparkContext.parallelize(List(0, 1, 2, 3, 5, 8))
    impairs.intersection(fibonacci).foreach(println)

  }

  def rdd_intersection(sc: SparkSession): Unit = {
    val impairs = sc.sparkContext.parallelize(List(1, 3, 5, 7, 9))
    val fibonacci = sc.sparkContext.parallelize(List(0, 1, 2, 3, 5, 8))
    impairs.subtract(fibonacci).foreach(println)

  }
  def rdd_pairs(sc:SparkSession):Unit={
    val data = List(new Tuple2("city", "San Francisco"), new Tuple2("state", "California"), new Tuple2("zip", "94102–94105"), new Tuple2("country", "USA"))
    val city_info = sc.sparkContext.parallelize(data)
    city_info.keys.foreach(println)
    city_info.values.foreach(println)

  }
  def rdd_keyby(sc:SparkSession):Unit={
    val data = List(new Tuple3("Rabat", "Morocco", 1), new Tuple3("Paris", "France", 2), new Tuple3("Madrid", "Spain", 3), new Tuple3("Lisbon", "Portugal", 4))
    val city_info = sc.sparkContext.parallelize(data)
    val city_info_pair = city_info.keyBy(tuple => tuple._3)
    city_info_pair.foreach(tuple => println(tuple._1 + "--->(" + tuple._2._1 + "/" + tuple._2._2 + "/" + tuple._2._3 + ")"))

  }
  def rdd_pairmap(sc:SparkSession): Unit = {
    //Application de mapValues et flatMapValues
    val temperature_file = sc.sparkContext.textFile("file:///C:/Users/Lenovo/Desktop/big data analyst/big data 2/tp/datasets/temperature.txt")
    val temp_PairRDD = temperature_file.map(s => (s.split(",")(0), s.split(",")(1)))

    val temp_Prdd_mapVal = temp_PairRDD.mapValues(v => v.split(";"))
    val temp_Prdd_flatMapVal = temp_PairRDD.flatMapValues(v => v.split(";"))
    temp_PairRDD.foreach(tuple => println(tuple._1 + "-->" + tuple._2))
    temp_Prdd_mapVal.foreach(tuple => println(tuple._1 + "-->" + tuple._2.toList))
    temp_Prdd_flatMapVal.foreach(tuple => println(tuple._1 + "-->" + tuple._2))

  }
  def rdd_groupbykey(sc:SparkSession):Unit={
    def Avg(array: Iterable[String]): Double = {
      var sum = 0;
      var count = 0;
      for (v <- array) {
        sum += v.toInt;
        count += 1;
      }
      return sum / count.toDouble
    }
    val temperature_file = sc.sparkContext.textFile("file:///C:/Users/Lenovo/Desktop/big data analyst/big data 2/tp/datasets/temperature.txt")
    val temp_PairRDD = temperature_file.map(s => (s.split(",")(0), s.split(",")(1)))


    val temp_Prdd_flatMapVal = temp_PairRDD.flatMapValues(v => v.split(";"))
    val sumTempByLoc = temp_Prdd_flatMapVal.groupByKey().mapValues(arr => Avg(arr))
    sumTempByLoc.foreach(tuple => println(tuple._1 + "-->" + tuple._2))

  }
  def rdd_reducebykey(sc:SparkSession): Unit = {
    val temperature_file = sc.sparkContext.textFile("file:///C:/Users/Lenovo/Desktop/big data analyst/big data 2/tp/datasets/temperature.txt")
    val temp_PairRDD = temperature_file.map(s => (s.split(",")(0), s.split(",")(1)))


    val temp_Prdd_flatMapVal = temp_PairRDD.flatMapValues(v => v.split(";"))
    val tmp_avg_values = temp_Prdd_flatMapVal.mapValues(s => (s.toInt, 1)).reduceByKey((t1, t2) => (t1._1 + t2._1, t1._2 + t2._2)).mapValues(tuple => tuple._1 / tuple._2.toDouble)

    tmp_avg_values.foreach(t => println(t._1 + "-->" + t._2))

  }
  def rdd_foldbykey(sc:SparkSession):Unit={
    val temperature_file = sc.sparkContext.textFile("file:///C:/Users/Lenovo/Desktop/big data analyst/big data 2/tp/datasets/temperature.txt")
    val temp_PairRDD = temperature_file.map(s => (s.split(",")(0), s.split(",")(1)))


    val temp_Prdd_flatMapVal = temp_PairRDD.flatMapValues(v => v.split(";"))
    val tmp_max_values = temp_Prdd_flatMapVal.mapValues(s => s.toInt).foldByKey(0)((v1, v2) => {
      if (v1 > v2)
        return v1
      else return v2
    })


    tmp_max_values.foreach(t => println(t._1 + "-->" + t._2))

  }
  def rdd_sortbykey(sc:SparkSession): Unit = {
    val temperature_file = sc.sparkContext.textFile("file:///C:/Users/Lenovo/Desktop/big data analyst/big data 2/tp/datasets/temperature.txt")
    val temp_PairRDD = temperature_file.map(s => (s.split(",")(0), s.split(",")(1)))


    val temp_Prdd_flatMapVal = temp_PairRDD.flatMapValues(v => v.split(";"))
    val sorted_tmp_values = temp_Prdd_flatMapVal.mapValues(s => s.toInt).sortByKey()
    //Tri par la valeur de temperature
    val sorted_tmp_values_2 = temp_Prdd_flatMapVal.mapValues(s => s.toInt).mapToPair(tuple => (tuple._2,tuple._1)).sortByKey()

    sorted_tmp_values.foreach(tuple => println(tuple._1 + "-->" + tuple._2))
    sorted_tmp_values_2.foreach(tuple => println(tuple._1 + "-->" + tuple._2))

  }
  def rdd_substructbykey(sc:SparkSession):Unit={
    //Application de la transformation substractByKey
    val cities1_info = List(new Tuple2("Paris", new Tuple2(37.668819, -122.080795)), new Tuple2("Rabat", new Tuple2(49.6489, 7.3975)),
      new Tuple2("Lisbon", new Tuple2(38.820450, -77.050552)), new Tuple2("Madrid", new Tuple2(37.663712, 144.844788)))
    val cities2_info = List(new Tuple2("Fez", new Tuple2(64.0708333, -148.2236111)), new Tuple2("Rabat", new Tuple2(37.668819, -122.080795)),
      new Tuple2("Tangier", new Tuple2(38.820450, -77.050552)), new Tuple2("Marrakech", new Tuple2(38.878337, -77.100703)))

    val cities1 = sc.sparkContext.parallelize(cities1_info)
    val cities2 = sc.sparkContext.parallelize(cities2_info)
    println(cities1.subtractByKey(cities2).collect().toList)
    println(cities2.subtractByKey(cities1).collect().toList)

  }
  def rdd_jointure(sc:SparkSession):Unit={
    val stores = List(new Tuple2(100, "Fez"), new Tuple2(101, "Rabat"), new Tuple2(102, "Casa"), new Tuple2(103, "Tanger"), new Tuple2(104, "Marrakech"))

    val vendeurs = List(new Tuple3(1, "Ahmed", 100), new Tuple3(2, "Layth", 100), new Tuple3(1, "Jalal", 101), new Tuple3(1, "Youssef", 102), new Tuple3(1, "Hatim", 105), new Tuple3(1, "Hind", 103), new Tuple3(1, "Rajae", 104))

    val storesRDD = sc.sparkContext.parallelize(stores)
    val vendeursRDD = sc.sparkContext.parallelize(vendeurs)
    vendeursRDD.keyBy(t => t._3).join(storesRDD).foreach(println)
    vendeursRDD.keyBy(t => t._3).leftOuterJoin(storesRDD).foreach(println)
    vendeursRDD.keyBy(t => t._3).rightOuterJoin(storesRDD).foreach(println)
    vendeursRDD.keyBy(t => t._3).fullOuterJoin(storesRDD).foreach(println)
    vendeursRDD.keyBy(t => t._3).cogroup(storesRDD).foreach(println)
    vendeursRDD.keyBy(t => t._3).cartesian(storesRDD).foreach(println)

  }
  def rdd_action_count(sc:SparkSession):Unit={
    val readME = sc.sparkContext.textFile("file:///C:/spark/spark/README.md")
    val flatReadME = readME.flatMap(s => s.split(" ").iterator)
    println("le nombre de mots dans ce fichier est : " + flatReadME.count)

  }
  def rdd_action_take(sc:SparkSession):Unit={
    //l'action collect – take – takeOrdered - first
    val RELEASE_file = sc.sparkContext.textFile("file:///C:/spark/spark/RELEASE")
    val flatRELEASE = RELEASE_file.flatMap(s => s.split(" ").iterator)
    println ("le contenu du fichier est le suivant : ")
    println (flatRELEASE.collect.toList)
    println (flatRELEASE.take(4).toList)
    println (flatRELEASE.takeOrdered(4).toList)

    println(flatRELEASE.take(1).toList)
    println (flatRELEASE.first)

  }
def rdd_action_reduce(sc:SparkSession):Unit={
  val numbers = sc.sparkContext.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9))
  val somme = numbers.reduce((integer1, integer2) => integer1 + integer2)
  println ("la somme est : " + somme)

}
  def rdd_action_fold(sc:SparkSession): Unit = {
    val numbers = sc.sparkContext.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9))
    val somme = numbers.reduce((integer1, integer2) => integer1 + integer2)
    println ("la somme est : " + somme)

  }
  def rdd_action_nume(sc:SparkSession):Unit={
    val numbers = sc.sparkContext.parallelize(List(1, 22, 13, 34, 5, 16, 37, 28, 9))
    println("la somme est :" + numbers.sum())
    println("le minimum est :" + numbers.min())
    println("le maximum est :" + numbers.max())
    println("la moyenne est :" + numbers.mean())
    println("l ecart type est :" + numbers.stdev())
    println("la variance est :" + numbers.variance())
    println("Statistiques Generales :" + numbers.stats())
  }
  def rdd_mise_cache(sc:SparkSession):Unit={
    val shakespeare_rdd = sc.sparkContext.textFile("file:///C:/Users/Lenovo/Desktop/big data analyst/big data 2/tp/datasets/shakespeare.txt")
    val flatRDD = shakespeare_rdd.flatMap(l => l.split(" ")).map(s => (s, 1)).reduceByKey(_ + _)
    flatRDD.cache()
    println(flatRDD.count()) //déclenche le calcul
    flatRDD.take(4).foreach(println) //aucun calcul requis
    println(flatRDD.count()) //aucun calcul requis

  }
  def rdd_persisatance(sc:SparkSession):Unit={
    val shakespeare_rdd = sc.sparkContext.textFile("file:///C:/Users/Lenovo/Desktop/big data analyst/big data 2/tp/datasets/shakespeare.txt")
    val flatRDD = shakespeare_rdd.flatMap(l => l.split(" "))
      .map(s => (s, 1)).reduceByKey(_ + _)
    flatRDD.persist(StorageLevel.MEMORY_AND_DISK_SER_2)
    println (flatRDD.count())
    flatRDD.take(4).foreach(println)
    println (flatRDD.toDebugString)

  }
  def rdd_checkpoint(sc:SparkSession): Unit = {
    sc.sparkContext.setCheckpointDir("file:///C:/Users/Lenovo/Desktop/big data analyst/big data 2/tp/Checkpoint/data")
    //Chargez les fichier
    val shakespeare_rdd = sc.sparkContext.textFile("file:///C:/Users/Lenovo/Desktop/big data analyst/big data 2/tp/datasets/shakespeare.txt")
    val wordsPair = shakespeare_rdd.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)
    wordsPair.checkpoint()
    println (wordsPair.count())
    println (wordsPair.isCheckpointed)
    println (wordsPair.getCheckpointFile)

  }



  def word_count(sc: SparkSession): Unit = {
    // Load the Shakespeare text file
    val shakespeare_rdd = sc.sparkContext.textFile("file:///C:/Users/Lenovo/Desktop/big data analyst/big data 2/tp/datasets/shakespeare.txt")

    // Load stopwords and convert them to a set
    val stopwords = sc.sparkContext.textFile("file:///C:/Users/Lenovo/Desktop/big data analyst/big data 2/tp/datasets/stop-word-list.csv")
      .flatMap(line => line.split(",").map(_.trim.toLowerCase))
      .collect()
      .toSet

    // Filter out empty lines, split by spaces, and flatten the list of words
    val flatten_rdd = shakespeare_rdd
      .filter(line => line.length > 0)
      .flatMap(line => line.split(" "))

    // Filter stopwords and convert to lowercase
    val filteredCounts = flatten_rdd
      .filter(word => word.length > 0 && !stopwords.contains(word.toLowerCase))

    // Transform the text to lowercase, remove empty strings,
    // and convert it to key-value pairs (word, 1)
    val word_pairs = filteredCounts
      .map(word => (word.toLowerCase(), 1))
    // Partition the RDD into 'partitions' partitions for parallel processing
    val partitionedRDD = word_pairs.partitionBy(new HashPartitioner(4))

    // Cache the RDD to both memory and disk
    partitionedRDD.persist(StorageLevel.MEMORY_AND_DISK_SER_2)
    // Count the occurrences of each word
    val countsbyword = partitionedRDD.reduceByKey(_ + _)

    // Save the RDD to a local directory
    countsbyword.saveAsTextFile("C:/Users/Lenovo/Desktop/word_count")

    // Sort the word counts by frequency in descending order and take the top 7
    val topWords = countsbyword
      .sortBy(_._2, ascending = false)
      .take(7)

    topWords.foreach(tuple => println(tuple._1 + " ---> " + tuple._2))
    // Count the number of output files
    val outputDirectory = new File("C:/Users/Lenovo/Desktop/word_count")
    val numOutputFiles = outputDirectory.listFiles().length

    println(s"Number of output files: $numOutputFiles")
  }


  def main(args: Array[String]): Unit = {
     val sc=build()
    /* getfile(sc)
    getallfile(sc)*/
    //createrdd(sc)
   // rdd_sample_takesample(sc)
    //rdd_filter(sc)
    //rdd_map(sc)
    //rdd_flatmap(sc)
    //rdd_distinct(sc)
   // rdd_groupby(sc)
    //rdd_sortby(sc)
    //rdd_union(sc)
    //rdd_substract(sc)
    //rdd_intersection(sc)
   // rdd_pairs(sc)
   // rdd_keyby(sc)
   // rdd_pairmap(sc)
   // rdd_groupbykey(sc)
    //rdd_reducebykey(sc)
  //  rdd_foldbykey(sc)
   // rdd_sortbykey(sc)
    //rdd_substructbykey(sc)
   // rdd_jointure(sc)
    //rdd_action_count(sc)
    //rdd_action_take(sc)
    //rdd_action_reduce(sc)
    //rdd_action_fold(sc)
    //rdd_action_nume(sc)
    //rdd_mise_cache(sc)
    //rdd_persisatance(sc)
    //rdd_checkpoint(sc)
    word_count(sc)
}
  }
