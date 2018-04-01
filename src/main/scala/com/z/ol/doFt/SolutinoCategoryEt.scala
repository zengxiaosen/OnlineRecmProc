package com.z.ol.doFt

import scala.collection.mutable

/**
  * Created by root on 18-4-1.
  */
class SolutinoCategoryEt(mongo_input_uri_item_profile: String, mongo_input_split_size: String,
                         entityScalar: Double,
                         modeOnline: Boolean = false,
                         only_memory_positive_tag: Boolean = true) extends DynamicFeatureSolution {
  val categorySparse: VectorSparseFeature = new VectorSparseFeature(13)

  val modelTrain = new ProfileModelWithTag3(null, null, categorySparse, null, null)
  val modelRec = new ProfileModelWithTag(null, null, categorySparse, null, null)

  //
  var pathGlobal: String = _
  var pathUsers: String = _
  var pathItems: String = _
  var pathUserInferredEntities: String = _

  def setPath(init_dir: String): Unit = {
    pathGlobal = s"$init_dir/global"
    pathUsers = s"$init_dir/user_inferred*"
    pathItems = s"$init_dir/item_inferred*"
    pathUserInferredEntities = s"$init_dir/user_entity*"
  }

  override def init(sc: SparkContext, init_dir: String, no_weight_change: Boolean): Unit = {
    setPath(init_dir)
  }

  override def save(sc: SparkContext, store_dir: String): Unit = {
    modelTrain.saveGlobal(sc, s"$store_dir/global")
    modelTrain.saveUsers(sc, s"$store_dir/user_inferred_update")
    modelTrain.saveItems(sc, s"$store_dir/item_inferred_update")
    modelTrain.saveUserInferredTags(sc, s"$store_dir/user_entity_update")
    if (pathUsers != null) filterSave(sc, pathUsers, s"$store_dir/user_inferred_keep", modelTrain.userInferredDict.keySet)
    if (pathItems != null) filterSave(sc, pathItems, s"$store_dir/item_inferred_keep", modelTrain.itemInferredDict.keySet)
    if (pathUserInferredEntities != null) filterSave(sc, pathUserInferredEntities, s"$store_dir/user_entity_keep", modelTrain.userInferredTagDict.keySet)
    setPath(store_dir)
    modelTrain.clearProfiles()
  }

  //Helper
  private def filterSave(sc: SparkContext, src_path: String, dst_path: String, white_set: collection.Set[String]) = {
    val rdd = sc.textFile(src_path)
    val filtered = rdd.map(_.split("\t", 2))
      .filter(kv => !white_set.contains(kv(0)))
      .map(_.mkString("\t"))
    val total_size = filtered.map(l => l.length + 1).sum()
    filtered.coalesce(1 + (total_size / 80000000).toInt).saveAsTextFile(dst_path)
  }


  private var infoStorePath: String = _

  override def setInfoStoreDir(path: String): Unit = infoStorePath = path

  // Source Data
  // dict for text_vector, category
  var rddDocProfile: RDD[(String, (SparseVector[Double], collection.mutable.Map[String, Double]))] = _

  override def prepareDependency(time_ms: Long, sc: SparkContext): Unit = {
    if (rddDocProfile != null) {
      rddDocProfile.unpersist(blocking = false)
    }
    val starting_ms = Input.calcStartingMs(time_ms, -1)
    rddDocProfile = Input.queryCategoryContentEntityTfIdfRDD(sc, mongo_input_split_size, mongo_input_uri_item_profile, starting_ms, modeOnline)
    rddDocProfile.persist(StorageLevel.MEMORY_AND_DISK)

    if (infoStorePath != null) {
      val profile_count = rddDocProfile.count
      Storage.saveSeq(Array(
        s"$profile_count since $starting_ms"
      ), sc, s"$infoStorePath/profile_info")
      rddDocProfile.saveAsTextFile(s"$infoStorePath/profile_dump")
    }
  }

  // For learning / evaluation
  override def stateAndFeatureForCollection(instance: RDD[(String, String, Double)], sc: SparkContext)
  : RDD[((String, String, Double), org.apache.spark.mllib.linalg.SparseVector)] = {
    //TODO: Ensure no duplicated key in profile rdd
    val with_profile = instance.keyBy(_._2).leftOuterJoin(rddDocProfile)
    with_profile.persist(StorageLevel.MEMORY_AND_DISK)

    val userEntities = with_profile.filter(_._2._2.isDefined)
      .map(kp => (kp._2._1._1, kp._2._2.get._2.keys.toSet))
      .reduceByKey((a, b) => a | b)
      .map(x => (x._1, {
        x._2.foldLeft(new mutable.HashSet[String]())((acc, value) => acc + value)
      })).collectAsMap()


    val users = instance.map(_._1).distinct().collect()
    val items = instance.map(_._2).distinct().collect()

    val temporaryTagGroup = new WeightedBookProfileGroup(userEntities, entityScalar)
    val cluster = modelTrain.makeDynamicFeatureCluster(users, items, temporaryTagGroup)
    clusterToUpdate = cluster

    val result = with_profile.map(joined => {
      val inst = joined._2._1
      cluster.setUser(inst._1)
      cluster.setItem(inst._2)
      val option = joined._2._2
      if (option.isDefined) {
        val profile = option.get
        categorySparse.set(profile._1)
        temporaryTagGroup.setObserved(profile._2)
      } else {
        categorySparse.setNeutral()
        temporaryTagGroup.setNeturalObserved()
      }
      val vector = cluster.getVector
      (inst, new org.apache.spark.mllib.linalg.SparseVector(vector.length, vector.index, vector.data))
    })
    with_profile.unpersist(blocking = false)

    result
  }

  var clusterToUpdate: TagDynamicFeatureCluster3 = _

  override def genWeights(sc: SparkContext): Array[Double] = {
    modelTrain.loadWeights(clusterToUpdate, clusterToUpdate.userProfileGroup.symbols, clusterToUpdate.itemProfileGroup.symbols,
      sc, this.pathGlobal, this.pathUsers, this.pathItems, this.pathUserInferredEntities)
  }

  override def updateByWeights(weights: Array[Double]): Unit = {
    modelTrain.updateByWeights(clusterToUpdate, clusterToUpdate.userTagProfileGroup.decodeArray, weights, only_memory_positive_tag)
  }

  // Recommendation
  private var latestRankingWeights: Array[Double] = _

  override def getLatestRankingWeights: Array[Double] = latestRankingWeights

  override def rankDocsForUsersWithFeature(rddDistinctUser: RDD[String], nTopCandidates: Int, sc: SparkContext)
  : RDD[(String, Iterable[(String, Double, breeze.linalg.SparseVector[Double])])] = {
    val rddItemProfile = rddDocProfile.filter(_._2._1.activeSize > 0) // Filter out candidates without category
    rddItemProfile.persist(StorageLevel.MEMORY_AND_DISK)

    val users = rddDistinctUser.collect()
    val pre_items = rddItemProfile.map(_._1).collect()
    val pre_cluster = modelRec.makeDynamicFeatureCluster(users, pre_items, new WhiteWeightFeatureSegment(Set.empty, 0))

    val pre_weights: Array[Double] = modelRec.loadWeights(pre_cluster, users, pre_items, sc, pathGlobal, pathUsers, pathItems, pathUserInferredEntities)
    val pre_weights_vector = breeze.linalg.SparseVector[Double](pre_weights)

    val topItems = rddItemProfile
      .map(item_profile => {
        pre_cluster.resetUser()
        try
          pre_cluster.setItem(item_profile._1)
        catch {
          case exception: java.util.NoSuchElementException =>
            println("Not match:\t" + item_profile._1)
            println("Not match:\t" + pre_items.toList)
            println("Not match:\t" + pre_cluster.itemProfileGroup.encodeMap.keys.toList)
            pre_cluster.itemProfileGroup.reset()
        }
        categorySparse.setNeutral()
        val score = pre_cluster.getVector dot pre_weights_vector

        categorySparse.set(item_profile._2._1)

        (score, item_profile._1, Utils.featureHitWeightsString(pre_cluster.getVector, pre_weights_vector))
      })
      .top(nTopCandidates)

    if (sc != null && infoStorePath != null) {
      Storage.saveSeq(topItems.map(
        p => s"${p._2}\t${p._1.toFloat}\t${p._3}"
      ), sc, s"$infoStorePath/candidates", 1)
    }

    val topItemSet = topItems.map(_._2).toSet
    val profileSubSet = rddDocProfile.filter(item_profile => topItemSet.contains(item_profile._1))

    //    val subTagDict = profileSubSet.map(x => {
    //      (x._1, x._2._2)
    //    }).collect().toMap
    //    val tag_set: Set[String] = subTagDict.filter(kv=> topItemSet.contains(kv._1)).values.flatten.toSet
    val tag_set = profileSubSet.flatMap(_._2._2.keys).distinct().collect().toSet
    val temporaryBookItemFeature = new WhiteWeightFeatureSegment(tag_set, entityScalar)
    val final_items = topItemSet.toSeq
    val final_cluster = modelRec.makeDynamicFeatureCluster(users, final_items, temporaryBookItemFeature)

    val final_weights: Array[Double] = modelRec.loadWeights(final_cluster, users, pre_items, sc, pathGlobal, pathUsers, pathItems, pathUserInferredEntities)
    val final_weights_vector = breeze.linalg.SparseVector(final_weights)
    latestRankingWeights = final_weights
    val result = rankDocsWithFeature(temporaryBookItemFeature, final_cluster, final_weights_vector, rddDistinctUser, profileSubSet, Utils.getNumExecutors(sc))

    rddItemProfile.unpersist(blocking = false)
    result
  }

  private def rankDocsWithFeature(temporaryBookItemFeature: WhiteWeightFeatureSegment, cluster: TagDynamicFeatureCluster2,
                                  weights_vector: breeze.linalg.Vector[Double],
                                  rddUser: RDD[String], rddCandidatesProfile: RDD[(String, (SparseVector[Double], collection.mutable.Map[String, Double]))],
                                  num_executors: Int)
  : RDD[(String, Iterable[(String, Double, breeze.linalg.SparseVector[Double])])] = {
    val cartesian = rddUser.cartesian(rddCandidatesProfile)
    //.repartition(num_executors)
    val rddUserItemScore = cartesian.map(user_docprofile => {
      val user = user_docprofile._1
      val item = user_docprofile._2._1
      val profile = user_docprofile._2._2
      cluster.setUser(user)
      cluster.setItem(item)
      categorySparse.set(profile._1)
      temporaryBookItemFeature.set(profile._2)

      val feature_vector = cluster.getVector
      val score = feature_vector dot weights_vector
      (user, (item, score, feature_vector))
    })
    rddUserItemScore.groupByKey()
  }

}
