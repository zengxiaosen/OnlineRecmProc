package com.z.ol.doFt

/**
  * Created by root on 18-4-1.
  */
trait DynamicFeatureSolution {
  def init(sc: SparkContext, init_dir: String, no_weight_change: Boolean = false): Unit

  def save(sc: SparkContext, store_dir: String): Unit

  def setInfoStoreDir(path: String): Unit

  def prepareDependency(timeInMilli: Long, sc: SparkContext): Unit

  def stateAndFeatureForCollection(instance: RDD[(String, String, Double)], sc: SparkContext = null): RDD[((String, String, Double), org.apache.spark.mllib.linalg.SparseVector)]

  def genWeights(sc: SparkContext = null): Array[Double]

  def updateByWeights(weights: Array[Double])

  def rankDocsForUsersWithFeature(rddUser: RDD[String], nTopCandidates: Int, sc: SparkContext = null): RDD[(String, Iterable[(String, Double, breeze.linalg.SparseVector[Double])])]

  def getLatestRankingWeights: Array[Double]
}
