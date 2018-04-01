package com.z.ol
import breeze.linalg.{DenseVector, SparseVector}
import org.apache.spark.SparkContext
/**
  * Created by root on 18-4-1.
  */
object Utils {
  def getNumExecutors(sc: SparkContext) = {
    val num_executors = sc.getConf.getInt("spark.executor.instances", -1)
    assume(num_executors != -1)
    num_executors
  }

  def featureHitWeightsString(feature: SparseVector[Double], weights: SparseVector[Double]) = {
    feature.index.map(ind => {
      val fval: Double = feature(ind)
      if (fval == 1) {
        s"$ind: ${weights(ind).toFloat}"
      } else if (fval == 0) {
        s"$ind: (${weights(ind).toFloat})"
      } else {
        s"$ind: ${weights(ind).toFloat} *${fval.toFloat}"
      }
    }).mkString(", ")
  }

  def featureHitWeightsString(feature: SparseVector[Double], weights: DenseVector[Double]) = {
    feature.index.map(ind => {
      val fval: Double = feature(ind)
      if (fval == 1) {
        s"$ind: ${weights(ind).toFloat}"
      } else if (fval == 0) {
        s"$ind: (${weights(ind).toFloat})"
      } else {
        s"$ind: ${weights(ind).toFloat} *${fval.toFloat}"
      }
    }).mkString(", ")
  }

  def featureHitWeightsString(feature: SparseVector[Double], weights: Array[Double]) = {
    feature.index.map(ind => {
      val fval: Double = feature(ind)
      if (fval == 1) {
        s"$ind: ${weights(ind).toFloat}"
      } else if (fval == 0) {
        s"$ind: (${weights(ind).toFloat})"
      } else {
        s"$ind: ${weights(ind).toFloat} *${fval.toFloat}"
      }
    }).mkString(", ")
  }
}
