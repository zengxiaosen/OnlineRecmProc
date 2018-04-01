package com.z.ol

import scala.reflect.ClassTag
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.{SparseVector, Vector}

import scala.reflect.ClassTag
/**
  * Created by root on 18-4-1.
  */
object Storage {

  def saveReg(dict: collection.Map[String, Int],
              sc: SparkContext, path: String) = {
    sc.parallelize(dict.toArray.sortBy(_._2), 1).saveAsTextFile(path)
  }

  def saveOrderedReg(keyValueArray: Array[(String, Int)],
                     sc: SparkContext, path: String) = {
    sc.parallelize(keyValueArray, 1).saveAsTextFile(path)
  }

  def loadReg(sc: SparkContext, path: String) = {
    val lines = sc.textFile(path, 1).collect()
    lines.map(line => {
      val a = line.substring(1, line.length - 1).split(",")
      (a(0), a(1).toInt)
    }).toMap
  }

  //
  def saveWeights(seq: Seq[Double],
                  sc: SparkContext, path: String) = {
    sc.parallelize(seq.map(_.toFloat), 1).saveAsTextFile(path)
  }

  // TODO: Double check risk from multiple partitions
  def loadWeights(sc: SparkContext, path: String)
  : Array[Double] = {
    val lines = sc.textFile(path, 1).collect()
    lines.map(_.toDouble)
  }

  def saveSparseWeights(vec: Vector,
                        sc: SparkContext, path: String) = {
    val sparse = vec.toSparse
    val lines = sparse.indices.zip(sparse.values).filter(_._2 != 0).map(iv => s"${iv._1}\t${iv._2.toFloat}")
    sc.parallelize(lines, lines.length / 4000000 + 1).saveAsTextFile(path)
  }

  def loadSparseWeightsSpark(sc: SparkContext, path: String, length: Int) = {
    val pairs = sc.textFile(path).map(line => {
      line.split("\t")
    }).collect()
    val indices = pairs.map(_ (0).toInt)
    val data = pairs.map(_ (1).toDouble)
    new SparseVector(length, indices, data)
  }

  def loadSparseWeightsBreeze(sc: SparkContext, path: String, length: Int) = {
    val pairs = sc.textFile(path).map(line => {
      line.split("\t")
    }).collect()
    val indices = pairs.map(_ (0).toInt)
    val data = pairs.map(_ (1).toDouble)
    new breeze.linalg.SparseVector[Double](indices, data, length)
  }

  //
  def saveStrings(seq: Seq[String], sc: SparkContext, path: String) = {
    sc.parallelize(seq, 1).saveAsTextFile(path)
  }

  def saveSeq[T: ClassTag](seq: Seq[T], sc: SparkContext, path: String, parallelism: Int = 1) = {
    sc.parallelize(seq, parallelism).saveAsTextFile(path)
  }

}
