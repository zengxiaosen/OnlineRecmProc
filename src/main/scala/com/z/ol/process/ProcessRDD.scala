package com.z.ol.process
import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithLBFGS, LogisticRegressionWithSGD}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
/**
  * Created by root on 18-4-1.
  */
object ProcessRDD {
  def trainPlus(rddLabledPoint: RDD[LabeledPoint],
                base_model: LogisticRegressionModel, numIterations: Int, stepSize: Double, miniBatchFraction: Double,
                sc: SparkContext, batch_dir: String): LogisticRegressionModel = {
    val rddTraining = rddLabledPoint.filter(_.features.numActives > 0)

    rddTraining.persist(StorageLevel.MEMORY_AND_DISK)
    val out_model = if (rddTraining.isEmpty()) {
      base_model
    } else if (numIterations < 0 || stepSize < 0 || miniBatchFraction < 0) {
      if (rddTraining.filter(_.label > 0).isEmpty()) {
        base_model
      } else {
        new LogisticRegressionWithLBFGS().run(rddTraining, base_model.weights)
      }
    } else {
      LogisticRegressionWithSGD.train(rddTraining, numIterations, stepSize, miniBatchFraction, base_model.weights)
    }

    rddTraining.unpersist(blocking = false)

    if (batch_dir != null) {
      //        model.save(ssc.sparkContext, s"$output_dir/$dt/raw_model")
      //        Storage.saveWeights(model.weights.toArray, ssc.sparkContext, s"$output_dir/$dt/weights")
      Storage.saveSparseWeights(out_model.weights, sc, s"$batch_dir/sparse_weights")
    }
    out_model
  }
}
