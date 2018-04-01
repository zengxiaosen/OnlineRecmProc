package com.z.ol
import breeze.linalg.SparseVector
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.bson.BSONObject
import org.bson.types.BasicBSONList
import scala.collection.JavaConversions.mapAsScalaMap
import  com.z.utils.ConfiguredMongoReader
/**
  * Created by root on 18-4-1.
  */
object Input {
  def makeSetQuery(key: String, set: Iterable[String]) : String = {
    "{%s: {'$in': %s}}".format(key, set.mkString("[", ",", "]"))
  }

  private val DAILY_MS: Long = 1000L * 24 * 60 * 60
  val MAX_ARTICLE_MS_DIFF: Long = 3 * DAILY_MS
  //  val MAX_USER_MS_DIFF :Long = 30 * DAILY_MS
  private val MARGIN = 0

  def updateCustomCategoriesDict(sc: SparkContext, mongo_input_split_size: String, mongo_input_uri_item_profile: String,
                                 batch_ms: Long, last_ms: Long, user_profile_dict: scala.collection.mutable.HashMap[String, SparseVector[Double]]) = {
    val starting_ms = if (user_profile_dict.isEmpty || last_ms < 0) {
      0
    } else {
      last_ms - MARGIN
    }
    val collected = queryCustomCategories(sc, mongo_input_split_size, mongo_input_uri_item_profile, starting_ms)
    user_profile_dict ++= collected

    (collected.length, starting_ms)
  }

  def queryCustomCategories(sc: SparkContext, mongo_input_split_size: String, mongo_input_uri_item_profile: String, starting_ms: Long): Array[(String, SparseVector[Double])] = {
    val hadoopConf = new Configuration()
    hadoopConf.set("mongo.input.split_size", mongo_input_split_size)
    hadoopConf.set("mongo.input.split.use_range_queries", "true")

    hadoopConf.set("mongo.input.uri", mongo_input_uri_item_profile)
    hadoopConf.set("mongo.input.query", "{updated_at:{'$gt':{'$date':%d}}}".format(starting_ms))
    hadoopConf.set("mongo.input.fields", "{user_seq_id : 1, custom_categories: 1}")

    new ConfiguredMongoReader(sc, hadoopConf).initRDD()
      .map(x => try {
        (x._2.get("user_seq_id").toString,
          categoryToVector(x._2.get("custom_categories").asInstanceOf[BasicBSONList].toArray.map(_.toString)))
      } catch {
        case _: Throwable => ("", null)
      })
      .filter(_._1 != "")
      .collect
  }

  def getFullCustomCategoriesMap(sc: SparkContext, mongo_input_split_size: String, mongo_input_uri_item_profile: String): collection.Map[String, SparseVector[Double]] = {
    val hadoopConf = new Configuration()
    hadoopConf.set("mongo.input.split_size", mongo_input_split_size)
    hadoopConf.set("mongo.input.split.use_range_queries", "true")

    hadoopConf.set("mongo.input.uri", mongo_input_uri_item_profile)
    hadoopConf.set("mongo.input.query", "{}")
    hadoopConf.set("mongo.input.fields", "{user_seq_id : 1, custom_categories: 1}")

    new ConfiguredMongoReader(sc, hadoopConf).initRDD()
      .map(x => try {
        (x._2.get("user_seq_id").toString,
          categoryToVector(x._2.get("custom_categories").asInstanceOf[BasicBSONList].toArray.map(_.toString)))
      } catch {
        case _: Throwable => ("", null)
      })
      .filter(_._1 != "")
      .collect
      .toMap
  }
  def updateCategoryDict(sc: SparkContext, mongo_input_split_size: String, mongo_input_uri_item_profile: String,
                         batch_ms: Long, last_ms: Long, doc_dict: scala.collection.mutable.HashMap[String, SparseVector[Double]]) = {
    val starting_ms = if (doc_dict.isEmpty || last_ms < 0 || (batch_ms - last_ms) > MAX_ARTICLE_MS_DIFF) {
      batch_ms - MAX_ARTICLE_MS_DIFF
    } else {
      last_ms
    } - MARGIN

    val collected = queryCategoryRDD(sc, mongo_input_split_size, mongo_input_uri_item_profile, starting_ms).collect
    doc_dict ++= collected

    (collected.length, starting_ms)
  }

  def queryCategoryRDD(sc: SparkContext, mongo_input_split_size: String, mongo_input_uri_item_profile: String,
                       starting_ms: Long, filterUsable: Boolean = false)
  : RDD[(String, SparseVector[Double])] = {
    val hadoopConf = new Configuration()
    hadoopConf.set("mongo.input.split_size", mongo_input_split_size)
    hadoopConf.set("mongo.input.split.use_range_queries", "true")

    hadoopConf.set("mongo.input.uri", mongo_input_uri_item_profile)
    hadoopConf.set("mongo.input.fields", "{article_seq_id : 1, category: 1}")
    if (filterUsable) {
      hadoopConf.set("mongo.input.query", "{crawled_at:{'$gt':{'$date':%d}}, usable:true}".format(starting_ms))
    } else {
      hadoopConf.set("mongo.input.query", "{crawled_at:{'$gt':{'$date':%d}}}".format(starting_ms))
    }

    new ConfiguredMongoReader(sc, hadoopConf).initRDD()
      .map(x => try {
        (x._2.get("article_seq_id").toString,
          categoryToVector(x._2.get("category").asInstanceOf[BasicBSONList].toArray.map(_.toString)))
      } catch {
        case _: Throwable => ("", null)
      }).filter(_._1 != "")
  }

  def queryCategoryRDD(sc: SparkContext, mongo_input_split_size: String, mongo_input_uri_item_profile: String,
                       article_seq_id_set: Iterable[String]): RDD[(String, SparseVector[Double])] = {
    val hadoopConf = new Configuration()
    hadoopConf.set("mongo.input.split_size", mongo_input_split_size)
    hadoopConf.set("mongo.input.split.use_range_queries", "true")

    hadoopConf.set("mongo.input.uri", mongo_input_uri_item_profile)
    hadoopConf.set("mongo.input.fields", "{article_seq_id : 1, category: 1}")
    hadoopConf.set("mongo.input.query", makeSetQuery("article_seq_id", article_seq_id_set))

    new ConfiguredMongoReader(sc, hadoopConf).initRDD()
      .map(x => try {
        (x._2.get("article_seq_id").toString,
          categoryToVector(x._2.get("category").asInstanceOf[BasicBSONList].toArray.map(_.toString)))
      } catch {
        case _: Throwable => ("", null)
      }).filter(_._1 != "")
  }

  def categoryToVectorWithCitys(category: Array[String]): SparseVector[Double] = {
    val categoryFeature = SparseVector.zeros[Double](CATEGORY_WITH_CITY_VECTOR_LENGTH)
    for (name <- category) {
      val index = categoryMappingWithCity.getOrElse(name, -1)
      if (index != -1) {
        categoryFeature(index) = 1D
      }
    }
    categoryFeature
  }



  def categoryToVector(category: Array[String]): SparseVector[Double] = {
    val categoryFeature = SparseVector.zeros[Double](CATEGORY_VECTOR_LENGTH)
    for (name <- category) {
      val index = categoryMapping.getOrElse(name, -1)
      if (index != -1) {
        categoryFeature(index) = 1D
      }
    }
    categoryFeature
  }

  //=============================================================================


  private val categoryMappingWithCity = Map(
    "zzz" ->0,
    "hhh" -> 2
  )
  val categoryArray = Array("others",
    "sports",
    "cricket",
    "entertainment",
    "auto",
    "technology",
    "lifestyle",
    "health",
    "business",
    "world",
    "politics",
    "education",
    "national")

  val categoryMapping = Map(
    "others" -> 0,
    "sports" -> 1,
    "cricket" -> 2,
    "entertainment" -> 3,
    "auto" -> 4,
    "technology" -> 5,
    "lifestyle" -> 6,
    "health" -> 7,
    "business" -> 8,
    "world" -> 9,
    "politics" -> 10,
    "education" -> 11,
    "national" -> 12)

  val CATEGORY_VECTOR_LENGTH = categoryMapping.values.max + 1
  val CATEGORY_WITH_CITY_VECTOR_LENGTH = categoryMappingWithCity.values.max + 1

  def customCategoryToVector(category: Array[String]): SparseVector[Double] = {
    val categoryFeature = SparseVector.zeros[Double](CUSTOM_CATEGORY_VECTOR_LENGTH)
    for (c <- category) {
      val index = customCategoryMapping.getOrElse(c, CUSTOM_CATEGORY_OTHERS_INDEX)
      categoryFeature(index) = 1D
    }
    categoryFeature
  }

  // TODO: Add "joke" which presents for Photo
  private val CUSTOM_CATEGORY_OTHERS_INDEX = 0
  private val customCategoryMapping = Map(
    "youtubes" -> 1,
    "sports" -> 2,
    "cricket" -> 3,
    "entertainment" -> 4,
    "auto" -> 5,
    "technology" -> 6,
    "lifestyle" -> 7,
    "health" -> 8,
    "business" -> 9,
    "world" -> 10,
    "politics" -> 11,
    "education" -> 12,
    "national" -> 13,
    "local" -> 14
  )
  val CUSTOM_CATEGORY_VECTOR_LENGTH = customCategoryMapping.values.max + 1
  //=======================================
  def queryCategoryTitleEntityRDD(sc: SparkContext, mongo_input_split_size: String, mongo_input_uri_item_profile: String,
                                  starting_ms: Long, filterUsable: Boolean = false)
  : RDD[(String, (SparseVector[Double], collection.immutable.Set[String]))] = {
    val hadoopConf = new Configuration()
    hadoopConf.set("mongo.input.split_size", mongo_input_split_size)
    hadoopConf.set("mongo.input.split.use_range_queries", "true")
    hadoopConf.set("mongo.input.uri", mongo_input_uri_item_profile)
    hadoopConf.set("mongo.input.fields", "{article_seq_id:1, category:1, title_name_entities:1}")
    if (filterUsable) {
      hadoopConf.set("mongo.input.query", "{crawled_at:{'$gt':{'$date':%d}}, usable:true}".format(starting_ms))
    } else {
      hadoopConf.set("mongo.input.query", "{crawled_at:{'$gt':{'$date':%d}}}".format(starting_ms))
    }
    new ConfiguredMongoReader(sc, hadoopConf).initRDD()
      .map(x => try {
        (x._2.get("article_seq_id").toString,
          (categoryToVector(x._2.get("category").asInstanceOf[BasicBSONList].toArray.map(_.toString)),
            x._2.get("title_name_entities").asInstanceOf[BasicBSONList].toArray.map(_.toString.toLowerCase()).toSet
          )
        )
      } catch {
        case _: Throwable =>
          println("UNEXPECTED PROFILE: " + x)
          ("", (null, null))
      }
      ).filter(_._1 != "")
  }

  def queryCategoryContentEntityRDD(sc: SparkContext, mongo_input_split_size: String, mongo_input_uri_item_profile: String,
                                    starting_ms: Long, filterUsable: Boolean = false)
  : RDD[(String, (SparseVector[Double], collection.immutable.Set[String]))] = {
    val hadoopConf = new Configuration()
    hadoopConf.set("mongo.input.split_size", mongo_input_split_size)
    hadoopConf.set("mongo.input.split.use_range_queries", "true")
    hadoopConf.set("mongo.input.uri", mongo_input_uri_item_profile)
    hadoopConf.set("mongo.input.fields", "{article_seq_id:1, category:1, content_name_entities:1}")
    if (filterUsable) {
      hadoopConf.set("mongo.input.query", "{crawled_at:{'$gt':{'$date':%d}}, usable:true}".format(starting_ms))
    } else {
      hadoopConf.set("mongo.input.query", "{crawled_at:{'$gt':{'$date':%d}}}".format(starting_ms))
    }
    new ConfiguredMongoReader(sc, hadoopConf).initRDD()
      .map(x => try {
        (x._2.get("article_seq_id").toString,
          (categoryToVector(x._2.get("category").asInstanceOf[BasicBSONList].toArray.map(_.toString)),
            x._2.get("content_name_entities").asInstanceOf[BasicBSONList].toArray.map(_.toString.toLowerCase()).toSet
          )
        )
      } catch {
        case _: Throwable =>
          println("UNEXPECTED PROFILE: " + x)
          ("", (null, null))
      }
      ).filter(_._1 != "")
  }

  def queryCategoryTitleEntityTfIdfRDD(sc: SparkContext, mongo_input_split_size: String, mongo_input_uri_item_profile: String,
                                       starting_ms: Long, filterUsable: Boolean = false)
  : RDD[(String, (SparseVector[Double], collection.mutable.Map[String, Double]))] = {
    val hadoopConf = new Configuration()
    hadoopConf.set("mongo.input.split_size", mongo_input_split_size)
    hadoopConf.set("mongo.input.split.use_range_queries", "true")
    hadoopConf.set("mongo.input.uri", mongo_input_uri_item_profile)
    hadoopConf.set("mongo.input.fields", "{article_seq_id:1, category:1, title_name_entities_tfidf: 1}")
    if (filterUsable) {
      hadoopConf.set("mongo.input.query", "{crawled_at:{'$gt':{'$date':%d}}, usable:true}".format(starting_ms))
    } else {
      hadoopConf.set("mongo.input.query", "{crawled_at:{'$gt':{'$date':%d}}}".format(starting_ms))
    }
    new ConfiguredMongoReader(sc, hadoopConf).initRDD()
      .map(x => try {
        (x._2.get("article_seq_id").toString,
          (categoryToVector(x._2.get("category").asInstanceOf[BasicBSONList].toArray.map(_.toString)), {
            val tfidf = x._2.get("title_name_entities_tfidf").asInstanceOf[BSONObject].toMap
            mapAsScalaMap(tfidf).asInstanceOf[collection.mutable.Map[String, Double]]
          }
          )
        )
      } catch {
        case _: Throwable =>
          println("UNEXPECTED PROFILE: " + x)
          ("", (null, null))
      }
      ).filter(_._1 != "")
  }

  def queryCategoryContentEntityTfIdfRDD(sc: SparkContext, mongo_input_split_size: String, mongo_input_uri_item_profile: String,
                                         starting_ms: Long, filterUsable: Boolean = false)
  : RDD[(String, (SparseVector[Double], collection.mutable.Map[String, Double]))] = {
    val hadoopConf = new Configuration()
    hadoopConf.set("mongo.input.split_size", mongo_input_split_size)
    hadoopConf.set("mongo.input.split.use_range_queries", "true")
    hadoopConf.set("mongo.input.uri", mongo_input_uri_item_profile)
    hadoopConf.set("mongo.input.fields", "{article_seq_id:1, category:1, content_name_entities_tfidf: 1}")
    if (filterUsable) {
      hadoopConf.set("mongo.input.query", "{crawled_at:{'$gt':{'$date':%d}}, usable:true}".format(starting_ms))
    } else {
      hadoopConf.set("mongo.input.query", "{crawled_at:{'$gt':{'$date':%d}}}".format(starting_ms))
    }
    new ConfiguredMongoReader(sc, hadoopConf).initRDD()
      .map(x => try {
        (x._2.get("article_seq_id").toString,
          (categoryToVector(x._2.get("category").asInstanceOf[BasicBSONList].toArray.map(_.toString)), {
            val tfidf = x._2.get("content_name_entities_tfidf").asInstanceOf[BSONObject].toMap
            mapAsScalaMap(tfidf).asInstanceOf[collection.mutable.Map[String, Double]]
          }
          )
        )
      } catch {
        case _: Throwable =>
          println("UNEXPECTED PROFILE: " + x)
          ("", (null, null))
      }
      ).filter(_._1 != "")
  }

  def queryCategoryWithCityContentEntityTfIdfRDD(sc: SparkContext, mongo_input_split_size: String, mongo_input_uri_item_profile: String,
                                                 starting_ms: Long, filterUsable: Boolean = false)
  : RDD[(String, (SparseVector[Double], collection.mutable.Map[String, Double]))] = {
    val hadoopConf = new Configuration()
    hadoopConf.set("mongo.input.split_size", mongo_input_split_size)
    hadoopConf.set("mongo.input.split.use_range_queries", "true")
    hadoopConf.set("mongo.input.uri", mongo_input_uri_item_profile)
    hadoopConf.set("mongo.input.fields", "{article_seq_id:1, category:1, content_name_entities_tfidf: 1}")
    if (filterUsable) {
      hadoopConf.set("mongo.input.query", "{crawled_at:{'$gt':{'$date':%d}}, usable:true}".format(starting_ms))
    } else {
      hadoopConf.set("mongo.input.query", "{crawled_at:{'$gt':{'$date':%d}}}".format(starting_ms))
    }
    new ConfiguredMongoReader(sc, hadoopConf).initRDD()
      .map(x => try {
        (x._2.get("article_seq_id").toString,
          (categoryToVectorWithCitys(x._2.get("category").asInstanceOf[BasicBSONList].toArray.map(_.toString)), {
            val tfidf = x._2.get("content_name_entities_tfidf").asInstanceOf[BSONObject].toMap
            mapAsScalaMap(tfidf).asInstanceOf[collection.mutable.Map[String, Double]]
          }
          )
        )
      } catch {
        case _: Throwable =>
          println("UNEXPECTED PROFILE: " + x)
          ("", (null, null))
      }
      ).filter(_._1 != "")
  }

  def queryCategoryTextRDD(sc: SparkContext, mongo_input_split_size: String, mongo_input_uri_item_profile: String,
                           starting_ms: Long, filterUsable: Boolean = false)
  : RDD[(String, (SparseVector[Double], SparseVector[Double]))] = {
    val hadoopConf = new Configuration()
    hadoopConf.set("mongo.input.split_size", mongo_input_split_size)
    hadoopConf.set("mongo.input.split.use_range_queries", "true")

    hadoopConf.set("mongo.input.uri", mongo_input_uri_item_profile)
    hadoopConf.set("mongo.input.fields", "{article_seq_id: 1, category: 1, text_vector: 1}")
    if (filterUsable) {
      hadoopConf.set("mongo.input.query", "{crawled_at:{'$gt':{'$date':%d}}, usable:true}".format(starting_ms))
    } else {
      hadoopConf.set("mongo.input.query", "{crawled_at:{'$gt':{'$date':%d}}}".format(starting_ms))
    }

    new ConfiguredMongoReader(sc, hadoopConf).initRDD()
      .map(x => try {
        (x._2.get("article_seq_id").toString, (
          categoryToVector(x._2.get("category").asInstanceOf[BasicBSONList].toArray.map(_.toString)),
          SparseVector(x._2.get("text_vector").asInstanceOf[BasicBSONList].toArray.map(_.toString.toDouble))
        ))
      } catch {
        case _: Throwable =>
          println("UNEXPECTED PROFILE: " + x)
          ("", (null, null))
      })
      .filter(_._1 != "")
  }


  //
  def calcStartingMs(target_ms: Long, last_ms: Long): Long = {
    if (target_ms <= MAX_ARTICLE_MS_DIFF + MARGIN) {
      0
    } else if (last_ms < 0 || (target_ms - last_ms) > MAX_ARTICLE_MS_DIFF) {
      target_ms - MAX_ARTICLE_MS_DIFF - MARGIN
    } else {
      last_ms - MARGIN
    }
  }

  // Doc profile
  // TODO: Removing of old article profile
  def updateArticleProfile3Dict(sc: SparkContext, mongo_input_split_size: String, mongo_input_uri_item_profile: String,
                                batch_ms: Long, last_ms: Long,
                                doc_dict: scala.collection.mutable.HashMap[String, (SparseVector[Double], SparseVector[Double], SparseVector[Double])]) = {
    val starting_ms: Long = calcStartingMs(batch_ms, last_ms)
    val collected = queryArticleProfile3RDD(sc, mongo_input_split_size, mongo_input_uri_item_profile, starting_ms).collect

    doc_dict ++= collected
    (collected.length, starting_ms)
  }

  def queryArticleProfile3RDD(sc: SparkContext, mongo_input_split_size: String, mongo_input_uri_item_profile: String, starting_ms: Long)
  : RDD[(String, (SparseVector[Double], SparseVector[Double], SparseVector[Double]))] = {
    val hadoopConf = new Configuration()
    hadoopConf.set("mongo.input.query", "{crawled_at:{'$gte':{'$date':%d}}}".format(starting_ms))
    hadoopConf.set("mongo.input.split.use_range_queries", "true")
    hadoopConf.set("mongo.input.split_size", mongo_input_split_size)
    hadoopConf.set("mongo.input.uri", mongo_input_uri_item_profile)
    hadoopConf.set("mongo.input.fields", "{article_seq_id : 1, text_vector: 1, title_vector: 1, category: 1}")

    new ConfiguredMongoReader(sc, hadoopConf).initRDD()
      .map(x => try {
        (x._2.get("article_seq_id").toString, (
          SparseVector(x._2.get("text_vector").asInstanceOf[BasicBSONList].toArray.map(_.toString.toDouble)),
          SparseVector(x._2.get("title_vector").asInstanceOf[BasicBSONList].toArray.map(_.toString.toDouble)),
          categoryToVector(x._2.get("category").asInstanceOf[BasicBSONList].toArray.map(_.toString))
        ))
      } catch {
        case _: Throwable => ("", (null, null, null))
      })
      .filter(_._1 != "")
  }

  def queryTextVectorCategoryRDD(sc: SparkContext, mongo_input_split_size: String, mongo_input_uri_item_profile: String, starting_ms: Long)
  : RDD[(String, (SparseVector[Double], SparseVector[Double]))] = {
    val hadoopConf = new Configuration()
    hadoopConf.set("mongo.input.query", "{crawled_at:{'$gte':{'$date':%d}}}".format(starting_ms))
    hadoopConf.set("mongo.input.split.use_range_queries", "true")
    hadoopConf.set("mongo.input.split_size", mongo_input_split_size)
    hadoopConf.set("mongo.input.uri", mongo_input_uri_item_profile)
    hadoopConf.set("mongo.input.fields", "{article_seq_id : 1, text_vector: 1, title_vector: 1, category: 1}")

    new ConfiguredMongoReader(sc, hadoopConf).initRDD()
      .map(x => try {
        (x._2.get("article_seq_id").toString, (
          SparseVector(x._2.get("text_vector").asInstanceOf[BasicBSONList].toArray.map(_.toString.toDouble)),
          categoryToVector(x._2.get("category").asInstanceOf[BasicBSONList].toArray.map(_.toString))
        ))
      } catch {
        case _: Throwable => ("", (null, null))
      })
      .filter(_._1 != "")
  }

  def updateArticleProfile2Dict(sc: SparkContext, mongo_input_split_size: String, mongo_input_uri_item_profile: String,
                                batch_ms: Long, last_ms: Long, doc_dict: scala.collection.mutable.HashMap[String, (SparseVector[Double], SparseVector[Double])]) = {
    val starting_ms = if (doc_dict.isEmpty || last_ms < 0 || (batch_ms - last_ms) > MAX_ARTICLE_MS_DIFF) {
      batch_ms - MAX_ARTICLE_MS_DIFF
    } else {
      last_ms
    } - MARGIN

    val hadoopConf = new Configuration()
    hadoopConf.set("mongo.input.query", "{crawled_at:{'$gte':{'$date':%d}}}".format(starting_ms))
    hadoopConf.set("mongo.input.split.use_range_queries", "true")
    hadoopConf.set("mongo.input.split_size", mongo_input_split_size)
    hadoopConf.set("mongo.input.uri", mongo_input_uri_item_profile)
    hadoopConf.set("mongo.input.fields", "{article_seq_id : 1, text_vector: 1, title_vector: 1}")

    val collected = new ConfiguredMongoReader(sc, hadoopConf).initRDD()
      .map(x => try {
        (x._2.get("article_seq_id").toString, (
          SparseVector(x._2.get("text_vector").asInstanceOf[BasicBSONList].toArray.map(_.toString.toDouble)),
          SparseVector(x._2.get("title_vector").asInstanceOf[BasicBSONList].toArray.map(_.toString.toDouble))
        ))
      } catch {
        case _: Throwable => ("", (null, null))
      })
      .filter(_._1 != "")
      .collect

    doc_dict ++= collected
    (collected.length, starting_ms)
  }
  def queryArticleProfile2RDD(sc: SparkContext, mongo_input_split_size: String, mongo_input_uri_item_profile: String, starting_ts: Long) = {
    val hadoopConf = new Configuration()
    hadoopConf.set("mongo.input.split_size", mongo_input_split_size)
    hadoopConf.set("mongo.input.split.use_range_queries", "true")

    hadoopConf.set("mongo.input.uri", mongo_input_uri_item_profile)
    hadoopConf.set("mongo.input.query", "{crawled_at:{'$gte':{'$date':%d}}}".format(starting_ts))
    hadoopConf.set("mongo.input.fields", "{article_seq_id : 1, text_vector: 1, title_vector: 1}")

    new ConfiguredMongoReader(sc, hadoopConf).initRDD()
      .map(x => try {
        (x._2.get("article_seq_id").toString, (
          SparseVector(x._2.get("text_vector").asInstanceOf[BasicBSONList].toArray.map(_.toString.toDouble)),
          SparseVector(x._2.get("title_vector").asInstanceOf[BasicBSONList].toArray.map(_.toString.toDouble))
        ))
      } catch {
        case _: Throwable => ("", (null, null))
      })
      .filter(_._1 != "")
  }

  def updateTextProfileDict(sc: SparkContext, mongo_input_split_size: String, mongo_input_uri_item_profile: String,
                            batch_ts: Long, last_ts: Long, doc_dict: scala.collection.mutable.HashMap[String, SparseVector[Double]]) = {
    val hadoopConf = new Configuration()
    hadoopConf.set("mongo.input.split_size", mongo_input_split_size)
    hadoopConf.set("mongo.input.split.use_range_queries", "true")

    val starting_ts = if (doc_dict.isEmpty || last_ts < 0 || (batch_ts - last_ts) > MAX_ARTICLE_MS_DIFF) {
      batch_ts - MAX_ARTICLE_MS_DIFF
    } else {
      last_ts
    } - MARGIN

    hadoopConf.set("mongo.input.uri", mongo_input_uri_item_profile)
    hadoopConf.set("mongo.input.query", "{crawled_at:{'$gte':{'$date':%d}}}".format(starting_ts))
    hadoopConf.set("mongo.input.fields", "{article_seq_id : 1, text_vector: 1}")

    val collected = new ConfiguredMongoReader(sc, hadoopConf).initRDD()
      .map(x => try {
        (x._2.get("article_seq_id").toString,
          SparseVector(x._2.get("text_vector").asInstanceOf[BasicBSONList].toArray.map(_.toString.toDouble)))
      } catch {
        case _: Throwable => ("", null)
      })
      .filter(_._1 != "")
      .collect

    doc_dict ++= collected

    (collected.length, starting_ts)
  }

  //  def getArticleProfiles(sc: SparkContext, mongo_input_split_size :String, mongo_input_uri_item_profile: String,
  //                         article_seqids: Iterable[String]): Map[String, SparseVector[Double]] = {
  //    sc.getConf.set("spark.mongodb.input.uri", mongo_input_uri_item_profile)
  //
  //    MongoSpark.load(sc).withPipeline(Seq(Document.parse()))
  //  }

  // Events
  case class UserRead(user_seq_id: Int, article_seq_id: Int, article_type: String, lang: String, ts: Int,
                      //                         article_cate: String,
                      source: String, custom_categories: List[String])

  case class UserRefresh(user_seq_id: Int, article_seq_id: List[Int], article_type: String, lang: String, ts: Int,
                         //                         article_cate: List[String],
                         subtype: String, req_mode: String, custom_categories: List[String])

  private def parseUserReadJson(jsonString: String, mapper: ObjectMapper): UserRead = {
    mapper.readValue(jsonString, classOf[UserRead])
  }

  private def parseUserRefreshJson(jsonString: String, mapper: ObjectMapper): UserRefresh = {
    mapper.readValue(jsonString, classOf[UserRefresh])
  }
  private val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)
  mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

  def parseInstancePlus(rddJson: RDD[String], lang: String): RDD[(String, String, Double, List[String])] = {
    rddJson
      .filter(_.contains("user_seq_id"))
      .flatMap(x => if (x.contains("source")) {
        val parsedRead = parseUserReadJson(x, mapper)
        val source = parsedRead.source
        if (parsedRead.lang == lang && (source == "main" || source == "hot" || source == "trending")) {
          List((parsedRead.user_seq_id.toString, parsedRead.article_seq_id.toString, 1.0, parsedRead.custom_categories))
        } else {
          Array[(String, String, Double, List[String])]()
        }
      } else {
        val parsedRefresh = parseUserRefreshJson(x, mapper)
        if (parsedRefresh.lang == lang && parsedRefresh.article_type == "article" && parsedRefresh.req_mode == "online") {
          parsedRefresh.article_seq_id.map(y => (parsedRefresh.user_seq_id.toString, y.toString, 0.0, parsedRefresh.custom_categories))
        } else {
          Array[(String, String, Double, List[String])]()
        }
      })
  }

  def parseInstanceFromPairLine(rddLine: RDD[String], lang: String): RDD[(String, String, Double)] = {
    parseInstance(rddLine.map(line => line.substring(6, line.length - 1)), lang)
  }

  def parseInstance(rddJson: RDD[String], lang: String): RDD[(String, String, Double)] = {
    rddJson
      .filter(_.contains("user_seq_id"))
      .flatMap(x => if (x.contains("source")) {
        val parsedRead = parseUserReadJson(x, mapper)
        val source = parsedRead.source
        if (parsedRead.lang == lang && (source == "main" || source == "hot" || source == "trending")) {
          List((parsedRead.user_seq_id.toString, parsedRead.article_seq_id.toString, 1.0))
        } else {
          Array[(String, String, Double)]()
        }
      } else {
        val parsedRefresh = parseUserRefreshJson(x, mapper)
        if (parsedRefresh.lang == lang && parsedRefresh.article_type == "article" && parsedRefresh.req_mode == "online") {
          parsedRefresh.article_seq_id.map(y => (parsedRefresh.user_seq_id.toString, y.toString, 0.0))
        } else {
          Array[(String, String, Double)]()
        }
      })
  }

  def parseTsFromPairLine(data: RDD[String]): RDD[Int] = {
    data.map(x => {
      val json = x.substring(6, x.length - 1)
      json.contains("source") match {
        case true =>
          val parsedData = parseUserReadJson(json, mapper)
          parsedData.ts
        case false =>
          val parsedData = parseUserRefreshJson(json, mapper)
          parsedData.ts
      }
    })
  }
}
