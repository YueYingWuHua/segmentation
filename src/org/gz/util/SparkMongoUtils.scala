package org.gz.util

import org.bson.Document
import org.bson.conversions.Bson
import com.mongodb.spark.MongoSpark
import org.apache.spark.storage.StorageLevel
import com.mongodb.MongoClientURI
import com.mongodb.MongoClient
import com.mongodb.client.model.Filters.{eq => eqq}
import com.mongodb.client.model.Aggregates._
import com.mongodb.client.model.UpdateOptions
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import scala.collection.mutable.HashMap
import scala.util.Try
import scala.collection.JavaConversions._

class SparkMongoUtils(filters: Seq[Bson] = null, inputuri: SparkMongoIOStruct = null, jarName: String, extJars: Array[String] = Array(), outputuri: MongoIOStruct) {
}

object SparkMongoUtils{
	
	/**
	 * these functions are used for scala coder
	 * this migrateData used for migrate data to server at 15th or somewhere else.
	 * @param sparkSession, if use spark to migrate data, then specify this param
	 * @param inputuri, if not use spark, specify this
	 * @param outputuri, use mongoclient to insert or update data
	 */
	def migrateData(spark: SparkSession = null, inputuri: MongoRWConfig = null, outputuri: MongoRWConfig = null, filters: Seq[Bson] = Seq()){
		 if (spark != null) migrateDataSpark(spark, outputuri, filters) else migrateDataClient(inputuri, outputuri, filters)
	}
	
	def migrateDataSpark(spark: SparkSession, outputuri: MongoRWConfig, filters: Seq[Bson]) = {
		if (outputuri == null) MongoSpark.write(MongoSpark.builder().sparkSession(spark).pipeline(filters).build().toDF()) else{			
			val rdd = MongoSpark.builder().sparkSession(spark).pipeline(filters).build().toRDD()
			rdd.foreachPartition { iter => 
				val muuLocal = new MongoUserUtils			
				val mongoURI = new MongoClientURI(outputuri.uri)
				println(s"output mongouri is: ${mongoURI.getURI}")
				val mongo = new MongoClient(mongoURI)
				val db = mongo.getDatabase(outputuri.db)
				val coll = db.getCollection(outputuri.coll)
				iter.foreach{ x => 
					try{
						coll.insertOne(x)
					}catch{
						case e: Throwable => e.printStackTrace() 
					}
				}
				mongo.close()
			}
		}
	}
	
	def migrateDataClient(inputuri: MongoRWConfig, outputuri: MongoRWConfig, filters: Seq[Bson]) = {
		val arr = filters.toArray
		assert(arr.length < 2, "too many filters")
		val mongoinURI = new MongoClientURI(inputuri.uri)
		val mongoin = new MongoClient(mongoinURI)
		val dbin = mongoin.getDatabase(inputuri.db)
		val collin = dbin.getCollection(inputuri.coll)
		val mongooutURI = new MongoClientURI(outputuri.uri)
		val mongoout = new MongoClient(mongooutURI)
		val dbout = mongoout.getDatabase(outputuri.db)
		val collout = dbout.getCollection(outputuri.coll)
		val iter = if (arr.length == 0)	collin.find().iterator() else collin.find(arr(0)).iterator()
		var count = 0
		iter.foreach{x =>
			try{				
				collout.insertOne(x)
				count = count + 1
				if (count % 10000 == 0) println(s"now insert： ${count}")
			}catch{
				case e: Throwable => e.printStackTrace() 
			}
		}
		mongoin.close()
		mongoout.close()
	}
	
	
	/**
	 * after this method is spark util for java coder
	 */
	def __init__(smu: SparkMongoUtilsStruct) = {
		val spark =
			if (smu.inputuri != null)
				new MongoUserUtils().sparkSessionBuilder(inputuri = smu.inputuri.mongoSparkURI, jarName = smu.jarName, extJars = smu.extJars)
			else 
				new MongoUserUtils().sparkSessionBuilder(jarName = smu.jarName, extJars = smu.extJars)
		val rdd = 
			if (smu.filters != null) 
				MongoSpark.builder().sparkSession(spark).pipeline(smu.filters).build().toRDD()
			else 
				MongoSpark.builder().sparkSession(spark).build().toRDD()
	
		println(rdd.count)
		rdd.persist(StorageLevel.MEMORY_AND_DISK)
	
		val uri = smu.outputuri.generateMongoURI
		val dbname = smu.outputuri.db
		val collname = smu.outputuri.coll			
		(spark, rdd, uri, dbname, collname)
	}
	
	def generateDefaultOrigin2OutputURI = {
		val mu = new MongoUserUtils
		MongoIOStruct(mu.clusterURI, "wenshu", "origin2", mu.clusterUser, mu.clusterPW, mu.clusterAuthDB)
	}
	
	def generateClusterURI(db: String, coll: String) = {
		val mu = new MongoUserUtils
		MongoIOStruct(mu.clusterURI, db, coll, mu.clusterUser, mu.clusterPW, mu.clusterAuthDB)
	}
	
	def replace(smu: SparkMongoUtilsStruct, func: Document => Document) = {
		val (spark, rdd, uri, dbname, collname) = __init__(smu)
		rdd.foreachPartition{ x =>
			val mongoURI = new MongoClientURI(uri)
			val mongo = new MongoClient(mongoURI)
			val db = mongo.getDatabase(dbname)
			val dbColl = db.getCollection(collname)			
			x.foreach { y =>
				try{
					dbColl.replaceOne(eqq("_id", y.get("_id")), func(y), new UpdateOptions().upsert(true))					
				}catch{
					case e: Throwable => e.printStackTrace()
				}
			}
			mongo.close
		}
  }
	
	def replace(smu: SparkMongoUtilsStruct, func: (Document, Serializable*) => Document, obj: Serializable*) = {
		val (spark, rdd, uri, dbname, collname) = __init__(smu)
		rdd.foreachPartition{ x =>
			val mongoURI = new MongoClientURI(uri)
			val mongo = new MongoClient(mongoURI)
			val db = mongo.getDatabase(dbname)
			val dbColl = db.getCollection(collname)			
			x.foreach { y =>
				try{
					dbColl.replaceOne(eqq("_id", y.get("_id")), func(y, obj: _*), new UpdateOptions().upsert(true))					
				}catch{
					case e: Throwable => e.printStackTrace()
				}
			}
			mongo.close
		}
	}
	
	def main(args: Array[String]): Unit = {
	  val smu = SparkMongoUtilsStruct(filters = Seq(`match`(eqq("basiclabel.procedure", "二审"))), inputuri = SparkMongoIOStruct(generateClusterURI("datamining", "testsparkIO").generateMongoSparkURI), jarName = "testSparkUtil.jar", outputuri = generateClusterURI("datamining", "testsparkIO"))
	  val map = HashMap[String, String]()
	  map.+=(("segdata", ""))
	  replace(smu, DoWork.DoctoDoc, map)
	}
}

object DoWork extends Serializable{
	def DoctoDoc(d: Document, ser: Serializable*): Document = {
		val doc = d
		Try{
			val map = ser(0).asInstanceOf[HashMap[String, String]]
			map.foreach(x => {
				doc.append(x._1, x._2)
			})
		}		
		doc
	}
}

case class MongoRWConfig(uri: String, db: String, coll: String)

case class SparkMongoUtilsStruct(filters: Seq[Bson] = null, inputuri: SparkMongoIOStruct = null, jarName: String, extJars: Array[String] = Array(), outputuri: MongoIOStruct)

case class SparkMongoIOStruct(mongoSparkURI: String = "")

case class MongoIOStruct(uri: String, db: String, coll: String, user: String = null, pw: String, authdb: String){
	def generateMongoSparkURI = s"mongodb://${user}:${pw}@${uri}/${db}.${coll}?authSource=${authdb}"
	def generateMongoURI = new MongoUserUtils().generateMongoURI(user, pw, uri, authdb) 
}