package org.gz.fixwenshudata

import org.gz.util.MongoUserUtils
import com.mongodb.spark.MongoSpark
import org.gz.util.Utils
import org.gz.util.Conf
import com.mongodb.MongoClientURI
import com.mongodb.MongoClient

object MergeNewData extends Conf{
	
	def mergeData(mergeFrom: String) = {
		val muu = new MongoUserUtils
	  val spark = muu.sparkSessionBuilder(inputuri = muu.customizeSparkClusterURI(mergeFrom), jarName = "MigrateSepCollTo15.jar")
	  val rdd = MongoSpark.builder().sparkSession(spark).build.toRDD()
	  val mongosUris = config.getString("mongodb.clusteruriall")
	  rdd.foreachPartition{ iter =>
			val muuLocal = new MongoUserUtils
			val uri = s"${Utils.getIpAddress}:27017"
			println(s"local uri is: ${uri}")
			val mongoURI = 				
				if (mongosUris.split(",").contains(uri))
					new MongoClientURI(muuLocal.clusterLocalMongoURI(uri))
				else 
					new MongoClientURI(muuLocal.clusterURI)
			println(s"local mongouri is: ${mongoURI.getURI}")
			val mongo = new MongoClient(mongoURI)
			val db = mongo.getDatabase("datamining")
			val dbColl = db.getCollection("origind")
			iter.foreach{x => {
	   		try{	   			
	   			dbColl.insertOne(x)
	   		}catch{
	   			case e: Throwable => e.printStackTrace()
	   		}
   		}}
			mongo.close()
		}
	}
	
  def main(args: Array[String]): Unit = {
    mergeData("datamining.processeddata")
  }
}