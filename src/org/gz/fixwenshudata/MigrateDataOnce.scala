package org.gz.fixwenshudata

import org.gz.util.MongoUserUtils
import com.mongodb.MongoClientURI
import com.mongodb.MongoClient
import scala.collection.JavaConversions._
import com.mongodb.client.model.Filters.{eq => eqq}


object MigrateDataOnce {
  def main(args: Array[String]): Unit = {
   	val mongoURI = new MongoClientURI(new MongoUserUtils().clusterMongoURI)
		val mongo = new MongoClient(mongoURI)
		val db = mongo.getDatabase("updatesdata")
		val dbColl = db.getCollection("newdata")
		val dbColl2 = mongo.getDatabase("datamining").getCollection("processeddata")
		val iter = dbColl.find()
		var count = 0
		iter.foreach(x => {
			count = count + 1
			dbColl2.insertOne(x)
			if ((count & 1023) == 0) println(count)
		})
  }
}