package org.gz.fixwenshudata

import org.gz.util.MongoUserUtils
import com.mongodb.MongoClientURI
import com.mongodb.MongoClient
import scala.collection.JavaConversions._
import com.mongodb.client.model.Filters.{eq => eqq}
import com.mongodb.client.model.Filters.{lt, gt, lte, gte, and}
import com.mongodb.client.MongoCollection
import org.bson.Document
import java.util.Date
import java.text.SimpleDateFormat
import com.mongodb.client.FindIterable


object MigrateDataOnce {
	
	val lastEndTime = "20170101"
	
	def migrateNewdata(startTime: String = lastEndTime) = {
		val mongoURI = new MongoClientURI(new MongoUserUtils().clusterMongoURI)
		val mongo = new MongoClient(mongoURI)
		val db = mongo.getDatabase("updatesdata")
		val dbColl = db.getCollection("newdata")
		val dbColl2 = mongo.getDatabase("datamining").getCollection("processeddata")
		val iter = findNewDataByDate(dbColl, startTime)
		var count = 0
		iter.foreach(x => {
			count = count + 1
			dbColl2.insertOne(x)
			if ((count & 1023) == 0) println(count)
		})
		mongo.close()
	}	
	
	def findNewDataByDate(dbColl: MongoCollection[Document], start: Date, endtime: Date) = {		
		val iter = dbColl.find(and(lte("insertdate", endtime), gt("insertdate", start)))
		iter
	}
	
	def findNewDataByDate(dbColl: MongoCollection[Document], start: String, end: String = null): FindIterable[Document] = {
		val sdf = new SimpleDateFormat("yyyyMMdd")
		val endtime = if (end != null) end else sdf.format(System.currentTimeMillis())
		findNewDataByDate(dbColl, sdf.parse(start), sdf.parse(endtime))
	}
	
	def moveTo15(source: MongoCollection[Document], dbName: String, collName: String) = {
		val mongoURI = new MongoClientURI(new MongoUserUtils().backupMongoURI)
		val mongo = new MongoClient(mongoURI)
		val db = mongo.getDatabase(dbName)
		val coll = db.getCollection(collName)
		val iter = source.find()
		var count = 0
		iter.foreach(x => {
			count = count + 1
			coll.insertOne(x)
			if ((count & 1023) == 0) println(count)
		})
		mongo.close()
	}
	 
	def migrateGuildAndGB = {
		val mongoURI = new MongoClientURI(new MongoUserUtils().clusterMongoURI)
		val mongo = new MongoClient(mongoURI)
		val db = mongo.getDatabase("datamining")
		val guiding = db.getCollection("guidingwenshu3")
		val gb = db.getCollection("gongbao_anli")
		moveTo15(guiding, "datamining", "guidingwenshu")
		moveTo15(gb, "datamining", "gongbaoanli")
		mongo.close()
	}
	
  def main(args: Array[String]): Unit = {
   	//migrateGuildAndGB  	
  }
}