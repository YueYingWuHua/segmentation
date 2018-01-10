package org.gz.fixwenshudata

import com.mongodb.MongoClientURI
import org.gz.util.MongoUserUtils
import com.mongodb.MongoClient
import scala.collection.JavaConversions._
import com.mongodb.client.model.Filters.{eq => eqq}
import com.mongodb.client.model.Updates._

object AlterGuidingID {
	
  def main(args: Array[String]): Unit = {
   	val mongoURI = new MongoClientURI(new MongoUserUtils().clusterMongoURI)
		val mongo = new MongoClient(mongoURI)
		val db = mongo.getDatabase("datamining")
		val dbColl = db.getCollection("guidingwenshu")		
		val dbColl2 = db.getCollection("guidingwenshu2")
		val dbColl3 = db.getCollection("guidingwenshu3")
		val md = java.security.MessageDigest.getInstance("MD5")
		val iter = dbColl.find().iterator()
		iter.foreach{ x => {				
			val id = x.getString("_id")			
			val md5 = md.digest(id.getBytes).map("%02x".format(_)).mkString			
			x.append("title", id)			
			val y = x
			x.append("_id", id.replace("\n", ""))
			dbColl2.insertOne(x)
			y.append("_id", md5)
			dbColl3.insertOne(y)
		}}
   	mongo.close()
  }
}