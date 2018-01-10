package org.gz.fixwenshudata

import org.gz.util.MongoUserUtils
import com.mongodb.MongoClient
import scala.collection.JavaConversions._
import com.mongodb.client.model.Filters.{eq => eqq}
import com.mongodb.client.model.Updates._
import com.mongodb.MongoClientURI

object FixGuildingKeyWords {
  val mongoURI = new MongoClientURI(new MongoUserUtils().clusterMongoURI)
  val mongo = new MongoClient(mongoURI)
  val db = mongo.getDatabase("datamining")
  val dbColl = db.getCollection("guidingwenshu")
  
  def main(args: Array[String]): Unit = {
    dbColl.find().iterator().foreach{x => 
    	val str = x.getString("关键词")
    	if (str != null){
    		val ss = str.trim.split("[/,、 ]").mkString(" ")
    		println(x.getString("_id"))
    		println(ss)
    		dbColl.updateOne(eqq("_id", x.getString("_id")), set("关键词", ss))
    	}
    }
  }
}