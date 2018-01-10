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
		val db = mongo.getDatabase("datamining")
		val dbColl = db.getCollection("定金合同纠纷")				
		val dbColl3 = db.getCollection("公司决议效力确认纠纷")		
		val iter = dbColl.find(eqq("basiclabel.casecause", "公司决议效力确认纠纷")).iterator()
		iter.foreach{ x => {				
			dbColl3.insertOne(x)
		}}
   	mongo.close()
  }
}