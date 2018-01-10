package org.gz.fixwenshudata

import com.mongodb.MongoClientURI
import org.gz.util.MongoUserUtils
import com.mongodb.MongoClient
import scala.collection.JavaConversions._
import com.mongodb.client.model.Filters.{eq => eqq}
import com.mongodb.client.model.Updates._

object FixGongBaoWrap {
  val mongoURI = new MongoClientURI(new MongoUserUtils().clusterMongoURI)
  val mongo = new MongoClient(mongoURI)
  val db = mongo.getDatabase("datamining")
  val dbColl = db.getCollection("gongbao_anli")
  
  def main(args: Array[String]): Unit = {
    dbColl.find().iterator().foreach{x => 
    	val str = x.getString("content")
    	var flag = false
    	if (str != null){
    		val strs = str.replace("\r", "").split("\n")
    		strs.foreach { y =>
    			if ((y.trim().length() == 1)) {
    				flag = true    				
    			}
    			
    		}
    		
    		
    		
    	}
    	if (flag) println(x.getString("_id"))
    }
    mongo.close()
  }
}