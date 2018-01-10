package org.gz.fixwenshudata

import org.apache.poi.xssf.usermodel.XSSFWorkbook
import java.io.FileInputStream
import org.gz.getershen
import java.io.File
import scala.collection.mutable.HashMap
import org.gz.util.IOUtils
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import com.mongodb.spark.config.ReadConfig
import org.gz.util.MongoUserUtils
import org.gz.util.Conf
import com.mongodb.spark.MongoSpark
import org.bson.Document
import java.util.ArrayList
import org.apache.spark.storage.StorageLevel
import org.gz.util.Utils
import com.mongodb.MongoClientURI
import com.mongodb.MongoClient
import com.mongodb.client.model.Filters.{eq => eqq}
import com.mongodb.client.model.Updates._

object FixAnyouBlank extends Conf{
  def countError = {
  	val files = IOUtils.getAllFiles(new File("C:/Users/cloud/Desktop/类案搜索/数据分析贾贺/标注数据集"))
  	files.foreach( x => {
  		val workbook = new XSSFWorkbook(new FileInputStream(x.getPath))
  		workbook.sheetIterator().foreach{ sheet => 
				val rows = sheet.rowIterator()		
				rows.foreach { _.cellIterator().foreach { cell => 
					val str = cell.getStringCellValue
					val str2 = str.replace(12288.toChar, ' ').replace(11.toChar, ' ').replace(65279.toChar, ' ').trim()
					if (str != str2) println(s"'${str}'\t${sheet.getSheetName}\t${x.getPath}")
				}}
  		}
  	})		
	}
  
  def doRemove(str: String) = {
  	if (str != null)
  		str.replace(12288.toChar, ' ').replace(11.toChar, ' ').replace(65279.toChar, ' ').split("/").map(_.trim).mkString("/")
  	else null
  }
  
  def main(args: Array[String]): Unit = {
  	val (user, passwd, authDB) = (config.getString("mongo.cluster.user"), config.getString("mongo.cluster.passwd"), config.getString("mongo.cluster.authDB"))
		val muu = new MongoUserUtils
		lazy val spark = muu.sparkSessionBuilder(inputuri = muu.customizeSparkClusterURI("datamining.testsparkIO"), jarName = "fixcaseblank.jar")
		val folder = new File("C:/Users/cloud/Desktop/类案搜索/数据拆分分组2")
  	folder.list.foreach { z => 
	//		val z = "定金合同纠纷"
  		println(z)
			val readConfig = ReadConfig(Map("uri" -> muu.customizeSparkClusterURI(s"datamining.${z}")))
			var rdd = MongoSpark.builder().sparkSession(spark).readConfig(readConfig).build().toRDD()
	//		rdd.persist(StorageLevel.MEMORY_AND_DISK)
	//		println(rdd.count())
			val mongosUris = config.getString("mongodb.clusteruriall")
			rdd.foreachPartition{ y =>
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
				val dbColl = db.getCollection(z)
				y.foreach{ x => 
					val ml = x.get("mininglabel", classOf[Document])
					if (ml != null){
						val 案情 = ml.get("案件特征", classOf[ArrayList[String]])		  		
			  		if (案情 != null){
				  		ml.append("案件特征", 案情.map{doRemove}.toList.asJava)
		  			}
						val 争议 = ml.get("争议焦点", classOf[ArrayList[String]])
						if (争议 != null){
				  		ml.append("争议焦点", 争议.map{doRemove}.toList.asJava)
		  			}
		  			dbColl.updateOne(eqq("_id", x.getString("_id")), set("mininglabel", ml))
					}}
				mongo.close()				
				rdd = null
			}		
  	}
  }
}