package org.gz.fixwenshudata

import org.gz.util.IOUtils
import java.io.File
import org.apache.poi.xssf.usermodel.XSSFWorkbook
import java.io.FileInputStream
import scala.collection.JavaConversions._
import scala.collection.mutable.HashSet
import org.gz.util.MongoUserUtils
import com.mongodb.MongoClientURI
import com.mongodb.MongoClient
import com.mongodb.client.model.Filters.{eq => eqq}
import java.io.PrintWriter

object ExtractNewCaseCause {
	
	val set = HashSet[String]()
	
	def getCaseCause(str: String) = {
		var res = ""		
		val i = str.indexOf("案情特征")
		if (i > 0) res = str.substring(0, i)
		val j = str.indexOf("争议焦点")
		if (j > 0) res = str.substring(0, j)
		res.replace(12288.toChar, ' ').replace(11.toChar, ' ').replace(65279.toChar, ' ').trim()
	}
	
	def main(args: Array[String]): Unit = {
	  val files = IOUtils.getAllFiles(new File("C:/Users/cloud/Desktop/类案搜索/数据分析贾贺/新增案由/新增案由"))
  	files.foreach( x => {
  		val workbook = new XSSFWorkbook(new FileInputStream(x.getPath))
  		workbook.sheetIterator().foreach{ sheet => 
  			set += getCaseCause(sheet.getSheetName)				
  		}
  	})
  	val muu = new MongoUserUtils
  	val uri = new MongoClientURI(muu.clusterMongoURI)
  	val mongo = new MongoClient(uri)
	  val db = mongo.getDatabase("wenshu")
	  val coll = db.getCollection("origin2")
	  val db2 = mongo.getDatabase("datamining")
	  val coll2 = db2.getCollection("其他")
	  var num: Long = 0
  	set.foreach{x =>  		
  		val iter = coll.find(eqq("basiclabel.casecause", x)).iterator()
  		iter.foreach { x => try{
  			coll2.insertOne(x)
  			num = num + 1
  			if (num % 10000 == 0) println(num)
  		} catch {
  			case e: Throwable => e.printStackTrace()
  		}}
  	}
	  val printer = new PrintWriter(new File("C:/Users/cloud/Desktop/类案搜索/数据拆分分组/其他"))
	  printer.write(set.mkString(","))
	  printer.close()
	  println(num)
	  mongo.close()
	} 
}