package org.gz.fixwenshudata

import com.mongodb.MongoClientURI
import org.gz.util.MongoUserUtils
import com.mongodb.MongoClient
import java.io.File
import scala.io.Source
import com.mongodb.client.model.Filters.{eq => eqq}
import com.mongodb.client.model.Filters.or
import com.mongodb.client.model.Filters.and
import com.mongodb.client.model.Filters.{ne => nee}
import scala.collection.mutable.ArrayBuffer
import java.io.PrintWriter
import scala.collection.mutable.HashSet
import scala.collection.mutable.HashMap

object CountAnQingNotNull {
	val mongoURI = new MongoClientURI(new MongoUserUtils().clusterMongoURI)
  val mongo = new MongoClient(mongoURI)
  val db = mongo.getDatabase("datamining")  
  
  def countAnQing() = {
		var 空案由 = ArrayBuffer[String]()
		var 没特征 = ArrayBuffer[String]()
		var 正常 = ArrayBuffer[String]()			
		val folder = new File("C:/Users/cloud/Desktop/类案搜索/数据拆分分组2")
  	folder.listFiles().foreach { x =>
  		Source.fromFile(x).getLines().foreach {_.split(",").foreach { casecause =>
  			var res = ""
  			print(casecause + ":\t")
  			val iter = db.getCollection(x.getName).find(and(eqq("basiclabel.casecause", casecause), or(nee("mininglabel.案件特征", null), nee("mininglabel.争议焦点", null)))).iterator()
  			//val iter = db.getCollection(x.getName).find(and(eqq("basiclabel.casecause", casecause), nee("mininglabel.争议焦点", null))).iterator()
  			if (iter.hasNext()){
  				正常 += casecause + "->" + x.getName
  				res = "正常"
  			}else {
  				val iter2 = db.getCollection(x.getName).find(eqq("basiclabel.casecause", casecause)).iterator()
  				if (iter2.hasNext()){
  					没特征 += casecause + "->" + x.getName
  					res = "没特征"
  				} else {
  					空案由 += casecause + "->" + x.getName
  					res = "空案由"
  				}
  			}
  			println(res)
  		}}
  	}
		println("正常 = " + 正常.length)
		println("没特征 = " + 没特征.length)
		println("空案由 = " + 空案由.length)
		def writeTo(str: String, arr: ArrayBuffer[String]) = {
			val writer = new PrintWriter(new File(s"C:/Users/cloud/Desktop/类案搜索/数据拆分分组数据统计/$str"))
			writer.write(arr.mkString("\n"))
			writer.flush()
			writer.close()
		}		
	}
	
	def countnew = {
		val file = new File(s"C:/Users/cloud/Desktop/类案搜索/数据拆分分组数据统计/")
		val map = HashMap[String, (String, String)]()
		file.listFiles().foreach { x =>			
			Source.fromFile(x).getLines().foreach { y =>
				val ss = y.split("->")
				if (map.contains(ss(0))){
					if (ss(1) == "其他")
						map += ((ss(0), (ss(1), x.getName)))
					else if (map.getOrElse(ss(0), ("", ""))._1 != "其他") println(ss(0))
				}else map += ((ss(0), (ss(1), x.getName)))
			}
		}
		val writer = new PrintWriter(new File(s"C:/Users/cloud/Desktop/类案搜索/数据拆分分组数据统计/正常1"))
		val writer1 = new PrintWriter(new File(s"C:/Users/cloud/Desktop/类案搜索/数据拆分分组数据统计/没特征1"))
		val writer2 = new PrintWriter(new File(s"C:/Users/cloud/Desktop/类案搜索/数据拆分分组数据统计/空案由1"))
		map.foreach(x => {
			println(x._1 + "->" + x._2._1 + "->" + x._2._2)
			if (x._2._2 == "正常") writer.write(x._1 + "->" + x._2._1 + "\n") else
			if (x._2._2 == "没特征") writer1.write(x._1 + "->" + x._2._1 + "\n") else
			if (x._2._2 == "空案由") writer2.write(x._1 + "->" + x._2._1 + "\n")	else
			println("1234321")			
		})
		writer.close()
		writer1.close()
		writer2.close()
		file.listFiles().foreach { x =>			
			println(x.getName + "\t" + Source.fromFile(x).getLines().size)
		}
  }
	
	def countold = {
		val file = new File(s"C:/Users/cloud/Desktop/类案搜索/数据拆分分组2/")
		val set = HashSet[String]()
		file.listFiles().foreach { x =>			
			Source.fromFile(x).getLines().foreach { y =>
				val ss = y.split("->")
				if (set.contains(ss(0))) println("duplicate " + ss(0))
				set += ss(0)
			}
		}
		println(set.size)
  }
  
  def main(args: Array[String]): Unit = {
  	countold
  }
}