package org.gz.dm.patternmatching.optimization

import com.mongodb.MongoClientURI
import org.gz.util.MongoUserUtils
import com.mongodb.MongoClient
import com.mongodb.client.model.Filters.{eq => eqq}
import com.mongodb.client.model.Filters.{ne => nne}
import com.mongodb.client.model.Filters.and
import scala.util.Random
import org.bson.Document
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashSet
import java.io.PrintWriter
import java.io.File
import scala.collection.JavaConversions._
import scala.io.Source
import scala.collection.mutable.HashMap
import java.util.List
import scala.collection.JavaConversions._

object ExtractNegative {
  private lazy val mongoURI = new MongoClientURI(new MongoUserUtils().clusterMongoURI)
	private lazy val mongo = new MongoClient(mongoURI)
	private lazy val db = mongo getDatabase "datamining"
	private lazy val dbColl = db getCollection "origind"
	val sampleCount = 100
	private val negativeWords = ArrayBuffer(("不予", 1), ("没有1", 10), ("不得1", 5), ("未1", 2))
	
	def addNegativeWords(word: String, range: Int = 1) = negativeWords += ((word, range))	
  
  def addSentenceToBuffer(str: String) = {
  	var arr = ArrayBuffer[String]()
  	if (str != null) {
  		val strs = str.split("。")
  		strs.foreach { x =>
  			negativeWords.foreach(y => {
  				if (x.contains(y._1)) arr += x 
  			})
  		}  		
  	}
  	arr
  }
  
  def sample = {
  	val iter = dbColl.find().iterator()  	
  	for (i <- 0 until sampleCount){
  		var arr = ArrayBuffer[String]()
  		val doc = iter.next()
  		val seg = doc.get("segdata", classOf[Document])
  		if (seg != null){
  			val 本院查明 = seg.getString("本院查明")
  			val 本院认为 = seg.getString("本院认为")
  			arr ++= addSentenceToBuffer(本院查明)
  			arr ++= addSentenceToBuffer(本院认为)  			
  		}
  		arr foreach println
  	}
  	mongo.close()
  }
  
  def getIndexOfWord(sentence: String) = {
  	10
  }  
  
  def containsNegativeKeyWord(sentence: String) = {
  	val wordIndex = getIndexOfWord(sentence)
  	var flag = false  	
  	negativeWords.foreach{x => 
  		val index = sentence.indexOf(x._1)
  		if ((index > 0)&&(math.abs(wordIndex - index) < x._2)) {
  			flag = !flag  			
  		}
  	}  	
  	flag
  }  
  
  def checkSentences(sens: List[String]) = {
  	val s2 = ArrayBuffer[String]()
  	sens.foreach { x =>
  		if (!containsNegativeKeyWord(x)) s2 += x 
  	}
  	s2
  }
  
  def processAnQing = {
  	val mongoURI = new MongoClientURI(new MongoUserUtils().clusterMongoURI)
  	val mongo = new MongoClient(mongoURI)
  	val db = mongo.getDatabase("datamining")
  	val dbColl = db.getCollection("民间借贷纠纷")
  	var res = ArrayBuffer[String]()
  	dbColl.find().foreach { x =>
  		val mining = x.get("mining", classOf[Document])
  		if (mining != null){
  			val aqSentence = mining.get("案情特征句子", classOf[List[String]])
  			res = if (aqSentence != null) checkSentences(aqSentence) else ArrayBuffer[String]()  			
  		}  			
  	}
  	mongo.close()
  	res
  }  
  
//  def getSentence(doc: Document): String = {
//  	val segdata = doc.get("segdata", classOf[Document])
//  	var sentence = ""
//  	if (segdata != null)  	
//  		segdata.keySet().foreach { x =>
//  			if ((x != "当事人")&&(x != "附")&&(x != "一审经过")&&(x != "全文")){
//  				val str = segdata.getString(x)
//  				if (str != null) sentence = sentence + "\n" + str
//  			} else if (x == "一审经过") {
//  				val 一审 = segdata.get(x, classOf[Document])
//  				if (一审 != null){
//  					val str = 一审.getString("全文")
//  					if (str != null) sentence = sentence + "\n" + str
//  				}
//  			}
//  		}
//  	sentence
//  }
  
  def main(args: Array[String]): Unit = {
  	sample
  }
}