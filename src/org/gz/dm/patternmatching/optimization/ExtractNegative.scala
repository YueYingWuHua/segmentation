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

object ExtractNegative {
  private lazy val mongoURI = new MongoClientURI(new MongoUserUtils().clusterMongoURI)
	private lazy val mongo = new MongoClient(mongoURI)
	private lazy val db = mongo getDatabase "datamining"
	private lazy val dbColl = db getCollection "sampledata"
	val sampleCount = 100
	private val negativeWords = ArrayBuffer(("不", 1), ("没有", 10), ("不得", 5), ("未", 2))
	
	def addNegativeWords(word: String, range: Int = 1) = negativeWords += ((word, range))
	
	def sampledata(casecause: String) = {
		val count1 = dbColl.count(and(eqq("basiclabel.casecause", casecause), nne("mininglabel", null)))
		val step = Math.floor((count1 / sampleCount * 2 - 1)).toInt 
		println(count1)
		println(step)
		val iter = dbColl.find(and(eqq("basiclabel.casecause", casecause), nne("mininglabel", null))).iterator()
		var r = new Random
    def nextInt = r.nextInt(step) + 1
    var i = nextInt
    var count = 0
    var doc = new Document() 
		val docs = new ArrayBuffer[Document]()
    var ins = 0
    while (iter.hasNext()){
    	doc = iter.next()
    	if (count == i){
    		ins = ins + 1
    		i = i + nextInt
	    	docs += doc	    	
    	}
    	count = count + 1    	
    }
		println(count)
		docs
  }
  
  
  
  def containsNegativeKeyWord(sentence: String, key: String) = {
  	val words = key.split(",|，")
  	var flag = true
  	val poi = ArrayBuffer[Integer]()
  	words.foreach{x => 
  		val index = sentence.indexOf(x)
  		if (index > 0) {
  			poi += index  			
  		} else flag = false
  	}
  	var flag2 = false
  	if (flag)
  		negativeWords.foreach(x => {  			
  			val index = sentence.indexOf(x)
  			poi.foreach { y => 
  			}
	  		flag2 = flag2 || sentence.contains(x._1)  		
  		})
  	flag&flag2
  }
  
  def getSentence(doc: Document): String = {
  	val segdata = doc.get("segdata", classOf[Document])
  	var sentence = ""
  	if (segdata != null)  	
  		segdata.keySet().foreach { x =>
  			if ((x != "当事人")&&(x != "附")&&(x != "一审经过")&&(x != "全文")){
  				val str = segdata.getString(x)
  				if (str != null) sentence = sentence + "\n" + str
  			} else if (x == "一审经过") {
  				val 一审 = segdata.get(x, classOf[Document])
  				if (一审 != null){
  					val str = 一审.getString("全文")
  					if (str != null) sentence = sentence + "\n" + str
  				}
  			}
  		}
  	sentence
  }
  
  def main(args: Array[String]): Unit = {
  }
}