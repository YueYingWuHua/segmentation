package org.gz.test

import com.mongodb.MongoClient
import org.bson.Document
import scala.io.Source
import com.mongodb.spark.config.ReadConfig
import scala.reflect.ClassTag
import org.bson.conversions.Bson
import org.gz.mongospark.SegWithOrigin2
import java.io.File
import scala.collection.mutable.ArrayBuffer

object testDocument {
	
	def findRegex(file: String, str: String) = {
		val arr = Source.fromFile(new File("关键词/" + file)).getLines().toArray.map{ x => x.replaceAll("\\.\\.", "[^:：]*").replaceAll("---", "[^:：,。.，]*").replaceAll("==", "[^:：。.]*").r}
		arr.foreach(x => {			
			val it = x.findAllIn(str.trim)
  		if (it.hasNext)
  			if (it.start < 1)
  				println(x.toString() + "\t:\t" + it.next())
		})
	}
	
  def main(args: Array[String]): Unit = {
//  	lazy val mongo = new MongoClient("192.168.12.161", 27017)
//		lazy val db = mongo.getDatabase("wenshu")
//		lazy val dbColl = db.getCollection("sample_gz")
//		val d = new Document
//		d.append("_id", "123asd")
//		d.append("测试", "123asd")
//		d.append("测试", "234sdf")
//		d.append("测试", "345dfg")
//		println(d.get("_id"))
//		dbColl.insertOne(d)
//  	val path = Array("被上诉人辩称", "本院查明", "本院认为", "裁判结果", "裁判日期", "当事人", "第三人称", "法庭辩论", "附", "上诉人诉称", "审理经过", "审判人员",
//				"书记员", "一审被告辩称", "一审第三人称", "一审法院查明", "一审法院认为", "一审原告称")
//		for (f <- path){
//			println(f)
//  		Source.fromInputStream(ClassLoader.getSystemClassLoader().getResourceAsStream("resources/" + f)).getLines().foreach(println)
//		}
//  	val regex = "[^:：。.]*(原审|一审)[^:：,。.，]*[^上]诉称|[^:：。.]*原审诉称".replaceAll("\\.\\.", "[^:：]*").replaceAll("---", "[^:：,。.，]*").replaceAll("==", "[^:：。.]*").r
////  	val r = "[^:：]*二审期间".r
//  	val it = regex.findAllIn("刘清芳原审六六六上诉称：刘清芳到达法定退休年龄后")
//  	if (it.hasNext){
//  		println(it.next())
//  		println(it.start)
//  	}
//  	import com.mongodb.client.model.Filters.{eq => eqq}
//  	val docs = dbColl.find(eqq("_id", "0e927fe8-f1b1-4b01-a007-8006ff3a8ece")).iterator()
//  	val str1 = docs.next().getString("content")
//  	val c = 11.toChar
//  	val arrr = Array(dddd.getString("content")).flatMap{ x => x.split(s"[${c}\n]") }
//  	println(arrr.size)
//  	arrr.foreach { x => {println(x + "\n")} }
//  	println(11.toChar)
  	
		val str4 = "仕丰科技有限公司与富钧新型复合材料（太仓）有限公司、\r\n第三人永利集团有限公司解散纠纷案\r\n【裁判摘要】\r\n一、公司法第一百八十三条既是公司解散诉讼的立案受理条件，同时也是判决公司解散的实质审查条件，公司能否解散取决于公司是否存在僵局且符合公司法第一百八十三条规定的实质条件，而不取决于公司僵局产生的原因和责任。即使一方股东对公司僵局的产生具有过错，其仍然有权提起公司解散之诉，过错方起诉不应等同于恶意诉讼。\r\n二、公司僵局并不必然导致公司解散，司法应审慎介入公司事务，凡有其他途径能够维持公司存续的，不应轻易解散公司。当公司陷入持续性僵局，穷尽其他途径仍无法化解，且公司不具备继续经营条件，继续存续将使股东利益受到重大损失的，法院可以依据公司法第一百八十三条的规定判决解散公司。\r\n最高人民法院民事判决书\r\n（2011）民四终字第29号\r\n上诉人（原审被告）：富钧新型复合材料（太仓）有限公司。住所地：中华人民共和国江苏省太仓市浮桥镇沪浮璜公路88号。\r\n法定代表人：黄崇胜，该公司董事长。\r\n委托代理人：包忠华，江苏金太律师事务所律师。\r\n被上诉人（原审原告）：仕丰科技有限公司（SHINFENGTECHNOLOGYCO.，LTD）。住所地：萨摩亚国爱匹亚境外会馆大楼217号（OFFSHORECHAMBERS，P.O.BOX217，APIA，SAMOA）。\r\n法定代表人：郑萍，该公司董事。\r\n委托代理人：于云斌，北京市法大律师事务所律师。\r\n委托代理人：张博钦，男，1965年10月23日出生，台湾地区高雄市人，现住山东省济南市济北经济开发区强劲街8号。\r\n原审第三人：永利集团有限公司（WINNINGGROUPLIMITED）。住所地：萨摩亚国爱匹亚境外会馆大楼217号（OFFSHORECHAMBERS，P.O.BOX217，APIA，SAMOA）。\r\n法定代表人：黄崇胜，该公司董事。\r\n委托代理人：曹全南，江苏金太律师事务所律师。\r\n上诉人富钧新型复合材料（太仓）有限公司（以下简称富钧公司）因与被上诉人仕丰科技有限公司（以下简称仕丰公司）、原审第三人永利集团有限公司（以下简称永利公司）公司解散纠纷一案，不服江苏省高级人民法院于2011年5月26日作出的（2007）苏民三初字第3号民事判决，向本院提起上诉。本院受理后，依法组成由审判员陈纪忠担任审判长，审判员高晓力、代理审判员沈红雨参加的合议庭，于2011年10月27日公开开庭审理了本案，书记员张伯娜担任本案记录。上诉人富钧公司的委托代理人包忠华，被上诉人仕丰公司的原委托代理人朱小波、朱雷，原审第三人永利公司的委托代理人曹全南，到庭参加诉讼。2012年3月22日，本院收到仕丰公司从境外寄交并已依法办理公证认证手续的授权委托书，终止对朱小波、朱雷的授权，并重新委托于云斌及张博钦担任该公司的委托代理人。本案现已审理终结。\r\n"
		
//		val md = java.security.MessageDigest.getInstance("MD5")
//		println(md.digest(str4.getBytes).map("%02x".format(_)).mkString)
//		findRegex("上诉人诉称", """123上诉人王晓培、贾维杰辩称，一审判决认定事实清楚，适用法律正确，请求依法驳回上诉，维持原判。
//		  """)
		val c = 11.toChar
		val strs = str4.replaceAll("\r", "").split(s"[${c}\n]")
		strs.foreach {println}
		val docu = SegWithOrigin2.segment(strs)
		import scala.collection.JavaConversions._
    val arr = docu.get("全文", classOf[java.util.List[Document]])
   	arr.foreach{ println }
		//println(docu)
//		import scala.collection.JavaConverters._
//		val readConfig = ReadConfig("wenshu", "origin2", connectionString = Some("mongodb://192.168.12.161:27017"))
//  	mongo.getDatabase("")
//  	.getCollection[Document](readConfig.collectionName, classOf[Document])
//  	.withReadConcern(readConfig.readConcern)
//  	.withReadPreference(readConfig.readPreference)
//  	.aggregate(List[Bson]().asJava)
// // 	.allowDiskUse(true).
 // 	.iterator()
  }
}