package org.gz.fixwenshudata

import org.gz.util.MongoUserUtils
import org.gz.util.SparkMongoUtils
import org.gz.util.MongoRWConfig
import java.io.File
import org.gz.SeperateCollection

object MigrateSepCollTo15 {
	
	def migrateTo15 = {
		val folder = new File("C:/Users/cloud/Desktop/类案搜索/数据拆分分组2")
  	folder.list.foreach { z => 
	//		val z = "定金合同纠纷"
  		println(z)			
	  	val muu = new MongoUserUtils
	    val spark = muu.sparkSessionBuilder(inputuri = muu.customizeSparkClusterURI(s"datamining.${z}"), jarName = "MigrateSepCollTo15.jar")
	    SparkMongoUtils.migrateData(spark = spark, outputuri = MongoRWConfig(muu.backupMongoURI, "datamining", z))
	    spark.close()
	    Thread.sleep(10000)
  	}
	}

	def migrateOrigin = {
		val muu = new MongoUserUtils
	  val spark = muu.sparkSessionBuilder(inputuri = muu.customizeSparkClusterURI(s"datamining.origind"), jarName = "MigrateSepCollTo15.jar")
	  SparkMongoUtils.migrateData(spark = spark, outputuri = MongoRWConfig(muu.backupMongoURI, "datamining", "origind2"))
	  spark.close()
	}
	
	def migrateOriginAndSeperate = {
		SeperateCollection.doSeperateData("backup")
	}
	
	def migratelocal = {
		val muu = new MongoUserUtils
	  val spark = muu.sparkSessionBuilder(inputuri = muu.customizeSparkClusterURI("wenshu.origin2"), jarName = "MigrateSepCollTo15.jar")
	  SparkMongoUtils.migrateData(spark = spark, outputuri = MongoRWConfig(muu.clusterMongoURI, "datamining", "origind"))
	}
	
	def migrategongbao = {
		val muu = new MongoUserUtils	  
	  SparkMongoUtils.migrateData(inputuri = MongoRWConfig(muu.clusterMongoURI, "datamining", "gongbao_anli_seg"), outputuri = MongoRWConfig(muu.backupMongoURI, "datap", "gongbao_anli_seg"))
	}
		
  def main(args: Array[String]): Unit = {
  	migrateOriginAndSeperate
  }
}