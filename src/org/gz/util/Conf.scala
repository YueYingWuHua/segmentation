package org.gz.util

import com.typesafe.config.ConfigFactory

trait Conf {
	private val configPaths = Array("wangshan.conf", "yangcongjun.conf", "yuanyuan.conf", "jiahe.conf", "gaolaoban.conf")
	private var configs = ConfigFactory.load()
	configPaths.foreach{x => configs = configs.withFallback(ConfigFactory.load(x))}		
  val config = configs
  def sparkURI = "spark://" + config.getString("spark.uri")
  def hdfsURI = "hdfs://" + config.getString("hdfs.uri") + "/"
}

object ConfigUtil extends Conf{
	//get不到的时候初始化失败
	val (ftpuser, ftppasswd, fptURI) = (config.getString("ftp.user"), config.getString("ftp.passwd"), config.getString("ftp.uri"))
}