package com.seven.spark.hdfs;


import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Utils {


	public static void saveHdfs(JavaRDD<String> rdd,JavaSparkContext sc,String savePath) {
		Configuration hadoopConf = sc.hadoopConfiguration();
		Path hdfsPath = new Path(savePath);
		try {
			URI uri = new java.net.URI("hdfs://vm-xaj-bigdata-da-d01:8020");
			FileSystem hdfs = org.apache.hadoop.fs.FileSystem.get(uri, hadoopConf);
			if (hdfs.exists(hdfsPath)) {
				hdfs.delete(hdfsPath, true);
			}
			rdd.repartition(1).saveAsTextFile(savePath);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void saveHdfs(JavaRDD<String> rdd,JavaSparkContext sc,String savePath,int part) {
		Configuration hadoopConf = sc.hadoopConfiguration();
		Path hdfsPath = new Path(savePath);
		try {
			URI uri = new java.net.URI("hdfs://vm-xaj-bigdata-da-d01:8020");
			FileSystem hdfs = org.apache.hadoop.fs.FileSystem.get(uri, hadoopConf);
			if (hdfs.exists(hdfsPath)) {
				hdfs.delete(hdfsPath, true);
			}
			if(part==0) {
				rdd.saveAsTextFile(savePath);
			}else {
				rdd.repartition(part).saveAsTextFile(savePath);
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void saveHdfs(JavaPairRDD<String,String> rdd,JavaSparkContext sc,String savePath) {
		Configuration hadoopConf = sc.hadoopConfiguration();
		Path hdfsPath = new Path(savePath);
		try {
			URI uri = new java.net.URI("hdfs://vm-xaj-bigdata-da-d01:8020");
			FileSystem hdfs = org.apache.hadoop.fs.FileSystem.get(uri, hadoopConf);
			if (hdfs.exists(hdfsPath)) {
				hdfs.delete(hdfsPath, true);
			}
			rdd.repartition(1).saveAsTextFile(savePath);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void saveHdfs(JavaPairRDD<String,String> rdd,JavaSparkContext sc,String savePath,int part) {
		Configuration hadoopConf = sc.hadoopConfiguration();
		Path hdfsPath = new Path(savePath);
		try {
			URI uri = new java.net.URI("hdfs://vm-xaj-bigdata-da-d01:8020");
			FileSystem hdfs = org.apache.hadoop.fs.FileSystem.get(uri, hadoopConf);
			if (hdfs.exists(hdfsPath)) {
				hdfs.delete(hdfsPath, true);
			}
			if(part==0) {
				rdd.saveAsTextFile(savePath);
			}else {
				rdd.repartition(part).saveAsTextFile(savePath);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void delHdfs(JavaSparkContext sc,String savePath) {
		Configuration hadoopConf = sc.hadoopConfiguration();
		Path hdfsPath = new Path(savePath);
		try {
			URI uri = new java.net.URI("hdfs://vm-xaj-bigdata-da-d01:8020");
			FileSystem hdfs = org.apache.hadoop.fs.FileSystem.get(uri, hadoopConf);
			if (hdfs.exists(hdfsPath)) {
				hdfs.delete(hdfsPath, true);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}



}
