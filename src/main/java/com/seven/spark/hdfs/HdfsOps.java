package com.seven.spark.hdfs;


import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class HdfsOps {

	private final static String hadoopPath = "hdfs://vm-xaj-bigdata-da-d01:8020";

	public static void saveHdfs(JavaRDD<String> rdd, JavaSparkContext sc, String savePath) {
		saveData(rdd, sc, savePath, 0);
	}

	public static void saveHdfs(JavaRDD<String> rdd, JavaSparkContext sc, String savePath, int part) {
		saveData(rdd, sc, savePath, part);
	}


	private static void saveData(JavaRDD<String> rdd, JavaSparkContext sc, String savePath, int part) {
		saveData(sc, savePath, rdd, null, part);
	}

	public static void saveHdfs(JavaPairRDD<String, String> rdd, JavaSparkContext sc, String savePath) {
		saveData(rdd, sc, savePath, 0);
	}

	public static void saveHdfs(JavaPairRDD<String, String> rdd, JavaSparkContext sc, String savePath, int part) {
		saveData(rdd, sc, savePath, part);
	}

	private static void saveData(JavaPairRDD<String, String> rdd, JavaSparkContext sc, String savePath, int part) {
		saveData(sc, savePath, null, rdd, part);
	}

	private static void saveData(JavaSparkContext sc, String savePath,
								 JavaRDD<String> javaRDD, JavaPairRDD<String, String> javaPairRDD, int part) {
		Configuration hadoopConf = sc.hadoopConfiguration();
		Path path = new Path(savePath);
		try {
			URI uri = new URI(hadoopPath);
			FileSystem fileSystem = FileSystem.get(uri, hadoopConf);
			if (fileSystem.exists(path)) {
				fileSystem.delete(path, true);
			}
			if (javaRDD != null) {
				if (part == 0) {
					javaRDD.repartition(1).saveAsTextFile(savePath);
				} else {
					javaRDD.repartition(part).saveAsTextFile(savePath);
				}
			} else {
				if (part == 0) {
					javaPairRDD.repartition(1).saveAsTextFile(savePath);
				} else {
					javaPairRDD.repartition(part).saveAsTextFile(savePath);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void delHdfs(JavaSparkContext sc, String savePath) {
		Configuration hadoopConf = sc.hadoopConfiguration();
		Path path = new Path(savePath);
		try {
			URI uri = new URI(hadoopPath);
			FileSystem fileSystem = FileSystem.get(uri, hadoopConf);
			if (fileSystem.exists(path)) {
				fileSystem.delete(path, true);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}



}
