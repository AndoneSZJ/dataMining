package com.seven.spark.vem.base;

import com.seven.spark.utils.Utils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.text.SimpleDateFormat;
import java.util.Date;

public class Records {

	public static void main(String[] args) {

		SparkConf sparkConf = new SparkConf().setAppName("tmp");
		if (args == null || args.length == 0) {
			sparkConf.setMaster("local[*]");
		}
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		SimpleDateFormat format2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date starttime = new Date();
		Logger.getRootLogger().info("jobStarttime:" + format2.format(starttime.getTime()));
		Logger.getRootLogger().setLevel(Level.OFF);

		// String path0 = "order";
		String path0 = "hdfs://vm-xaj-bigdata-da-d01:8020/yst/vem/sales/order";
		
		JavaRDD<String> initialData = sc.textFile(path0).cache();
		System.out.println(initialData.count());

		String basePath = "hdfs://vm-xaj-bigdata-da-d01:8020/yst/vem/sales/record/";
		JavaPairRDD<String, String> recordProdOfMonthly = getRecordData(initialData, sc, "Pr", "M");
		Utils.saveHdfs(recordProdOfMonthly, sc, basePath + "Pr_M");
		JavaPairRDD<String, String> recordProdOfDaily = getRecordData(initialData, sc, "Pr", "D");
		Utils.saveHdfs(recordProdOfDaily, sc, basePath + "Pr_D");
		JavaPairRDD<String, String> recordProdOfWeekly = getRecordData(initialData, sc, "Pr", "w");
		Utils.saveHdfs(recordProdOfWeekly, sc, basePath + "Pr_w");

		JavaPairRDD<String, String> recordPointOfMonthly = getRecordData(initialData, sc, "P", "M");
		Utils.saveHdfs(recordPointOfMonthly, sc, basePath + "P_M");
		JavaPairRDD<String, String> recordPointOfDaily = getRecordData(initialData, sc, "P", "D");
		Utils.saveHdfs(recordPointOfDaily, sc, basePath + "P_D");
		JavaPairRDD<String, String> recordPointOfWeekly = getRecordData(initialData, sc, "P", "w");
		Utils.saveHdfs(recordPointOfWeekly, sc, basePath + "P_w");

		JavaPairRDD<String, String> recordNetOfMonthly = getRecordData(initialData, sc, "N", "M");
		Utils.saveHdfs(recordNetOfMonthly, sc, basePath + "N_M");
		JavaPairRDD<String, String> recordNetOfDaily = getRecordData(initialData, sc, "N", "D");
		Utils.saveHdfs(recordNetOfDaily, sc, basePath + "N_D");
		JavaPairRDD<String, String> recordNetOfWeekly = getRecordData(initialData, sc, "N", "w");
		Utils.saveHdfs(recordNetOfWeekly, sc, basePath + "N_w");

		JavaPairRDD<String, String> recordCityOfMonthly = getRecordData(initialData, sc, "C", "M");
		Utils.saveHdfs(recordCityOfMonthly, sc, basePath + "C_M");
		JavaPairRDD<String, String> recordCityOfDaily = getRecordData(initialData, sc, "C", "D");
		Utils.saveHdfs(recordCityOfDaily, sc, basePath + "C_D");
		JavaPairRDD<String, String> recordCityOfWeekly = getRecordData(initialData, sc, "C", "w");
		Utils.saveHdfs(recordCityOfWeekly, sc, basePath + "C_w");

		JavaPairRDD<String, String> recordProdOfPointOfMonthly = getRecordData(initialData, sc, "Pr_P", "M");
		Utils.saveHdfs(recordProdOfPointOfMonthly, sc, basePath + "Pr_P_M");
		JavaPairRDD<String, String> recordProdOfNetOfMonthly = getRecordData(initialData, sc, "Pr_N", "M");
		Utils.saveHdfs(recordProdOfNetOfMonthly, sc, basePath + "Pr_N_M");
		JavaPairRDD<String, String> recordProdOfPointOfWeekly = getRecordData(initialData, sc, "Pr_P", "w");
		Utils.saveHdfs(recordProdOfPointOfWeekly, sc, basePath + "Pr_P_w");
		JavaPairRDD<String, String> recordProdOfNetOfWeekly = getRecordData(initialData, sc, "Pr_N", "w");
		Utils.saveHdfs(recordProdOfNetOfWeekly, sc, basePath + "Pr_N_w");

		JavaPairRDD<String, String> recordProdOfCityOfMonthly = getRecordData(initialData, sc, "Pr_C", "M");
		Utils.saveHdfs(recordProdOfCityOfMonthly, sc, basePath + "Pr_C_M");
		JavaPairRDD<String, String> recordProdOfCityOfWeekly = getRecordData(initialData, sc, "Pr_C", "w");
		Utils.saveHdfs(recordProdOfCityOfWeekly, sc, basePath + "Pr_C_w");
		JavaPairRDD<String, String> recordProdOfCityOfDaily = getRecordData(initialData, sc, "Pr_C", "D");
		Utils.saveHdfs(recordProdOfCityOfDaily, sc, basePath + "Pr_C_D");

		JavaPairRDD<String, String> recordMonthly = getRecordData(initialData, sc, "A", "M");
		Utils.saveHdfs(recordMonthly, sc, basePath + "A_M");
		JavaPairRDD<String, String> recordDaily = getRecordData(initialData, sc, "A", "D");
		Utils.saveHdfs(recordDaily, sc, basePath + "A_D");
		JavaPairRDD<String, String> recordWeekly = getRecordData(initialData, sc, "A", "w");
		Utils.saveHdfs(recordWeekly, sc, basePath + "A_w");

		Logger.getRootLogger().setLevel(Level.INFO);
		Logger.getRootLogger().info("jobEndtime:" + format2.format(new Date().getTime()) + ";");
		Logger.getRootLogger().info("jobResult:Sucessfull!");
		Logger.getRootLogger().setLevel(Level.OFF);
		sc.close();

	}

	@SuppressWarnings("serial")
	private static JavaPairRDD<String, String> getRecordData(JavaRDD<String> initialData, JavaSparkContext sc,
			String type, String dttype) {

		final Broadcast<String> bv = sc.broadcast(type + "," + dttype);
		JavaPairRDD<String, String> record = initialData.mapToPair(new PairFunction<String, String, String>() {

			@Override
			public Tuple2<String, String> call(String s) throws Exception {
				// System.out.println(1);
				String[] ss = s.split(",");
				String amount = ss[4];
				String count = ss[14];
				String acountType = ss[3];
				String dt = ss[0];

				String types[] = bv.getValue().split(",");
				String focus = "1";
				if ("N".equals(types[0])) {
					focus = ss[8];
				} else if ("P".equals(types[0])) {
					focus = ss[9];
				} else if ("C".equals(types[0])) {
					focus = ss[10];
				} else if ("Pr".equals(types[0])) {
					focus = ss[11];
				} else if ("Pr_P".equals(types[0])) {
					focus = ss[9] + "," + ss[11];
				} else if ("Pr_N".equals(types[0])) {
					focus = ss[8] + "," + ss[11];
				} else if ("Pr_C".equals(types[0])) {
					focus = ss[10] + "," + ss[11];
				}

				if ("D".equals(types[1])) {
					dt = ss[0].substring(0, 10);
				} else if ("M".equals(types[1])) {
					dt = ss[0].substring(0, 7);
				} else if ("w".equals(types[1])) {
					SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");
					SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-w");
					dt = sdf2.format(sdf.parse(ss[0]));
				}

				String status = ss[2];
				String invld = "0";
				String ismember = "0";
				String memberAmount = "0";
				if ("3".equals(status)) {
					invld = "0";
				} else {
					invld = "1";
					amount = "0";
					count = "0";
				}

				if ("102".equals(acountType)) {
					ismember = "1";
					memberAmount = amount;
				}
				// System.out.println(2);
				// return new Tuple2<String, String>("1","2");
				return new Tuple2<String, String>(focus + "," + dt,
						amount + "," + count + "," + invld + "," + ismember + "," + memberAmount);
			}
		}).reduceByKey(new Function2<String, String, String>() {

			@Override
			public String call(String s1, String s2) throws Exception {
				String[] ss1 = s1.split(",");
				String[] ss2 = s2.split(",");
				return (Double.parseDouble(ss1[0]) + Double.parseDouble(ss2[0])) + ","
						+ (Integer.parseInt(ss1[1]) + Integer.parseInt(ss2[1])) + ","
						+ (Integer.parseInt(ss1[2]) + Integer.parseInt(ss2[2])) + ","
						+ (Integer.parseInt(ss1[3]) + Integer.parseInt(ss2[3])) + ","
						+ (Double.parseDouble(ss1[4]) + Double.parseDouble(ss2[4]));
			}
		});
		return record;
	}

}
