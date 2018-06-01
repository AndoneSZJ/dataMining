package com.seven.spark.vem.base;

import com.seven.spark.hdfs.Utils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class NetBaseInfo {

	@SuppressWarnings("serial")
	public static void main(String[] args) {

		SparkConf sparkConf = new SparkConf().setAppName("tmp");
		if (args == null || args.length == 0) {
			sparkConf.setMaster("local[*]");
		}
		JavaSparkContext sc = new JavaSparkContext(sparkConf);

		String path0 = "hdfs://vm-xaj-bigdata-da-d01:8020/yst/sta_vem/vem_nettype/*";
		SimpleDateFormat format2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date starttime = new Date();
		Logger.getRootLogger().info("jobStarttime:" + format2.format(starttime.getTime()));
		Logger.getRootLogger().setLevel(Level.OFF);
		List<Tuple2<String, String>> initialData0 = sc.textFile(path0)
				 .filter(new Function<String, Boolean>() {
				
				 @Override
				 public Boolean call(String s) throws Exception {
				 String[] ss = s.split(",");
				 return "net".equals(ss[8]) && !"620".equals(ss[0])
							 && !"101".equals(ss[0]) && !"7667".equals(ss[0]);
				 }
				 })
				.mapToPair(new PairFunction<String, String, String>() {

					@Override
					public Tuple2<String, String> call(String s) throws Exception {

						String[] ss = s.split(",");
						String wdid = ss[0];//网点编号
						String id = ss[20];//小区网点/点位id
						String jd = ss[22];//经度
						String wd = ss[21];//纬度
						String isDel = ss[7];//是否删除状态 0正常，1删除
						String stat = ss[19];//审核状态 0:未审核 1:审核通过 2:审核不通过 3.审核中

						return new Tuple2<String, String>(id, wdid + "," + jd + "," + wd+ "," + isDel + "," + stat);
					}
				}).collect();

		System.out.println(initialData0);

		Map<String, String> map = new HashMap<String, String>();
		for (Tuple2<String, String> t : initialData0) {
			map.put(t._1, t._2);
		}
		final Broadcast<Map<String, String>> bv = sc.broadcast(map);
		String path = "hdfs://vm-xaj-bigdata-da-d01:8020/yst/sta_vem/net_community/*";
		Logger.getRootLogger().setLevel(Level.OFF);
		JavaRDD<String> result = sc.textFile(path).filter(new Function<String, Boolean>() {

			@Override
			public Boolean call(String s) throws Exception {
				String[] ss = s.split(",");
				return bv.getValue().containsKey(ss[0]);
			}
		}).mapToPair(new PairFunction<String, String, String>() {

			@Override
			public Tuple2<String, String> call(String s) throws Exception {
				String[] ss = s.split(",");
				String id = ss[0];
				String nm = ss[1];
				String qd = ss[2];
				String wy = ss[3];
				String ssq = ss[4] + "_" + ss[5];
				String hs = ss[8];
				String rzhs = ss[9];
				String ds = ss[10];
				String cks = ss[11];
				String cws = ss[12];
				String ckdts = ss[13];
				String cdt = ss[14];
				SimpleDateFormat sdf = new SimpleDateFormat("yyyy-mm-dd HH:mm:ss.S");
				Date now = new Date();
				double timeDif = (double) (now.getTime() - sdf.parse(ss[14]).getTime()) / 1000.0 / 3600.0 / 24.0;
				String wyf = ss[15];
				String lpjg = ss[16];
				String zzzd = ss[17];
				String qysj = ss[20];
				String[] netInfos = bv.getValue().get(id).split(",");
				String wdid = netInfos[0];//网点编号
				String jd = netInfos[1];//经度
				String wd = netInfos[2];//纬度
				String isdel = netInfos[3];//是否删除状态 0正常，1删除
				String stat = netInfos[4];//审核状态 0:未审核 1:审核通过 2:审核不通过 3.审核中
				
				
				return new Tuple2<String, String>(id,
						wdid+","+jd+","+wd
						+ "," + nm + "," + qd + "," + wy + "," + ssq + "," + hs + "," + rzhs + ","
								+ ds + "," + cks + "," + cws + "," + ckdts + "," + cdt + "," + timeDif + "," + wyf + ","
								+ lpjg + "," + zzzd + "," + qysj+","+isdel+","+stat);
			}
		}).map(new Function<Tuple2<String, String>, String>() {
			@Override
			public String call(Tuple2<String, String> t) throws Exception {

				return t._2;
			}
		})

		;

		System.out.println(result.collect());
		System.out.println(result.count());

		String savePath = "hdfs://vm-xaj-bigdata-da-d01:8020/yst/vem/info/net";
		Utils.saveHdfs(result, sc, savePath);
		// result.repartition(1).saveAsTextFile("net");
		Logger.getRootLogger().setLevel(Level.INFO);
		Logger.getRootLogger().info("jobEndtime:" + format2.format(new Date().getTime()) + ";");
		Logger.getRootLogger().info("jobResult:Sucessfull!");
		Logger.getRootLogger().setLevel(Level.OFF);
		sc.close();

	}

}
