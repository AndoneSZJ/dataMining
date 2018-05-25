package com.seven.spark.vem.base;

import com.seven.spark.utils.Utils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.text.SimpleDateFormat;
import java.util.*;

public class PointBaseInfo {

	@SuppressWarnings("serial")
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
		String path0 = "hdfs://vm-xaj-bigdata-da-d01:8020/yst/sta_vem/vem_nettype/*";
		List<Tuple2<String, String>> pList = sc.textFile(path0)
				 .filter(new Function<String, Boolean>() {
				
				 @Override
				 public Boolean call(String s) throws Exception {
				 String[] ss = s.split(",");
				 return "point".equals(ss[8]) && !"620".equals(ss[2])
						 && !"101".equals(ss[2]) && !"7667".equals(ss[2]) ;
				
				 }
				 })
				.mapToPair(new PairFunction<String, String, String>() {

					@Override
					public Tuple2<String, String> call(String s) throws Exception {
						String[] ss = s.split(",");
						String pointid = ss[0];//网点id
						String perid = ss[2];//父节点id
						String id = ss[20];//小区网点/点位id
						String isDel = ss[7];//是否删除状态位 0:正常 1:冻结
						String stat = ss[19];//审核状态 0:未审核 1:审核通过 2:审核不通过 3.审核中
     if("4006".equals(pointid)) {
    	 System.out.println(pointid + "," + perid + "," + isDel + "," + stat);
     }
						return new Tuple2<String, String>(id, pointid + "," + perid + "," + isDel + "," + stat);
					}
				}).collect();

		String netPath = "hdfs://vm-xaj-bigdata-da-d01:8020/yst/vem/info/net";
		List<Tuple2<String, String>> nList = sc.textFile(netPath).mapToPair(new PairFunction<String, String, String>() {

			@Override
			public Tuple2<String, String> call(String s) throws Exception {
				String[] ss = s.split(",");
				if("3999".equals(ss[0])) {
			    	 System.out.println(ss[0]+","+ss[6]);
			     }
				return new Tuple2<String, String>(ss[0], ss[6]);//省市区
			}
		}).collect();
		Map<String, String> nMap = new HashMap<String, String>();
		for (Tuple2<String, String> t : nList) {
			nMap.put(t._1, t._2);
		}
		Map<String, String> pMap = new HashMap<String, String>();
		for (Tuple2<String, String> t : pList) {
			String netid = t._2.split(",")[1];
			if("3999".equals(netid)) {
		    	 System.out.println(t._1+","+t._2 + "," + nMap.get(netid));
		     }
			pMap.put(t._1, t._2 + "," + nMap.get(netid));
		}
		System.out.println(pMap);
		final Broadcast<Map<String, String>> bv = sc.broadcast(pMap);

		String path = "hdfs://vm-xaj-bigdata-da-d01:8020/yst/sta_vem/point_community/*";
		JavaPairRDD<String, String> pointData = sc.textFile(path)
				.flatMapToPair(new PairFlatMapFunction<String, String, String>() {

					@Override
					public Iterator<Tuple2<String, String>> call(String s) throws Exception {
						String[] ss = s.split(",");
						String id = ss[0];//点位id
						String nm = ss[1];//点位名称
						String dwlx = ss[4];//点位类型 字典 （地面:0 地上:1）
						String fghs = ss[5];//覆盖户数
						String rzhs = ss[6];//入住户数
						//电信信号强度 字典 无/1/2/3/4/5_移动信号强度 字典 无/1/2/3/4/5_联通信号强度 字典 无/1/2/3/4/5
						String xhqd = ss[7] + "_" + ss[8] + "_" + ss[9];
						String qysj = ss[11];//创建时间
						Map<String, String> pMap = bv.getValue();

						List<Tuple2<String, String>> list = new ArrayList<Tuple2<String, String>>();
						
						
						if (pMap.containsKey(id)) {

							String[] pNetInfos = pMap.get(id).split(",");
							String pointid = pNetInfos[0];//网点id
							String netid = pNetInfos[1];//父节点id
							String isdel = pNetInfos[2];//是否删除状态位 0:正常 1:冻结
							String stat = pNetInfos[3];//审核状态 0:未审核 1:审核通过 2:审核不通过 3.审核中
							String city = pNetInfos[4];//省市区
							
							//父节点id，省市区，点位名称，点位类型，覆盖户数
							//入住户数，信号强弱，创建时间，是否删除状态，审核状态
							String result =  netid + "," + city + "," + nm + "," + dwlx + "," + fghs
									+ "," + rzhs + "," + xhqd + "," + qysj+","+isdel+","+stat;
							if("3999".equals(netid)) {
						    	 System.out.println(pointid+","+result);
						     }
							if("2718".equals(id)) {
								System.out.println("----"+pointid+","+result);
						     }
							list.add(new Tuple2<String, String>(pointid, result));

						}

						return list.iterator();

					}
				});

		 System.out.println(pointData.collect());
		// System.out.println(initialData.collect());
		String savePath = "hdfs://vm-xaj-bigdata-da-d01:8020/yst/vem/info/point";
		Utils.saveHdfs(pointData, sc, savePath);
		Logger.getRootLogger().setLevel(Level.INFO);
		Logger.getRootLogger().info("jobEndtime:" + format2.format(new Date().getTime()) + ";");
		Logger.getRootLogger().info("jobResult:Sucessfull!");
		Logger.getRootLogger().setLevel(Level.OFF);
		sc.close();

	}

}
