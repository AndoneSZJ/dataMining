package com.seven.spark.vem.data;



import java.util.*;

import com.seven.spark.utils.Utils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;


import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

public class Process {
	

	//static final String ORGIDS ="1049"; 
	
	@SuppressWarnings({ "serial" })
	public static void main(String[] args) {

		SparkConf sparkConf = new SparkConf().setAppName(Process.class.getSimpleName());
		if (args==null||args.length==0) {
			sparkConf.setMaster("local[*]");
		}
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		String pathAvg = "hdfs://vm-xaj-bigdata-da-d01:8020/yst/sta_ods/dw_dim_zfj/*";

		/**
		 * 获取渠道id和渠道名称关系
		 */
		List<Tuple2<String, String>> list1 = sc.textFile(pathAvg).filter(new Function<String, Boolean>() {
			@Override
			public Boolean call(String s) throws Exception {
				String[] ss = s.split(",");
				if (ss.length < 63) {
					return false;
				}
				return "1".equals(ss[4]);
			}
		}).mapPartitions(new FlatMapFunction<Iterator<String>, Tuple2<String,String>>() {
			@Override
			public Iterator<Tuple2<String, String>> call(Iterator<String> stringIterator) throws Exception {
				ArrayList<Tuple2<String,String>> arrayList = new ArrayList<Tuple2<String, String>>();
				while (stringIterator.hasNext()){
					String s = stringIterator.next();
					String ss[] = s.split(",");
					String chanidId = ss[28];
					String chanidInfo = ss[30];
					arrayList.add(new Tuple2<String, String>(chanidId,chanidInfo));

				}
				return arrayList.iterator();
			}
		}).collect();

		Map<String, String> chanMap = new HashMap<String, String>();
		for (Tuple2<String, String> a : list1) {
			chanMap.put(a._1, a._2);
		}

		//广播
		final Broadcast<Map<String, String>> chanMapBv = sc.broadcast(chanMap);


		/**
		 * 获取大区id和大区名称关系
		 */
		List<Tuple2<String, String>> list2 = sc.textFile(pathAvg).filter(new Function<String, Boolean>() {
			@Override
			public Boolean call(String s) throws Exception {
				String[] ss = s.split(",");
				if (ss.length < 63) {
					return false;
				}
				return "1".equals(ss[4]);
			}
		}).mapPartitions(new FlatMapFunction<Iterator<String>, Tuple2<String,String>>() {
			@Override
			public Iterator<Tuple2<String, String>> call(Iterator<String> stringIterator) throws Exception {
				ArrayList<Tuple2<String,String>> arrayList = new ArrayList<Tuple2<String, String>>();
				while (stringIterator.hasNext()){
					String s = stringIterator.next();
					String ss[] = s.split(",");
					String disId = ss[51];
					String disInfo = ss[52];
					arrayList.add(new Tuple2<String, String>(disId,disInfo));

				}
				return arrayList.iterator();
			}
		}).collect();

		Map<String, String> disMap = new HashMap<String, String>();
		for (Tuple2<String, String> a : list2) {
			disMap.put(a._1, a._2);
		}

		final Broadcast<Map<String, String>> disBv = sc.broadcast(disMap);

		String abnormalPath = "hdfs://vm-xaj-bigdata-da-d01:8020/yst/zfj/result/abnormalVM/*";

		List<Tuple2<String, String>> list = sc.textFile(abnormalPath).mapPartitions(new FlatMapFunction<Iterator<String>, Tuple2<String,String>>() {
			@Override
			public Iterator<Tuple2<String, String>> call(Iterator<String> stringIterator) throws Exception {
				ArrayList<Tuple2<String,String>> arrayList = new ArrayList<Tuple2<String, String>>();
				while (stringIterator.hasNext()){
					String s = stringIterator.next();
					String ss[] = s.split(",");
					arrayList.add(new Tuple2<String, String>(ss[0],""));
				}
				return arrayList.iterator();
			}
		}).collect();

		Map<String, String> map = new HashMap<String, String>();
		for(Tuple2<String,String> tuple2 : list){
			map.put(tuple2._1,tuple2._2);
		}

		final Broadcast<Map<String, String>> bv = sc.broadcast(map);


		String path = "hdfs://vm-xaj-bigdata-da-d01:8020/yst/zfj/find/DW_ZFJ_RLB_ALL";
		JavaRDD<String> initialData = sc.textFile(path);
		//orgid,VMNO+","+CUSTID+","+chanid+","+disid+","+mon+","+saam+","+1+","+payType+","+card
		JavaPairRDD<String, String> resultData = initialData.filter(new Function<String, Boolean>() {
			@Override
			public Boolean call(String s) throws Exception {
				String[] ss = s.replaceAll("[()]", "").split(",");
				Map<String,String> abnormalMap = bv.getValue();
				String vmno =ss[1];
				String CUSTID = ss[2];
				String chanid = ss[3];
				String disid = ss[4];
				String mon = ss[5];
				String saam = ss[6];


				return !abnormalMap.containsKey(vmno) && !"".equals(vmno) && !"".equals(CUSTID) && !"".equals(chanid) && !"".equals(disid) && !"".equals(mon) && !"".equals(saam);
			}
		}).mapToPair(new PairFunction<String, String, String>() {
			@Override
			public Tuple2<String, String> call(String s) throws Exception {
				String[] ss = s.replaceAll("[()]", "").split(",");
				String vmno =ss[1];
				String chanid  = ss[3];
				String disid  = ss[4];
				String mon  = ss[5].substring(0,7);
				Double saam = Double.parseDouble(ss[6]);


//				return new Tuple2<String, String>(vmno+","+chanid+","+disid+","+mon,saam+","+1);
				return new Tuple2<String, String>(vmno+","+chanid+","+mon,saam+","+1);
//				return new Tuple2<String, String>(vmno+","+chanName+","+mon,saam+","+1);
			}
		}).reduceByKey(new Function2<String, String, String>() {
			@Override
			public String call(String s1, String s2) throws Exception {
				String[] ss1 = s1.split(",");
				String[] ss2 = s2.split(",");
				double a = Double.parseDouble(ss1[0])+Double.parseDouble(ss2[0]);
				double b = Double.parseDouble(ss1[1])+Double.parseDouble(ss2[1]);

				return a+","+b;
			}
		}).filter(new Function<Tuple2<String,String>, Boolean>() {

			@Override
			public Boolean call(Tuple2<String, String> t) throws Exception {
				String[] ss =t._2.split(",");
				return Double.parseDouble(ss[1])>7;
			}
		}).mapToPair(new PairFunction<Tuple2<String,String>, String, String>() {
					@Override
					public Tuple2<String, String> call(Tuple2<String, String> t) throws Exception {
						String[] ss = t._1.split(",");
						String chanid  = ss[1];
//						String disid  = ss[2];
//						String mon  = ss[3];

						String mon  = ss[2];
						String[] ss2 =t._2.split(",");

//						return new Tuple2<String, String>(chanid+","+disid+","+mon,ss2[0]);
						return new Tuple2<String, String>(chanid+","+mon,ss2[0]);
					}
		}).reduceByKey(new Function2<String, String, String>() {

					@Override
					public String call(String s1, String s2) throws Exception {

						return s1+","+s2;
					}
		}).filter(new Function<Tuple2<String,String>, Boolean>() {

			@Override
			public Boolean call(Tuple2<String, String> t) throws Exception {
				String[] ss = t._2.split(",");
				return ss.length>10;
			}
		}).mapToPair(new PairFunction<Tuple2<String,String>, String, String>() {

					@Override
					public Tuple2<String, String> call(Tuple2<String, String> t) throws Exception {
						String[] ss = t._2.split(",");
						List<Double> l = new ArrayList<Double>();
						for(String s:ss) {
							l.add(Double.parseDouble(s));
						}
						Collections.sort(l);
						int size = l.size();
						double lower = l.get(1);
						double upper = l.get(size-2);
						double midder = l.get(size/2);
						double q3 = l.get(size*4/5);
						double q1 = l.get(size*1/5);
						Map<String,String> map1 = chanMapBv.getValue();
						String[] aa = t._1.split(",");
						String chanid = aa[0];
						String mon = aa[1];
						String chanName = map1.get(chanid);

//						Map<String,String> map2 = disBv.getValue();
//						String disId = aa[0];
//						String disName = map2.get(disId);

//						return new Tuple2<String, String>(t._1,upper+","+q3+","+midder+","+q1+","+lower);
//						return new Tuple2<String, String>(disName+","+chanName+","+mon,upper+","+q3+","+midder+","+q1+","+lower);
						return new Tuple2<String, String>(chanName+","+mon,upper+","+q3+","+midder+","+q1+","+lower);
					}
				});
		String savePath = "hdfs://vm-xaj-bigdata-da-d01:8020/yst/zfj/find/ZFJ_DATA/";
		System.out.println(resultData.take(3));
		Utils.saveHdfs(resultData, sc, savePath,1);
	}

}
