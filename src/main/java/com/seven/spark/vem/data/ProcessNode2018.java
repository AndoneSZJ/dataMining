package com.seven.spark.vem.data;

import java.util.*;

import com.seven.spark.hdfs.Utils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;


import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

public class ProcessNode2018 {

	static final boolean useSparkLocal = true;
	static final String ORGIDS = "_864_863_352_2934_1430_1049_";

	// static final String ORGIDS ="864";
	@SuppressWarnings("serial")
	public static void main(String[] args) {

		SparkConf sparkConf = new SparkConf().setAppName("Your App's name");
		if (useSparkLocal) {
			sparkConf.setMaster("local[2]");
		}

		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		String path = "hdfs://vm-xaj-bigdata-da-d01:8020/yst/sta_ods/dw_dim_zfj/*";
		Logger.getRootLogger().setLevel(Level.OFF);

		String avgPath = "hdfs://vm-xaj-bigdata-da-d01:8020/yst/zfj/DW_ZFJ_RLB_HZ_AVG/*";
		List<String> avgRdd = sc.textFile(avgPath).map(new Function<String, String>() {
			@Override
			public String call(String s) throws Exception {
				String[] ss = s.substring(1,s.length()-1).split(",");
				return ss[0]+","+ss[2];
			}
		}).collect();

		Map<String, String> avgMap = new HashMap<String, String>();
		for (String a : avgRdd) {
			String[] avg = a.split(",");
			avgMap.put(avg[0], avg[1]);
		}

		final Broadcast<Map<String, String>> bv = sc.broadcast(avgMap);

		JavaRDD<String> initialData = sc.textFile(path);
		JavaPairRDD<String, String> resultRDD = initialData.filter(new Function<String, Boolean>() {
			@Override
			public Boolean call(String s) throws Exception {
				String[] ss = s.split(",");
				if (ss.length < 63) {
					return false;
				}
				String orgid = ss[47];
				return "1".equals(ss[4]) && ORGIDS.contains("_"+orgid+"_") && orgid.length() > 0;
			}
		}).flatMapToPair(new PairFlatMapFunction<String, String, String>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<Tuple2<String, String>> call(String s) throws Exception {
				Map<String,String> map = bv.getValue();
				String[] ss = s.split(",");
				String vmno = ss[1];

				String avgNum = map.get(vmno);

				String avg = avgNum == null ? "0" : avgNum;


				String vmInfo = "名称：" + ss[1] + "_购买日期：" + ss[6] + "_购买价格：" + ss[7] +"_日均销量："+avg+ "_生产厂商：" + ss[8] + "_出厂日期：" + ss[9]
						+ "_所属型号：" + ss[10];
				String chanid = "chan" + "_" + ss[28];//渠道id
				String chanidInfo = "名称：" + ss[30];
				String pchanid = "pchan" + "_" + ss[31];//上级渠道id
				String pchanidInfo = "名称：" + ss[33];
				String custid = "cust" + "_" + ss[34];//网点id
				String custInfo = "名称：" + ss[35];
				String lgsid = "lgs" + "_" + ss[36];//运营商id
				String lgsInfo = "名称：" + ss[37];
				String dealerid = "dealer" + "_" + ss[38];//经销商id
				String dealerInfo = "名称：" + ss[40];
				String orgid = "org" + "_" + ss[47];//组织id
				String orgInfo = "名称：" + ss[48];
				String offid = "off" + "_" + ss[49];//办事处id
				String offInfo = "名称：" + ss[50];
				String disid = "dis" + "_" + ss[51];//
				String disInfo = "名称：" + ss[52];
				List<Tuple2<String, String>> result = new ArrayList<Tuple2<String, String>>();
				result.add(new Tuple2<String, String>(vmno, vmInfo));
				result.add(new Tuple2<String, String>(chanid, chanidInfo));
				result.add(new Tuple2<String, String>(pchanid, pchanidInfo));
				result.add(new Tuple2<String, String>(custid, custInfo));
				result.add(new Tuple2<String, String>(lgsid, lgsInfo));
				result.add(new Tuple2<String, String>(dealerid, dealerInfo));
				result.add(new Tuple2<String, String>(offid, offInfo));
				result.add(new Tuple2<String, String>(orgid, orgInfo));
				result.add(new Tuple2<String, String>(disid, disInfo));
				return result.iterator();
			}
		}).reduceByKey(new Function2<String, String, String>() {
			@Override
			public String call(String s1, String s2) throws Exception {
				return s2;
			}
		});
//		resultRDD.foreach(new VoidFunction<Tuple2<String, String>>() {
//			@Override
//			public void call(Tuple2<String, String> stringStringTuple2) throws Exception {
//				System.out.println(stringStringTuple2._1+":"+stringStringTuple2._2);
//			}
//		});

		String savePath = "hdfs://vm-xaj-bigdata-da-d01:8020/yst/zfj/Graph/NodesSales";
		Utils.saveHdfs(resultRDD, sc, savePath);
		System.out.println(resultRDD.take(15));
		System.out.println(resultRDD.count());
	}

}
