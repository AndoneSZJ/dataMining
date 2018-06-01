package com.seven.spark.vem.data;

import java.text.SimpleDateFormat;
import java.util.*;

import com.seven.spark.hdfs.Utils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;


import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

/**
 * 发现异常的操作
 * 现金，高频，10元，连续，异常
 * @author lilong
 *
 */
public class FindAbnormal2018 {

	public static void main(String[] args) {

		SparkConf sparkConf = new SparkConf().setAppName("findAbnormal2018");
		if (args==null||args.length==0) {
			sparkConf.setMaster("local[*]");
		}
		JavaSparkContext sc = new JavaSparkContext(sparkConf);

		String pathAvg = "hdfs://vm-xaj-bigdata-da-d01:8020/yst/sta_ods/dw_dim_zfj/*";

		List<String> list = sc.textFile(pathAvg).filter(new Function<String, Boolean>() {
			@Override
			public Boolean call(String s) throws Exception {
				String[] ss = s.split(",");
				if (ss.length < 63) {
					return false;
				}
				return "1".equals(ss[4]);
			}
		}).map(new Function<String, String>() {
			@Override
			public String call(String s) throws Exception {
				//名称，地址，网点ID，网点名称，运营商ID，运营商名称,渠道ID，渠道名称，组织ID，组织名称 1-10
				String[] ss = s.split(",");
				String vmno = ss[1];//编号
				String vmName = ss[1];//名称
				String adress = ss[2];//地址
				String custid =  ss[34];//网点id
				String custInfo = ss[35];
				String lgsid = ss[36];//运营商id
				String lgsInfo = ss[37];
				String chanid = ss[28];//渠道id
				String chanidInfo = ss[30];
				String orgid = ss[47];//组织id
				String orgInfo = ss[48];
				return vmno+"@"+vmName+","+adress+","+custid+","+custInfo+","+lgsid+","+lgsInfo+","+chanid+","+chanidInfo+","+orgid+","+orgInfo;
			}
		}).collect();

		Map<String, String> avgMap = new HashMap<String, String>();
		for (String a : list) {
			String[] avg = a.split("@");
			avgMap.put(avg[0], avg[1]);
		}

		System.out.println(avgMap);

		final Broadcast<Map<String, String>> bv = sc.broadcast(avgMap);

		String path = "hdfs://vm-xaj-bigdata-da-d01:8020/yst/zfj/find/DW_ZFJ_RLB_ALL";
		Logger.getRootLogger().setLevel(Level.OFF);
		JavaRDD<String> initialData = sc.textFile(path);
		//System.out.println(initialData.take(1));
		JavaPairRDD<String, String> resultData = initialData.filter(new Function<String, Boolean>() {
			
			@Override
			public Boolean call(String s) throws Exception {
				String[] ss = s.replaceAll("[()\\[\\]]", "").split(",");
				if(ss.length<9) {
					return false;
				}
				String payType = ss[8];
				String time = ss[4];
				if(payType.length()!=7) {
					return false;
				}
//				if(time.contains("2018")){
//					return false;
//				}
				return "1".equals(payType.substring(1, 2))||"1".equals(payType.substring(2, 3));
			}
		}).mapToPair(new PairFunction<String, String,String>() {
			@Override
			public Tuple2<String, String> call(String s) throws Exception {
			
				String[] ss = s.replaceAll("[()\\[\\]]", "").split(",");
				//组织id
				String orgid = ss[0];
				//自贩机id
				String VMNO = ss[1];
				//网点id
				String CUSTID = ss[2];
				//订单交易时间
				String docti = ss[5];
				//订单交易金额
				String saam =ss[6];
				SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
				SimpleDateFormat sdf2 = new SimpleDateFormat("D");
				String day = sdf2.format(sdf.parse(docti));
				String flag = "0";
				if(Double.parseDouble(saam)>=10) {//大额消费
					flag ="1";
				}
				return new Tuple2<String, String>(VMNO+","+CUSTID+","+day,saam+","+1+","+flag);
			}
		}).reduceByKey(new Function2<String, String, String>() {
			
			@Override
			public String call(String s1, String s2) throws Exception {
				String[] ss1 = s1.split(",");
				String[] ss2 = s2.split(",");
				double t1 = Double.parseDouble(ss1[0]);
				double t2 = Double.parseDouble(ss2[0]);
				int c1 = Integer.parseInt(ss1[1]);
				int c2 = Integer.parseInt(ss2[1]);
				int bigC1 = Integer.parseInt(ss1[2]);
				int bigC2 = Integer.parseInt(ss2[2]);
				return (t1+t2)+","+(c1+c2)+","+(bigC1+bigC2);
			}
		}).mapToPair(new PairFunction<Tuple2<String,String>, String, String>() {

			@Override
			public Tuple2<String, String> call(Tuple2<String, String> t) throws Exception {
				String[] ss = t._1.split(",");

				return new Tuple2<String, String>(ss[0]+","+ss[1],ss[2]+","+t._2);
			}
		}).reduceByKey(new Function2<String, String, String>() {

			@Override
			public String call(String s0, String s1) throws Exception {
				
				return s0+"_"+s1;
			}
		});

		System.out.println(resultData.take(3));
				

		//名称，地址，网点ID，网点名称，运营商ID，运营商名称,渠道ID，渠道名称，组织ID，组织名称 1-10
		JavaPairRDD<String, String> resultData2 = 	resultData.filter(new Function<Tuple2<String,String>, Boolean>() {
			//
			@Override
			public Boolean call(Tuple2<String, String> t) throws Exception {
				// 发现异常，
				double[] saams = new double[366];
				//int[] counts = new int[330];
				int[] bigCounts = new int[366];
				String[] ss = t._2.split("_");
				for(String s:ss) {
					String[] fs = s.split(",");
					int day = Integer.parseInt(fs[0]);
					double saam = Double.parseDouble(fs[1]);//金额
					//int count = Integer.parseInt(fs[2]);
					int bigCount = Integer.parseInt(fs[3]);//是否是大额订单
					saams[day]=saam;
					//counts[day]=count;
					bigCounts[day]=bigCount;
				}
				for(int i=1;i<364;i++) {
					
					if(bigCounts[i]>=200) {
						return true;
					}
					if(bigCounts[i]>=40) {
						if(saams[i]-saams[i-1]>400||saams[i]-saams[i-2]>400) {
							if(saams[i]-saams[i+1]>400||saams[i]-saams[i+2]>400) {
								if((saams[i]+1)/(saams[i-1]+1)>10||(saams[i]+1)/(saams[i-2]+1)>10) {
									if((saams[i]+1)/(saams[i+1]+1)>10||(saams[i]+1)/(saams[i+2]+1)>10) {
										//119-121 147-150 273-281
										if(i>=119&&i<=121) {
											//劳动节
										}else if(i>147&&i<=150) {
											//端午
										}else if(i>273&&i<281) {
											//十一
										}else {
											return true;
										}
										
									}
								}
								
							}
						}
					}
				}
				return false;
			}
		}).mapToPair(new PairFunction<Tuple2<String, String>, String, String>() {
			@Override
			public Tuple2<String, String> call(Tuple2<String, String> stringStringTuple2) throws Exception {
				String [] ss = stringStringTuple2._1.split(",");
				Map<String,String> map = bv.getValue();
				String data = map.get(ss[0]) == null ? ",,,,,,,,," : map.get(ss[0]);

				return new Tuple2<String, String>(ss[0]+","+data,stringStringTuple2._2);
			}
		});

		String savePath = "hdfs://vm-xaj-bigdata-da-d01:8020/yst/zfj/result/abnormalVM";
//		String savePath = "hdfs://vm-xaj-bigdata-da-d01:8020/yst/vem/sales/seven/abnormalVM";
		Utils.saveHdfs(resultData2, sc, savePath,18);
		System.out.println(resultData2.take(3));
		System.out.println("success");
		//System.out.println(resultData.count());
		//System.out.println(resultData2.count());
	}
	
	

}
