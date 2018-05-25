package com.seven.spark.vem.base;



import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

import com.seven.spark.utils.Utils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;


import scala.Tuple2;

public class GetAllRlbData2018 {
	

	//static final String ORGIDS ="1049"; 
	
	@SuppressWarnings({ "serial" })
	public static void main(String[] args) {

		SparkConf sparkConf = new SparkConf().setAppName(GetAllRlbData2018.class.getSimpleName());
		if (args==null||args.length==0) {
			sparkConf.setMaster("local[*]");
		}
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		String path = "hdfs://vm-xaj-bigdata-da-d01:8020/yst/sta_ods/dw_zfj_rlb";
		Logger.getRootLogger().setLevel(Level.OFF);
		String pathRel = "hdfs://vm-xaj-bigdata-da-d01:8020/yst/sta_ods/dw_dim_zfj/*";
	
		JavaPairRDD<String, String> rel =  sc.textFile(pathRel).filter(new Function<String, Boolean>() {
			@Override
			public Boolean call(String s) throws Exception {
				String[] ss = s.split(",");
				if(ss.length!=69) {
					return false;
				}
				String orgid=ss[47];
				return "1".equals(ss[4])&&orgid.length()>0;
			}
		}).mapToPair(new PairFunction<String, String, String>() {

			@Override
			public Tuple2<String, String> call(String s) throws Exception {
				String[] ss = s.split(",");
				String custId=ss[34];//网点id
				String disId=ss[51];//大区id
				return new Tuple2<String, String>(custId,disId);
			}
		});
		
		Map<String,String> map = new HashMap<String,String>();
		for(Tuple2<String, String> t:rel.collect()) {
			map.put(t._1, t._2);
		}
		Broadcast<Map<String, String>> bv = sc.broadcast(map);
		
		
		
		
		JavaRDD<String> initialData = sc.textFile(path);
//		System.out.println(initialData.take(1));
		JavaPairRDD<String, String> resultData = initialData.filter(new Function<String, Boolean>() {

			@Override
			public Boolean call(String s) throws Exception {
				String[] ss = s.split(",");
				if(ss.length<30) {
					return false;
				}
				//网点id
				String CUSTID = ss[5];
				if("".equals(CUSTID) || !bv.getValue().containsKey(CUSTID)) {
					return false;
				}
				//订单交易时间
				String day = ss[26].substring(0, 10);
				SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
				long start = sdf.parse("2017-05-01").getTime();
				long end = sdf.parse("2018-05-01").getTime();
				//订单交易时间
				long docti =sdf.parse(day).getTime();
				return start<=docti&&docti<end;
			}
		})
				.mapToPair(new PairFunction<String, String,String>() {
			@Override
			public Tuple2<String, String> call(String s) throws Exception {
			
				String[] ss = s.split(",");
				if(ss.length<30) {
					System.out.println(s);
				}
				//组织id
				String orgid = ss[8];
				//自贩机id
				String VMNO = ss[2];
				//网点id
				String CUSTID = ss[5];
				//订单交易时间
				String mon = ss[26].substring(0, 10);
				//产品id
				String prdid = ss[12];
				//订单交易金额
				String saam =ss[16];
				//渠道
				String chanid = ss[21];
				
				String payType = ss[19];
				//交易账号
				String card ="";
				if(ss.length>=29) {
					card = ss[27];
				}		
				String disid = "";
				if(bv.getValue().containsKey(CUSTID)) {
					disid = bv.getValue().get(CUSTID);
				}
				return new Tuple2<String, String>(orgid,VMNO+","+CUSTID+","+chanid+","+disid+","+mon+","+saam+","+1+","+payType+","+card);
			}
		});
		
		String savePath = "hdfs://vm-xaj-bigdata-da-d01:8020/yst/zfj/find/DW_ZFJ_RLB_ALL";
		System.out.println(resultData.take(3));
		Utils.saveHdfs(resultData, sc, savePath ,18);
	}

}
