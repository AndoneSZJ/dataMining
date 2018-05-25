package com.seven.spark.vem.data;


import com.seven.spark.utils.Utils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class GetAllRlbData2018 {
	
	//static final boolean useSparkLocal = true;
//	864	余杭办事处
//	863	富阳办事处
//	352	杭州特通办事处
//	2934	杭州西湖办事处
//	1430	杭州钱江办事处
//	1049	杭州KA办事处
	static final String ORGIDS ="864_863_352_2934_1430_1049"; 
	//static final String ORGIDS ="1049"; 
	
	@SuppressWarnings({ "serial", "resource" })
	public static void main(String[] args) {

		SparkConf sparkConf = new SparkConf().setAppName("filter all data");
		if (args==null||args.length==0) {
			sparkConf.setMaster("local[*]");
		}
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		String path = "hdfs://vm-xaj-bigdata-da-d01:8020/yst/sta_ods/dw_zfj_rlb";
		Logger.getRootLogger().setLevel(Level.OFF);
		JavaRDD<String> initialData = sc.textFile(path);
		System.out.println(initialData.take(1));
		JavaPairRDD<String, String> resultData = initialData.mapToPair(new PairFunction<String, String,String>() {
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
				String docti = ss[26];
				//产品id
				String PRODID = ss[12];
				//订单交易金额
				String saam =ss[16];
				
				String payType = ss[19];
				//交易账号
				String card ="";
				if(ss.length>=29) {
					card = ss[27];
				}		
				return new Tuple2<String, String>(orgid,VMNO+","+CUSTID+","+PRODID+","+docti+","+saam+","+1+","+payType+","+card);
			}
		});
		
		String savePath = "hdfs://vm-xaj-bigdata-da-d01:8020/yst/zfj/DW_ZFJ_RLB_ALL";
		
		Utils.saveHdfs(resultData, sc, savePath);
	}

}
