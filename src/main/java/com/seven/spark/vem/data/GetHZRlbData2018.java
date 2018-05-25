package com.seven.spark.vem.data;


import com.seven.spark.utils.Utils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class GetHZRlbData2018 {
	
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

		SparkConf sparkConf = new SparkConf().setAppName("filter hz data");
		if (args==null||args.length==0) {
			sparkConf.setMaster("local[*]");
		}
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		String path = "hdfs://vm-xaj-bigdata-da-d01:8020/yst/zfj/DW_ZFJ_RLB_ALL";
		Logger.getRootLogger().setLevel(Level.OFF);
		JavaRDD<String> initialData = sc.textFile(path);
		JavaRDD<String> resultData = initialData.filter(new Function<String, Boolean>() {

			@Override
			public Boolean call(String s) throws Exception {
				String[] ss = s.replaceAll("[()]", "").split(",");
				//组织id
				String orgid = ss[0];
				
				return ORGIDS.contains(orgid)&&orgid.length()>0;
			}
		});
		
		String savePath = "hdfs://vm-xaj-bigdata-da-d01:8020/yst/zfj/DW_ZFJ_RLB_HZ";
		Utils.saveHdfs(resultData, sc, savePath);
		System.out.println(resultData.take(2));
	}

}
