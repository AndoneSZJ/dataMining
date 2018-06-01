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

public class BaseOrder {

	static List<String> nets;

	@SuppressWarnings("serial")
	public static void main(String[] args) {

		SparkConf sparkConf = new SparkConf().setAppName("tmp");
		if (args == null || args.length == 0) {
			sparkConf.setMaster("local[*]");
		}
		JavaSparkContext sc = new JavaSparkContext(sparkConf);

		String pathMac = "hdfs://vm-xaj-bigdata-da-d01:8020/yst/sta_vem/vem_machine/*";
		SimpleDateFormat format2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date starttime = new Date();
		Logger.getRootLogger().info("jobStarttime:" + format2.format(starttime.getTime()));
		Logger.getRootLogger().setLevel(Level.OFF);
		
		
		List<String> macList = sc.textFile(pathMac).filter(new Function<String, Boolean>() {

			@Override
			public Boolean call(String s) throws Exception {
				String[] ss = s.split(",");

				return "2".equals(ss[21]);
			}
		}).map(new Function<String, String>() {
			@Override
			public String call(String s) throws Exception {
				String[] ss = s.split(",");
				String id = ss[0];

				return id;
			}
		}).collect();

		String pathNet = "hdfs://vm-xaj-bigdata-da-d01:8020/yst/sta_vem/vem_nettype/*";
		List<String> netList = sc.textFile(pathNet).filter(new Function<String, Boolean>() {

			@Override
			public Boolean call(String s) throws Exception {
				String[] ss = s.split(",");
				return "net".equals(ss[8]) && "0".equals(ss[7]) && "1".equals(ss[19]);
			}
		}).map(new Function<String, String>() {

			@Override
			public String call(String s) throws Exception {

				String[] ss = s.split(",");
				String wdid = ss[0];
				String ssq = ss[11] + "_" + ss[12];

				return wdid + "," + ssq;
			}
		}).collect();

		Map<String, String> netMap = new HashMap<String, String>();
		for (String net : netList) {
			String[] nets = net.split(",");
			netMap.put(nets[0], nets[1]);
		}
		System.out.println(netMap);

		Map<String, Object> bvMap = new HashMap<String, Object>();

		bvMap.put("mac", macList);
		bvMap.put("net", netMap);
		final Broadcast<Map<String, Object>> bv = sc.broadcast(bvMap);

		String path0 = "hdfs://vm-xaj-bigdata-da-d01:8020/yst/sta_vem/vem_order/*";
		Logger.getRootLogger().setLevel(Level.OFF);
		List<Tuple2<String, String>> initialData0 = sc.textFile(path0).filter(new Function<String, Boolean>() {

			@SuppressWarnings("unchecked")
			@Override
			public Boolean call(String s) throws Exception {
				String[] ss = s.split(",");
				String testids = "'onw921spAgbQ_qGufI2uR3GsfLVU','onw921mf-l_wfEqyBR7u7fg7cDIk','onw921kONAQo_KLSHIu2-fWsW6VQ','onw921kLbmMWws4aSQtKbAdoYTyY','onw921kGScL-iqWQoSZPhE9spO94','ocpQoxPSsQzmJ0E0Oub4m_FRj6sI','ocpQoxP4xABVw5Xz8SdnTf2u3nro','ocpQoxKfkHTFlKq0pR1vJElSQRBA','ocpQoxJG1FFL0r1rCTi4RA_Pke-c','ocpQoxB0c7nPmPYD82RmC6QcwoJA'";
				// return !"620".equals(ss[12])&&!testids.contains(ss[7]);

				String dt = ss[1];
				SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");
				long time = sdf.parse(dt).getTime();
				long start = sdf.parse("2018-01-14 00:00:00.0").getTime();
				List<String> macList = (List<String>) bv.getValue().get("mac");

				return !ss[7].isEmpty() && macList.contains(ss[2]) && time > start && !"620".equals(ss[15])
						&& !"101".equals(ss[15]) && !testids.contains(ss[10]) && !"7667".equals(ss[15]);
			}
		}).mapToPair(new PairFunction<String, String, String>() {

			@SuppressWarnings("unchecked")
			@Override
			public Tuple2<String, String> call(String s) throws Exception {
				String[] ss = s.split(",");
				String id = ss[0];//订单编号
				String dt = ss[1];//创建时间
				String vmid = ss[2];//机器编号
				String status = ss[3];//订单状态 0:待支付 1:支付成功 2:待出货 3:出货成功 4:出货失败 5:出货未知 6:退款中 7:退款成功 8:退款失败
				String zffs = ss[4];//支付方式
				String zfje = ss[7];//应付金额（参加完农夫活动后剩余应付金额）
				String zfzh = ss[10];//支付账号
				String zfsj = ss[11];//付款时间
				String zfptid = ss[12];//第三方支付平台ID
				String wdid = ss[15];//网点ID
				String dwid = ss[16];//点位ID
				Map<String, String> map = (Map<String, String>) bv.getValue().get("net");
				if ("0".equals(zfje)) {
					System.out.println(s);
				}
				String ssq = map.get(wdid);
				// 日期，机器编号，状态，支付方式，支付金额，支付账号，支付时间，支付平台，网点，点位，省市区
				return new Tuple2<String, String>(id, dt + "," + vmid + "," + status + "," + zffs + "," + zfje + ","
						+ zfzh + "," + zfsj + "," + zfptid + "," + wdid + "," + dwid + "," + ssq);
			}

		}).collect();



		Map<String, String> map = new HashMap<String, String>();
		for (Tuple2<String, String> t : initialData0) {
			map.put(t._1, t._2);
		}
		final Broadcast<Map<String, String>> bv2 = sc.broadcast(map);

		String path = "hdfs://vm-xaj-bigdata-da-d01:8020/yst/sta_vem/vem_order_detail/*";
		Logger.getRootLogger().setLevel(Level.OFF);
		JavaRDD<String> result = sc.textFile(path).filter(new Function<String, Boolean>() {

			@Override
			public Boolean call(String s) throws Exception {
				String[] ss = s.split(",");
				String id = ss[1];
				return bv2.getValue().containsKey(id);
			}
		}).mapToPair(new PairFunction<String, String, String>() {

			@Override
			public Tuple2<String, String> call(String s) throws Exception {
				String[] ss = s.split(",");
				String id = ss[1];//订单编号
				String pxid = ss[2];//品项编号
				String spmc = ss[3];//商品名称
				String spdj = ss[4];//商品单价
				String spsl = ss[5];//数量
				String zfjchs = ss[6];//自贩机出货数
				String tksps = ss[7];//退款的商品数
				String jqbh = ss[10];//实际售卖机器编号
				String dt = ss[11];//创建时间
				// 日期，机器编号，状态，支付方式，支付金额，支付账号，支付时间，支付平台，网点，点位，省市区
				// 品项编号，商品名称，商品单价，商品数量，自贩机出货量，退款商品数，机器编号，日期
				return new Tuple2<String, String>(id, bv2.getValue().get(id)
						+ ',' + pxid + "," + spmc + "," + spdj
						+ "," + spsl + "," + zfjchs + "," + tksps + "," + jqbh + "," + dt);
			}
		}).map(new Function<Tuple2<String, String>, String>() {

			@Override
			public String call(Tuple2<String, String> t) throws Exception {
				// TODO Auto-generated method stub
				return t._2;
			}
		});

		String savePath = "hdfs://vm-xaj-bigdata-da-d01:8020/yst/vem/sales/order";
		Utils.saveHdfs(result, sc, savePath);
		// result.repartition(1).saveAsTextFile("order");
		
		
		  Logger.getRootLogger().setLevel(Level.INFO);
		  Logger.getRootLogger().info("jobEndtime:"+format2.format(new Date().getTime()) +";");
		  Logger.getRootLogger().info("jobResult:Sucessfull!");
		  Logger.getRootLogger().setLevel(Level.OFF);
		  sc.close();
	}

}
