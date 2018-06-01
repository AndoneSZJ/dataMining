package com.seven.spark.vem.user;

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

import java.text.SimpleDateFormat;
import java.util.*;

public class UserInfo {

	@SuppressWarnings("serial")
	public static void main(String[] args) {

		SparkConf sparkConf = new SparkConf().setAppName("tmp");
		String endString = null;
		if (args == null || args.length == 0) {
			sparkConf.setMaster("local[*]");
			endString = "2018-04-14 00:00:00.0";
		} else {

			endString = args[0];
		}
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		SimpleDateFormat format2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date starttime = new Date();
		Logger.getRootLogger().info("jobStarttime:" + format2.format(starttime.getTime()));
		Logger.getRootLogger().setLevel(Level.OFF);
		final Broadcast<String> endDayBV = sc.broadcast(endString);
		String path1 = "hdfs://vm-xaj-bigdata-da-d01:8020/yst/sta_mbs/ms_member/*";
		JavaRDD<String> initialData1 = sc.textFile(path1).cache();
		List<Tuple2<String, String>> memberData = initialData1

				.filter(new Function<String, Boolean>() {

					@Override
					public Boolean call(String s) throws Exception {
						String[] ss = s.split(",");
						if (ss.length < 28) {
							return false;
						}

						String chan = ss[2];
						String isDel = ss[18];
						String dt = ss[20];
						SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");
						long start = sdf.parse("2018-01-14 00:00:00.0").getTime();
						long createTime = sdf.parse(dt).getTime();

						if (createTime < start) {
							return false;
						}
						if ("1".equals(isDel)) {
							return false;
						}
						if (!"1".equals(chan)) {
							return false;
						}
						return true;
					}
				}).mapToPair(new PairFunction<String, String, String>() {

					@Override
					public Tuple2<String, String> call(String s) throws Exception {
						String[] ss = s.split(",");
						String id = ss[0];
						String num = ss[1];
						String lv = ss[5];
						String recharge = ss[16];
						String dt = ss[20].substring(0, 10);

						// 等级1，等级2，等级3，等级4，等级5
						return new Tuple2<String, String>(id, dt + "#" + num + "#" + lv + "#" + recharge);
					}
				}).collect();

		Map<String, String> memMap = new HashMap<String, String>();
		for (Tuple2<String, String> t : memberData) {
			memMap.put(t._1, t._2);
		}

		final Broadcast<Map<String, String>> bv = sc.broadcast(memMap);

		String path0 = "hdfs://vm-xaj-bigdata-da-d01:8020/yst/sta_mbs/ms_user_wechat/*";
		Logger.getRootLogger().setLevel(Level.OFF);
		JavaPairRDD<String, String> wechatData = sc.textFile(path0).filter(new Function<String, Boolean>() {

			@Override
			public Boolean call(String s) throws Exception {
				String[] ss = s.split(",");

				return ss.length == 25;
			}
		}).mapToPair(new PairFunction<String, String, String>() {

			@Override
			public Tuple2<String, String> call(String s) throws Exception {
				String[] ss = s.split(",");
				String id = ss[1];
				String opid = ss[3];
				String sex = ss[5];
				Map<String, String> memMap = bv.getValue();
				String memberInfo = memMap.get(id);
				if (memberInfo == null) {
					memberInfo = "###";
				}
				return new Tuple2<String, String>(opid, sex + "#" + memberInfo);
			}
		});
		System.out.println(wechatData.take(3));
		Map<String, String> memMap2 = new HashMap<String, String>();
		for (Tuple2<String, String> t : wechatData.collect()) {
			memMap2.put(t._1, t._2);
		}

		final Broadcast<Map<String, String>> bv2 = sc.broadcast(memMap2);
		String pathOrder = "hdfs://vm-xaj-bigdata-da-d01:8020/yst/vem/sales/order";

		JavaPairRDD<String, String> userActionInfo = sc.textFile(pathOrder)
				.mapToPair(new PairFunction<String, String, String>() {

					@Override
					public Tuple2<String, String> call(String s) throws Exception {
						String[] ss = s.split(",");
						String payid = ss[5];
						String prodid = ss[11];
						String dt = ss[0];
						String netid = ss[8];
						String status = ss[2];
						String acountType = ss[3];
						String amount = ss[4];
						return new Tuple2<String, String>(payid,
								dt + "," + prodid + "," + netid + "," + status + "," + acountType + "," + amount);
					}
				}).reduceByKey(new Function2<String, String, String>() {

					@Override
					public String call(String s0, String s1) throws Exception {

						return s0 + "#" + s1;
					}
				}).mapToPair(new PairFunction<Tuple2<String, String>, String, String>() {

					@Override
					public Tuple2<String, String> call(Tuple2<String, String> t) throws Exception {

						String[] ss = t._2.split("#");

						Map<String, String> memMap = bv2.getValue();
						int prCount = 0;
						int buyCountAll = 0;
						int buyCount7 = 0;
						int buyCount30 = 0;
						double amountAll = 0;
						double amount7 = 0;
						double amount30 = 0;
						int memBuyCount = 0;
						int weekDayBuyCount = 0;
						int reqCount = 0;
						String net;

						int buyDays = 0;
						int difDayLast = -1;
						int difDayFirst = -1;
						double avTimeGap;
						// dt+","+prodid+","+netid+","+status+","+acountType
						Map<String, Integer> prMap = new HashMap<String, Integer>();
						Map<String, Integer> timePartMap = new HashMap<String, Integer>();
						timePartMap.put("1", 0);
						timePartMap.put("2", 0);
						timePartMap.put("3", 0);
						timePartMap.put("4", 0);
						Map<String, Integer> weekMap = new HashMap<String, Integer>();
						weekMap.put("周末", 0);
						weekMap.put("平时", 0);
						Map<String, Integer> accountTypeMap = new HashMap<String, Integer>();
						accountTypeMap.put("0", 0);
						accountTypeMap.put("1", 0);

						Set<String> daySet = new HashSet<String>();
						Set<String> netSet = new HashSet<String>();
						SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");
						long minTime = sdf.parse("2050-01-01 00:00:00.0").getTime();
						long maxTime = sdf.parse("1970-01-01 00:00:00.0").getTime();

						SimpleDateFormat sdf2 = new SimpleDateFormat("E");
						long offset = 1000L*3600*24;
						long now = sdf.parse(endDayBV.getValue()).getTime()+offset;

						for (String s : ss) {
							String[] fs = s.split(",");
							String dt = fs[0];
							long actionTime = sdf.parse(dt).getTime();
							String prodid = fs[1];
							String netid = fs[2];
							String status = fs[3];
							String acountType = fs[4];
							double amount = Double.parseDouble(fs[5]);

							if ("7".equals(status)) {
								reqCount++;
							} else if ("3".equals(status)) {
								buyCountAll++;
								amountAll += amount;
								if (now - actionTime <= 1000 * 3600 * 24 * 30L) {
									buyCount30++;
									amount30 += amount;
								}
								if (now - actionTime <= 1000 * 3600 * 24 * 7L) {
									buyCount7++;
									amount7 += amount;
								}
								if (prMap.containsKey(prodid)) {
									prMap.put(prodid, prMap.get(prodid) + 1);
								} else {
									prMap.put(prodid, 1);
								}
								if (actionTime < minTime) {
									minTime = actionTime;
								}
								if (actionTime > maxTime) {
									maxTime = actionTime;
								}
								daySet.add(dt.substring(0, 10));
								netSet.add(netid);
								int tp = Integer.parseInt(dt.substring(11, 13));
								if (tp >= 5 && tp < 9) {
									timePartMap.put("1", timePartMap.get("1") + 1);
								} else if (tp >= 9 && tp < 17) {
									timePartMap.put("2", timePartMap.get("2") + 1);
								} else if (tp >= 17 && tp < 22) {
									timePartMap.put("3", timePartMap.get("3") + 1);
								} else {
									timePartMap.put("4", timePartMap.get("4") + 1);
								}

								String week = sdf2.format(actionTime);
								if ("星期六".equals(week) || "星期日".equals(week)) {
									weekDayBuyCount++;
								}
								if ("102".equals(acountType)) {
									memBuyCount++;
								}
							}
						}

						prCount = prMap.size();
						net = netSet.toString();
						buyDays = daySet.size();
						if (buyDays > 0) {
							difDayLast = (int) ((now - maxTime) / 1000 / 3600 / 24);
							difDayFirst = (int) ((now - minTime) / 1000 / 3600 / 24);
						}

						double timeGap = -1;
						if (buyDays > 1) {
							timeGap = (double) ((maxTime - minTime) / 1000 / 3600 / 24);
						}

						if (buyDays >= 2) {
							avTimeGap = (double) timeGap / (double) (buyDays - 1);
						} else {
							avTimeGap = -1;
						}

						String memInfo = memMap.get(t._1);
						if (memInfo == null) {
							memInfo = "####";
						}
						// 买了几种商品，退货次数，有几天有购买行为，
						// 买了几次,最近30天买了几次，最近7天买了几次
						// 消费多少钱,最近30天消费多少钱，最近7天消费多少钱
						// 间隔时间，平均间隔时间，最后一次购买距离今天的天数，第一次购买距离今天的天数
						// 会员购买次数，周末购买次数，所在网点
						// 会员基本信息 [17-21]
						// 购买时段，产品情况

						String resultInfo = prCount + "#" + reqCount + "#" + buyDays
								+ "#" + buyCountAll + "#" + buyCount30 + "#" + buyCount7
								+ "#" + amountAll + "#" + amount30 + "#" + amount7 + "#"
								+ timeGap + "#" + avTimeGap + "#" + difDayLast + "#" + difDayFirst
								+ "#" + memBuyCount + "#" + weekDayBuyCount + "#" + net + "#" +
								memInfo + "#" +
								timePartMap + "#" + prMap;

						//
						return new Tuple2<String, String>(t._1, resultInfo.replace(", ", "|").replace("#", ","));
					}
				}).cache();

		System.out.println(userActionInfo.take(1));
		String yestodayString = new SimpleDateFormat("yyyy-MM-dd").format(new Date().getTime() - 24 * 3600 * 1000);

		System.out.println(userActionInfo.count());

		String savePath = "hdfs://vm-xaj-bigdata-da-d01:8020/yst/vem/user/history/" + yestodayString + "/";
		Utils.saveHdfs(userActionInfo, sc, savePath);
		
		if (!endString.substring(0, 10).equals(yestodayString)) {
		} else {
			savePath = "hdfs://vm-xaj-bigdata-da-d01:8020/yst/vem/user/main";
			Utils.saveHdfs(userActionInfo, sc, savePath);
		}
		Logger.getRootLogger().setLevel(Level.INFO);
		Logger.getRootLogger().info("jobEndtime:" + format2.format(new Date().getTime()) + ";");
		Logger.getRootLogger().info("jobResult:Sucessfull!");
		Logger.getRootLogger().setLevel(Level.OFF);
		sc.close();

	}

}
