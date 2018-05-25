package com.seven.spark.vem.base;

import com.seven.spark.utils.Utils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;

//import org.apache.spark.api.java.function.VoidFunction;

public class Stat {

	public static void main(String[] args) {

		SparkConf sparkConf = new SparkConf().setAppName("tmp");
		String endString = null;
		if (args == null || args.length == 0) {
			sparkConf.setMaster("local[*]");
			endString = "2018-05-14 00:00:00.0";
		} else {

			endString = args[0];
		}
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		SimpleDateFormat format2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date starttime = new Date();
		Logger.getRootLogger().info("jobStarttime:" + format2.format(starttime.getTime()));
		Logger.getRootLogger().setLevel(Level.OFF);
		String path0 = "hdfs://vm-xaj-bigdata-da-d01:8020/yst/vem/sales/order";
		JavaRDD<String> orderData = sc.textFile(path0).cache();
		String basePath = "hdfs://vm-xaj-bigdata-da-d01:8020/yst/vem/sales/stat/";

		String yestodayString = new SimpleDateFormat("yyyy-MM-dd").format(new Date().getTime() - 24 * 3600 * 1000);

		JavaPairRDD<String, String> pointData = getRecordData(orderData, sc, "P", endString).cache();
		JavaPairRDD<String, String> prodData = getRecordData(orderData, sc, "Pr", endString).cache();
		JavaPairRDD<String, String> netData = getRecordData(orderData, sc, "N", endString).cache();
		JavaPairRDD<String, String> cityData = getRecordData(orderData, sc, "C", endString).cache();
		JavaPairRDD<String, String> allData = getRecordData(orderData, sc, "A", endString).cache();
		JavaPairRDD<String, String> prodOfPointData = getRecordData(orderData, sc, "Pr_P", endString).cache();
		JavaPairRDD<String, String> prodOfNetData = getRecordData(orderData, sc, "Pr_N", endString).cache();
		JavaPairRDD<String, String> prodOfCityData = getRecordData(orderData, sc, "Pr_C", endString).cache();

		Utils.saveHdfs(pointData, sc, basePath + "P/history/" + yestodayString);
		Utils.saveHdfs(prodData, sc, basePath + "Pr/history/" + yestodayString);
		Utils.saveHdfs(netData, sc, basePath + "N/history/" + yestodayString);
		Utils.saveHdfs(cityData, sc, basePath + "C/history/" + yestodayString);
		Utils.saveHdfs(allData, sc, basePath + "A/history/" + yestodayString);
		Utils.saveHdfs(prodOfPointData, sc, basePath + "Pr_P/history/" + yestodayString);
		Utils.saveHdfs(prodOfNetData, sc, basePath + "Pr_N/history/" + yestodayString);
		Utils.saveHdfs(prodOfCityData, sc, basePath + "Pr_C/history/" + yestodayString);

		if (!endString.substring(0, 10).equals(yestodayString)) {
		} else {
			Utils.saveHdfs(pointData, sc, basePath + "P/main");
			Utils.saveHdfs(prodData, sc, basePath + "Pr/main");
			Utils.saveHdfs(netData, sc, basePath + "N/main");
			Utils.saveHdfs(cityData, sc, basePath + "C/main");
			Utils.saveHdfs(allData, sc, basePath + "A/main");
			Utils.saveHdfs(prodOfPointData, sc, basePath + "Pr_P/main");
			Utils.saveHdfs(prodOfNetData, sc, basePath + "Pr_N/main");
			Utils.saveHdfs(prodOfCityData, sc, basePath + "Pr_C/main");
		}
//		cityData.foreach(new VoidFunction<Tuple2<String, String>>() {
//			@Override
//			public void call(Tuple2<String, String> stringStringTuple2) throws Exception {
//				System.out.println(stringStringTuple2._1+":"+stringStringTuple2._2);
//			}
//		});
		Logger.getRootLogger().setLevel(Level.INFO);
		Logger.getRootLogger().info("jobEndtime:" + format2.format(new Date().getTime()) + ";");
		Logger.getRootLogger().info("jobResult:Sucessfull!");
		Logger.getRootLogger().setLevel(Level.OFF);
		sc.close();
	}

	@SuppressWarnings("serial")
	private static JavaPairRDD<String, String> getRecordData(JavaRDD<String> orderData, JavaSparkContext sc,
			String type, String endDay) {

		final Broadcast<String> bv = sc.broadcast(type);
		final Broadcast<String> endDayBV = sc.broadcast(endDay);
		JavaPairRDD<String, String> orderData2 = null;
		orderData2 = orderData.mapToPair(new PairFunction<String, String, String>() {
			@Override
			public Tuple2<String, String> call(String s) throws Exception {
				String[] ss = s.split(",");
				String payid = ss[5];
				String prodid = ss[11];
				String amount = ss[4];
				String count = ss[14];
				String dt = ss[0];
				String status = ss[2];
				String acountType = ss[3];
				String focus = "1";
				String type = bv.getValue();
				if ("P".equals(type)) {
					focus = ss[9];
				} else if ("N".equals(type)) {
					focus = ss[8];
				} else if ("C".equals(type)) {
					focus = ss[10];
				} else if ("Pr".equals(type)) {
					focus = ss[11];
				} else if ("Pr_N".equals(type)) {
					focus = ss[11] + "," + ss[8];
				} else if ("Pr_P".equals(type)) {
					focus = ss[11] + "," + ss[9];
				} else if ("Pr_C".equals(type)) {
					focus = ss[11] + "," + ss[10];
				}

				return new Tuple2<String, String>(focus,
						dt + "," + prodid + "," + payid + "," + amount + "," + count + "," + status + "," + acountType);
			}
		});

		JavaPairRDD<String, String> recordData = orderData2.reduceByKey(new Function2<String, String, String>() {

			@Override
			public String call(String l1, String l2) throws Exception {

				return l1 + "#" + l2;
			}
		}).mapToPair(new PairFunction<Tuple2<String, String>, String, String>() {

			@Override
			public Tuple2<String, String> call(Tuple2<String, String> t) throws Exception {
				SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");
				//系统传进来的是昨天的时间
				long offset = 1000L*3600*24;
				long end = sdf.parse(endDayBV.getValue()).getTime()+offset;
				String[] ss = t._2.split("#");
				String firstDt = "";// 首次有销量的时间
				long diffDays = 0;// 首次销量距离今天的天数
				int count7 = 0;// 7日销量
				int countLast7 = 0;// 上7日销量
				int count30 = 0;// 30日销量
				int countLast30 = 0;// 上30日销量
				int countAll = 0;// 总销量
				double amount7 = 0;// 7日销售额
				double amountLast7 = 0;// 上7日销售额
				double amount30 = 0;// 30日销售额
				double amountLast30 = 0;// 上30日销售额
				double amountAll = 0;// 总销售额
				int reback7 = 0;// 7日退货
				int reback30 = 0;// 30日退货
				int invld7 = 0;// 7日的非正常状态
				int invld30 = 0;// 30日非正常状态
				Set<String> u7Set = new HashSet<String>();// 7日用户集合
				Set<String> uLast7Set = new HashSet<String>();// 上7日用户集合
				Set<String> uB7Set = new HashSet<String>();// 7日前用户集合
				Set<String> uBB7Set = new HashSet<String>();// 14日前用户集合

				Set<String> u30Set = new HashSet<String>();// 30日用户集合
				Set<String> uLast30Set = new HashSet<String>();// 上30日用户集合
				Set<String> uB30Set = new HashSet<String>();// 30日前用户集合
				Set<String> uSet = new HashSet<String>();// 总的用户集合

				// 用来计算留存率
				Set<String> u20Set = new HashSet<String>();// 20日用户集合
				Set<String> uLast20Set = new HashSet<String>();// 上20日用户集合
				Map<String, Integer> uLast20Map = new HashMap<String, Integer>();// 上20日用户购买频率
				Map<String, Integer> u20Map = new HashMap<String, Integer>();// 20日用户购买频率

				Set<String> m7Set = new HashSet<String>();// 7日会员消费集合
				Set<String> m30Set = new HashSet<String>();// 30日会员消费集合
				Set<String> mSet = new HashSet<String>();// 总的会员消费集合
				Map<String, Integer> uLast30Map = new HashMap<String, Integer>();// 上30日用户购买频率
				Map<String, Integer> u30Map = new HashMap<String, Integer>();// 30日用户购买频率
				Map<String, Integer> p7Map = new HashMap<String, Integer>();// 7日产品销售
				Map<String, Integer> p30Map = new HashMap<String, Integer>();// 30日产品销售
				Map<String, Integer> pMap = new HashMap<String, Integer>();// 总产品销售
				// 30日时段销售列表1，2.。。。。24
				List<Integer> time30List = new ArrayList<Integer>();
				// 近30日用户消费频率列表 1,2,[3-5],[6-10],[11,∞)
				List<Integer> cf30List = new ArrayList<Integer>();
				int lu20Count = 0; // 20日流失用户数
				double lu20Rate = 0.0; // 20日流失用户比例
				int retention20Count = 0; // 20日留存用户数
				double retention20Rate = 0.0; // 20日留存用户比例

				Integer[] ts = { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
				Integer[] cfs = { 0, 0, 0, 0, 0 };
				long r7 = 1000 * 3600 * 24 * 7L;// 步长7
				long r20 = 1000 * 3600 * 24 * 20L;// 步长20
				long r30 = 1000 * 3600 * 24 * 30L;// 步长30
				for (String s : ss) {
					String[] fs = s.split(",");
					// prodid + "," + payid + "," + amount + "," + count + "," + status
					String dateString = fs[0];
					int h = Integer.parseInt(dateString.substring(11, 13));
					String pid = fs[1];
					String uid = fs[2];
					Double amount = Double.parseDouble(fs[3]);
					Double count = Double.parseDouble(fs[4]);
					String statu = fs[5];
					String acountType = fs[6];
					long dateTime = sdf.parse(dateString).getTime();
					if (dateTime > end) {
						continue;
					}
					long p7 = (end - dateTime) / r7;
					long p20 = (end - dateTime) / r20;
					long p30 = (end - dateTime) / r30;
					if ("7".equals(statu)) {
						if (p7 == 0L) {
							reback7++;
						}
						if (p30 == 0L) {
							reback30++;
						}
						continue;

					} else if (!"3".equals(statu)) {

						if (p7 == 0L) {
							invld7++;
						}
						if (p30 == 0L) {
							invld30++;
						}
						continue;
					}
					if (end - dateTime > diffDays) {
						diffDays = end - dateTime;
						firstDt = dateString;
					}

					if (p7 == 0L) {
						if ("102".equals(acountType)) {
							m7Set.add(uid);
						}

						count7 += count;
						amount7 += amount;
						u7Set.add(uid);
						if (p7Map.containsKey(pid)) {
							p7Map.put(pid, p7Map.get(pid) + 1);
						} else {
							p7Map.put(pid, 1);
						}
					} else if (p7 == 1L) {
						countLast7 += count;
						amountLast7 += amount;
						uLast7Set.add(uid);
						uB7Set.add(uid);
					} else {
						uB7Set.add(uid);
						uBB7Set.add(uid);
					}
					// 留存计算
					if (p20 == 0L) {
						u20Set.add(uid);
						if (u20Map.containsKey(uid)) {
							u20Map.put(uid, u20Map.get(uid) + 1);
						} else {
							u20Map.put(uid, 1);
						}
					} else if (p20 == 1L) {
						if (uLast20Map.containsKey(uid)) {
							uLast20Map.put(uid, uLast20Map.get(uid) + 1);
						} else {
							uLast20Map.put(uid, 1);
						}
						uLast20Set.add(uid);
					}

					if (p30 == 0L) {
						count30 += count;
						amount30 += amount;
						if ("102".equals(acountType)) {
							m30Set.add(uid);
						}
						u30Set.add(uid);
						if (p30Map.containsKey(pid)) {
							p30Map.put(pid, p30Map.get(pid) + 1);
						} else {
							p30Map.put(pid, 1);
						}
						if (u30Map.containsKey(uid)) {
							u30Map.put(uid, u30Map.get(uid) + 1);
						} else {
							u30Map.put(uid, 1);
						}
						ts[h]++;
					} else if (p30 == 1L) {
						countLast30 += count;
						amountLast30 += amount;
						if (uLast30Map.containsKey(uid)) {
							uLast30Map.put(uid, uLast30Map.get(uid) + 1);
						} else {
							uLast30Map.put(uid, 1);
						}
						uLast30Set.add(uid);
						uB30Set.add(uid);
					} else {
						uB30Set.add(uid);
					}

					countAll += count;
					amountAll += amount;
					uSet.add(uid);
					if ("102".equals(acountType)) {
						mSet.add(uid);
					}
					if (pMap.containsKey(pid)) {
						pMap.put(pid, pMap.get(pid) + 1);
					} else {
						pMap.put(pid, 1);
					}

				}
				// System.out.println(uLast30Map);
				double amount7o7 = 1;
				if (amountLast7 != 0) {
					amount7o7 = (amount7-amountLast7) / amountLast7;// 7日销售额环比
				}
				double count7o7 = 1;
				if (countLast7 != 0) {
					count7o7 = (double) (count7-countLast7) / (double) countLast7;// 7日销售量环比
				}
				int uCount7 = u7Set.size();// 7日用户数
				double uCount7o7 = 1;
				if (uLast7Set.size() != 0) {
					uCount7o7 = (double) (uCount7-uLast7Set.size()) / (double) uLast7Set.size();// 7日用户环比
				}

				Set<String> nuSet = new HashSet<String>();
				nuSet.clear();
				nuSet.addAll(u7Set);
				nuSet.removeAll(uB7Set);
				int nuCount7 = nuSet.size();// 7日新用户数

				nuSet.clear();
				nuSet.addAll(uB7Set);
				nuSet.removeAll(uBB7Set);
				int nuCountLast7 = nuSet.size();// 上7日新用户数

				double nuCount7o7 = 1;
				if (nuCountLast7 != 0) {
					nuCount7o7 = (double) (nuCount7- nuCountLast7) / (double) nuCountLast7;// 新用户数环比
				}

				double amount30o30 = 1;
				if (amountLast30 != 0) {
					amount30o30 = (amount30-amountLast30) / amountLast30;// 30日销售额环比
				}
				double count30o30 = 1;
				if (countLast30 != 0) {
					count30o30 = (double) (count30- countLast30) / (double) countLast30;// 30日销售数量环比
				}
				
				
				int uCount30 = u30Set.size();// 30日用户数
				nuSet.clear();
				nuSet.addAll(u30Set);
				nuSet.removeAll(uB30Set);
				int nuCount30 = nuSet.size();// 30日新用户数

				int uCountAll = uSet.size();// 总用户数

				int mCount7 = m7Set.size();// 7日消费会员数
				int mCount30 = m30Set.size();// 30日消费会员数
				int mCountAll = mSet.size();// 消费会员数

				Iterator<Entry<String, Integer>> entriesLast20 = uLast20Map.entrySet().iterator();

				int lu20Base = 0;
				int retention20Base = 0;

				while (entriesLast20.hasNext()) {
					Entry<String, Integer> entry = entriesLast20.next();
					int value = entry.getValue();
					String key = entry.getKey();
					if (value >= 1) {// 上30日购买过，近30日也购买过，视为留存
						retention20Base++;
						if (u20Map.containsKey(key)) {
							retention20Count++;
						}
					}
					if (value >= 2) {// 上20日购买2次以上近20日无购买行为，视为流失
						lu20Base++;
						if (!u20Map.containsKey(key)) {
							lu20Count++;
						}
					}
				}
				if (lu20Base == 0) {
					lu20Rate = 1;
				} else {
					lu20Rate = (double) lu20Count / (double) lu20Base;
				}
				if (retention20Base == 0) {
					retention20Rate = 1;
				} else {
					retention20Rate = (double) retention20Count / (double) retention20Base;
				}

				Iterator<Entry<String, Integer>> entries = u30Map.entrySet().iterator();

				while (entries.hasNext()) {
					Entry<String, Integer> entry = entries.next();
					int value = entry.getValue();
					if (value == 1) {
						cfs[0]++;
					} else if (value == 2) {
						cfs[1]++;
					} else if (value <= 5) {
						cfs[2]++;
					} else if (value <= 10) {
						cfs[3]++;
					} else {
						cfs[4]++;
					}
				}

				time30List = Arrays.asList(ts);
				cf30List = Arrays.asList(cfs);

				// 7日销量，7日销售额，7日消费客户总数，7日新消费客户，7日消费会员数，7日不正常状态，7日退货，7日销量环比，7日营收环比，7日客户环比，7日新客户环比
				// 30日销量，30日销售额，30日消费客户总数，30日新消费客户，30日消费会员数，30日不正常状态，30日退货，30日销量环比，30日营收环比
				// 20日流失用户数（上20日内有两次以上购买，行为而近20日没有购买行为的用户数），流失率
				// 20日留存用户数，20日留存率（上20日和近20日均有购买行为的用户数／上20日有购买行为的用户数）
				// 总销量，总营收，总用户数，消费会员数
				// 首销时间,首销距离今天的天数
				// 7日产品售卖情况,30日产品售卖情况，总产品售卖情况
				// 30日销售时段分布，30日消费频率分布

				String result = count7 + "," + amount7 / 100 + "," + uCount7 + "," + nuCount7 + "," + mCount7 + ","
						+ invld7 + "," + reback7 + "," + count7o7 + "," + amount7o7 + "," + uCount7o7 + "," + nuCount7o7
						+ "," + count30 + "," + amount30 / 100 + "," + uCount30 + "," + nuCount30 + "," + mCount30 + ","
						+ invld30 + "," + reback30 + "," + count30o30 + "," + amount30o30 + "," + lu20Count + ","
						+ lu20Rate + "," + retention20Count + "," + retention20Rate + "," + countAll + ","
						+ amountAll / 100 + "," + uCountAll + "," + mCountAll + "," + firstDt + "," + diffDays + ","
						+ p7Map.toString() + "," + p30Map.toString() + "," + pMap.toString() + ","
						+ time30List.toString() + "," + cf30List.toString();

//				if("黑龙江省_哈尔滨市".equals(t._1) || "吉林省_长春市".equals(t._1)){
//					System.out.println("********************");
//					System.out.println(t._1+","+result);
//					System.out.println(t._2);
//					System.out.println("七日销量："+count7+"，上七日销量："+countLast7);
//					System.out.println("七日销售额："+amount7+"，上七日销售额："+amountLast7);
//					System.out.println("七日营收环比："+amount7o7);
//					System.out.println("七日销量环比："+count7o7);
//					System.out.println("七日新用户数："+nuCount7);
//					System.out.println("七日新用户环比："+nuCount7o7);
//					System.out.println("七日用户环比："+uCount7o7);
//					System.out.println("七日用户数："+uCount7);
//					System.out.println("********************");
//				}
				return new Tuple2<String, String>(t._1, result.replace(", ", "|"));
			}
		});
		System.out.println(recordData.take(1));
		return recordData;
	}

}
