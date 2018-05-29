package com.seven.spark.vem.base;

import com.seven.spark.utils.Utils;
import org.apache.commons.collections.map.HashedMap;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.text.SimpleDateFormat;
import java.util.*;

public class OperateStat {

	@SuppressWarnings("serial")
	public static void main(String[] args) {

		SparkConf sparkConf = new SparkConf().setAppName("tmp");
		String endString = null;
//		if (args == null || args.length == 0) {
			sparkConf.setMaster("local[*]");
//			endString = "2018-05-08 00:00:00.0";
//		} else {
//			endString = args[0];
//		}
		endString = "2018-05-22 00:00:00.0";
		System.out.println(endString);
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		SimpleDateFormat format2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date starttime = new Date();
		Logger.getRootLogger().info("jobStarttime:" + format2.format(starttime.getTime()));
		Logger.getRootLogger().setLevel(Level.OFF);
		final Broadcast<String> endDayBV = sc.broadcast(endString);
		String path0 = "hdfs://vm-xaj-bigdata-da-d01:8020/yst/sta_vem/vem_slave_machine/*";
		List<Tuple2<String, Long>> macs = sc.textFile(path0).filter(new Function<String, Boolean>() {

			@Override
			public Boolean call(String s) throws Exception {
				String[] ss = s.split(",");
				return "0".equals(ss[8]);
			}
		}).mapToPair(new PairFunction<String, String, Long>() {

			@Override
			public Tuple2<String, Long> call(String s) throws Exception {
				String[] ss = s.split(",");
				String mst = ss[1];
				return new Tuple2<String, Long>(mst, 1L);
			}
		}).reduceByKey(new Function2<Long, Long, Long>() {

			@Override
			public Long call(Long l1, Long l2) throws Exception {
				return l1 + l2;
			}
		}).collect();
		System.out.println(macs);
		Map<String, Long> macMap0 = new HashMap<String, Long>();
		for (Tuple2<String, Long> t : macs) {
			macMap0.put(t._1, t._2);
		}
		final Broadcast<Map<String, Long>> bv0 = sc.broadcast(macMap0);

		//根据网点编号获取城市
		String netPath = "hdfs://vm-xaj-bigdata-da-d01:8020/yst/vem/info/net/*";
		List<Tuple2<String, String>> netData = sc.textFile(netPath).mapToPair(
				new PairFunction<String, String, String>() {
			@Override
			public Tuple2<String, String> call(String s) throws Exception {
				String[] ss = s.split(",");
				String mst = ss[0];//网点编号
				String city = ss[6];//城市信息
				return new Tuple2<String, String>(mst, city);
			}
		}).collect();
		System.out.println(netData);
		Map<String, String> netMap = new HashMap<String, String>();
		for (Tuple2<String, String> t : netData) {
			netMap.put(t._1, t._2);
		}
		final Broadcast<Map<String,String>> netBv = sc.broadcast(netMap);
		//--------------------


		String path = "hdfs://vm-xaj-bigdata-da-d01:8020/yst/sta_vem/vem_machine/*";
		Logger.getRootLogger().setLevel(Level.OFF);
		List<Tuple2<String, String>> maclist = sc.textFile(path).filter(new Function<String, Boolean>() {

			@Override
			public Boolean call(String s) throws Exception {
				String[] ss = s.split(",");

				return "2".equals(ss[21]) && "1".equals(ss[4]) && "0".equals(ss[5]) && !"620".equals(ss[18])
						&& !"101".equals(ss[18]) && !"7667".equals(ss[18]);
			}
		}).mapToPair(new PairFunction<String, String, String>() {
			@Override
			public Tuple2<String, String> call(String s) throws Exception {
				String[] ss = s.split(",");
				String id = ss[0];//自动贩卖机编号
				String yys = ss[1];//运营商ID
				String wdid = ss[18];//网点ID
				String dwid = ss[19];//点位ID
				String model = ss[16];//设备型号ID
				Map<String, Long> map = bv0.getValue();
				Map<String,String> mapNet = netBv.getValue();
				String city = mapNet.get(wdid);//城市
				Long macCount = 1L;
				if (map.containsKey(id)) {
					macCount = macCount + map.get(id);
				}

				return new Tuple2<String, String>(id, yys + "," + wdid + "," + dwid+ "," + city + "," + macCount + "," + model);
			}
		}).collect();

		Map<String, String> macMap = new HashMap<String, String>();
		for (Tuple2<String, String> t : maclist) {
			macMap.put(t._1, t._2);
		}
		final Broadcast<Map<String, String>> bv = sc.broadcast(macMap);
		String path2 = "hdfs://vm-xaj-bigdata-da-d01:8020/yst/sta_vem/vem_onoff_line_log/*";
		JavaRDD<String> initialData2 = sc.textFile(path2).filter(new Function<String, Boolean>() {

			@Override
			public Boolean call(String s) throws Exception {
				Map<String, String> macMap = bv.getValue();
				String[] ss = s.split(",");
				String dt = ss[3];//上下线时间
				SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");
				long offset = 1000L*3600*24;

				long end = sdf.parse(endDayBV.getValue()).getTime()+offset;
				long nowTime = sdf.parse(dt).getTime();
				return macMap.containsKey(ss[1]) && nowTime < end;
			}
		}).mapToPair(new PairFunction<String, String, String>() {

			@Override
			public Tuple2<String, String> call(String s) throws Exception {
				String[] ss = s.split(",");
				String vmid = ss[1];//设备编号
				String dt = ss[3];//上下线时间

				String isOn = ss[4];//是否在线 0不在，1在线
				return new Tuple2<String, String>(vmid, dt + "," + isOn);
			}
		}).reduceByKey(new Function2<String, String, String>() {

			@Override
			public String call(String s1, String s2) throws Exception {
				return s1 + "#" + s2;
			}
		}).mapToPair(new PairFunction<Tuple2<String, String>, String, String>() {

			@Override
			public Tuple2<String, String> call(Tuple2<String, String> t) throws Exception {
				Set<String> set = new TreeSet<String>();
				for (String s : t._2.split("#")) {
					set.add(s);
				}
				String firstDt = null;
				long offTime = 0L;
				long onTime = 0L;
				long lastTime = 0L;
				double difDay = 0;
				int offCount = 0;
				int offCount7 = 0;
				int offCountLast7 = 0;
				String LastSta = "";
				List<String> list = new ArrayList<String>();
				SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");
				long offset = 1000L*3600*24;
				long end = sdf.parse(endDayBV.getValue()).getTime()+offset;
				long timeHourSeven = 1000*3600*17L;//七点之后计算
				long timehourTen = 1000*3600*10L;//十小时时间间隔


				TreeMap<Long,String> maptime7 = new TreeMap<Long, String>();//前一天
				TreeMap<Long,String> maptime6 = new TreeMap<Long, String>();//前两天
				TreeMap<Long,String> maptime5 = new TreeMap<Long, String>();//前三天
				TreeMap<Long,String> maptime4 = new TreeMap<Long, String>();//前四天
				TreeMap<Long,String> maptime3 = new TreeMap<Long, String>();//前五天
				TreeMap<Long,String> maptime2 = new TreeMap<Long, String>();//前六天
				TreeMap<Long,String> maptime1 = new TreeMap<Long, String>();//前七天
				TreeMap<Long,String> maptime0 = new TreeMap<Long, String>();//前八天

				int timeNumber = 0;//记录七天掉线情况

				for (String s : set) {
					String[] fs = s.split(",");
					long nowTime = sdf.parse(fs[0]).getTime();

					if(nowTime<end && nowTime > end-offset){
						maptime7.put(nowTime,fs[1]);
					}else if(nowTime > end-2*offset){
						maptime6.put(nowTime,fs[1]);
					}else if(nowTime > end-3*offset){
						maptime5.put(nowTime,fs[1]);
					}else if(nowTime > end-4*offset){
						maptime4.put(nowTime,fs[1]);
					}else if(nowTime > end-5*offset){
						maptime3.put(nowTime,fs[1]);
					}else if(nowTime > end-6*offset){
						maptime2.put(nowTime,fs[1]);
					}else if(nowTime > end-7*offset){
						maptime1.put(nowTime,fs[1]);
					}else if(nowTime > end-8*offset){
						maptime0.put(nowTime,fs[1]);
					}

					if (firstDt == null && "1".equals(fs[1])) {
						firstDt = fs[0];
					}
					if (nowTime > end) {
						continue;
					}
					if (LastSta.equals(fs[1])) {// 状态相同的不管
					} else {
						if (lastTime != 0) {
							if ("0".equals(fs[1])) {
								offCount++;
								onTime = onTime + (nowTime - lastTime);
								if ((end - nowTime) <= 7 * 24 * 3600 * 1000L) {
									offCount7++;
								} else if ((end - nowTime) <= 14 * 24 * 3600 * 1000L) {
									offCountLast7++;
								}
							} else {
								offTime = offTime + (nowTime - lastTime);
							}
						}
						lastTime = nowTime;
						list.add(s);
					}
					LastSta = fs[1];
				}

				//倒序
				maptime7.descendingKeySet();
				maptime6.descendingKeySet();
				maptime5.descendingKeySet();
				maptime4.descendingKeySet();
				maptime3.descendingKeySet();
				maptime2.descendingKeySet();
				maptime1.descendingKeySet();
				maptime0.descendingKeySet();


				List<Long> timeList = new ArrayList<Long>();//存放离线时长

				//******************************
				long time7 = -1L;//计算第七天离线时长
//				Iterator it = null;
				boolean flag = true;//判断是否走了else

				if(maptime7.size()>0){
					long time77 = maptime7.firstKey();//记录倒序上一次的时间节点
					if(time77 < end - timeHourSeven){//p判断最晚的时间节点是否在7点之前
						if("0".equals(maptime7.firstEntry().getValue())){
							time7 += timeHourSeven;
						}
					}else{
						if("0".equals(maptime7.firstEntry().getValue())){
							time7 += end - maptime7.firstKey();
						}
						if(maptime7.size() > 1){
							maptime7.remove(maptime7.firstKey());//移除最后一次

							Iterator it = maptime7.keySet().iterator();
							while (it.hasNext()) {
								//it.next()得到的是key，tm.get(key)得到obj
								String time = it.next().toString();
								long timeout = Long.parseLong(time);
								if(timeout > end - timeHourSeven){
									if("0".equals(maptime7.get(timeout))){
										time7 += time77 - timeout;
									}
								}else{//判断今天的最早的记录是否是7点之前
									if("0".equals(maptime7.get(timeout))){//七点之前的数据则取时到七点
										time7 += time77 - (end - timeHourSeven);
									}
									flag = false;
									break;
								}
								time77 = timeout;
							}
						}
						if(flag){//判断最早一条记录是否是当天七点之前
							String s = "";
							if(maptime6.size() > 0){
								s = maptime6.firstEntry().getValue();
							}else if(maptime5.size() > 0){
								s = maptime5.firstEntry().getValue();
							}else if(maptime4.size() > 0){
								s = maptime4.firstEntry().getValue();
							}else if(maptime3.size() > 0){
								s = maptime3.firstEntry().getValue();
							}else if(maptime2.size() > 0){
								s = maptime2.firstEntry().getValue();
							}else if(maptime1.size() > 0){
								s = maptime1.firstEntry().getValue();
							}else if(maptime0.size() > 0){
								s = maptime0.firstEntry().getValue();
							}
							if("0".equals(s)){//不是则取上一天最后一个时间节点
								time7 += time77 - (end - timeHourSeven);
							}
						}
					}
				}else{
					String s = "";
					if(maptime6.size() > 0){
						s = maptime6.firstEntry().getValue();
					}else if(maptime5.size() > 0){
						s = maptime5.firstEntry().getValue();
					}else if(maptime4.size() > 0){
						s = maptime4.firstEntry().getValue();
					}else if(maptime3.size() > 0){
						s = maptime3.firstEntry().getValue();
					}else if(maptime2.size() > 0){
						s = maptime2.firstEntry().getValue();
					}else if(maptime1.size() > 0){
						s = maptime1.firstEntry().getValue();
					}else if(maptime0.size() > 0){
						s = maptime0.firstEntry().getValue();
					}
					if("0".equals(s)){//不是则取上一天最后一个时间节点
						time7 += timeHourSeven;
					}
				}
				timeList.add(time7);//第七天数据
				flag = true;
				//******************************
				long time6 = -1L;//计算第六天离线时长
				if(maptime6.size()>0) {
					long time66 = maptime6.firstKey();//记录倒序上一次的时间节点
					if (time66 < end - offset - timeHourSeven) {
						if ("0".equals(maptime6.firstEntry().getValue())) {
							time6 += timeHourSeven;
						}
					} else {

						if ("0".equals(maptime6.firstEntry().getValue())) {
							time6 += (end - offset) - maptime6.firstKey();
						}

						if (maptime6.size() > 1) {
							maptime6.remove(maptime6.firstKey());//移除最后一次
							Iterator it = maptime6.keySet().iterator();
							while (it.hasNext()) {
								//it.next()得到的是key，tm.get(key)得到obj
								String time = it.next().toString();
								long timeout = Long.parseLong(time);
								if (timeout > end - timeHourSeven - offset) {
									if ("0".equals(maptime6.get(timeout))) {
										time6 += time66 - timeout;
									}
								} else {//判断今天的最早的记录是否是7点之前
									if ("0".equals(maptime6.get(timeout))) {//七点之前的数据则取时到七点
										time6 += time66 - (end - offset - timeHourSeven);
									}
									flag = false;
									break;
								}
								time66 = timeout;
							}
						}

						if (flag) {//判断最早一条记录是否是当天七点之前
							String s = "";
							if(maptime5.size() > 0){
								s = maptime5.firstEntry().getValue();
							}else if(maptime4.size() > 0){
								s = maptime4.firstEntry().getValue();
							}else if(maptime3.size() > 0){
								s = maptime3.firstEntry().getValue();
							}else if(maptime2.size() > 0){
								s = maptime2.firstEntry().getValue();
							}else if(maptime1.size() > 0){
								s = maptime1.firstEntry().getValue();
							}else if(maptime0.size() > 0){
								s = maptime0.firstEntry().getValue();
							}
							if ("0".equals(s)) {//不是则取上一天最后一个时间节点
								time6 += time66 - (end - offset - timeHourSeven);
							}
						}
					}
				}else{
					String s = "";
					if(maptime5.size() > 0){
						s = maptime5.firstEntry().getValue();
					}else if(maptime4.size() > 0){
						s = maptime4.firstEntry().getValue();
					}else if(maptime3.size() > 0){
						s = maptime3.firstEntry().getValue();
					}else if(maptime2.size() > 0){
						s = maptime2.firstEntry().getValue();
					}else if(maptime1.size() > 0){
						s = maptime1.firstEntry().getValue();
					}else if(maptime0.size() > 0){
						s = maptime0.firstEntry().getValue();
					}
					if ("0".equals(s)) {//不是则取上一天最后一个时间节点
						time6 += timeHourSeven;
					}
				}
				timeList.add(time6);//第六天数据
				flag = true;
				//******************************
				long time5 = -1L;//计算第五天离线时长
				if(maptime5.size()>0) {
					long time55 = maptime5.firstKey();//记录倒序上一次的时间节点
					if (time55 < end - 2 * offset - timeHourSeven) {
						if ("0".equals(maptime5.firstEntry().getValue())) {
							time5 += timeHourSeven;
						}
					} else {
						if ("0".equals(maptime5.firstEntry().getValue())) {
							time5 += (end - 2 * offset) - maptime5.firstKey();
						}

						if (maptime5.size() > 1) {
							maptime5.remove(maptime5.firstKey());//移除最后一次

							Iterator	it = maptime5.keySet().iterator();
							while (it.hasNext()) {
								//it.next()得到的是key，tm.get(key)得到obj
								String time = it.next().toString();
								long timeout = Long.parseLong(time);
								if (timeout > end - 2 * offset - timeHourSeven) {
									if ("0".equals(maptime5.get(timeout))) {
										time5 += time55 - timeout;
									}
								} else {//判断今天的最早的记录是否是7点之前
									if ("0".equals(maptime5.get(timeout))) {//七点之前的数据则取时到七点
										time5 += time55 - (end - 2 * offset - timeHourSeven);
									}
									flag = false;
									break;
								}
								time55 = timeout;
							}
						}

						if (flag) {//判断最早一条记录是否是当天七点之前
							String s = "";
							if(maptime4.size() > 0){
								s = maptime4.firstEntry().getValue();
							}else if(maptime3.size() > 0){
								s = maptime3.firstEntry().getValue();
							}else if(maptime2.size() > 0){
								s = maptime2.firstEntry().getValue();
							}else if(maptime1.size() > 0){
								s = maptime1.firstEntry().getValue();
							}else if(maptime0.size() > 0){
								s = maptime0.firstEntry().getValue();
							}
							if ("0".equals(s)) {//不是则取上一天最后一个时间节点
								time5 += time55 - (end - 2 * offset - timeHourSeven);
							}
						}
					}
				}else {
					String s = "";
					if(maptime4.size() > 0){
						s = maptime4.firstEntry().getValue();
					}else if(maptime3.size() > 0){
						s = maptime3.firstEntry().getValue();
					}else if(maptime2.size() > 0){
						s = maptime2.firstEntry().getValue();
					}else if(maptime1.size() > 0){
						s = maptime1.firstEntry().getValue();
					}else if(maptime0.size() > 0){
						s = maptime0.firstEntry().getValue();
					}
					if ("0".equals(s)) {//不是则取上一天最后一个时间节点
						time5 += timeHourSeven;
					}
				}
				timeList.add(time5);//第五天数据
				flag = true ;


				//******************************
				long time4 = -1L;//计算第四天离线时长
				if(maptime4.size()>0) {
					long time44 = maptime4.firstKey();//记录倒序上一次的时间节点
					if (time44 < end - 3 * offset - timeHourSeven) {
						if ("0".equals(maptime4.firstEntry().getValue())) {
							time4 += timeHourSeven;
						}
					} else {
						if ("0".equals(maptime4.firstEntry().getValue())) {
							time4 += (end - 3 * offset) - maptime4.firstKey();
						}

						if (maptime4.size() > 1) {
							maptime4.remove(maptime4.firstKey());//移除最后一次

							Iterator	it = maptime4.keySet().iterator();
							while (it.hasNext()) {
								//it.next()得到的是key，tm.get(key)得到obj
								String time = it.next().toString();
								long timeout = Long.parseLong(time);
								if (timeout > end - 3 * offset - timeHourSeven) {
									if ("0".equals(maptime4.get(timeout))) {
										time4 += time44 - timeout;
									}
								} else {//判断今天的最早的记录是否是7点之前
									if ("0".equals(maptime4.get(timeout))) {//七点之前的数据则取时到七点
										time4 += time44 - (end - 3 * offset - timeHourSeven);
									}
									flag = false;
									break;
								}
								time44 = timeout;
							}
						}

						if (flag) {//判断最早一条记录是否是当天七点之前
							String s = "";
							if (maptime3.size() > 0){
								s = maptime3.firstEntry().getValue();
							}else if(maptime2.size() > 0){
								s = maptime2.firstEntry().getValue();
							}else if(maptime1.size() > 0){
								s = maptime1.firstEntry().getValue();
							}else if(maptime0.size() > 0){
								s = maptime0.firstEntry().getValue();
							}
							if ("0".equals(s)) {//不是则取上一天最后一个时间节点
								time4 += time44 - (end - 3 * offset - timeHourSeven);
							}
						}
					}
				}else {
					String s = "";
					if (maptime3.size() > 0){
						s = maptime3.firstEntry().getValue();
					}else if(maptime2.size() > 0){
						s = maptime2.firstEntry().getValue();
					}else if(maptime1.size() > 0){
						s = maptime1.firstEntry().getValue();
					}else if(maptime0.size() > 0){
						s = maptime0.firstEntry().getValue();
					}
					if ("0".equals(s)) {//不是则取上一天最后一个时间节点
						time4 += timeHourSeven;
					}
				}
				timeList.add(time4);//第四天数据
				flag = true ;


				//******************************
				long time3 = -1L;//计算第三天离线时长
				if(maptime3.size()>0) {
					long time33 = maptime3.firstKey();//记录倒序上一次的时间节点

					if (time33 < end - 4 * offset - timeHourSeven) {
						if ("0".equals(maptime3.firstEntry().getValue())) {
							time3 += timeHourSeven;
						}
					} else {
						if ("0".equals(maptime3.firstEntry().getValue())) {
							time3 += (end - 4 * offset) - maptime3.firstKey();
						}

						if (maptime3.size() > 1) {
							maptime3.remove(maptime3.firstKey());//移除最后一次

							Iterator	it = maptime3.keySet().iterator();
							while (it.hasNext()) {
								//it.next()得到的是key，tm.get(key)得到obj
								String time = it.next().toString();
								long timeout = Long.parseLong(time);
								if (timeout > end - 4 * offset - timeHourSeven) {
									if ("0".equals(maptime3.get(timeout))) {
										time3 += time33 - timeout;
									}
								} else {//判断今天的最早的记录是否是7点之前
									if ("0".equals(maptime3.get(timeout))) {//七点之前的数据则取时到七点
										time3 += time33 - (end - 4 * offset - timeHourSeven);
									}
									flag = false;
									break;
								}
								time33 = timeout;
							}
						}

						if (flag) {//判断最早一条记录是否是当天七点之前
							String s = "";
							if(maptime2.size() > 0){
								s = maptime2.firstEntry().getValue();
							}else if(maptime1.size() > 0){
								s = maptime1.firstEntry().getValue();
							}else if(maptime0.size() > 0){
								s = maptime0.firstEntry().getValue();
							}
							if ("0".equals(s)) {//不是则取上一天最后一个时间节点
								time3 += time33 - (end - 4 * offset - timeHourSeven);
							}
						}
					}
				}
				timeList.add(time3);//第三天数据
				flag = true ;


				//******************************
				long time2 = -1L;//计算第二天离线时长
				if(maptime2.size()>0) {
					long time22 = maptime2.firstKey();//记录倒序上一次的时间节点

					if (time22 < end - 5 * offset - timeHourSeven) {
						if ("0".equals(maptime2.firstEntry().getValue())) {
							time2 += timeHourSeven;
						}
					} else {
						if ("0".equals(maptime2.firstEntry().getValue())) {
							time2 += (end - 5 * offset) - maptime2.firstKey();
						}

						if (maptime2.size() > 1) {
							maptime2.remove(maptime2.firstKey());//移除最后一次

							Iterator	it = maptime2.keySet().iterator();
							while (it.hasNext()) {
								//it.next()得到的是key，tm.get(key)得到obj
								String time = it.next().toString();
								long timeout = Long.parseLong(time);
								if (timeout > end - 5 * offset - timeHourSeven) {
									if ("0".equals(maptime2.get(timeout))) {
										time2 += time22 - timeout;
									}
								} else {//判断今天的最早的记录是否是7点之前
									if ("0".equals(maptime2.get(timeout))) {//七点之前的数据则取时到七点
										time2 += time22 - (end - 5 * offset - timeHourSeven);
									}
									flag = false;
									break;
								}
								time22 = timeout;
							}
						}
						if (flag) {//判断最早一条记录是否是当天七点之前
							String s = "";
							if(maptime1.size() > 0){
								s = maptime1.firstEntry().getValue();
							}else if(maptime0.size() > 0){
								s = maptime0.firstEntry().getValue();
							}
							if ("0".equals(s)) {//不是则取上一天最后一个时间节点
								time2 += time22 - (end - 5 * offset - timeHourSeven);
							}
						}
					}
				}else{
					String s = "";
					if(maptime1.size() > 0){
						s = maptime1.firstEntry().getValue();
					}else if(maptime0.size() > 0){
						s = maptime0.firstEntry().getValue();
					}
					if ("0".equals(s)) {//不是则取上一天最后一个时间节点
						time2 += timeHourSeven;
					}
				}
				timeList.add(time2);//第二天数据
				flag = true ;


				//******************************
				long time1 = -1L;//计算第一天离线时长

				if(maptime1.size()>0) {
					long time11 = maptime1.firstKey();//记录倒序上一次的时间节点
					if (time11 < end - 6 * offset - timeHourSeven) {
						if ("0".equals(maptime1.firstEntry().getValue())) {
							time1 += timeHourSeven;
						}
					} else {
						if ("0".equals(maptime1.firstEntry().getValue())) {
							time1 += (end - 6 * offset) - maptime1.firstKey();
						}

						if (maptime1.size() > 1) {
							maptime1.remove(maptime1.firstKey());//移除最后一次
							Iterator	it = maptime1.keySet().iterator();
							while (it.hasNext()) {
								//it.next()得到的是key，tm.get(key)得到obj
								String time = it.next().toString();
								long timeout = Long.parseLong(time);
								if (timeout > end - 6 * offset - timeHourSeven) {
									if ("0".equals(maptime1.get(timeout))) {
										time1 += time11 - timeout;
									}
								} else {//判断今天的最早的记录是否是7点之前
									if ("0".equals(maptime1.get(timeout))) {//七点之前的数据则取时到七点
										time1 += time11 - (end - 6 * offset - timeHourSeven);
									}
									flag = false;
									break;
								}
								time11 = timeout;
							}
						}
						if (flag) {//判断最早一条记录是否是当天七点之前
							String s = "";
							if(maptime0.size() > 0){
								s = maptime0.firstEntry().getValue();
							}
							if ("0".equals(s)) {//不是则取上一天最后一个时间节点
								time1 += time11 - (end - 6 * offset - timeHourSeven);
							}
						}
					}
				}else {
					String s = "";
					if(maptime0.size() > 0){
						s = maptime0.firstEntry().getValue();
					}
					if ("0".equals(s)) {//不是则取上一天最后一个时间节点
						time1 += timeHourSeven;
					}
				}
				timeList.add(time1);//第一天数据



				for(long l : timeList){
					if(l > timehourTen){//判断掉线7-24点的掉线时长是否超过10小时
						timeNumber += 1;
					}
				}

				if ("0".equals(LastSta)) {
					offTime = offTime + (end - lastTime);
				} else {
					onTime = onTime + (end - lastTime);
				}
				difDay = (double) (end - sdf.parse(firstDt).getTime()) / 1000.0 / 3600.0 / 24.0;
				Map<String, String> macMap = bv.getValue();
				double avonTime = 0;
				double avoffTime = 0;
				double avoffCount = 0;
				double avoffCount7 = 0;
				double offCount7on7 = -1;
				if (difDay > 0) {
					avoffTime = offTime / difDay / 1000.0 / 3600.0;
					avonTime = onTime / difDay / 1000.0 / 3600.0;
					avoffCount = offCount / difDay;
					avoffCount7 = offCount7 / Math.min(difDay, 7);
					if (difDay >= 14) {
						offCount7on7 = (offCount7 + 1.0) / (offCountLast7 + 1.0);
					}

				}
				// 首次上线时间，运营天数，离线时长，在线时长，日均离线次数
				// 近7日日离线次数，7日日离线次数环比,运营商，网点，点位,机器数,型号
				String resultString = firstDt + "," + difDay + "," + avoffTime + "," + avonTime + "," + avoffCount + ","
						+ avoffCount7 + "," + offCount7on7 + "," + macMap.get(t._1) +"," + timeNumber ;
				return new Tuple2<String, String>(t._1, resultString);
			}
		}).map(new Function<Tuple2<String, String>, String>() {

			@Override
			public String call(Tuple2<String, String> t) throws Exception {

				return t._1 + "," + t._2;
			}
		}).cache();

		JavaPairRDD<String, String> pointOperateStat = getOperateStat(initialData2, sc, "P").cache();
		JavaPairRDD<String, String> netOperateStat = getOperateStat(initialData2, sc, "N").cache();
		JavaPairRDD<String, String> operatorOperateStat = getOperateStat(initialData2, sc, "O").cache();
		JavaPairRDD<String, String> allOperateStat = getOperateStat(initialData2, sc, "A").cache();
		JavaPairRDD<String, String> cityOperateStat = getOperateStat(initialData2, sc, "C").cache();

//		cityOperateStat.foreach(new VoidFunction<Tuple2<String, String>>() {
//			@Override
//			public void call(Tuple2<String, String> stringStringTuple2) throws Exception {
//				System.out.println(stringStringTuple2._1+"\t :" + stringStringTuple2._2);
//			}
//		});

//		pointOperateStat.repartition(1).saveAsTextFile("/yst/seven/data/pointOperateStat/");
//
		String basePath = "hdfs://vm-xaj-bigdata-da-d01:8020/yst/vem/operate/";
//		String basePath = "hdfs://vm-xaj-bigdata-da-d01:8020/yst/seven/operate/";
		String yestodayString = new SimpleDateFormat("yyyy-MM-dd").format(new Date().getTime() - 24 * 3600 * 1000);
//
//
//
//
		Utils.saveHdfs(pointOperateStat, sc, basePath + "P/history/" + yestodayString);
		Utils.saveHdfs(netOperateStat, sc, basePath + "N/history/" + yestodayString);
		Utils.saveHdfs(operatorOperateStat, sc, basePath + "O/history/" + yestodayString);
		Utils.saveHdfs(allOperateStat, sc, basePath + "A/history/" + yestodayString);
		Utils.saveHdfs(cityOperateStat, sc, basePath + "C/history/" + yestodayString);
		if (!endString.substring(0, 10).equals(yestodayString)) {
		} else {
			Utils.saveHdfs(pointOperateStat, sc, basePath + "P/main");
			Utils.saveHdfs(netOperateStat, sc, basePath + "N/main");
			Utils.saveHdfs(operatorOperateStat, sc, basePath + "O/main");
			Utils.saveHdfs(allOperateStat, sc, basePath + "A/main");
			Utils.saveHdfs(cityOperateStat, sc, basePath + "C/main");
		}
//
		 Logger.getRootLogger().setLevel(Level.INFO);
		  Logger.getRootLogger().info("jobEndtime:"+format2.format(new Date().getTime()) +";");
		  Logger.getRootLogger().info("jobResult:Sucessfull!");
		  Logger.getRootLogger().setLevel(Level.OFF);

	  sc.close();


	}


	@SuppressWarnings("serial")
	private static JavaPairRDD<String, String> getOperateStat(JavaRDD<String> rdd, JavaSparkContext sc, String type) {

		final Broadcast<String> bv = sc.broadcast(type);
		JavaPairRDD<String, String> result = rdd.mapToPair(new PairFunction<String, String, String>() {

			@Override
			public Tuple2<String, String> call(String s) throws Exception {
				// 首次上线时间，运营天数，日均离线时长，日均在线时长，日均离线次数，近7日日离线次数，7日日离线次数环比,运营商，网点，点位,机器数，型号
				String type = bv.getValue();
				String[] ss = s.split(",");
				String firstDt = ss[1];
				String difDay = ss[2];
				String avoffTime = ss[3];
				String offCount = ss[5];
				String offCount7 = ss[6];
				String yys = ss[8];
				String wd = ss[9];
				String dw = ss[10];
				String city = ss[11];
				String timeNumber = "".equals(ss[14]) ? "0" : ss[14];
				String key = "1";
				if ("P".equals(type)) {
					key = dw;
				} else if ("N".equals(type)) {
					key = wd;
				} else if ("O".equals(type)) {
					key = yys;
				} else if ("C".equals(type)){
					key = city;
				}
				String macCount = ss[12];
//				System.out.println(firstDt + "," + difDay + "," + avoffTime + "," + offCount + "," + offCount7 + "," + macCount+","+dw +"," + timeNumber);
				return new Tuple2<String, String>(key,
						firstDt + "," + difDay + "," + avoffTime + "," + offCount + "," + offCount7 + "," + macCount+","+dw +"," + timeNumber);
			}
		}).reduceByKey(new Function2<String, String, String>() {

			@Override
			public String call(String s1, String s2) throws Exception {

				return s1 + "#" + s2;
			}
		}).mapToPair(new PairFunction<Tuple2<String, String>, String, String>() {

			@Override
			public Tuple2<String, String> call(Tuple2<String, String> t) throws Exception {
				String[] ss = t._2.split("#");

				String firstDay = "";
				double diffDays = 0;
				int mstMacCount = 0;
				int macCount = 0;
				int offenOffMacCount = 0;
				int offenOff7MacCount = 0;
				int offenOff7MacCount2 = 0;
				int longTimeOffMacCount = 0;
				int longTimeOffMacCount2 = 0;
				int timeNunber = 0;
				Map<String,Integer> map = new HashedMap();
				for (String s : ss) {
					String[] fs = s.split(",");
					if(!map.containsKey(fs[6])){
						map.put(fs[6],1);
					}
					timeNunber += Integer.parseInt(fs[7]);
					double diffDaysTmp = Double.parseDouble(fs[1]);
					if (diffDaysTmp > diffDays) {
						diffDays = diffDaysTmp;
						firstDay = fs[0];
					}
					mstMacCount++;
					try {
						macCount = macCount + Integer.parseInt(fs[5]);
					} catch (Exception e) {
						System.out.println(t._1);
					}

					double offCount7 = Double.parseDouble(fs[4]);
					double offCount = Double.parseDouble(fs[3]);
					double offTime = Double.parseDouble(fs[2]);
					if (offCount7 >= 5) {
						offenOff7MacCount++;
					}
					if (offCount7 >= 20) {
						offenOff7MacCount2++;
					}
					if (offCount >= 5) {
						offenOffMacCount++;
					}
					if (offTime >= 6) {
						longTimeOffMacCount++;
					}
					if (offTime >= 10) {
						longTimeOffMacCount2++;
					}
				}

				// 最早运营时间，运营天数，主机数量，机器数量，日均掉线5次以上的主机数,
				// 近7日日均掉线5次以上的主机数,近7日日均掉线20次以上的主机数,日均掉线时长超过6小时的主机数,
				// 日均掉线超过10次的主机数，点位数，七日离线天数
				String result = firstDay + "," + diffDays + "," + mstMacCount + "," + macCount + "," + offenOffMacCount
						+ "," + offenOff7MacCount + "," + offenOff7MacCount2 + "," + longTimeOffMacCount + ","
						+ longTimeOffMacCount2+","+map.size()+","+timeNunber;
				return new Tuple2<String, String>(t._1, result);
			}
		});
		return result;

	}

}
