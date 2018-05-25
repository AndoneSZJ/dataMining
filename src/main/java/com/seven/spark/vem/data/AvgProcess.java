package com.seven.spark.vem.data;

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

import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * author   seven
 * email    zjshi01@mail.nfsq.com.cn
 * date     2018/5/22 下午2:13
 */
public class AvgProcess {

    //static final String ORGIDS ="1049";

    @SuppressWarnings({"serial"})
    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf().setAppName(AvgProcess.class.getSimpleName());
        if (args == null || args.length == 0) {
            sparkConf.setMaster("local[*]");
        }
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        String pathAvg = "hdfs://vm-xaj-bigdata-da-d01:8020/yst/sta_ods/dw_dim_zfj/*";

        JavaRDD<String> rdd = sc.textFile(pathAvg).filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
                String[] ss = s.split(",");
                if (ss.length < 63) {
                    return false;
                }
                return "1".equals(ss[4]);
            }
        }).cache();
        /**
         * 获取自贩机id和自贩机名称关系
         */
        List<Tuple2<String, String>> vmList = rdd.mapPartitions(new FlatMapFunction<Iterator<String>, Tuple2<String, String>>() {
            @Override
            public Iterator<Tuple2<String, String>> call(Iterator<String> stringIterator) throws Exception {
                ArrayList<Tuple2<String, String>> arrayList = new ArrayList<Tuple2<String, String>>();
                while (stringIterator.hasNext()) {
                    String s = stringIterator.next();
                    String ss[] = s.split(",");
                    String vmId = ss[1];
                    String vmInfo = ss[13];
                    if ("".equals(vmInfo)) {
                        vmInfo = "名称缺失->" + vmId;
                    }
                    arrayList.add(new Tuple2<String, String>(vmId, vmInfo));

                }
                return arrayList.iterator();
            }
        }).collect();

        Map<String, String> vmMap = new HashMap<String, String>();
        for (Tuple2<String, String> a : vmList) {
            vmMap.put(a._1, a._2);
        }

        //广播
        final Broadcast<Map<String, String>> vmMapBv = sc.broadcast(vmMap);
        /**
         * 获取渠道id和渠道名称关系
         */
        List<Tuple2<String, String>> list1 = rdd.mapPartitions(new FlatMapFunction<Iterator<String>, Tuple2<String, String>>() {
            @Override
            public Iterator<Tuple2<String, String>> call(Iterator<String> stringIterator) throws Exception {
                ArrayList<Tuple2<String, String>> arrayList = new ArrayList<Tuple2<String, String>>();
                while (stringIterator.hasNext()) {
                    String s = stringIterator.next();
                    String ss[] = s.split(",");
                    String chanidId = ss[28];
                    String chanidInfo = ss[30];
                    arrayList.add(new Tuple2<String, String>(chanidId, chanidInfo));

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
        List<Tuple2<String, String>> list2 = rdd.mapPartitions(new FlatMapFunction<Iterator<String>, Tuple2<String, String>>() {
            @Override
            public Iterator<Tuple2<String, String>> call(Iterator<String> stringIterator) throws Exception {
                ArrayList<Tuple2<String, String>> arrayList = new ArrayList<Tuple2<String, String>>();
                while (stringIterator.hasNext()) {
                    String s = stringIterator.next();
                    String ss[] = s.split(",");
                    String disId = ss[51];
                    String disInfo = ss[52];
                    arrayList.add(new Tuple2<String, String>(disId, disInfo));

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

        List<Tuple2<String, String>> list = sc.textFile(abnormalPath).mapPartitions(new FlatMapFunction<Iterator<String>, Tuple2<String, String>>() {
            @Override
            public Iterator<Tuple2<String, String>> call(Iterator<String> stringIterator) throws Exception {
                ArrayList<Tuple2<String, String>> arrayList = new ArrayList<Tuple2<String, String>>();
                while (stringIterator.hasNext()) {
                    String s = stringIterator.next();
                    String ss[] = s.split(",");
                    arrayList.add(new Tuple2<String, String>(ss[0], ""));
                }
                return arrayList.iterator();
            }
        }).collect();

        Map<String, String> map = new HashMap<String, String>();
        for (Tuple2<String, String> tuple2 : list) {
            map.put(tuple2._1, tuple2._2);
        }

        final Broadcast<Map<String, String>> bv = sc.broadcast(map);


        String path = "hdfs://vm-xaj-bigdata-da-d01:8020/yst/zfj/find/DW_ZFJ_RLB_ALL";
        JavaRDD<String> initialData = sc.textFile(path);
        //orgid,VMNO+","+CUSTID+","+chanid+","+disid+","+mon+","+saam+","+1+","+payType+","+card
        JavaPairRDD<String, String> resultData = initialData.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
                String[] ss = s.replaceAll("[()]", "").split(",");
                Map<String, String> abnormalMap = bv.getValue();
                String vmno = ss[1];
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
                String str = s.replaceAll("[()]", "");
                String[] ss = str.split(",");
                String vmno = ss[1];
                String chanid = ss[3];
                String disid = ss[4];
                String mon = ss[5].substring(0, 7);
                Double saam = Double.parseDouble(ss[6]);
                return new Tuple2<String, String>(vmno, str);
            }
        }).reduceByKey(new Function2<String, String, String>() {
            @Override
            public String call(String s, String s2) throws Exception {
                return s + "@" + s2;
            }
        }).mapToPair(new PairFunction<Tuple2<String, String>, String, String>() {
            @Override
            public Tuple2<String, String> call(Tuple2<String, String> stringStringTuple2) throws Exception {
                String[] ss = stringStringTuple2._2.split("@");
                String[] strings = ss[0].split(",");
                String vmno = strings[1];
                String chanid = strings[3];
                String disid = strings[4];
                Double saam = 0.0;
                for (String s : ss) {
                    String[] str = s.split(",");
                    saam += Double.parseDouble(str[6]);
                }
                saam = saam / 12;
                return new Tuple2<String, String>(vmno + "," + chanid, saam + "," + disid + "," + ss.length);
            }
        }).filter(new Function<Tuple2<String, String>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, String> t) throws Exception {
                String[] ss = t._2.split(",");
                return Double.parseDouble(ss[2]) > 7;
            }
        }).mapToPair(new PairFunction<Tuple2<String, String>, String, String>() {
            @Override
            public Tuple2<String, String> call(Tuple2<String, String> t) throws Exception {
                String[] ss = t._1.split(",");
                String chanid = ss[1];
                String vmId = ss[0];
                String[] ss2 = t._2.split(",");
                return new Tuple2<String, String>(chanid, ss2[0] + "," + vmId + "," + ss2[1]);
            }
        }).reduceByKey(new Function2<String, String, String>() {
            @Override
            public String call(String s1, String s2) throws Exception {

                return s1 + "@" + s2;
            }
        }).filter(new Function<Tuple2<String, String>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, String> t) throws Exception {
                String[] ss = t._2.split("@");
                //String str = "ebde393e0ac14518a1059be90b30cfb7,1503,1501,2502,1004,1401,1201,2404,1102,2301,1301,2101,1502,1002,2405,1601,1101,2305";
                String[] aa = ss[0].split(",");//浙北大区
                return ss.length > 10 && "691".equals(aa[2]);// && str.contains(t._1);
            }
        }).mapToPair(new PairFunction<Tuple2<String, String>, String, String>() {
            @Override
            public Tuple2<String, String> call(Tuple2<String, String> t) throws Exception {
                String[] ss = t._2.split("@");
                List<Double> l = new ArrayList<Double>();
                TreeMap<Double, String> vmMap = new TreeMap<Double, String>();
                for (String s : ss) {
                    String[] str = s.split(",");
                    String vmId = str[1];
                    String disId = str[2];
                    Double money = Double.parseDouble(str[0]);
                    l.add(money);
                    vmMap.put(money, vmId);
                }
                Collections.sort(l);
                int size = l.size();
                double lower = l.get(1);
                double upper = l.get(size - 2);
                double middle = l.get(size / 2);
                double q3 = l.get(size * 4 / 5);
                double q1 = l.get(size * 1 / 5);

                String str1 = "";
                String str2 = "";
                String str3 = "";
                String str4 = "";
                vmMap.descendingKeySet();//倒叙

//                Map<String,Integer> m1 = new HashMap<>();
//                Map<String,Integer> m2 = new HashMap<>();
//                Map<String,Integer> m3 = new HashMap<>();
//                Map<String,Integer> m4 = new HashMap<>();
//                Map<String,Integer> mm = new HashMap<>();


                Map<String, String> vmMap1 = vmMapBv.getValue();

                Iterator iter = vmMap.entrySet().iterator();
                while (iter.hasNext()) {
                    Map.Entry entry = (Map.Entry) iter.next();
                    Double key = Double.parseDouble(entry.getKey().toString());
                    String val = entry.getValue().toString();
                    String vmName = vmMap1.get(val).replaceAll("[,&]", "");
                    vmName = vmName.replaceAll("\t|\n", "");//获取自贩机地址
                    if (key < upper && key > q3) {
//                        mm.put(val,1);
                        str1 += vmName + ",";
//                        if(m1.containsKey(val)){
//                            m1.put(val,m1.get(val)+1);
//                        }else {
//                            m1.put(val,1);
//                        }
                    } else if (key > middle) {
//                        mm.put(val,1);
                        str2 += vmName + ",";
//                        if(m2.containsKey(val)){
//                            m2.put(val,m2.get(val)+1);
//                        }else {
//                            m2.put(val,1);
//                        }
                    } else if (key > q1) {
//                        mm.put(val,1);
                        str3 += vmName + ",";
//                        if(m3.containsKey(val)){
//                            m3.put(val,m3.get(val)+1);
//                        }else {
//                            m3.put(val,1);
//                        }
                    } else if (key > lower) {
//                        mm.put(val,1);
                        str4 += vmName + ",";
//                        if(m4.containsKey(val)){
//                            m4.put(val,m4.get(val)+1);
//                        }else {
//                            m4.put(val,1);
//                        }
                    }
                }

//                String ssss = "&";
//                Map<String,String> mmm = disBv.getValue();
//                iter = mm.entrySet().iterator();
//                while (iter.hasNext()) {
//                    Map.Entry entry = (Map.Entry) iter.next();
//                    String key = entry.getKey().toString();
//                    String disName = mmm.get(key);
//                    ssss += disName+":";
//                    if(m1.containsKey(key)){
//                        ssss += m1.get(key)+",";
//                    }else{
//                        ssss += "0,";
//                    }
//                    if(m2.containsKey(key)){
//                        ssss += m2.get(key)+",";
//                    }else{
//                        ssss += "0,";
//                    }
//                    if(m3.containsKey(key)){
//                        ssss += m3.get(key)+",";
//                    }else{
//                        ssss += "0,";
//                    }
//                    if(m4.containsKey(key)){
//                        ssss += m4.get(key);
//                    }else{
//                        ssss += "0";
//                    }
//                    ssss += "&";
//                }
//
//                if(!"".equals(ssss)){
//                    ssss = ssss.substring(0,ssss.length()-1);
//                }

                if (!"".equals(str1)) {
                    str1 = str1.substring(0, str1.length() - 1);
                }
                if (!"".equals(str2)) {
                    str2 = str2.substring(0, str2.length() - 1);
                }
                if (!"".equals(str3)) {
                    str3 = str3.substring(0, str3.length() - 1);
                }
                if (!"".equals(str4)) {
                    str4 = str4.substring(0, str4.length() - 1);
                }

                String sss = "&" + str1 + "&" + str2 + "&" + str3 + "&" + str4;


                Map<String, String> map1 = chanMapBv.getValue();
                String chanName = map1.get(t._1);
                return new Tuple2<String, String>(chanName, String.format("%.2f", upper) + "," + String.format("%.2f", q3) + "," + String.format("%.2f", middle) + "," + String.format("%.2f", q1) + "," + String.format("%.2f", lower) + sss);
            }
        }).cache();
        String savePath = "hdfs://vm-xaj-bigdata-da-d01:8020/yst/zfj/find/ZFJ_AVG_DATA/";
        System.out.println(resultData.take(3));
        Utils.saveHdfs(resultData, sc, savePath, 1);
    }

}
