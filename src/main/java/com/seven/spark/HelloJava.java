package com.seven.spark;


import com.seven.spark.hbase.rowkey.RowKeyGenerator;
import com.seven.spark.hbase.rowkey.generator.HashRowKeyGenerator;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class HelloJava {
    public static void main(String[] args) {

//        TreeMap<Integer,String> maptime = new TreeMap<Integer, String>();
//        maptime.put(11111,"ssss");
////        maptime.put(null,"");
//        System.out.println(maptime.firstKey());
//        System.out.println("0".equals(null));
//
//        System.out.println("hello java");

//        Map<String,Double> orderMap = new HashMap<String, Double>();
//        orderMap.put("1",0.1);
//        orderMap.put("12",0.1);
//        orderMap.put("13",0.1);
//        orderMap.put("14",0.1);
//        orderMap.put("1",0.2);
//        orderMap.put("12",orderMap.get("12")+0.3);
//
//        Iterator iter = orderMap.entrySet().iterator();
//        while (iter.hasNext()) {
//            Map.Entry entry = (Map.Entry) iter.next();
//            Object key = entry.getKey();
//            Object val = entry.getValue();
//            System.out.println(key+":"+val);
//        }


//
//        SimpleDateFormat format2 = new SimpleDateFormat("yyyy-MM-dd");
//        Date starttime = new Date();
//        System.out.println(format2.format(starttime.getTime()));

        SimpleDateFormat sdf2 = new SimpleDateFormat("D");


        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

        System.out.println("2018-05-01".substring(5, 9));
        try {
            System.out.println(sdf2.format(sdf.parse("2018-05-01")));
        } catch (ParseException e) {
            e.printStackTrace();
        }

        System.out.println("werty".contains("wer"));

        String[] s = "3705,\"17栋D单元电梯出口旁 （龙岗可园）,940,7,1,500,500,5,5,3,bqlai,2018-03-20 09:24:18.0,bqlai,2018-03-20 09:24:18.0,,,0,DS_ZFJVEM_PRD,job_hsta_vem_point_community,point_community,20180613,2018-06-14 09:54:58.0".split(",");


        System.out.println(s[16]);
//        System.out.println("2018-01-31".substring(0,7));
//        System.out.println("2018-01-31".length());
////        long[] ls = new long[24];
////
////        ls[1] += 1;
////        for(long l : ls){
////            System.out.println(l);
////        }
//
//
//        RowKeyGenerator rowKeyGenerator = new HashRowKeyGenerator();
//        byte[] bytes = rowKeyGenerator.generate("");
//        System.out.println(bytes.toString());
//        System.out.println(new String(bytes));

        try {
            System.out.println(getLastMouth("2018-01"));
            System.out.println(getWeek("2018-01"));
        } catch (ParseException e) {
            e.printStackTrace();
        }


    }

    private static String getLastMouth(String str) throws ParseException {
        SimpleDateFormat sdf = new java.text.SimpleDateFormat("yyyy-MM");
        Date sourceDate = sdf.parse(str);
        Calendar cal = Calendar.getInstance();
        cal.setTime(sourceDate);
        cal.add(Calendar.MONTH, -1);
        return sdf.format(cal.getTime());
    }

    private static String getWeek(String str) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-w");
        Date sourceDate = sdf.parse(str);
        Calendar cal = Calendar.getInstance();
        cal.setTime(sourceDate);
        cal.add(Calendar.DAY_OF_WEEK, -7);
        return sdf.format(cal.getTime());

    }


}
