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

        try {
            System.out.println(sdf2.format(sdf.parse("2018-01-31")));
        } catch (ParseException e) {
            e.printStackTrace();
        }


        System.out.println("2018-01-31".substring(0,7));
        System.out.println("2018-01-31".length());
//        long[] ls = new long[24];
//
//        ls[1] += 1;
//        for(long l : ls){
//            System.out.println(l);
//        }


        RowKeyGenerator rowKeyGenerator = new HashRowKeyGenerator();
        byte[] bytes = rowKeyGenerator.generate("");
        System.out.println(bytes.toString());
        System.out.println(new String(bytes));
    }
}
