package com.seven.spark.vem.data;

import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.fpm.AssociationRules;
import org.apache.spark.mllib.fpm.FPGrowth;
import org.apache.spark.mllib.fpm.FPGrowthModel;

public class FP {

	public static void main(String[] args) {

		SparkConf sparkConf = new SparkConf().setAppName("tmp");
		if (args == null || args.length == 0) {
			sparkConf.setMaster("local[*]");
		}
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		Logger.getRootLogger().setLevel(Level.OFF);
		JavaRDD<String> data = sc.textFile("/yst/seven/data/user/day/*");

		JavaRDD<List<String>> transactions = data.map(line -> Arrays.asList(line.split(",")));

		FPGrowth fpg = new FPGrowth().setMinSupport(0.02).setNumPartitions(10);
		FPGrowthModel<String> model = fpg.run(transactions);

		for (FPGrowth.FreqItemset<String> itemset : model.freqItemsets().toJavaRDD().collect()) {
			System.out.println("[" + itemset.javaItems() + "], " + itemset.freq());
		}

		double minConfidence = 0.0;
		for (AssociationRules.Rule<String> rule : model.generateAssociationRules(minConfidence).toJavaRDD().collect()) {
			System.out.println(rule.javaAntecedent() + " => " + rule.javaConsequent() + ", " + rule.confidence());
		}
	}
}
