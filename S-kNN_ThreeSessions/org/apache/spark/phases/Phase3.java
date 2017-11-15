package org.apache.spark.phases;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.tools.ExecConf;
import org.apache.spark.tools.Functions;
import org.apache.spark.transformations.MapPhase3;
import org.apache.spark.transformations.ReducePhase3;
import org.apache.spark.values.Phase3Value;

public class Phase3 {

	public static void main(String[] args) throws Exception {

		ExecConf conf = new ExecConf();

		double totalPercentage = 0.0;
                Path fpt = new Path(conf.getPath() + "ClassificationResults");
                FileSystem fs = fpt.getFileSystem(new Configuration());
		fs.delete(fpt, true);

		// Read arguments and configure
		if (args.length > 0) {
			try {
				conf.setKnn(Integer.parseInt(args[0]));
				conf.setNoOfClasses(Integer.parseInt(args[1]));
			} catch (Exception e) {
				System.out.println("Error! Please check arguments.");
				System.exit(0);
			}
		}

		SparkConf config = new SparkConf().setAppName("S-kNN_Ph3").setMaster("yarn-client");
		config.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        	Class[] classes = new Class[1];
        	classes[0] = Phase3Value.class;
        	config.registerKryoClasses(classes);
		JavaSparkContext sc = new JavaSparkContext(config);

		/******** PHASE 3 ***********/
		long startExecuteTime = System.currentTimeMillis();

		JavaRDD<String> inputPh3 = sc.textFile(conf.getPath() + "Results_Phase2", 16);

		/////////// MAP
                JavaPairRDD<String, Phase3Value> map3 = inputPh3.mapToPair(new MapPhase3());

                /////////// REDUCE
                JavaRDD<String> resultPhase3 = map3.groupByKey().mapPartitions(new ReducePhase3(conf));

		resultPhase3.saveAsTextFile(conf.getPath() + "ClassificationResults");

	 	sc.close();

		long totalElapsedExecuteTime = System.currentTimeMillis() - startExecuteTime;

                // Count execution time
                int ExecuteMillis = (int) totalElapsedExecuteTime % 1000;
                int ExecuteSeconds = (int) (totalElapsedExecuteTime / 1000) % 60;
                int ExecuteMinutes = (int) ((totalElapsedExecuteTime / (1000 * 60)) % 60);
                int ExecuteHours = (int) ((totalElapsedExecuteTime / (1000 * 60 * 60)) % 24);
                System.out.println("Thread " + Thread.currentThread().getId()
                                                        + " total time: " + ExecuteHours + "h " + ExecuteMinutes
                                                        + "m " + ExecuteSeconds + "sec " + ExecuteMillis + "mil");

                // Calculate accuracy
                double acc = Functions.calculateAccuracy(conf);
                totalPercentage += acc;
                System.out.println("Final accuracy: " + totalPercentage);

	}
}

