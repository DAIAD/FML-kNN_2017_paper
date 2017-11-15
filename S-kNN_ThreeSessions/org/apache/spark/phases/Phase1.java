package org.apache.spark.phases;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.LineNumberReader;

import org.apache.spark.storage.StorageLevel;
import org.apache.commons.io.FileUtils;
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
import org.apache.spark.transformations.MapPhase1;
import org.apache.spark.transformations.MapPhase1Sampling;
import org.apache.spark.transformations.MapPhase2;
import org.apache.spark.transformations.MapPhase3;
import org.apache.spark.transformations.ReducePhase1;
import org.apache.spark.transformations.ReducePhase2;
import org.apache.spark.transformations.ReducePhase3;
import org.apache.spark.values.Phase1_2Value;
import org.apache.spark.values.Phase3Value;
import org.apache.spark.storage.StorageLevel;

import scala.Tuple4;

public class Phase1 {

	public static void main(String[] args) throws Exception {

		ExecConf conf = new ExecConf();

		if (args.length > 0) {
                        try {
                                conf.setKnn(Integer.parseInt(args[0]));
                                conf.setShift(Integer.parseInt(args[1]));
                                conf.setDimension(Integer.parseInt(args[2]));
                                conf.setEpsilon(Double.parseDouble(args[3]));
                                conf.setNumOfPartition(Integer.parseInt(args[4]));
                                conf.setHilbertOrZ(Boolean.parseBoolean(args[5]));
                        } catch(Exception e) {
                                System.out.println("Error! Please check arguments.");
                                System.exit(0);
                        }
                }

	int[][] shiftvectors = new int[conf.getShift()][conf.getDimension()];
        Path pt = new Path(conf.getPath() + "RandomShiftVectors");
        FileSystem fs = pt.getFileSystem(new Configuration());
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
        int j = 0;
        while(true) {
                String l = br.readLine();
                if ((l==null) || (j>conf.getShift()-1)) break;
                        String[] p = l.split(" ");
                for (int i = 0 ; i < conf.getDimension(); i++)
                        shiftvectors[j][i] = Integer.valueOf(p[i]);
                j++;
        }
        br.close();
        conf.setShiftvectors(shiftvectors);

		Path fpt = new Path(conf.getPath() + "Rrange");
                fs = fpt.getFileSystem(new Configuration());
                fs.delete(fpt, true);
		fpt = new Path(conf.getPath() + "Srange");
                fs = fpt.getFileSystem(new Configuration());
                fs.delete(fpt, true);
		fpt = new Path(conf.getPath() + "Results_Phase1");
                fs = fpt.getFileSystem(new Configuration());
                fs.delete(fpt, true);


		Functions.createAlicanteDatasetsRandom("/home/local/chgeorgakidis/datasets/synthetic/water_consumption.csv", conf);

		File S_local = new File("/home/local/chgeorgakidis/S-kNN/Spark_zkNN_3Ph/S_local/");
                FileUtils.cleanDirectory(S_local);

		pt = new Path(conf.getPath() + "datasets/R");
		fs = pt.getFileSystem(new Configuration());
		br = new BufferedReader(new InputStreamReader(fs.open(pt)));

		LineNumberReader lnr = new LineNumberReader(br);
		lnr.skip(Long.MAX_VALUE);
		conf.setNr(lnr.getLineNumber() + 1);
		lnr.close();

		pt = new Path(conf.getPath() + "datasets/S");
		fs = pt.getFileSystem(new Configuration());
		br = new BufferedReader(new InputStreamReader(fs.open(pt)));

		lnr = new LineNumberReader(br);
		lnr.skip(Long.MAX_VALUE);
		conf.setNs(lnr.getLineNumber() + 1);
		lnr.close();

		//*************************** Sampling setting ****************************//
		conf.setSampleRateOfR(1 / (conf.getEpsilon() * conf.getEpsilon() * conf.getNr()));
		conf.setSampleRateOfS(1 / (conf.getEpsilon() * conf.getEpsilon() * conf.getNs()));

		if (conf.getSampleRateOfR() > 1) conf.setSampleRateOfR(1);
		if (conf.getSampleRateOfS() > 1) conf.setSampleRateOfS(1);

		if (conf.getSampleRateOfR() * conf.getNr() < 1) {
			System.out.printf("Increase sampling rate of R :  " + conf.getSampleRateOfR());
			System.exit(-1);
		}

		if (conf.getSampleRateOfS() * conf.getNs() < 1) {
			System.out.printf("Increase sampling rate of R :  " + conf.getSampleRateOfS());
			System.exit(-1);
		}

		// Set up the execution environment
		SparkConf config = new SparkConf().setAppName("S-kNN_Ph1").setMaster("yarn-client");
		config.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		Class[] classes = new Class[1];
        	classes[0] = Phase1_2Value.class;
        	config.registerKryoClasses(classes);
		JavaSparkContext sc = new JavaSparkContext(config);

		//////////////*********** SPARK EXECUTE **************///////////////////
		long startExecuteTime = System.currentTimeMillis();
		/******** PHASE 1 ***********/
		JavaRDD<String> inputR = sc.textFile(conf.getPath() + "datasets/R");
		JavaRDD<String> inputS = sc.textFile(conf.getPath() + "datasets/S");

		JavaPairRDD<Integer, Phase1_2Value> resultR = inputR.flatMapToPair(new MapPhase1(0, conf));
		JavaPairRDD<Integer, Phase1_2Value> resultS = inputS.flatMapToPair(new MapPhase1(1, conf));
		JavaPairRDD<Integer, Phase1_2Value> result = resultR.union(resultS);

		JavaPairRDD<Integer, Phase1_2Value> resultSampled = result.flatMapToPair(new MapPhase1Sampling(conf));

		JavaRDD<String> empty = resultSampled.groupByKey(conf.getShift()).mapPartitions(new ReducePhase1(conf));

		empty.count();
		result.saveAsTextFile(conf.getPath() + "Results_Phase1");
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
	}
}
