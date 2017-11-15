package org.apache.spark.mainApp;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.LineNumberReader;

import java.io.Serializable;
import java.util.Comparator;
import org.apache.spark.Partitioner;
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
import org.apache.spark.broadcast.Broadcast;
import java.util.List;
import scala.Tuple2;
import scala.Tuple4;

public class MainApp {

	public static void main(String[] args) throws Exception {

		ExecConf conf = new ExecConf();

		if (args.length > 0) {
			try {
				conf.setKnn(Integer.parseInt(args[0]));
				conf.setShift(Integer.parseInt(args[1]));
				conf.setDimension(Integer.parseInt(args[2]));
				conf.setNoOfClasses(Integer.parseInt(args[3]));
				conf.setEpsilon(Double.parseDouble(args[4]));
				conf.setNumOfPartition(Integer.parseInt(args[5]));
				conf.setHilbertOrZ(Boolean.parseBoolean(args[6]));
			} catch (Exception e) {
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

		double totalPercentage = 0.0;
		Path fpt = new Path(conf.getPath() + "ClassificationResults");
		fs = fpt.getFileSystem(new Configuration());
		fs.delete(fpt, true);
		fpt = new Path(conf.getPath() + "Rrange");
                fs = fpt.getFileSystem(new Configuration());
                fs.delete(fpt, true);
		fpt = new Path(conf.getPath() + "Srange");
                fs = fpt.getFileSystem(new Configuration());
                fs.delete(fpt, true);

		Functions.createAlicanteDatasetsRandom("/home/local/chgeorgakidis/datasets/synthetic/water_consumption.csv", conf);

		File S_local = new File("/home/local/chgeorgakidis/S-kNN/Spark_zkNN_1Ph/S_local");
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
		SparkConf config = new SparkConf().setAppName("S-kNN").setMaster("yarn-client");
		config.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        	Class[] classes = new Class[4];
        	classes[0] = Phase1_2Value.class;
        	classes[1] = MyPartitioner.class;
        	classes[2] = MyComparator.class;
		classes[3] = Phase3Value.class;
        	config.registerKryoClasses(classes);
		JavaSparkContext sc = new JavaSparkContext(config);

		//////////////*********** SPARK EXECUTE **************///////////////////
		/******** PHASE 1 ***********/
		long startExecuteTime = System.currentTimeMillis();
		JavaRDD<String> inputR = sc.textFile(conf.getPath() + "datasets/R", 8);
		JavaRDD<String> inputS = sc.textFile(conf.getPath() + "datasets/S", 8);

		JavaPairRDD<Integer, Phase1_2Value> resultR = inputR.flatMapToPair(new MapPhase1(0, conf));
		JavaPairRDD<Integer, Phase1_2Value> resultS = inputS.flatMapToPair(new MapPhase1(1, conf));
		JavaPairRDD<Integer, Phase1_2Value> result = resultR.union(resultS);

		JavaPairRDD<Integer, Phase1_2Value> resultSampled = result.flatMapToPair(new MapPhase1Sampling(conf));

		JavaRDD<String> ranges = resultSampled.groupByKey(conf.getShift()).mapPartitions(new ReducePhase1(conf));

            	Broadcast<List<String>> broadcastVar = sc.broadcast(ranges.collect());

            	/******** PHASE 2 ***********/
		/////////// MAP
		JavaPairRDD<Tuple2<Integer, String>, Phase1_2Value> map2 = result.flatMapToPair(new MapPhase2(conf, broadcastVar));

	 	/////////// REDUCE
		JavaPairRDD<String, Phase3Value> resultPhase2 = map2.repartitionAndSortWithinPartitions(new MyPartitioner(conf), new MyComparator()).mapPartitionsToPair(new ReducePhase2(conf));

		/******** PHASE 3 ***********/
	 	/////////// REDUCE
		JavaRDD<String> resultPhase3 = resultPhase2.groupByKey().mapPartitions(new ReducePhase3(conf));

		// Write final results to DFS
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

    private static class MyPartitioner extends Partitioner {

        ExecConf conf;
        public MyPartitioner(ExecConf conf) {
            this.conf = conf;
        }

        @Override
        public int numPartitions() {
            return conf.getShift()*conf.getNumOfPartition();
        }

        @Override
        public int getPartition(Object o) {
            Tuple2<Integer, String> key = (Tuple2<Integer, String>) o;
            return key._1()%numPartitions();
        }
    }

    private static class MyComparator implements Comparator, Serializable {

        @Override
        public int compare(Object o1, Object o2) {
            Tuple2<Integer, String> s1 = (Tuple2<Integer, String>) o1;
            Tuple2<Integer, String> s2 = (Tuple2<Integer, String>) o2;

            int cmp = s1._2().compareTo(s2._2());
            if( cmp != 0 ) return cmp;
            cmp = s1._1().toString().compareTo(s2._1().toString());

            return cmp;
        }
    }

}
