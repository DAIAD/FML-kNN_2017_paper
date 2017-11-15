package org.apache.flink.phases;

import java.io.File;
import java.io.InputStreamReader;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.LineNumberReader;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.tools.ExecConf;
import org.apache.flink.tools.Functions;
import org.apache.flink.transformations.MapPhase1;
import org.apache.flink.transformations.MapPhase1Sampling;
import org.apache.flink.transformations.ReducePhase1;
import org.apache.flink.values.Phase1_2Value;
import org.apache.flink.api.common.ExecutionConfig;

public class Phase1 {

	public static void main(String[] args) throws Exception {

		ExecConf conf = new ExecConf();
		Functions.createAlicanteDatasetsRandom("/home/local/chgeorgakidis/datasets/synthetic/water_consumption.csv", conf);

		File S_local = new File("/home/local/chgeorgakidis/F-kNN/Flink_kNN_3Phases/S_local/");
		FileUtils.cleanDirectory(S_local);

		// Count the lines of the datasets
                Path pt = new Path(conf.getPath() + "datasets/R");
                FileSystem fs = pt.getFileSystem();
                BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));

                LineNumberReader  lnr = new LineNumberReader(br);
                lnr.skip(Long.MAX_VALUE);
                conf.setNr(lnr.getLineNumber() + 1);
                lnr.close();

                pt = new Path(conf.getPath() + "datasets/S");
                fs = pt.getFileSystem();
                br = new BufferedReader(new InputStreamReader(fs.open(pt)));

                lnr = new LineNumberReader(br);
                lnr.skip(Long.MAX_VALUE);
                conf.setNs(lnr.getLineNumber() + 1);
                lnr.close();

                // Read arguments and configure
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
                pt = new Path(conf.getPath() + "RandomShiftVectors");
                fs = pt.getFileSystem();
                br = new BufferedReader(new InputStreamReader(fs.open(pt)));
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

		pt = new Path(conf.getPath() + "Results_Phase1");
                fs = pt.getFileSystem();
                fs.delete(pt, true);

		pt = new Path(conf.getPath() + "Rrange");
                fs = pt.getFileSystem();
                fs.delete(pt, true);

		pt = new Path(conf.getPath() + "Srange");
                fs = pt.getFileSystem();
                fs.delete(pt, true);

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
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		//ExecutionConfig executionConfig = env.getConfig();
        	//executionConfig.enableObjectReuse();

		/******** PHASE 1 **********/
	   	/////////// MAP
	        DataSet<String> inputR = env.readTextFile(conf.getPath() + "datasets/R");
	        DataSet<String> inputS = env.readTextFile(conf.getPath() + "datasets/S");

		/////////// MAP
		DataSet<Phase1_2Value> resultR = inputR.rebalance().flatMap(new MapPhase1(0, conf));
		DataSet<Phase1_2Value> resultS = inputS.rebalance().flatMap(new MapPhase1(1, conf));
		DataSet<Phase1_2Value> resultRS = resultR.union(resultS);

		resultRS.writeAsText(conf.getPath() + "Results_Phase1");

		/////////// SAMPLING MAP
		DataSet<Phase1_2Value> resultsRSsampled = resultRS.flatMap(new MapPhase1Sampling(conf));

		/////////// REDUCE
		DataSet<String> Rrange = resultsRSsampled.groupBy("fourth").reduceGroup(new ReducePhase1(0, conf));
		DataSet<String> Srange = resultsRSsampled.groupBy("fourth").reduceGroup(new ReducePhase1(1, conf));

		Rrange.writeAsText(conf.getPath() + "Rrange").setParallelism(1);
		Srange.writeAsText(conf.getPath() + "Srange").setParallelism(1);

		// Execute program
	 	long startExecuteTime = System.currentTimeMillis();
		env.execute("Flink zKNN Phase 1");
		long totalElapsedExecuteTime = System.currentTimeMillis() - startExecuteTime;

		int ExecuteMillis = (int) totalElapsedExecuteTime % 1000;
		int ExecuteSeconds = (int) (totalElapsedExecuteTime / 1000) % 60 ;
	 	int ExecuteMinutes = (int) ((totalElapsedExecuteTime / (1000*60)) % 60);
	 	int ExecuteHours   = (int) ((totalElapsedExecuteTime / (1000*60*60)) % 24);
	 	System.out.println("Phase 1: Thread " + Thread.currentThread().getId() + " total time: " +  ExecuteHours + "h " + ExecuteMinutes + "m " + ExecuteSeconds + "sec " + ExecuteMillis + "mil");

	}

}
