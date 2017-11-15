package org.apache.flink.mainApp;

import org.apache.flink.api.common.operators.Order;
import org.apache.commons.io.FileUtils;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.tools.CurveRecord;
import org.apache.flink.tools.ExecConf;
import org.apache.flink.tools.Functions;
import org.apache.flink.transformations.*;
import org.apache.flink.transformations.MapPhase1Sampling;
import org.apache.flink.api.common.functions.Partitioner;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.io.LineNumberReader;

public class FML_kNN {

	public double[] execute(ExecConf conf) throws Exception {

		double[] results = new double[2];
		// Generate random shift vectors
		Functions.genRandomShiftVectors(conf.getHdfsPath() + "RandomShiftVectors", conf.getDimension(), conf.getShift());
		int[][] shiftVectors = new int[conf.getShift()][conf.getDimension()];
		Path pt = new Path(conf.getHdfsPath() + "RandomShiftVectors");
		FileSystem fs = pt.getFileSystem();
		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
		int j = 0;
		while (true) {
			String l = br.readLine();
			if ((l == null) || (j > conf.getShift() - 1))
				break;
			String[] p = l.split(" ");
			for (int i = 0; i < conf.getDimension(); i++)
				shiftVectors[j][i] = Integer.valueOf(p[i]);
			j++;
		}
		br.close();
		conf.setShiftVectors(shiftVectors);

		if (!conf.isCrossValidation()) conf.setCrossValidationFolds(1);
		double totalPercentage = 0.0;
		double totalFMeasure = 0.0;
        	double totalRMSE = 0.0;
	        double totalRSquared = 0.0;

		for (int i = 0; i < conf.getCrossValidationFolds(); i++) {

			File S_local = new File(conf.getLocalPath() + "S_local/");
			FileUtils.cleanDirectory(S_local);

			Path fpt;
            		if (conf.getClassifyOrRegress() == 1) {
                		fpt = new Path(conf.getHdfsPath() + "ClassificationResults");
	                	fs = fpt.getFileSystem();
        	        	fs.delete(fpt, true);
            		}

            		else if (conf.getClassifyOrRegress() == 2) {
                		fpt = new Path(conf.getHdfsPath() + "RegressionResults");
		                fs = fpt.getFileSystem();
	        	        fs.delete(fpt, true);
			}

			if (conf.isCrossValidation()) {
	                	if (conf.getClassifyOrRegress() == 1) {
	                   		Functions.createDatasetsCrossValid(conf.getSourcesPath() + "new/AlicanteDatasetNoOutliers.csv", conf, conf.getCrossValidationFolds(), i);
	                	}
		                else if (conf.getClassifyOrRegress() == 2) {
	        	            	Functions.createDatasetsCrossValid(conf.getHdfsPath() + "datasets/UnifiedNonZeroDataset", conf, conf.getCrossValidationFolds(), i);
	                	}
	                	else {
	                    		System.out.println("Wrong algorithm selection input! Exiting...");
	                    		System.exit(0);
	                	}
			}
			else {
	                	if (conf.getClassifyOrRegress() == 1) {
					//Functions.createDatasetsClassification(conf.getSourcesPath() + "synthetic/water_consumption.csv", conf);
			        	Functions.createDatasetsSpecific(conf.getSourcesPath() + "new/AlicanteDatasetNoOutliers.csv", conf);
					//Functions.createDatasetsClassification(conf.getSourcesPath() + "new/AlicanteDatasetNoOutliers.csv", conf);
	                	}
	                	else if (conf.getClassifyOrRegress() == 2) {
	                    		Functions.createDatasetsRegression(conf);
	                	}
	                	else {
	                    		System.out.println("Wrong algorithm selection input! Exiting...");
	                    		System.exit(0);
	                	}
			}

			// Count the lines of the datasets
	            	LineNumberReader lnr;
	            	if (conf.getClassifyOrRegress() == 1) {
	                	pt = new Path(conf.getHdfsPath() + "datasets/RClassification");
	                	fs = pt.getFileSystem();
		                br = new BufferedReader(new InputStreamReader(fs.open(pt)));

	        	        lnr = new LineNumberReader(br);
	                	lnr.skip(Long.MAX_VALUE);
		                conf.setNr(lnr.getLineNumber() + 1);
	        	        lnr.close();

	                	pt = new Path(conf.getHdfsPath() + "datasets/SClassification");
		                fs = pt.getFileSystem();
	        	        br = new BufferedReader(new InputStreamReader(fs.open(pt)));

	                	lnr = new LineNumberReader(br);
		                lnr.skip(Long.MAX_VALUE);
	        	        conf.setNs(lnr.getLineNumber() + 1);
	                	lnr.close();
			}

	            	else if (conf.getClassifyOrRegress() == 2) {
	                	pt = new Path(conf.getHdfsPath() + "datasets/RRegression");
		                fs = pt.getFileSystem();
	        	        br = new BufferedReader(new InputStreamReader(fs.open(pt)));

	                	lnr = new LineNumberReader(br);
		                lnr.skip(Long.MAX_VALUE);
	        	        conf.setNr(lnr.getLineNumber() + 1);
	                	lnr.close();

		                pt = new Path(conf.getHdfsPath() + "datasets/SRegression");
	        	        fs = pt.getFileSystem();
	                	br = new BufferedReader(new InputStreamReader(fs.open(pt)));

		                lnr = new LineNumberReader(br);
	        	        lnr.skip(Long.MAX_VALUE);
	                	conf.setNs(lnr.getLineNumber() + 1);
		                lnr.close();
			}

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

			//////////////*********** FLINK EXECUTE **************///////////////////
			/******** PHASE 1 ***********/
	            	DataSet<String> inputR = null;
	            	DataSet<String> inputS = null;

		        if (conf.getClassifyOrRegress() == 1) {
		                inputR = env.readTextFile(conf.getHdfsPath() + "datasets/RClassification");
	        	        inputS = env.readTextFile(conf.getHdfsPath() + "datasets/SClassification");
	            	}
	            	else if (conf.getClassifyOrRegress() == 2) {
	                	inputR = env.readTextFile(conf.getHdfsPath() + "datasets/RRegression");
		                inputS = env.readTextFile(conf.getHdfsPath() + "datasets/SRegression");
	        	}

		        //########## MAP
	            	DataSet<CurveRecord> resultR = inputR.flatMap(new MapPhase1(0, conf));
	            	DataSet<CurveRecord> resultS = inputS.flatMap(new MapPhase1(1, conf));
	            	DataSet<CurveRecord> resultRS = resultR.union(resultS);

			/////////// SAMPLING MAP
                	DataSet<CurveRecord> resultsRSsampled = resultRS.flatMap(new MapPhase1Sampling(conf));

			//########## REDUCE
			DataSet<String> ranges = resultsRSsampled.groupBy("fourth").reduceGroup(new ReducePhase1(conf));

			/******** PHASE 2 ***********/
			//########## MAP
			DataSet<CurveRecord> result = resultRS.rebalance().flatMap(new MapPhase2(conf)).withBroadcastSet(ranges, "ranges");
			DataSet<CurveRecord> partitionedData = result.partitionCustom(new MyPartitioner(), "fourth").sortPartition("first", Order.ASCENDING);

		        //########## REDUCE
			DataSet<Tuple4<String, String, String, String>> reduceResultPh2 = partitionedData.groupBy("fourth").reduceGroup(new ReducePhase2(conf));

			//DataSet<CurveRecord> result = resultRS.flatMap(new MapPhase2(conf)).withBroadcastSet(ranges, "ranges");
			//DataSet<Tuple4<String, String, String, String>> reduceResultPh2 = result.groupBy("fourth").reduceGroup(new ReducePhase2(conf));

			/******** PHASE 3 ***********/
			//########## REDUCE AND WRITE RESULT
			DataSet<String> reduceResultPh3;
			if (conf.getClassifyOrRegress() == 1) {
				reduceResultPh3 = reduceResultPh2.groupBy(0).reduceGroup(new ReducePhase3Classification(conf));
	                	reduceResultPh3.rebalance().writeAsText(conf.getHdfsPath() + "ClassificationResults");
			}
			else if (conf.getClassifyOrRegress() == 2) {
				reduceResultPh3 = reduceResultPh2.groupBy(0).reduceGroup(new ReducePhase3Regression(conf));
	                	reduceResultPh3.rebalance().writeAsText(conf.getHdfsPath() + "RegressionResults");
			}
	            	else {
	                	System.out.println("Wrong algorithm selection input! Exiting...");
	               		System.exit(0);
	            	}

			// execute program
			long startExecuteTime = System.currentTimeMillis();
			env.execute("FML-kNN");
			long totalElapsedExecuteTime = System.currentTimeMillis() - startExecuteTime;

			// Count execution time
			int ExecuteMillis = (int) totalElapsedExecuteTime % 1000;
			int ExecuteSeconds = (int) (totalElapsedExecuteTime / 1000) % 60;
			int ExecuteMinutes = (int) ((totalElapsedExecuteTime / (1000 * 60)) % 60);
			int ExecuteHours = (int) ((totalElapsedExecuteTime / (1000 * 60 * 60)) % 24);
			System.out.println("Thread " + Thread.currentThread().getId()
					+ " total time: " + ExecuteHours + "h " + ExecuteMinutes
					+ "m " + ExecuteSeconds + "sec " + ExecuteMillis + "mil");

	            	if (conf.getClassifyOrRegress() == 1) {
	                	// Calculate accuracy
		                double acc = Functions.calculateAccuracy(conf);
	        	        totalPercentage += acc;

	                	// Calculate F-Measure
		                double fmeas = Functions.calculateFMeasure(conf);
	        	        totalFMeasure += fmeas;
	            	}

			else if (conf.getClassifyOrRegress() == 2) {
        	        	Functions.produceFinalDataset(conf);

		                // Calculate Root Mean Square Error (RMSE)
	        	        totalRMSE += Functions.calculateRMSE(conf);

	                	// Calculate Coefficient of determination (R^2)
		                totalRSquared += Functions.calculateRSquared(conf);

	        	}
		}

		if (conf.getClassifyOrRegress() == 1) {
	        	results[0] = totalPercentage / conf.getCrossValidationFolds();
        		results[1] = totalFMeasure / conf.getCrossValidationFolds();
        	}

	        else if (conf.getClassifyOrRegress() == 2) {
        		results[0] = totalRSquared / conf.getCrossValidationFolds();
			results[1] = totalRMSE / conf.getCrossValidationFolds();
        	}

        	return results;

	}

	static class MyPartitioner implements Partitioner<Integer> {

                @Override
                public int partition(Integer key, int numPartitions) {
                        return key % numPartitions;
                }
        }
}
