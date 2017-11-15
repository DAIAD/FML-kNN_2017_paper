package org.apache.flink.phases;

import org.apache.flink.api.common.operators.Order;
import java.io.File;
import java.io.InputStreamReader;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import java.io.BufferedReader;
import java.io.IOException;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.tools.ExecConf;
import org.apache.flink.transformations.MapPhase2;
import org.apache.flink.transformations.ReducePhase2;
import org.apache.flink.values.Phase1_2Value;

public class Phase2 {

	public static void main(String[] args) throws Exception {

		ExecConf conf = new ExecConf();
                // Read arguments and configure
                if (args.length > 0) {
                        try {
                                conf.setKnn(Integer.parseInt(args[0]));
                                conf.setShift(Integer.parseInt(args[1]));
                                conf.setDimension(Integer.parseInt(args[2]));
                                conf.setNumOfPartition(Integer.parseInt(args[3]));
                                conf.setHilbertOrZ(Boolean.parseBoolean(args[4]));
                        } catch(Exception e) {
                                System.out.println("Error! Please check arguments.");
                                System.exit(0);
                        }
                }

                int[][] shiftvectors = new int[conf.getShift()][conf.getDimension()];
                Path pt = new Path(conf.getPath() + "RandomShiftVectors");
                FileSystem fs = pt.getFileSystem();
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

		pt = new Path(conf.getPath() + "Results_Phase2");
                fs = pt.getFileSystem();
                fs.delete(pt, true);

		// Set up the execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		/******** PHASE 2 ***********/
		DataSet<String> inputPh2 = env.readTextFile(conf.getPath() + "Results_Phase1");

		/////////// MAP
		DataSet<Phase1_2Value> result = inputPh2.flatMap(new MapPhase2(conf));
	        DataSet<Phase1_2Value> partitionedData = result.rebalance().partitionCustom(new MyPartitioner(), "fourth").sortPartition("fourth", Order.ASCENDING);

	 	/////////// REDUCE
	 	DataSet<String> reduceResultPh2 = partitionedData.groupBy("fourth").reduceGroup(new ReducePhase2(conf));

	 	reduceResultPh2.writeAsText(conf.getPath() + "Results_Phase2");

		// Execute program
	 	long startExecuteTime = System.currentTimeMillis();
		env.execute("Flink zKNN Phase 2");
		long totalElapsedExecuteTime = System.currentTimeMillis() - startExecuteTime;

		int ExecuteMillis = (int) totalElapsedExecuteTime % 1000;
	 	int ExecuteSeconds = (int) (totalElapsedExecuteTime / 1000) % 60 ;
	 	int ExecuteMinutes = (int) ((totalElapsedExecuteTime / (1000*60)) % 60);
	 	int ExecuteHours   = (int) ((totalElapsedExecuteTime / (1000*60*60)) % 24);
	 	System.out.println("Phase 2: Thread " + Thread.currentThread().getId() + " total time: " +  ExecuteHours + "h " + ExecuteMinutes + "m " + ExecuteSeconds + "sec " + ExecuteMillis + "mil");

	}

	static class KeyExtractor implements KeySelector<Phase1_2Value, String> {

		private static final long serialVersionUID = 1L;

		@Override
		public String getKey(Phase1_2Value value) throws Exception {
			return value.getFourth().toString();
		}
	}

	public static class MyPartitioner implements Partitioner<Integer> {

		@Override
        	public int partition(Integer key, int numPartitions) {
            		return key % numPartitions;
        	}
    	}

}
