package org.apache.flink.phases;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.tools.ExecConf;
import org.apache.flink.transformations.MapPhase3;
import org.apache.flink.transformations.ReducePhase3;
import org.apache.flink.values.Phase3Value;
import org.apache.flink.tools.Functions;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

public class Phase3 {

	public static void main(String[] args) throws Exception {

		ExecConf conf = new ExecConf();
                // Read arguments and configure
                if (args.length > 0) {
                        try {
                                conf.setKnn(Integer.parseInt(args[0]));
                                conf.setNoOfClasses(Integer.parseInt(args[1]));
                        } catch(Exception e) {
                                System.out.println("Error! Please check arguments.");
                                System.exit(0);
                        }
                }

		Path pt = new Path(conf.getPath() + "ClassificationResults");
                FileSystem fs = pt.getFileSystem();
                fs.delete(pt, true);

		// Set up the execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		/******** PHASE 2 ***********/
		DataSet<String> inputPh3 = env.readTextFile(conf.getPath() + "Results_Phase2");

		/////////// MAP
	 	DataSet<Tuple2<String, Phase3Value>> resultMap = inputPh3.flatMap(new MapPhase3());

	 	/////////// REDUCE
	 	DataSet<String> reduceResultPh3 = resultMap.groupBy(0).reduceGroup(new ReducePhase3(conf));

	 	reduceResultPh3.writeAsText(conf.getPath() + "ClassificationResults");

		// Execute program
		long startExecuteTime = System.currentTimeMillis();
		env.execute("Flink zKNN Phase 3");
		long totalElapsedExecuteTime = System.currentTimeMillis() - startExecuteTime;

		int ExecuteMillis = (int) totalElapsedExecuteTime % 1000;
	 	int ExecuteSeconds = (int) (totalElapsedExecuteTime / 1000) % 60 ;
	 	int ExecuteMinutes = (int) ((totalElapsedExecuteTime / (1000*60)) % 60);
	 	int ExecuteHours   = (int) ((totalElapsedExecuteTime / (1000*60*60)) % 24);
	 	System.out.println("Phase 3: Thread " + Thread.currentThread().getId() + " total time: " +  ExecuteHours + "h " + ExecuteMinutes + "m " + ExecuteSeconds + "sec " + ExecuteMillis + "mil");
		System.out.println("Final average accuracy: " + Functions.calculateAccuracy(conf));

	}
}
