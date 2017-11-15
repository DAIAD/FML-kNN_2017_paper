package org.apache.flink.transformations;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.apache.flink.values.Phase3Value;

public class MapPhase3 implements FlatMapFunction<String, Tuple2<String, Phase3Value>> {

	private static final long serialVersionUID = 1L;

	@Override
	public void flatMap(String input, Collector<Tuple2<String, Phase3Value>> output) throws Exception {

		//long startExecuteTime = System.currentTimeMillis();

		String line = input.toString();
		// Remove parentheses
		//line = line.substring(1);
		//line = line.substring(0, line.length()-1);

		String[] parts = line.split(" +");

		// key format <rid1>
		String mapKey = parts[0].trim();

		// value format <rid2, dist>
		Phase3Value np2v = new Phase3Value(parts[1].trim(), Float.valueOf(parts[2]), Integer.valueOf(parts[3]));

		output.collect(new Tuple2<String, Phase3Value>(mapKey, np2v));

		//long totalElapsedExecuteTime = System.currentTimeMillis() - startExecuteTime;
		//BufferedWriter timeWriter = new BufferedWriter(new FileWriter(ExecConf.path + "Timings_MapPhase3", true));
		//timeWriter.write(Thread.currentThread().getId() + " " + totalElapsedExecuteTime + "\n");
		//timeWriter.close();
	}
}
