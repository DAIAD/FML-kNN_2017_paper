package org.apache.spark.transformations;

import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.values.Phase3Value;

import scala.Tuple2;
import scala.Tuple4;

public class MapPhase3 implements PairFunction<Tuple4<String, String, String, String>, String, Phase3Value> {

	private static final long serialVersionUID = 1L;

	@Override
	public Tuple2<String, Phase3Value> call(Tuple4<String, String, String, String> t) throws Exception {

		String line = t.toString();
		// Remove parentheses
		line = line.substring(1);
		line = line.substring(0, line.length()-1);

		String[] parts = line.split(",+");

		// key format <rid1>
		String mapKey = parts[0].trim();

		// value format <rid2, dist>
		Phase3Value np2v = new Phase3Value(parts[1].trim(), Float.valueOf(parts[2]), Integer.valueOf(parts[3]));

		return new Tuple2<String, Phase3Value>(mapKey, np2v);

	}
}
