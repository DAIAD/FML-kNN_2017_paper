package org.apache.spark.transformations;

import java.util.LinkedList;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.tools.ExecConf;
import org.apache.spark.tools.HilbertOrder;
import org.apache.spark.tools.Zorder;
import org.apache.spark.values.Phase1_2Value;

import scala.Tuple2;

public class MapPhase1 implements PairFlatMapFunction<String, Integer, Phase1_2Value> {

	private static final long serialVersionUID = 1L;
	private int fileId = 0;
	private ExecConf conf;

	public MapPhase1(int fileId, ExecConf conf) {
		this.fileId = fileId;
		this.conf= conf;
	}

	@Override
	public Iterable<Tuple2<Integer, Phase1_2Value>> call(String arg0) throws Exception {

		String zval = null;
		char ch = ',';
		int pos = arg0.indexOf(ch);
		String id = arg0.substring(0, pos);
		String rest = arg0.substring(pos + 1, arg0.length()).trim();
		String[] parts = rest.split(",");
		float[] coord = new float[conf.getDimension()];
		for (int i = 0; i < conf.getDimension(); i++) {
			float tmp = 0;
			try { tmp = Float.valueOf(parts[i]); }
			catch (NumberFormatException e) { continue; }
			if (tmp > -9999) coord[i] = Float.valueOf(parts[i]);
		}

		LinkedList<Tuple2<Integer, Phase1_2Value>> result = new LinkedList<Tuple2<Integer, Phase1_2Value>>();
		for (int i = 0; i < conf.getShift(); i++) {
			int[] converted_coord = new int[conf.getDimension()];
                        for (int k = 0; k < conf.getDimension(); k++) {
                                //tmp_coord[k] = coord[k];
                                //converted_coord[k] = (int) tmp_coord[k]; 
                                //tmp_coord[k] -= converted_coord[k];
                                coord[k] = coord[k]*conf.getScale()[k];
                                converted_coord[k] = (int) (coord[k]*10);
                                //converted_coord[k] += (tmp_coord[k] * conf.getScale()[k]);
                                if (i != 0) converted_coord[k] += conf.getShiftvectors()[i][k];
                        }

			if (conf.isHilbertOrZ()) {
				zval = HilbertOrder.valueOf(converted_coord, conf);
			} else {
				zval = Zorder.valueOf(conf.getDimension(), converted_coord);
			}

			Phase1_2Value bp1v = new Phase1_2Value(zval, id, fileId, i, Integer.valueOf(parts[conf.getDimension()+1]));
			result.add(new Tuple2<Integer, Phase1_2Value>(i, bp1v));
		}

		return result;
	}
}

