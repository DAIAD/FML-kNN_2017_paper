package org.apache.spark.transformations;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Random;
import java.util.Comparator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.tools.ExecConf;
import org.apache.spark.tools.Functions;
import org.apache.spark.tools.Zorder;
import org.apache.spark.values.Phase1_2Value;
import scala.Tuple2;

public class ReducePhase1 implements FlatMapFunction<Iterator<Tuple2<Integer,Iterable<Phase1_2Value>>>, String> {

	private static final long serialVersionUID = 1L;
	private double sampleRate;
	private ExecConf conf;

	public ReducePhase1(ExecConf conf) {
		this.conf = conf;
	}

	private class ValueComparator implements Comparator<String> {

		@Override
		public int compare(String w1, String w2) {

			int cmp = w1.compareTo(w2);
			if (cmp != 0)
				return cmp;
			cmp = w1.toString().compareTo(w2.toString());

			return cmp;
		}
	}

	@Override
	public Iterable<String> call(Iterator<Tuple2<Integer, Iterable<Phase1_2Value>>> t) throws Exception {

		ArrayList<String> RtmpList = new ArrayList<String>();
		ArrayList<String> StmpList = new ArrayList<String>();
		LinkedList<String> ranges = new LinkedList<String>();

		String shiftId = "";

		Tuple2<Integer, Iterable<Phase1_2Value>> input;
        	if (t.hasNext()) {
            		input = t.next();
        	} else {
           		return ranges;
        	}
        	Iterator<Phase1_2Value> iterator_input = input._2().iterator();
        	while (iterator_input.hasNext()) {
            		Phase1_2Value entry = iterator_input.next();
            		if (entry.getThird() == 0) {
                		RtmpList.add(entry.getFirst());
                		shiftId = entry.getFourth().toString();
            		} else {
                		StmpList.add(entry.getFirst());
                		shiftId = entry.getFourth().toString();
            		}
        	}

		int Rsize = RtmpList.size();
		int Ssize = StmpList.size();

		ValueComparator com = new ValueComparator();
		Collections.sort(RtmpList, com);
		Collections.sort(StmpList, com);

		String q_start = "";
		int len = Zorder.maxDecDigits(conf.getDimension());
		q_start = Functions.createExtra(len);

		// *************************** Estimate ranges ****************************//
		/** source: http://www.cs.utah.edu/~lifeifei/knnj/#codes **/
		for (int i = 1; i <= conf.getNumOfPartition(); i++) {

			int estRank = Functions.getEstimatorIndex(i, conf.getNr(),
					conf.getSampleRateOfR(), conf.getNumOfPartition());
			if (estRank - 1 >= Rsize)
				estRank = Rsize;

			String q_end;
			if (i == conf.getNumOfPartition()) {
				q_end = Functions.maxDecString(conf.getDimension());
			} else
				q_end = RtmpList.get(estRank - 1);

			ranges.add("(" + shiftId + "," + "0," + q_start + " " + q_end + ")");

			int low;
			if (i == 1)
				low = 0;
			else {
				int newKnn = (int) Math
						.ceil((double) conf.getKnn()
								/ (conf.getEpsilon() * conf.getEpsilon() * conf.getNs()));
				low = Collections.binarySearch(StmpList, q_start);
				if (low < 0)
					low = -low - 1;
				if ((low - newKnn) < 0)
					low = 0;
				else
					low -= newKnn;
			}

			String s_start;
			if (i == 1) {
				len = Functions.maxDecDigits(conf.getDimension());
				s_start = Functions.createExtra(len);
			} else
				s_start = StmpList.get(low);

			int high;
			if (i == conf.getNumOfPartition()) {
				high = Ssize - 1;
			} else {
				int newKnn = (int) Math
						.ceil((double) conf.getKnn()
								/ (conf.getEpsilon() * conf.getEpsilon() * conf
								.getNs()));
				high = Collections.binarySearch(StmpList, q_end);
				if (high < 0)
					high = -high - 1;
				if ((high + newKnn) > Ssize - 1)
					high = Ssize - 1;
				else
					high += newKnn;
			}

			String s_end;
			if (i == conf.getNumOfPartition()) {
				s_end = Functions.maxDecString(conf.getDimension());
			} else {
				s_end = StmpList.get(high);
			}

			ranges.add("(" + shiftId + "," + "1," + s_start + " " + s_end + ")");

			q_start = q_end;
		}
		return ranges;
	}
}
