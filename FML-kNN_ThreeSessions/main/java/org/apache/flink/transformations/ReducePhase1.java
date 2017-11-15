package org.apache.flink.transformations;

import java.io.FileReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.Random;
import org.apache.flink.tools.ExecConf;
import java.util.Comparator;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.tools.Functions;
import org.apache.flink.tools.ExecConf;
import org.apache.flink.util.Collector;
import org.apache.flink.values.Phase1_2Value;
import org.apache.flink.api.java.functions.FunctionAnnotation;

//@FunctionAnnotation.ReadFields("first; third; fourth")
public class ReducePhase1 extends RichGroupReduceFunction<Phase1_2Value,  String> implements GroupReduceFunction<Phase1_2Value, String> {

	private static final long serialVersionUID = 1L;
	private static double sampleRateOfR;
	private static double sampleRateOfS;
	private ExecConf conf;
	private int fileId = 0;

	public ReducePhase1(int fileId, ExecConf conf) {
		this.conf = conf;
		this.fileId = fileId;
	}

	@Override
	public void reduce(Iterable<Phase1_2Value> input, Collector<String> output) throws Exception {

		//long startExecuteTime = System.currentTimeMillis();
		ArrayList<String> RtmpList = new ArrayList<String>();
		ArrayList<String> StmpList = new ArrayList<String>();

		String shiftId = "";

		Iterator<Phase1_2Value> iterator = input.iterator();
		while (iterator.hasNext()) {
			Phase1_2Value entry = iterator.next();
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
		int len = Functions.maxDecDigits(conf.getDimension());
		q_start = Functions.createExtra(len);

		for (int i = 1; i <= conf.getNumOfPartition(); i++) {

                        int estRank = Functions.getEstimatorIndex(i, conf.getNr(), conf.getSampleRateOfR(), conf.getNumOfPartition());
                        if (estRank - 1 >= Rsize) estRank = Rsize;

                        String q_end;
                        if (i == conf.getNumOfPartition()) {
                                q_end = Functions.maxDecString(conf.getDimension());
                        } else
                                q_end = RtmpList.get(estRank - 1);

			if (fileId == 0) {
	                        output.collect("(" + shiftId + "," + q_start + " " + q_end + ")");
			}
			else {
	                        int low;
        	                if (i == 1)
	                                low = 0;
	                        else {
	                                int newKnn = (int)Math.ceil((double) conf.getKnn() / (conf.getEpsilon()*conf.getEpsilon()*conf.getNs()));
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
	                                int newKnn = (int)Math.ceil((double) conf.getKnn() / (conf.getEpsilon()*conf.getEpsilon()*conf.getNs()));
	                                high = Collections.binarySearch(StmpList, q_end);
	                                if (high < 0)
	                                        high = -high - 1;
	                                if ((high + newKnn) > Ssize -1)
	                                        high = Ssize -1;
	                                else
	                                        high += newKnn;
	                        }

				String s_end;
	                        if (i == conf.getNumOfPartition()) {
	                                s_end = Functions.maxDecString(conf.getDimension());
	                        } else {
	                                s_end = StmpList.get(high);
	                        }

        	                output.collect("(" + shiftId + "," + s_start + " " + s_end + ")");
			}
                        q_start = q_end;
                }

		//long totalElapsedExecuteTime = System.currentTimeMillis() - startExecuteTime;
		//BufferedWriter timeWriter = new BufferedWriter(new FileWriter(ExecConf.path + "Timings_ReducePhase1", true));
		//timeWriter.write(Thread.currentThread().getId() + " " + totalElapsedExecuteTime + "\n");
		//timeWriter.close();
	}

	private class ValueComparator implements Comparator<String> {

		@Override
		public int compare(String w1, String w2) {

                    	int cmp = w1.compareTo(w2);
	                if( cmp != 0 ) return cmp;
                    	cmp = w1.toString().compareTo(w2.toString());

                	return cmp;
		}
	}
}
