package org.apache.spark.transformations;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;

import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.tools.ExecConf;
import org.apache.spark.values.Phase1_2Value;
import java.util.List;
import scala.Tuple2;

public class MapPhase2 implements PairFlatMapFunction<Tuple2<Integer, Phase1_2Value>, Tuple2<Integer, String>, Phase1_2Value> {

	private static final long serialVersionUID = 1L;
	private String[][] RrangeArray;
	private String[][] SrangeArray;
	private ExecConf conf;

	public MapPhase2(ExecConf conf, Broadcast<List<String>> broadcastVar) throws IOException {
        this.conf = conf;
        Object[] arrayRanges = broadcastVar.value().toArray();
        RrangeArray = new String[conf.getShift()][conf.getNumOfPartition()];
        SrangeArray = new String[conf.getShift()][conf.getNumOfPartition()];
        int cnt1 = 0, cnt2 = 0, cnt3 = 0, cnt4 = 0;

        for (int i = 0; i < arrayRanges.length; i++) {
            String val = arrayRanges[i].toString();
            if (val.charAt(1) == '0') {
                if (val.charAt(3) == '0') {
                    RrangeArray[0][cnt1++] = val.substring(2);
                } else {
                    SrangeArray[0][cnt2++] = val.substring(2);
                }
            } else if (val.charAt(1) == '1') {
                if (val.charAt(3) == '0') {
                    RrangeArray[1][cnt3++] = val.substring(2);
                } else {
                    SrangeArray[1][cnt4++] = val.substring(2);
                }
            }
        }
	}

	public ArrayList<String> getPartitionId(String z, String src, String sid) throws IOException {

		String ret = null;
		String[] mark = null;
		int sidInt = Integer.valueOf(sid);
		ArrayList<String> idList = new ArrayList<String>(conf.getNumOfPartition());

		// 0 - R from Outer 1 - S from inner
		if (src.compareTo("0") == 0)
			mark = RrangeArray[sidInt];
		else if (src.compareTo("1") == 0)
			mark = SrangeArray[sidInt];
		else {
			System.out.println(src);
			System.out.println("Unknown source for input record !!!");
			System.exit(-1);
		}

		// Partition is ordered from 0
		for (int i = 0; i < conf.getNumOfPartition(); i++) {
			String range = mark[i];
			String[] parts = range.split(" +");
			String low = parts[0].substring(3);
			String high = parts[1].substring(0, parts[1].length()-1);

			// Check z against all partitions, this is a must
			if (z.compareTo(low) >=0 && z.compareTo(high) <= 0) {
				ret = Integer.toString(i);
				idList.add(ret);
			}
		}
		return idList;
	}

	@Override
	public Iterable<Tuple2<Tuple2<Integer, String>, Phase1_2Value>> call(Tuple2<Integer, Phase1_2Value> input) throws Exception {

		String line = input._2().toString();
		String[] parts = line.split(" ");
		int zOffset, ridOffset, srcOffset, sidOffset, classOffset;
		zOffset = 0;
		ridOffset = zOffset + 1;
		srcOffset = ridOffset + 1;
		sidOffset = srcOffset + 1;
		classOffset = sidOffset + 1;

		// Figure out to which partition range the record belong to.
		ArrayList<String> pidList = getPartitionId(parts[zOffset],
				parts[srcOffset], parts[sidOffset]);
		if (pidList.size() == 0) {
			System.out.println("Cannot get pid");
			System.exit(-1);
		}

		LinkedList<Tuple2<Tuple2<Integer, String>, Phase1_2Value>> result = new LinkedList<>();
        	for (int i = 0; i < pidList.size(); i++) {

            		String pid = pidList.get(i);
            		int intSid = Integer.valueOf(parts[sidOffset]);
            		int intPid = Integer.valueOf(pid);
            		int groupKey = intSid * conf.getNumOfPartition() + intPid;

            		Phase1_2Value bp2v = new Phase1_2Value(parts[zOffset],
                    	parts[ridOffset], Integer.parseInt(parts[srcOffset]),
                    	groupKey, Math.round(Float.parseFloat(parts[classOffset])));
            		result.add(new Tuple2<>(new Tuple2<>(groupKey, parts[zOffset]), bp2v));
        	}

		return result;
	}
}
