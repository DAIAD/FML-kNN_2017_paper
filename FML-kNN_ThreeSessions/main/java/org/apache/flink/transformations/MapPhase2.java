package org.apache.flink.transformations;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.io.File;
import java.io.InputStreamReader;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.tools.ExecConf;
import org.apache.flink.util.Collector;
import org.apache.flink.values.Phase1_2Value;

public class MapPhase2 extends RichFlatMapFunction<String,Phase1_2Value> implements FlatMapFunction<String, Phase1_2Value> {

	private static final long serialVersionUID = 1L;
	private String[][] RrangeArray;
	private String[][] SrangeArray;
	private ExecConf conf;

        public MapPhase2(ExecConf conf) throws IOException {

		this.conf = conf;
		RrangeArray = new String[conf.getShift()][conf.getNumOfPartition()];
                SrangeArray = new String[conf.getShift()][conf.getNumOfPartition()];
                int[] countersR =  new int[conf.getShift()];
                int[] countersS =  new int[conf.getShift()];

                Path rPt = new Path("hdfs://localhost:9000/user/chgeorgakidis/Rrange");
		Path sPt = new Path("hdfs://localhost:9000/user/chgeorgakidis/Srange");
                FileSystem fs = rPt.getFileSystem();
		BufferedReader rBr = new BufferedReader(new InputStreamReader(fs.open(rPt)));
		fs = sPt.getFileSystem();
		BufferedReader sBr = new BufferedReader(new InputStreamReader(fs.open(sPt)));

		int doneR = 0, doneS = 0;
                while((doneR != 1) && (doneS != 1)) {
                        String lineR = rBr.readLine();
                        if (lineR == null) {
				doneR = 1;
			}
                        else { RrangeArray[Character.getNumericValue(lineR.charAt(1))][countersR[Character.getNumericValue(lineR.charAt(1))]++] = lineR.trim(); }

                        String lineS = sBr.readLine();
                        if (lineS == null) {
				doneS = 1;
			}
                        else { SrangeArray[Character.getNumericValue(lineS.charAt(1))][countersS[Character.getNumericValue(lineS.charAt(1))]++] = lineS.trim(); }
                }

                rBr.close();
                sBr.close();
	}

	@Override
	public void flatMap(String input, Collector<Phase1_2Value> output) throws Exception {

		//long startExecuteTime = System.currentTimeMillis();
		String line = input.trim();
		String[] parts = line.split(" ");
		int zOffset, ridOffset, srcOffset, sidOffset, classOffset;
		zOffset = 0;
		ridOffset = zOffset + 1;
		srcOffset = ridOffset + 1;
		sidOffset = srcOffset + 1;
		classOffset = sidOffset + 1;

		// Figure out to which partition range the record belong to.
		ArrayList<String> pidList = getPartitionId(parts[zOffset], parts[srcOffset], parts[sidOffset]);
		if (pidList.size() == 0) {
			System.out.println("Cannot get pid");
			System.exit(-1);
		}

		for (int i = 0; i < pidList.size(); i++) {
			String pid  = pidList.get(i);
			int intSid = Integer.valueOf(parts[sidOffset]);
			int intPid = Integer.valueOf(pid);
			int groupKey = intSid * conf.getNumOfPartition() + intPid;

			// ((zvalue, groupid), (zvalue, rid, src))
			Phase1_2Value bp2v = new Phase1_2Value(parts[zOffset], parts[ridOffset], Integer.parseInt(parts[srcOffset]), groupKey, Integer.parseInt(parts[classOffset]));
			output.collect(bp2v);
		}

		//long totalElapsedExecuteTime = System.currentTimeMillis() - startExecuteTime;
		//BufferedWriter timeWriter = new BufferedWriter(new FileWriter(ExecConf.path + "Timings_MapPhase2", true));
		//timeWriter.write(Thread.currentThread().getId() + " " + totalElapsedExecuteTime + "\n");
		//timeWriter.close();
	}

	public ArrayList<String> getPartitionId(String z, String src, String sid) throws IOException {

		String ret = null;
		int sidInt = Integer.valueOf(sid);
		String[] mark = null;
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
			String[] parts = null;
			parts = range.split(" +");
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
}
