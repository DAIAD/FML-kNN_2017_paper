package org.apache.flink.transformations;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.io.*;
import java.io.FileWriter;
import java.io.FileReader;
import java.io.BufferedReader;
import java.util.Collections;
import java.util.Comparator;
import java.io.PrintWriter;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.tools.ExecConf;
import org.apache.flink.values.KnnRecord;
import org.apache.flink.tools.Zorder;
import org.apache.flink.tools.HilbertOrder;
import org.apache.flink.util.Collector;
import org.apache.flink.values.Phase1_2Value;
import com.mellowtech.collections.BPlusTree;
import com.mellowtech.collections.KeyValue;
import com.mellowtech.disc.CBString;

public class ReducePhase2 implements GroupReduceFunction<Phase1_2Value, String> {

	private static final long serialVersionUID = 1L;
	private int zOffset, ridOffset, srcOffset, sidOffset, classOffset, sid=0;
	private ExecConf conf;

	public ReducePhase2(ExecConf conf) {
               	this.conf = conf;
       	}

	@Override
	public void reduce(Iterable<Phase1_2Value> input, Collector<String> output) throws Exception {

		//long startExecuteTime = System.currentTimeMillis();
		Iterator<Phase1_2Value> iterator = input.iterator();
		Phase1_2Value entry = iterator.next();

		String outerTable = "/home/local/chgeorgakidis/F-kNN/Flink_kNN_3Phases/R_local/R_local" + entry.getFourth();
		String innerTable = "/home/local/chgeorgakidis/F-kNN/Flink_kNN_3Phases/S_local/S_local" + entry.getFourth();

		zOffset = 0;
		ridOffset = zOffset + 1;
		srcOffset = ridOffset + 1;
		sidOffset = srcOffset + 1;
		classOffset = sidOffset + 1;

		String line = entry.toString();
		String[] parts = line.split(" +");
		//if (Integer.parseInt(parts[sidOffset]) >= conf.getNumOfPartition()) sid = 1;

		// Create seperate local files for different key value
        	int bufferSize = 8 * 1024 * 1024;
		FileWriter fwForR = new FileWriter(outerTable);
        	BufferedWriter writerR = new BufferedWriter(fwForR, bufferSize);
		FileWriter fwForS = new FileWriter(innerTable);
        	BufferedWriter writerS = new BufferedWriter(fwForS, bufferSize);
		//List<String> Rlist = new ArrayList<String>();
		//List<String> Slist = new ArrayList<String>();

		while (iterator.hasNext()) {

			line = entry.toString();

			parts = line.split(" +");
			String zvalue = parts[zOffset];
			String rid = parts[ridOffset];
			String src = parts[srcOffset];
			int srcId = Integer.valueOf(src);
			String theClass = parts[classOffset];

			String tmpRecord = zvalue + " " + rid + ":" + theClass + "\n";

			if (srcId == 0) // from R
                		writerR.write(tmpRecord);
			else if (srcId == 1) // from S
                		writerS.write(tmpRecord);
			else {
				System.out.println(srcId);
				System.out.println("The record has an unknown source!!");
				System.exit(-1);
			}
			entry = iterator.next();
		}

        	writerR.close();
        	writerS.close();

        	/**
		ValueComparator com = new ValueComparator();
		Collections.sort(Slist, com);
		Collections.sort(Rlist, com);

		for (String s : Rlist) {
			writerR.write(s);
		}
		for (String s : Slist) {
			writerS.write(s);
		}**/

		// Check if the created files are larger than 3GB
		//File R = new File(outerTable);
		//File S = new File(innerTable);
		//double Rmegabytes = 0;
		//double Smegabytes = 0;

		//if (R.exists() && S.exists()) {
		//	double Rbytes = R.length();
		//	double Sbytes = S.length();
		//	Rmegabytes = ((Rbytes / 1024) / 1024);
		//	Smegabytes = ((Sbytes / 1024) / 1024);
		//} else {
		//	System.out.println("File does not exist!");
		//}

		/****** Perform binary search for each R over the S ********/
		// If the datasets fit in memory, proceed with binary search...
		//if ((Rmegabytes < 1000) && (Smegabytes < 1000)) {

			/** source: http://www.cs.utah.edu/~lifeifei/knnj/#codes **/
			CBString keyType = new CBString();
			CBString valueType = new CBString();
			int indexBlockSize = 1024 * 32; // 4k size
			int valueBlockSize = 1024 * 32;
			int bufInLength = 8 * 1024 * 1024;

			BPlusTree bpt = new BPlusTree(innerTable, keyType, valueType,
					valueBlockSize, indexBlockSize);
			bpt.setTreeCache(32 * 1024 * 1024, 32 * 1024 * 1024);

			int flag = 0;
			bpt.createIndexBL(innerTable, bufInLength, flag);
			bpt.save();

			float hashTableLoadFactor = 0.75f;
			final int knnFactor = 4;

			int hashTableCapacity = (int) Math.ceil((knnFactor * conf.getKnn())
					/ hashTableLoadFactor) + 1;

			LinkedHashMap<String, ArrayList<Integer>> coordLRUCache =
					new LinkedHashMap<String, ArrayList<Integer>>(
					hashTableCapacity, hashTableLoadFactor, true) {
				private static final long serialVersionUID = 1L;

				@Override
				protected boolean removeEldestEntry(
						Map.Entry<String, ArrayList<Integer>> eldest) {
					return size() > knnFactor * conf.getKnn();
				}
			};

			FileReader frForR = new FileReader(outerTable);
			BufferedReader brForR = new BufferedReader(frForR, bufInLength);

			boolean loop = true;
			while (loop) {
				line = brForR.readLine();
				if (line == null)
					break;

				parts = line.split(" +");
				String val = parts[0];
				String rid = parts[1];

				int[] coord = Zorder.toCoord(val, conf.getDimension());
				//if (conf.isHilbertOrZ() == true) coord = HilbertOrder.toCoord(val, conf);

				// Unshift
//				if (sid != 0) {
//					for (int i=0; i<conf.getDimension(); i++)
//						coord[i] = coord[i] - conf.getShiftvectors()[sid][i];
//				}

				ArrayList<ArrayList<KeyValue>> knnList = bpt.rangeSearch(
						new CBString(val), conf.getKnn());

				ArrayList<KnnRecord> knnListSorted = new ArrayList<KnnRecord>();
				Comparator<KnnRecord> krc = new KnnRecordComparator();
				for (ArrayList<KeyValue> l : knnList) {
					for (KeyValue e : l) {

						String val2 = ((CBString) e.getKey()).getString();
						String rid2 = ((CBString) e.getValue()).getString();
						int[] coord2 = null;

						ArrayList<Integer> cachedCoord2 = coordLRUCache.get(val2);

						if (cachedCoord2 == null) {
							//if (conf.isHilbertOrZ() == true) coord2 = HilbertOrder.toCoord(val2, conf);
			                                coord2 = Zorder.toCoord(val2, conf.getDimension());

							// Unshift
//							if (sid != 0) {
//								for (int i=0; i<conf.getDimension(); i++)
//									coord2[i] = coord2[i] - conf.getShiftvectors()[sid][i];
//							}

							ArrayList<Integer> ai = new ArrayList<Integer>(
									conf.getDimension());
							for (int i = 0; i < conf.getDimension(); i++) {
								ai.add(coord2[i]);
							}
							coordLRUCache.put(val2, ai);
						} else {
							coord2 = new int[conf.getDimension()];
							for (int i = 0; i < conf.getDimension(); i++)
								coord2[i] = cachedCoord2.get(i);
						}

						float dist = (float) 0.0;
						for (int i = 0; i < conf.getDimension(); i++)
							dist += (float) Math.abs(((coord[i] - coord2[i]) * (coord[i] - coord2[i])));

						KnnRecord kr = new KnnRecord(rid2, (float) Math.sqrt(dist), coord2);
						knnListSorted.add(kr);
					}
				}

				Collections.sort(knnListSorted, krc);

				KnnRecord tmp = knnListSorted.get(0);
				for (int i = 0; i < conf.getKnn(); i++) {
					KnnRecord kr;
					try {
						kr = knnListSorted.get(i);
					} catch (IndexOutOfBoundsException e) {
						kr = tmp;
					}

					int k = kr.getRid().indexOf(":");
					//output.collect(new Tuple4<>(rid, kr.getRid().substring(0, k), Float.toString(kr.getDist()), kr.getRid().substring(k+1)));
					output.collect(rid + " " +  kr.getRid().substring(0, k) + " " + Float.toString(kr.getDist()) + " " + kr.getRid().substring(k+1));
				}
			}
			brForR.close();

		//}
		// ...else store them locally and perform binary search
		//else {
			// TODO Binary search in file
		//}

		//R.delete();
		//S.delete();
	}

	private class ValueComparator implements Comparator<String> {

		@Override
		public int compare(String w1, String w2) {
			return w1.compareTo(w2);
		}
	}

	/** source: http://www.cs.utah.edu/~lifeifei/knnj/#codes **/
	class KnnRecordComparator implements Comparator<KnnRecord> {
		public int compare(KnnRecord o1, KnnRecord o2) {
			int ret = 0;
			float dist = o1.getDist() - o2.getDist();

			if (dist > 0) ret = 1;
			else if (dist < 0) ret = -1;
			return -ret;
		}
	}

}
