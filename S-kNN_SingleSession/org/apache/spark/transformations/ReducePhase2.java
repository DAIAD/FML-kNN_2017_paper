package org.apache.spark.transformations;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.io.File;
import java.io.FileWriter;
import java.io.FileReader;
import java.io.BufferedReader;
import java.util.Collections;
import java.util.Comparator;

import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.tools.ExecConf;
import org.apache.spark.tools.Zorder;
import org.apache.spark.values.KnnRecord;
import org.apache.spark.values.Phase1_2Value;
import org.apache.spark.values.Phase3Value;

import scala.Tuple2;
import scala.Tuple4;

import com.mellowtech.collections.BPlusTree;
import com.mellowtech.collections.KeyValue;
import com.mellowtech.disc.CBString;

public class ReducePhase2 implements PairFlatMapFunction<Iterator<Tuple2<Tuple2<Integer, String>, Phase1_2Value>>, String, Phase3Value> {

		private static final long serialVersionUID = 1L;
		private int zOffset, ridOffset, srcOffset, sidOffset, classOffset, sid=0;
		private ExecConf conf;

		public ReducePhase2(ExecConf conf) {
                	this.conf = conf;
        	}

		@Override
		public Iterable<Tuple2<String, Phase3Value>> call(
			Iterator<Tuple2<Tuple2<Integer, String>, Phase1_2Value>> arg0) throws Exception {

		LinkedList<Tuple2<String, Phase3Value>> result = new LinkedList<>();
		Iterator<Tuple2<Tuple2<Integer, String>, Phase1_2Value>> iterator = arg0;
		Tuple2<Tuple2<Integer, String>, Phase1_2Value> input = iterator.next();
		Phase1_2Value entry = input._2();

			String outerTable = "/home/local/chgeorgakidis/S-kNN/Spark_zkNN_1Ph/R_local/R_local" + entry.getFourth();
			String innerTable = "/home/local/chgeorgakidis/S-kNN/Spark_zkNN_1Ph/S_local/S_local" + entry.getFourth();

			zOffset = 0;
			ridOffset = zOffset + 1;
			srcOffset = ridOffset + 1;
			sidOffset = srcOffset + 1;
			classOffset = sidOffset + 1;

			String line = entry.toString();
	                String[] parts = line.split(" +");
//        	        if (Integer.parseInt(parts[sidOffset]) >= conf.getNumOfPartition()) sid = 1;

			// Create seperate local files for different key value
			FileWriter writerR = new FileWriter(outerTable);
			FileWriter writerS = new FileWriter(innerTable);

			// value format <zvalue, rid, src>
			while (iterator.hasNext()) {

				line = entry.toString();

				parts = line.split(" +");
				String zvalue = parts[zOffset];
				String rid = parts[ridOffset];
				String src = parts[srcOffset];
				int srcId = Integer.valueOf(src);
				String demandclass = parts[classOffset];

				String tmpRecord = zvalue + " " + rid + ":" + demandclass + "\n";

				if (srcId == 0)  // from R
					//Rlist.add(tmpRecord);
					writerR.write(tmpRecord);
				else if (srcId == 1) // from S
					//Slist.add(tmpRecord);
					writerS.write(tmpRecord);
				else {
					System.out.println(srcId);
					System.out.println("The record has an unknown source!!");
					System.exit(-1);
				}

				input = iterator.next();
				entry = input._2();
			}

			line = entry.toString();

			parts = line.split(" +");
			String zvalue = parts[zOffset];
			String rid = parts[ridOffset];
			String src = parts[srcOffset];
			int srcId = Integer.valueOf(src);
			String demandclass = parts[classOffset];

			String tmpRecord = zvalue + " " + rid + ":" + demandclass + "\n";

			if (srcId == 0)  // from R
				//Rlist.add(tmpRecord);
				writerR.write(tmpRecord);
			else if (srcId == 1) // from S
				//Slist.add(tmpRecord);
				writerS.write(tmpRecord);
			else {
				System.out.println(srcId);
				System.out.println("The record has an unknown source!!");
				System.exit(-1);
			}

			writerR.close();
			writerS.close();

			// Calculate the kNN on the z-values, unshift where necessary,
			// calculate distances and collect
			// in the following format: <rid, sid, d(r,s)>
			// key type and value type for B+ tree
			CBString keyType = new CBString();
			CBString valueType = new CBString();
			int indexBlockSize = 1024 * 32; // 4k size
			int valueBlockSize = 1024 * 32;
			int bufInLength = 8 * 1024 * 1024;

			BPlusTree bpt = new BPlusTree(innerTable, keyType, valueType, valueBlockSize, indexBlockSize);
			bpt.setTreeCache(32 * 1024 * 1024, 32 * 1024 * 1024);

			int flag = 0; // 0 for CBString, CBInt
			bpt.createIndexBL(innerTable, bufInLength, flag);
			bpt.save();

			float hashTableLoadFactor = 0.75f;
			final int knnFactor = 4;

			int hashTableCapacity = (int) Math.ceil((knnFactor * conf.getKnn())
					/ hashTableLoadFactor) + 1;

			LinkedHashMap<String, ArrayList<Integer>> coordLRUCache = new LinkedHashMap<String, ArrayList<Integer>>(hashTableCapacity, hashTableLoadFactor, true) {
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
                                rid = parts[1];

                                int[] coord = Zorder.toCoord(val, conf.getDimension());

                                // Unshift
//                                if (sid != 0) {
//                                        for (int i=0; i<conf.getDimension(); i++)
//                                                coord[i] = coord[i] - conf.getShiftvectors()[sid][i];
//                                }

                                ArrayList<ArrayList<KeyValue>> knnList = bpt.rangeSearch(new CBString(val), conf.getKnn());

                                ArrayList<KnnRecord> knnListSorted = new ArrayList<KnnRecord>();
                                Comparator<KnnRecord> krc = new KnnRecordComparator();
                                for (ArrayList<KeyValue> l : knnList) {
                                        for (KeyValue e : l) {

                                                String val2 = ((CBString) e.getKey()).getString();
                                                String rid2 = ((CBString) e.getValue()).getString();
                                                int[] coord2 = null;

                                                ArrayList<Integer> cachedCoord2 = coordLRUCache.get(val2);

                                                if (cachedCoord2 == null) {
                                                	coord2 = Zorder.toCoord(val2, conf.getDimension());

                                                        // Unshift
//                                                        if (sid != 0) {
//								for (int i=0; i<conf.getDimension(); i++)
//                              			                coord[i] = coord[i] - conf.getShiftvectors()[sid][i];
//                                			}

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
					Phase3Value np2v = new Phase3Value(kr.getRid().substring(0, k), kr.getDist(), Integer.parseInt(kr.getRid().substring(k+1)));
                    			result.add(new Tuple2<>(rid, np2v));

                                }
                        }
                        brForR.close();

		//}
		// ...else store them locally and perform binary search
		//else {
			// TODO Binary search in file
			// File Rsorted = new File("R_local" + entry.getFourth() +
			// "_sorted");
			// File Ssorted = new File("S_local" + entry.getFourth() +
			// "_sorted");

			// Sort the records in the files
			/** Library from https://code.google.com/p/externalsortinginjava/ **/
			// ExternalSort.sort(R, Rsorted);
			// ExternalSort.sort(S, Ssorted);

		//}

		//R.delete();
		//S.delete();
		return result;
	}

	private class ValueComparator implements Comparator<String> {

        	@Override
            	public int compare(String w1, String w2) {
                    	return w1.compareTo(w2);
            	}
	}

	class KnnRecordComparator implements Comparator<KnnRecord>{
		public int compare(KnnRecord o1, KnnRecord o2) {
			int ret = 0;
			float dist = o1.getDist() - o2.getDist();

			if (dist > 0)
				ret = 1;
			else
				ret = -1;
			return -ret;
		}
	}
}
