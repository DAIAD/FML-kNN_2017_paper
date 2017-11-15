package org.apache.flink.transformations;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.PriorityQueue;
import java.util.TreeSet;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.tools.ExecConf;
import org.apache.flink.util.Collector;
import org.apache.flink.values.Phase3Value;

public class ReducePhase3 implements GroupReduceFunction<Tuple2<String, Phase3Value>, String> {

	private static final long serialVersionUID = 1L;
	private ExecConf conf;

        public ReducePhase3(ExecConf conf) {
                this.conf = conf;
        }

	/** source: http://www.cs.utah.edu/~lifeifei/knnj/#codes **/
	class Record {
		public String id2;
		public float dist;
		public int theClass;

		Record(String id2, float dist, int theClass) {
			this.id2 = id2;
			this.dist = dist;
			this.theClass = theClass;
		}

		public String toString() {
			return id2 + " " + Float.toString(dist) + " "
					+ Integer.toString(theClass);
		}
	}

	/** source: http://www.cs.utah.edu/~lifeifei/knnj/#codes **/
	class RecordComparator implements Comparator<Record> {
		public int compare(Record o1, Record o2) {
			int ret = 0;
			float dist = o1.dist - o2.dist;

			if (dist > 0)
				ret = 1;
			else
				ret = -1;
			return -ret;
		}
	}

	@Override
	public void reduce(Iterable<Tuple2<String, Phase3Value>> input, Collector<String> output) throws Exception {

		Iterator<Tuple2<String, Phase3Value>> iterator = input.iterator();
		String id1 = null;

		RecordComparator rc = new RecordComparator();
		PriorityQueue<Record> pq = new PriorityQueue<Record>(conf.getKnn() + 1, rc);
		TreeSet<String> ts = new TreeSet<String>();

		/** source: http://www.cs.utah.edu/~lifeifei/knnj/#codes **/
		Tuple2<String, Phase3Value> entry = iterator.next();
		String id2 = entry.f1.getFirst();
		id1 = entry.f0;
		while (iterator.hasNext()) {
			if (!ts.contains(id2)) {
				ts.add(id2);
		       		float dist = entry.f1.getSecond();
		       		int demandClass = entry.f1.getdemandClass();
	       			Record record = new Record(id2, dist, demandClass);
	       			pq.add(record);
		       		if (pq.size() > conf.getKnn())
		       			pq.poll();
			}

			entry = iterator.next();
			id2 = entry.f1.getFirst();
		}

		ts.add(id2);
                float dist = entry.f1.getSecond();
                int demandClass = entry.f1.getdemandClass();
                Record record = new Record(id2, dist, demandClass);
                pq.add(record);
                if (pq.size() > conf.getKnn())
	                pq.poll();

		// Calculate kNN weights, class probabilities and classify
		Record[] kNNs = new Record[conf.getKnn()];
		int count = 0;
		while (pq.size() > 0) {
			kNNs[count] = pq.poll();
			count++;
		}

		float maxDistance = kNNs[0].dist;
		float minDistance = 0;
		try {
			minDistance = kNNs[conf.getKnn() - 1].dist;
		} catch (NullPointerException e) {
			System.out.println("Error element: " + id1);
			if (kNNs[conf.getKnn() - 1] != null)
				System.out.println("Error kNN: " + kNNs[conf.getKnn() - 1].id2);
			output.collect(new String(id1.toString() + " ERROR!"));
			return;
		}

		float[] weights = new float[conf.getKnn()];
		for (int i = 0; i < conf.getKnn(); i++) {
			if (maxDistance == minDistance)
				weights[i] = 1;
			else {
				weights[i] = (((maxDistance - kNNs[i].dist) / (maxDistance - minDistance)));
			}
		}

		float totalWeight = 0;
		HashMap<Integer, Float> classVotes = new HashMap<>();
		for (int i = 0; i < conf.getKnn(); i++) {
			int currClass = kNNs[i].theClass;
			if (!classVotes.containsKey(currClass)) {
				classVotes.put(currClass, weights[i]);
				totalWeight += weights[i];
			} else {
				float weight = classVotes.get(currClass);
				weight += weights[i];
				classVotes.put(currClass, weight);
				totalWeight += weights[i];
			}
		}

		Object[] classes = classVotes.keySet().toArray();
		int resultClass = (int) classes[0];
		HashMap<Integer, Float> classProb = new HashMap<>();
		for (int i = 0; i < classes.length; i++) {
			if (classVotes.get(classes[i]) > classVotes.get(resultClass)) {
				resultClass = (int) classes[i];
			}
			classProb.put((int) classes[i], classVotes.get(classes[i]) / totalWeight);
		}

		String results = " | Result " + resultClass + " | ";
		for (int i = 0; i < conf.getNoOfClasses(); i++) {
			if (classProb.containsKey((int) (i+1)))
				results += "Class " + (i+1) + " probability " + classProb.get((int) i+1) + " | ";
			else
				results += "Class " + (i+1) + " probability 0.0 | ";
		}

		output.collect(new String(id1.toString() + results));

	}
}
