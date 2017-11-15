package org.apache.spark.transformations;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.PriorityQueue;
import java.util.TreeSet;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.tools.ExecConf;
import org.apache.spark.values.Phase3Value;

import scala.Tuple2;

public class ReducePhase3 implements FlatMapFunction<Iterator<Tuple2<String,Iterable<Phase3Value>>>, String> {

	private static final long serialVersionUID = 1L;
	private ExecConf conf;

        public ReducePhase3(ExecConf conf) {
                this.conf = conf;
        }

	class Record {
		public String id2;
                public float dist;
                public float theClass;

                Record(String id2, float dist, float theClass) {
                        this.id2 = id2;
                        this.dist = dist;
                        this.theClass = theClass;
                }

                public String toString() {
                        return id2 + " " + Float.toString(dist) + " " + Float.toString(theClass);
                }
	}

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
	public Iterable<String> call(Iterator<Tuple2<String, Iterable<Phase3Value>>> t) throws Exception {

		Iterator<Tuple2<String,Iterable<Phase3Value>>> main_iterator = t;
		LinkedList<String> result = new LinkedList<String>();

		while (main_iterator.hasNext()) {
			String id1 = null;

			RecordComparator rc = new RecordComparator();
			PriorityQueue<Record> pq = new PriorityQueue<Record>(conf.getKnn() + 1, rc);
			TreeSet<String> ts = new TreeSet<String>();

			// For each record we have a reduce task
			Tuple2<String, Iterable<Phase3Value>> input = main_iterator.next();
			Iterator<Phase3Value> iterator = input._2().iterator();
			id1 = input._1();
			while (iterator.hasNext()) {
				Phase3Value entry = iterator.next();
				String id2 = entry.getFirst();

				if (!ts.contains(id2)) {
					ts.add(id2);
			       		float dist = entry.getSecond();
			       		float demandClass = entry.getdemandClass();
		       			Record record = new Record(id2, dist, demandClass);
		       			pq.add(record);
			       		if (pq.size() > conf.getKnn())
			       			pq.poll();
				}
			}

			Record[] kNNs = new Record[conf.getKnn()];
                        int count = 0;
                        while(pq.size() > 0) {
                                kNNs[count] = pq.poll();
                                count++;
                        }

                        float maxDistance = kNNs[0].dist;
                        float minDistance = 0;
                        try { minDistance = kNNs[conf.getKnn()-1].dist; }
                        catch (NullPointerException e) {
                                System.out.println( "Error element: " + id1 );
                                if (kNNs[conf.getKnn()-1] != null) System.out.println( "Error kNN: " + kNNs[conf.getKnn()-1].id2 );
                                result.add(new String(id1.toString() + " ERROR!"));
                                return result;
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
                        HashMap<Float, Float> classVotes = new HashMap<>();
                        for (int i=0; i<conf.getKnn(); i++) {
                                float currClass = kNNs[i].theClass;
                                if (!classVotes.containsKey(currClass)) {
                                        classVotes.put(currClass, weights[i]);
                                        totalWeight += weights[i];
                                }
                                else {
                                        float weight = classVotes.get(currClass);
                                        weight += weights[i];
                                        classVotes.put(currClass, weight);
                                        totalWeight += weights[i];
                                }
                        }

			Object[] classes = classVotes.keySet().toArray();
                        float resultClass = (float) classes[0];
                        HashMap<Float, Float> classProb = new HashMap<>();
                        for (int i = 0; i < classes.length; i++) {
                                if (classVotes.get(classes[i]) > classVotes.get(resultClass)) {
                                        resultClass = (float) classes[i];
                                }
                                classProb.put((float) classes[i], classVotes.get(classes[i]) / totalWeight);
                        }

                        String results = " | Result " + resultClass + " | ";
                        for (int i=0; i<conf.getNoOfClasses(); i++) {
                                if (classProb.containsKey(i+1)) results += "Class " + (i+1) + " probability " + classProb.get(i+1) + " | ";
                                else results += "Class " + (i+1) + " probability 0.0 | ";
                        }
                        result.add(new String(id1.toString() + results));

		}

		return result;
	}
}
