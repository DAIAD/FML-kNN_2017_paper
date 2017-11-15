package org.apache.spark.phases;

import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;

import java.util.Comparator;
import java.io.Serializable;
import java.io.InputStreamReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.commons.io.FileUtils;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.tools.ExecConf;
import org.apache.spark.tools.Functions;
import org.apache.spark.transformations.MapPhase2;
import org.apache.spark.transformations.ReducePhase2;
import org.apache.spark.values.Phase1_2Value;

import scala.Tuple2;
import scala.Tuple4;

public class Phase2 {

	public static void main(String[] args) throws Exception {

	ExecConf conf = new ExecConf();
	// Read arguments and configure
		 if (args.length > 0) {
			try { conf.setKnn(Integer.parseInt(args[0]));
			 	conf.setShift(Integer.parseInt(args[1]));
				conf.setDimension(Integer.parseInt(args[2]));
		 		conf.setNumOfPartition(Integer.parseInt(args[3]));
		 		conf.setHilbertOrZ(Boolean.parseBoolean(args[4])); } catch (Exception e) { System.out.println("Error! Please check arguments.");
		 		System.exit(0);
			}
		}

	Path fpt = new Path(conf.getPath() + "Results_Phase2");
       	FileSystem fs = fpt.getFileSystem(new Configuration());
        fs.delete(fpt, true);

	int[][] shiftvectors = new int[conf.getShift()][conf.getDimension()];
        Path pt = new Path(conf.getPath() + "RandomShiftVectors");
        fs = pt.getFileSystem(new Configuration());
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
        int j = 0;

        while(true) {
                String l = br.readLine();
                if ((l==null) || (j>conf.getShift()-1)) break;
                        String[] p = l.split(" ");
                for (int i = 0 ; i < conf.getDimension(); i++)
                        shiftvectors[j][i] = Integer.valueOf(p[i]);
                j++;
        }

        br.close();
        conf.setShiftvectors(shiftvectors);

	SparkConf config = new SparkConf().setAppName("S-kNN_Ph2").setMaster("yarn-client");
	config.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
	Class[] classes = new Class[3];
        classes[0] = Phase1_2Value.class;
	classes[1] = MyPartitioner.class;
	classes[2] = MyComparator.class;
        config.registerKryoClasses(classes);
	JavaSparkContext sc = new JavaSparkContext(config);

	/******** PHASE 2 ***********/
	long startExecuteTime = System.currentTimeMillis();

	JavaRDD<String> inputPh2 = sc.textFile(conf.getPath() + "Results_Phase1", 16);

	/////////// MAP
	JavaPairRDD<Tuple2<Integer, String>, Phase1_2Value> map2 = inputPh2.flatMapToPair(new MapPhase2(conf));

        /////////// REDUCE
	JavaRDD<Tuple4<String, String, String, String>> resultPhase2 = map2.repartitionAndSortWithinPartitions(new MyPartitioner(conf), new MyComparator()).mapPartitions(new ReducePhase2(conf));

	resultPhase2.saveAsTextFile(conf.getPath() + "Results_Phase2");

	sc.close();
	long totalElapsedExecuteTime = System.currentTimeMillis() - startExecuteTime;

                // Count execution time
                int ExecuteMillis = (int) totalElapsedExecuteTime % 1000;
                int ExecuteSeconds = (int) (totalElapsedExecuteTime / 1000) % 60;
                int ExecuteMinutes = (int) ((totalElapsedExecuteTime / (1000 * 60)) % 60);
                int ExecuteHours = (int) ((totalElapsedExecuteTime / (1000 * 60 * 60)) % 24);
                System.out.println("Thread " + Thread.currentThread().getId()
                                                        + " total time: " + ExecuteHours + "h " + ExecuteMinutes
                                                        + "m " + ExecuteSeconds + "sec " + ExecuteMillis + "mil");

	}

    private static class MyPartitioner extends Partitioner implements Serializable {

        ExecConf conf;
        public MyPartitioner(ExecConf conf) {
            this.conf = conf;
        }

        @Override
        public int numPartitions() {
            return conf.getShift()*conf.getNumOfPartition();
        }

        @Override
        public int getPartition(Object o) {
            Tuple2<Integer, String> key = (Tuple2<Integer, String>) o;
            return key._1()%numPartitions();
        }
    }

    private static class MyComparator implements Comparator, Serializable {

        @Override
        public int compare(Object o1, Object o2) {
            Tuple2<Integer, String> s1 = (Tuple2<Integer, String>) o1;
            Tuple2<Integer, String> s2 = (Tuple2<Integer, String>) o2;

            int cmp = s1._2().compareTo(s2._2());
            if( cmp != 0 ) return cmp;
            cmp = s1._1().toString().compareTo(s2._1().toString());

            return cmp;
        }
    }

}
