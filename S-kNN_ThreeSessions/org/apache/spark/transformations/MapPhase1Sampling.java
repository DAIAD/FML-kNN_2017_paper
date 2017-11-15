package org.apache.spark.transformations;

import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.tools.ExecConf;
import org.apache.spark.values.Phase1_2Value;
import scala.Tuple2;

import java.io.*;
import java.util.LinkedList;
import java.util.Random;

/**
 * Created by whashnez on 21/03/16.
 */
public class MapPhase1Sampling implements PairFlatMapFunction<Tuple2<Integer, Phase1_2Value>, Integer, Phase1_2Value> {

    private ExecConf conf;
    private Random r;
    private double sampleRate;

    public MapPhase1Sampling(ExecConf conf) {
	r = new Random();
        this.conf= conf;
    }

    @Override
    public Iterable<Tuple2<Integer, Phase1_2Value>> call(Tuple2<Integer, Phase1_2Value> value) throws Exception {

	LinkedList<Tuple2<Integer, Phase1_2Value>> result = new LinkedList<>();
        if (value._2.getThird() == 0)
            sampleRate = conf.getSampleRateOfR();
        else if (value._2.getThird() == 1)
            sampleRate = conf.getSampleRateOfS();
        else {
            System.out.println("Wrong source file!");
            System.exit(-1);
        }

        boolean sampled = false;
        if (r.nextDouble() < sampleRate)
            sampled = true;
        if (sampled) {
            result.add(value);
        }

        return result;
    }
}
