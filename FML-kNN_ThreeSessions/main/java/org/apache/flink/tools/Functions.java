package org.apache.flink.tools;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Writer;
import java.io.LineNumberReader;
import java.math.BigInteger;
import java.util.Collections;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

import org.apache.flink.shaded.com.google.common.collect.Lists;

public class Functions {

	public static void createAlicanteDatasetsRandom(String alicanteFilename, ExecConf conf) throws IOException {

               	InputStream in = new BufferedInputStream(new FileInputStream(alicanteFilename));
                BufferedReader br = new BufferedReader(new InputStreamReader(in));

                Path rPt = new Path(conf.getPath() + "datasets/R");
                Path sPt = new Path(conf.getPath() + "datasets/S");

                FileSystem fs = rPt.getFileSystem();
                fs.delete(rPt, true);
                BufferedWriter rBw = new BufferedWriter(new OutputStreamWriter(fs.create(rPt, true)));

                fs = sPt.getFileSystem();
                fs.delete(sPt, true);
                BufferedWriter sBw = new BufferedWriter(new OutputStreamWriter(fs.create(sPt, true)));

		LineNumberReader lnr = new LineNumberReader(br);
	        lnr.skip(Long.MAX_VALUE);
        	double noOfLines = lnr.getLineNumber() + 1;

	        int counter = 0;
	        in = new BufferedInputStream(new FileInputStream(alicanteFilename));
        	br = new BufferedReader(new InputStreamReader(in));

		// Throw first line
        	String line;
	        br.readLine();
        	while ((line = br.readLine()) != null) {
	            if (counter < ((9*noOfLines)/10)) {
        	        sBw.write(line + "\n");
	            } else {
        	        rBw.write(line + "\n");
	            }
        	    counter++;
	        }

        	rBw.close();
	        sBw.close();
	        br.close();
	        lnr.close();
        }

	// Method that generates the random shift vectors and stores them to file
	public static void genRandomShiftVectors(String filename, int dimension, int shift) throws IOException {

		Random r = new Random();
		int[][] shiftvectors = new int[shift][dimension];

		// Generate random shift vectors
		for (int i = 0; i < shift; i++) {
			shiftvectors[i] = createShift(dimension, r, true);
		}

		File vectorFile = new File(filename);
		if(!vectorFile.exists()) {
			vectorFile.createNewFile();
		}

		OutputStream outputStream = new FileOutputStream(filename);
		Writer writer = new OutputStreamWriter(outputStream);

		for (int j = 0; j < shift; j ++)  {
			String shiftVector = "";
			for (int k = 0; k < dimension; k++)
				shiftVector += Integer.toString(shiftvectors[j][k]) + " ";
				writer.write(shiftVector + "\n");
		}
		writer.close();
	}

	// Pad a String
	public static String createExtra(int num) {
		if( num < 1 ) return "";

		char[] extra = new char[num];
		for (int i = 0; i < num; i++)
			extra[i] = '0';
		return (new String(extra));
	}

	public static int[] createShift(int dimension, Random rin, boolean shift) {
		Random r = rin;
		int[] rv = new int[dimension];  // random vector

		if (shift) {
			for (int i = 0; i < dimension; i++) {
				rv[i] = ((int) Math.abs(r.nextInt(10001)));
			}
		} else {
			for (int i = 0; i < dimension; i++)
				rv[i] = 0;
		}

		return rv;
	}

	public static int maxDecDigits( int dimension ) {
		int max = 32;
		BigInteger maxDec = new BigInteger( "1" );
		maxDec = maxDec.shiftLeft( dimension * max );
		maxDec.subtract( BigInteger.ONE );
		return maxDec.toString().length();
	}

	public static String maxDecString( int dimension ) {
		int max = 32;
		BigInteger maxDec = new BigInteger( "1" );
		maxDec = maxDec.shiftLeft( dimension * max );
		maxDec.subtract( BigInteger.ONE );
		return maxDec.toString();
	}

	public static double calculateAccuracy(ExecConf conf) throws NumberFormatException, IOException	{

		Path pt = new Path(conf.getPath() + "datasets/R");
                FileSystem fs = pt.getFileSystem();
                BufferedReader rBr = new BufferedReader(new InputStreamReader(fs.open(pt)));
		HashMap<String, Float> real = new HashMap<String, Float>();

		String lineR;
		while((lineR = rBr.readLine()) != null) {
			String[] partsR = lineR.split(",");
			real.put(partsR[0], Float.parseFloat(partsR[conf.getDimension()+2]));
		}

		int i = 1;
		String lineResult;
		BufferedReader resultBr = null;
		int total_correct = 0;
		while (true) {
			try {
				pt = new Path(conf.getPath() + "ClassificationResults/" + i);
				fs = pt.getFileSystem();
				resultBr = new BufferedReader(new InputStreamReader(fs.open(pt)));
			} catch (Exception e) {
				break;
			}

			while ((lineResult = resultBr.readLine()) != null) {
				String[] partsResult = lineResult.split(" +");
				int k = 0;
				try {
					k = partsResult[0].indexOf(":");
					if (Float.parseFloat(partsResult[3]) ==	real.get(partsResult[0].substring(0, k))) {
						total_correct++;
					}
				} catch (Exception e) {
					System.out.println("Error in item " + partsResult[0].substring(0, k));
				}
			}

			i++;
			resultBr.close();
		}

		resultBr.close();
		rBr.close();
		return 100.0*total_correct/real.size();
	}

	/**
	 * To calculate the index of the estimator for i-th q-quantiles
	 * in a given sampled data set (size).
	 */
	// !! sampleRate is different for R and S
	public static int getEstimatorIndex(int i, int size, double sampleRate, int numOfPartition)
	{
		double iquantile = (i * 1.0 / numOfPartition);
		int orgRank = (int) Math.ceil((iquantile * size));
		int estRank = 0;

		int val1  = (int) Math.floor(orgRank * sampleRate);
		int val2  = (int) Math.ceil(orgRank * sampleRate);

		int est1 = (int) (val1 * (1 / sampleRate));
		int est2 = (int) (val2 * (1 / sampleRate));

		int dist1 = (int) Math.abs(est1 - orgRank);
		int dist2 = (int) Math.abs(est2 - orgRank);

		if (dist1 < dist2)
			estRank = val1;
		else
			estRank = val2;

		return estRank;
	}

	// Method to count the time measured for each thread
	public static void countTime(int phase) throws IOException {

		InputStream in = new BufferedInputStream(new FileInputStream("Timings_MapPhase" + phase));
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		Hashtable<Integer, Integer> entries1 = new Hashtable<Integer, Integer>();
		Hashtable<Integer, Integer> entries2 = new Hashtable<Integer, Integer>();

		while(true) {
			String l = br.readLine();
			if (l == null) break;
			String[] p = l.split(" ");
			try {
				int entry = entries1.get(Integer.parseInt(p[0]));
				entry += Integer.parseInt(p[1]);
				entries1.put(Integer.parseInt(p[0]), entry);
			} catch (Exception e) {
				entries1.put(Integer.parseInt(p[0]), Integer.parseInt(p[1]));
			}
		}

		br.close();
		in = new BufferedInputStream(new FileInputStream("Timings_ReducePhase" + phase));
		br = new BufferedReader(new InputStreamReader(in));

		while(true) {
			String l = br.readLine();
			if (l == null) break;
			String[] p = l.split(" ");
			try {
				int entry = entries2.get(Integer.parseInt(p[0]));
				entry += Integer.parseInt(p[1]);
				entries2.put(Integer.parseInt(p[0]), entry);
			} catch (Exception e) {
				entries2.put(Integer.parseInt(p[0]), Integer.parseInt(p[1]));
			}
		}
		br.close();

		for (int key : entries1.keySet()) {

			int totalElapsedExecuteTime = entries1.get(key);
		    int ExecuteMillis = (int) totalElapsedExecuteTime % 1000;
		 	int ExecuteSeconds = (int) (totalElapsedExecuteTime / 1000) % 60 ;
		 	int ExecuteMinutes = (int) ((totalElapsedExecuteTime / (1000*60)) % 60);
		 	int ExecuteHours   = (int) ((totalElapsedExecuteTime / (1000*60*60)) % 24);
		 	System.out.println("Map phase " + phase + ": Thread " + key + " time: " +  ExecuteHours + "h " + ExecuteMinutes + "m " + ExecuteSeconds + "sec " + ExecuteMillis + "mil");
		}

		for (int key : entries2.keySet()) {

			int totalElapsedExecuteTime = entries2.get(key);
		    int ExecuteMillis = (int) totalElapsedExecuteTime % 1000;
		 	int ExecuteSeconds = (int) (totalElapsedExecuteTime / 1000) % 60 ;
		 	int ExecuteMinutes = (int) ((totalElapsedExecuteTime / (1000*60)) % 60);
		 	int ExecuteHours   = (int) ((totalElapsedExecuteTime / (1000*60*60)) % 24);
		 	System.out.println("Reduce phase " + phase + ": Thread " + key + " time: " +  ExecuteHours + "h " + ExecuteMinutes + "m " + ExecuteSeconds + "sec " + ExecuteMillis + "mil");
		}
	}
}
