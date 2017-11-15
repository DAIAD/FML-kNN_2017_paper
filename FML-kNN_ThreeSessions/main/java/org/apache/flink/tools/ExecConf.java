package org.apache.flink.tools;

import java.io.*;

public class ExecConf implements Serializable {

	private int knn = 15;
	private int shift = 2;
	private int nr = 27296;
	private int ns = 244169;
	private int dimension = 9;
	private int noOfClasses = 2;
	private double epsilon = 0.003;
	private int numOfPartition = 8;
	private boolean hilbertOrZ = false;
	private int[] scale = { 1, 1, 1, 1, 1, 1, 1, 1, 1 };
	private int[][] shiftvectors;
	private double sampleRateOfR;
        private double sampleRateOfS;
	private String path = "hdfs://localhost:9000/user/chgeorgakidis/";

	public ExecConf() {}

	public double getSampleRateOfR() { return sampleRateOfR; }

        public void setSampleRateOfR(double sampleRateOfR) { this.sampleRateOfR = sampleRateOfR; }

        public double getSampleRateOfS() { return sampleRateOfS; }

        public void setSampleRateOfS(double sampleRateOfS) { this.sampleRateOfS = sampleRateOfS; }

	public int getKnn() {
		return knn;
	}

	public void setKnn(int knn) {
		this.knn = knn;
	}

	public int getShift() {
		return shift;
	}

	public void setShift(int shift) {
		this.shift = shift;
	}

	public int getNr() {
		return nr;
	}

	public void setNr(int nr) {
		this.nr = nr;
	}

	public int getNs() {
		return ns;
	}

	public void setNs(int ns) {
		this.ns = ns;
	}

	public int getDimension() {
		return dimension;
	}

	public void setDimension(int dimension) {
		this.dimension = dimension;
	}

	public int getNoOfClasses() {
		return noOfClasses;
	}

	public void setNoOfClasses(int noOfClasses) {
		this.noOfClasses = noOfClasses;
	}

	public double getEpsilon() {
		return epsilon;
	}

	public void setEpsilon(double epsilon) {
		this.epsilon = epsilon;
	}

	public int getNumOfPartition() {
		return numOfPartition;
	}

	public void setNumOfPartition(int numOfPartition) {
		this.numOfPartition = numOfPartition;
	}

	public boolean isHilbertOrZ() {
		return hilbertOrZ;
	}

	public void setHilbertOrZ(boolean hilbertOrZ) {
		this.hilbertOrZ = hilbertOrZ;
	}

	public int[] getScale() {
		return scale;
	}

	public void setScale(int[] scale) {
		this.scale = scale;
	}

	public int[][] getShiftvectors() {
		return shiftvectors;
	}

	public void setShiftvectors(int[][] shiftvectors) {
		this.shiftvectors = shiftvectors;
	}

	public String getPath() {
		return path;
	}

	public void setPath(String path) {
		this.path = path;
	}
}
