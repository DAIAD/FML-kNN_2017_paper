package org.apache.flink.tools;

import java.io.Serializable;

public class ExecConf implements Serializable {

	private static final long serialVersionUID = 1L;

	private int knn = 15;
	private int shift = 2;
	private int nr = 27296;
	private int ns = 244169;
	private int dimension = 9;
	private int classDim = 10;
	private int valueDim = 9;
	private int noOfClasses = 2;
	private double epsilon = 0.003;
	private int numOfPartition = 8;
	private int hOrZOrG = 2;
	private int classifyOrRegress = 1;
	private boolean crossValidation = false;
	private int crossValidationFolds = 10;
	private boolean geneticScaleCalc = false;
	private int[] scale = { 10, 9, 5, 5, 8, 3, 17, 8, 29 };
	private int[] scaleDomain = {10, 10, 10, 10, 10, 10, 50, 50, 50};
	private int[][] shiftVectors;
	private double sampleRateOfR;
        private double sampleRateOfS;
	private String hdfsPath = "hdfs://localhost:9000/user/chgeorgakidis/";
	private String localPath = "/home/local/chgeorgakidis/F-kNN/FML_kNN/";
	private String sourcesPath = "/home/local/chgeorgakidis/datasets/";

	public ExecConf() {
	}

	public double getSampleRateOfR() { return sampleRateOfR; }

        public void setSampleRateOfR(double sampleRateOfR) { this.sampleRateOfR = sampleRateOfR; }

        public double getSampleRateOfS() { return sampleRateOfS; }

        public void setSampleRateOfS(double sampleRateOfS) { this.sampleRateOfS = sampleRateOfS; }

	public int getKnn() { return knn; }

	public void setKnn(int knn) { this.knn = knn; }

	public int getShift() { return shift; }

	public void setShift(int shift) { this.shift = shift; }

	public int getNr() { return nr; }

	public void setNr(int nr) { this.nr = nr; }

	public int getNs() { return ns; }

	public void setNs(int ns) { this.ns = ns; }

	public int getDimension() { return dimension; }

	public void setDimension(int dimension) { this.dimension = dimension; }

	public int getNoOfClasses() { return noOfClasses; }

	public void setNoOfClasses(int noOfClasses) { this.noOfClasses = noOfClasses; }

	public double getEpsilon() { return epsilon; }

	public void setEpsilon(double epsilon) { this.epsilon = epsilon; }

	public int getNumOfPartition() { return numOfPartition; }

	public void setNumOfPartition(int numOfPartition) { this.numOfPartition = numOfPartition; }

	public int getHOrZOrG() { return hOrZOrG; }

	public void setHOrZOrG(int hOrZOrG) { this.hOrZOrG = hOrZOrG; }

	public int[] getScale() { return scale; }

	public void setScale(int[] scale) { this.scale = scale; }

	public int[][] getShiftVectors() { return shiftVectors; }

	public void setShiftVectors(int[][] shiftvectors) { this.shiftVectors = shiftvectors; }

	public String getHdfsPath() { return hdfsPath; }

	public void setHdfsPath(String hdfsPath) { this.hdfsPath = hdfsPath; }

	public String getLocalPath() { return localPath; }

	public void setLocalPath(String localPath) { this.localPath = localPath; }

	public String getSourcesPath() { return sourcesPath; }

	public void setSourcesPath(String sourcesPath) { this.sourcesPath = sourcesPath; }

	public int getClassDim() { return classDim; }

	public void setClassDim(int classDim) { this.classDim = classDim; }

	public int getClassifyOrRegress() { return classifyOrRegress; }

	public void setClassifyOrRegress(int classifyOrRegress) { this.classifyOrRegress = classifyOrRegress; }

	public int getValueDim() { return valueDim; }

	public void setValueDim(int valueDim) { this.valueDim = valueDim; }

	public int getCrossValidationFolds() { return crossValidationFolds; }

	public void setCrossValidationFolds(int crossValidationFolds) { this.crossValidationFolds = crossValidationFolds; }

	public boolean isCrossValidation() { return crossValidation; }

	public void setCrossValidation(boolean crossValidation) { this.crossValidation = crossValidation; }

	public boolean isGeneticScaleCalc() { return geneticScaleCalc; }

	public void setGeneticScaleCalc(boolean geneticScaleCalc) { this.geneticScaleCalc = geneticScaleCalc; }

	public int[] getScaleDomain() { return scaleDomain; }

	public void setScaleDomain(int[] scaleDomain) { this.scaleDomain = scaleDomain; }

}
