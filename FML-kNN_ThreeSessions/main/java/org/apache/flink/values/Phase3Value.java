package org.apache.flink.values;

import java.io.*;

public class Phase3Value implements Serializable {

	private static final long serialVersionUID = 1L;
	private String first;
	private Float second;
	private Integer demandClass;

	public Phase3Value() {
	}

	public Phase3Value(String f1, Float second, Integer demandClass) {
		this.first = f1;
		this.second = second;
		this.demandClass = demandClass;
	}

	public String getFirst() {
		return first;
	}

	public Float getSecond() {
		return second;
	}

	public Integer getdemandClass() {
		return demandClass;
	}

	public void setFirst(String first) {
		this.first = first;
	}

	public void setSecond(Float second) {
		this.second = second;
	}

	public void setdemandClass(Integer demandClass) {
		this.demandClass = demandClass;
	}

	public String toString() {
		return first + " " + second.toString() + " " + demandClass.toString();
	}
}
