package org.apache.flink.tools;

import java.io.Serializable;

public class KnnRecord implements Serializable {

	private static final long serialVersionUID = 1L;
	private String rid;
	private float dist;
	private int[] coord;

	public KnnRecord(String rid, float dist, int[] coord) {
		this.rid = rid;
		this.dist = dist;
		this.coord = coord;
	}

	public float getDist() {
		return this.dist;
	}

	public String getRid() {
		return this.rid;
	}

	public String toString() {
		return this.rid + " " + Float.toString(this.dist);
	}

	public int[] getCoord() {
		return coord;
	}

	public void setCoord(int[] coord) {
		this.coord = coord;
	}
}
