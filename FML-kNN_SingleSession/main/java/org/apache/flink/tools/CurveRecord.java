package org.apache.flink.tools;

import java.io.Serializable;

public class CurveRecord implements Serializable {

	private static final long serialVersionUID = 1L;
	private String first;
	private String second;
	private Integer third;
	private Integer fourth;
	private Float classOrValue;

    public CurveRecord() {}

	public CurveRecord(String first, String id, Integer third, Integer fourth, Float classOrValue) {
		this.first = first;
		this.second = id;
		this.third = third;
		this.fourth = fourth;
		this.classOrValue = classOrValue;
	}

	public String getFirst() { return first; }

	public void setFirst(String first) { this.first = first; }

	public String getSecond() { return second; }

	public void setSecond(String second) { this.second = second; }

	public Integer getThird() { return third; }

	public void setThird(Integer third) { this.third = third; }

	public Integer getFourth() { return fourth;	}

	public void setFourth(Integer fourth) { this.fourth = fourth; }

	public Float getClassOrValue() { return classOrValue; }

	public void setClassOrValue(Float classOrValue) { this.classOrValue = classOrValue;	}

	public String toString() {
		return first + " " + second.toString() + " " + third.toString() + " "
				+ fourth.toString() + " " + classOrValue.toString();
	}
}
