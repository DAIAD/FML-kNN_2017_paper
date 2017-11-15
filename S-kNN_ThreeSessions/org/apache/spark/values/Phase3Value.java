package org.apache.spark.values;

import java.io.*;
import java.io.Serializable;

public class Phase3Value implements Serializable {

	private static final long serialVersionUID = 1L;
        private String first;
        private Float second;
        private Float demandClass;

        public Phase3Value() {          
        }
        
        public Phase3Value(String f1, Float second, Float demandClass) {
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
        
        public Float getdemandClass() {
                return demandClass;
        }

        public void setFirst(String first) {
                this.first = first;
        }
        
        public void setSecond(Float second) {
                this.second = second;
        }

        public void setdemandClass(Float demandClass) {
                this.demandClass = demandClass;
        }
        
        public String toString() {
                return first + " " + second.toString() + " " + demandClass.toString();  
        }
}
