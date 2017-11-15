package org.apache.flink.tools;

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

import java.io.*;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.Random;

public class Functions {

    /*******************************************
     * This class contains various functions that
     * are required throughout the code.
     *******************************************/

    // Method that generates the random shift vectors and stores them to file
	/** source: http://www.cs.utah.edu/~lifeifei/knnj/#codes **/
	public static void genRandomShiftVectors(String filename, int dimension, int shift) throws IOException {

		Random r = new Random();
		int[][] shiftvectors = new int[shift][dimension];

		// Generate random shift vectors
		for (int i = 0; i < shift; i++) {
			shiftvectors[i] = createShift(dimension, r, true);
		}

		Path pt = new Path(filename);
 		FileSystem fs = pt.getFileSystem();
        	fs.delete(pt, true);
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fs.create(pt, true)));

		for (int j = 0; j < shift; j++) {
			String shiftVector = "";
			for (int k = 0; k < dimension; k++)
				shiftVector += Integer.toString(shiftvectors[j][k]) + " ";
			bw.write(shiftVector + "\n");
		}
		bw.close();
	}

	/** source: http://www.cs.utah.edu/~lifeifei/knnj/#codes **/
	public static String createExtra(int num) {
		if (num < 1)
			return "";

		char[] extra = new char[num];
		for (int i = 0; i < num; i++)
			extra[i] = '0';
		return (new String(extra));
	}

	/** source: http://www.cs.utah.edu/~lifeifei/knnj/#codes **/
	public static int[] createShift(int dimension, Random rin, boolean shift) {
		Random r = rin;
		int[] rv = new int[dimension]; // random vector

		if (shift) {
			for (int i = 0; i < dimension; i++) {
				rv[i] = (Math.abs(r.nextInt(10001)));
			}
		} else {
			for (int i = 0; i < dimension; i++)
				rv[i] = 0;
		}
		return rv;
	}

	/** source: http://www.cs.utah.edu/~lifeifei/knnj/#codes **/
	public static int getEstimatorIndex(int i, int size, double sampleRate,
			int numOfPartition) {
		double iquantile = (i * 1.0 / numOfPartition);
		int orgRank = (int) Math.ceil((iquantile * size));
		int estRank = 0;

		int val1 = (int) Math.floor(orgRank * sampleRate);
		int val2 = (int) Math.ceil(orgRank * sampleRate);

		int est1 = (int) (val1 * (1 / sampleRate));
		int est2 = (int) (val2 * (1 / sampleRate));

		int dist1 = Math.abs(est1 - orgRank);
		int dist2 = Math.abs(est2 - orgRank);

		if (dist1 < dist2)
			estRank = val1;
		else
			estRank = val2;

		return estRank;
	}

	public static int maxDecDigits(int dimension) {
		int max = 32;
		BigInteger maxDec = new BigInteger("1");
		maxDec = maxDec.shiftLeft(dimension * max);
		maxDec.subtract(BigInteger.ONE);
		return maxDec.toString().length();
	}

	public static String maxDecString(int dimension) {
		int max = 32;
		BigInteger maxDec = new BigInteger("1");
		maxDec = maxDec.shiftLeft(dimension * max);
		maxDec.subtract(BigInteger.ONE);
		return maxDec.toString();
	}

	// Mutation function for the genetic algorithm
    	public static int[] mutate(int[] element, ExecConf conf, int step) {
        	Random rand = new Random();
        	int r = rand.nextInt(conf.getDimension());
        	if ((rand.nextDouble()<0.5) && element[r]>step-1) {
            	element[r] = element[r] - step;
	            return  element;
        	}
	        else if (element[r] < conf.getScaleDomain()[r]) {
        	    element[r] = element[r] + step;
	            return  element;
        	}
	        return element;
	}

    	// Crossover function for the genetic algorithm
	public static int[] crossover(int[] element1, int[] element2, ExecConf conf) {
        	Random rand = new Random();
	        int r = rand.nextInt(((conf.getDimension()-2) - 1) + 1) + 1;
        	int[] result = new int[conf.getDimension()];
	        for (int i=0; i<r; i++) {
        	    result[i] = element1[i];
	        }
        	for (int i=r; i<conf.getDimension(); i++) {
	            result[i] = element2[i];
        	}
	        return result;
    	}

	/**************** METRICS ****************/
	// Accuracy
	public static double calculateAccuracy(ExecConf conf)
			throws NumberFormatException, IOException {

		Path pt = new Path(conf.getHdfsPath() + "datasets/RClassification");
		FileSystem fs = pt.getFileSystem();
		BufferedReader rBr = new BufferedReader(new InputStreamReader(fs.open(pt)));
		HashMap<String, Float> real = new HashMap<>();

		String lineR;
		while ((lineR = rBr.readLine()) != null) {
			String[] partsR = lineR.split(",");
			real.put(partsR[0], Float.parseFloat(partsR[conf.getClassDim()+1]));
		}

		int i = 1;
		String lineResult;
		BufferedReader resultBr = null;
		int total_correct = 0;
		while (true) {
			try {
				pt = new Path(conf.getHdfsPath() + "ClassificationResults/" + i);
				fs = pt.getFileSystem();
				resultBr = new BufferedReader(
						new InputStreamReader(fs.open(pt)));
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
					System.out.println("Error in item " + partsResult[0]);
				}
			}

			i++;
			resultBr.close();
		}

		resultBr.close();
		rBr.close();
		return 100.0 * total_correct / real.size();
	}

	// F-Measure
	public static double calculateFMeasure(ExecConf conf)
			throws NumberFormatException, IOException {

		Path pt = new Path(conf.getHdfsPath() + "datasets/RClassification");
		FileSystem fs = pt.getFileSystem();
		BufferedReader rBr = new BufferedReader(new InputStreamReader(fs.open(pt)));
		HashMap<String, Float> real = new HashMap<>();

		String lineR;
		while ((lineR = rBr.readLine()) != null) {
			String[] partsR = lineR.split(",");
			real.put(partsR[0], Float.parseFloat(partsR[conf.getClassDim()+1]));
		}

		double truePositive = 0, falsePositive = 0, falseNegative = 0;
		BufferedReader resultBr;
		String lineResult;
		int i = 1;
		while (true) {
			try {
				pt = new Path(conf.getHdfsPath() + "ClassificationResults/" + i);
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
					if ((Float.parseFloat(partsResult[3]) == 2) && (real.get(partsResult[0].substring(0, k)) == 2)) {
						truePositive++;
					}
					else if ((Float.parseFloat(partsResult[3]) == 2) && (real.get(partsResult[0].substring(0, k)) == 1)){
						falsePositive++;
					}
					else if ((Float.parseFloat(partsResult[3]) == 1) && (real.get(partsResult[0].substring(0, k)) == 2)){
						falseNegative++;
					}
				} catch (Exception e) {
					System.out.println("Error in item " + partsResult[0]);
				}
			}

			i++;
			resultBr.close();
		}

		double precision = truePositive/(truePositive+falsePositive);
		double recall = truePositive/(truePositive+falseNegative);
		double fmeasure = 2*((precision*recall)/(precision+recall));
		return fmeasure;
	}

    // Root Mean Squared Error (RMSE)
    public static double calculateRMSE(ExecConf conf) throws NumberFormatException, IOException {

        Path pt = new Path(conf.getHdfsPath() + "datasets/RRegression");
        FileSystem fs = pt.getFileSystem();
        BufferedReader rBr = new BufferedReader(new InputStreamReader(fs.open(pt)));
        HashMap<String, Float> real = new HashMap<>();

        String lineR;
        while ((lineR = rBr.readLine()) != null) {
            String[] partsR = lineR.split(",");
            real.put(partsR[0], Float.parseFloat(partsR[conf.getValueDim()+1]));
        }

        BufferedReader resultBr;
        String lineResult;
        int lineCount = 0;
        double totalSquaredError = 0.0;

        // Count the RMSE for the regression itself
        int i = 1;
		while (true) {
			try {
				pt = new Path(conf.getHdfsPath() + "RegressionResults/" + i);
				fs = pt.getFileSystem();
				resultBr = new BufferedReader(new InputStreamReader(fs.open(pt)));
			} catch (Exception e) {
				break;
			}

			while ((lineResult = resultBr.readLine()) != null) {
				String[] partsResult = lineResult.split(" +");
				int k = partsResult[0].indexOf(":");
				/** For Alicante case study only: Calculate RMSE only for elements that are non-zero
        		         This is for the cases that the Regression is performed
	                	 on the results of classification. The classifier
	                	 will naturaly have mistaken several values. We should not take
        		         under consideration those values. Only the correct
	        	         classification guesses will indicate the regression's performance.
                		 Future work: In case of pure regression the following if statement
	        	         is obsolete and should be removed. **/
		                if (real.get(partsResult[0].substring(0, k)) != 0) {
					totalSquaredError += Math.pow((Float.parseFloat(partsResult[3]) - real.get(partsResult[0].substring(0, k))), 2);
					lineCount++;
				}
			}

			i++;
			resultBr.close();
		}

        /*
        // Count the RMSE for the final time series
        pt = new Path(conf.getHdfsPath() + "FinalResults");
        fs = pt.getFileSystem();
        resultBr = new BufferedReader(new InputStreamReader(fs.open(pt)));

        while ((lineResult = resultBr.readLine()) != null) {
            String[] partsResult = lineResult.split(" +");
            try {totalSquaredError += Math.pow((Float.parseFloat(partsResult[1]) - real.get(partsResult[0])), 2);}
            catch (Exception e) {
                System.out.println();
            }
            lineCount++;
        }
        */

        totalSquaredError = totalSquaredError/lineCount;
        return Math.sqrt(totalSquaredError);
    }

    // Coefficient of Determination (R^2)
    public static double calculateRSquared(ExecConf conf) throws NumberFormatException, IOException {

        Path pt = new Path(conf.getHdfsPath() + "datasets/RRegression");
        FileSystem fs = pt.getFileSystem();
        BufferedReader rBr = new BufferedReader(new InputStreamReader(fs.open(pt)));
        HashMap<String, Float> real = new HashMap<>();

        String lineR;
        double totalReal = 0.0;
        int counter = 0;
        while ((lineR = rBr.readLine()) != null) {
            String[] partsR = lineR.split(",");
            real.put(partsR[0], Float.parseFloat(partsR[conf.getValueDim()+1]));
            totalReal += Float.parseFloat(partsR[conf.getValueDim()+1]);
            counter++;
        }
        double mean = totalReal / counter;

        BufferedReader resultBr;
        String lineResult;
        double totalSumSquares = 0.0;
        double regresSumSquares = 0.0;

        // Count the R^2 for the regression itself
        int i = 1;
		while (true) {
			try {
				pt = new Path(conf.getHdfsPath() + "RegressionResults/" + i);
				fs = pt.getFileSystem();
				resultBr = new BufferedReader(new InputStreamReader(fs.open(pt)));
			} catch (Exception e) {
				break;
			}

			while ((lineResult = resultBr.readLine()) != null) {
				String[] partsResult = lineResult.split(" +");
				int k = partsResult[0].indexOf(":");
				/** For Alicante case study only: Calculate R^2 only for elements that are non-zero
		                 This is for the cases that the Regression is performed
                		 on the results of classification. The classifier
		                 will naturaly have mistaken several values. We should not take
                		 under consideration those values. Only the correct
		                 classification guesses will indicate the regression's performance.
                		 Future work: In case of pure regression the following if statement
		                 is obsolete and should be removed. **/
                		if (real.get(partsResult[0].substring(0, k)) != 0) {
					regresSumSquares += Math.pow((Float.parseFloat(partsResult[3]) - mean), 2);
					totalSumSquares += Math.pow((real.get(partsResult[0].substring(0, k)) - mean), 2);
				}
			}

			i++;
			resultBr.close();
		}

        /*
        // Count the R^2 for the final time series
        pt = new Path(conf.getHdfsPath() + "FinalResults");
        fs = pt.getFileSystem();
        resultBr = new BufferedReader(new InputStreamReader(fs.open(pt)));

        while ((lineResult = resultBr.readLine()) != null) {
            String[] partsResult = lineResult.split(" +");
            regresSumSquares += Math.pow((Float.parseFloat(partsResult[1]) - mean), 2);
            totalSumSquares += Math.pow((real.get(partsResult[0]) - mean), 2);
        }
        */

        double rSquared = 1 - (regresSumSquares/totalSumSquares);
        return rSquared;
    }

    /*******************************************
     * The Following methods related to dataset
     * generation are only used for experimentation
     * on the water consumption data. A final
     * generic version of the algorithm will
     * not contain these methods. The user should
     * be responsible for creating the R and S
     * datasets and storing them to the proper
     * file and with the proper filename
     *******************************************/

   public static void createDatasetsSpecific(String alicanteFilename, ExecConf conf) throws IOException {

        Path rPt = new Path(conf.getHdfsPath() + "datasets/RClassification");
        Path sPt = new Path(conf.getHdfsPath() + "datasets/SClassification");

        FileSystem fs = rPt.getFileSystem();
        fs.delete(rPt, true);
        BufferedWriter rBw = new BufferedWriter(new OutputStreamWriter(fs.create(rPt, true)));

        fs = sPt.getFileSystem();
        fs.delete(sPt, true);
        BufferedWriter sBw = new BufferedWriter(new OutputStreamWriter(fs.create(sPt, true)));

        InputStream in = new BufferedInputStream(new FileInputStream(alicanteFilename));
        BufferedReader br = new BufferedReader(new InputStreamReader(in));

        // Throw first line
        String line;
        br.readLine();
        while ((line = br.readLine()) != null) {
            String parts[] = line.split(",");
		if (//parts[0].substring(0,11).equals("C11FA586148") &&
                    parts[0].substring(12,16).equals("2014") &&
                    parts[0].substring(17,19).equals("06") &&
                    Integer.parseInt(parts[0].substring(20,22)) >= 16 &&
                    Integer.parseInt(parts[0].substring(20,22)) <= 30) {
                rBw.write(line + "\n");
            } else {
                sBw.write(line + "\n");
            }
        }

        rBw.close();
        sBw.close();
        br.close();

    }

    public static void createDatasetsClassification(String alicanteFilename, ExecConf conf) throws IOException {

        Path rPt = new Path(conf.getHdfsPath() + "datasets/RClassification");
        Path sPt = new Path(conf.getHdfsPath() + "datasets/SClassification");

        FileSystem fs = rPt.getFileSystem();
        fs.delete(rPt, true);
        BufferedWriter rBw = new BufferedWriter(new OutputStreamWriter(fs.create(rPt, true)));

        fs = sPt.getFileSystem();
        fs.delete(sPt, true);
        BufferedWriter sBw = new BufferedWriter(new OutputStreamWriter(fs.create(sPt, true)));

        InputStream in = new BufferedInputStream(new FileInputStream(alicanteFilename));
        BufferedReader br = new BufferedReader(new InputStreamReader(in));
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
        //lnr.close();
    }

    public static void createDatasetsRegression(ExecConf conf) throws IOException {

        Path rPt = new Path(conf.getHdfsPath() + "datasets/RRegression");
        Path sPt = new Path(conf.getHdfsPath() + "datasets/SRegression");
        Path unifiedNonZero = new Path(conf.getHdfsPath() + "datasets/UnifiedNonZeroDataset");

        FileSystem fs = rPt.getFileSystem();
        fs.delete(rPt, true);
        BufferedWriter rBw = new BufferedWriter(new OutputStreamWriter(fs.create(rPt, true)));

        fs = sPt.getFileSystem();
        fs.delete(sPt, true);
        BufferedWriter sBw = new BufferedWriter(new OutputStreamWriter(fs.create(sPt, true)));

        fs = unifiedNonZero.getFileSystem();
        fs.delete(unifiedNonZero, true);
        BufferedWriter unifiedNonZeroWriter = new BufferedWriter(new OutputStreamWriter(fs.create(unifiedNonZero, true)));

        Path pt;
        int i = 1;
        BufferedReader resultBr;
        String lineResult;
        HashMap<String, Integer> results = new HashMap<>();

        while (true) {
            try {
                pt = new Path(conf.getHdfsPath() + "ClassificationResults/" + i);
                fs = pt.getFileSystem();
                resultBr = new BufferedReader(new InputStreamReader(fs.open(pt)));
            } catch (Exception e) {
                break;
            }

            while ((lineResult = resultBr.readLine()) != null) {
                String[] partsResult = lineResult.split(" +");
                if (Float.parseFloat(partsResult[3]) == 2) {
                    int k = partsResult[0].indexOf(":");
                    results.put(partsResult[0].substring(0, k), 1);
                }
            }

            i++;
            resultBr.close();
        }

        pt = new Path(conf.getHdfsPath() + "datasets/RClassification");
        fs = pt.getFileSystem();
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
        String line;

        while ((line = br.readLine()) != null) {
            String[] partsLine = line.split(",+");
            if(results.get(partsLine[0]) != null) {
                // Write non-zero consumption elements to the unified file for cross-validation reasons
//                if(Float.parseFloat(partsLine[conf.getValueDim()+2]) == 2) {
			rBw.write(line + "\n");
	        	unifiedNonZeroWriter.write(line + "\n");
//                }
            }
        }

        pt = new Path(conf.getHdfsPath() + "datasets/SClassification");
        fs = pt.getFileSystem();
        br = new BufferedReader(new InputStreamReader(fs.open(pt)));

        while ((line = br.readLine()) != null) {
            String[] partsLine = line.split(",+");
            if(Float.parseFloat(partsLine[conf.getValueDim()+2]) == 2) {
                sBw.write(line + "\n");
                // Write non-zero consumption elements to the unified file for cross-validation reasons
                unifiedNonZeroWriter.write(line + "\n");
            }
        }

        br.close();
        rBw.close();
        sBw.close();
        unifiedNonZeroWriter.close();
    }

    // Method that produces the final Time Series
    public static void produceFinalDataset(ExecConf conf) throws IOException {

        Path pt;
        int i = 1;
        BufferedReader resultBr;
        String lineResult;
        HashMap<String, Float> results = new HashMap<>();

        while (true) {
            try {
                pt = new Path(conf.getHdfsPath() + "RegressionResults/" + i);
                FileSystem fs = pt.getFileSystem();
                resultBr = new BufferedReader(new InputStreamReader(fs.open(pt)));
            } catch (Exception e) {
                break;
            }

            while ((lineResult = resultBr.readLine()) != null) {
                String[] partsResult = lineResult.split(" +");
                int k = partsResult[0].indexOf(":");
                results.put(partsResult[0].substring(0, k), Float.parseFloat(partsResult[3]));
            }

            i++;
            resultBr.close();
        }

        Path rPt = new Path(conf.getHdfsPath() + "datasets/RClassification");
        Path resPt = new Path(conf.getHdfsPath() + "FinalResults");
        FileSystem fsR = rPt.getFileSystem();
        FileSystem fsRes = rPt.getFileSystem();
        BufferedReader rBr = new BufferedReader(new InputStreamReader(fsR.open(rPt)));
        BufferedWriter resBw = new BufferedWriter(new OutputStreamWriter(fsRes.create(resPt, true)));

        String lineR;
        while ((lineR = rBr.readLine()) != null) {
            String[] partsR = lineR.split(",");
            if (results.get(partsR[0]) != null){
                resBw.write(partsR[0] + " " + results.get(partsR[0]) + "\n");
            }
            else {
                resBw.write(partsR[0] + " 0.0\n");
            }
        }

        resBw.close();
        rBr.close();
    }

    // Create datasets for cross-validation
    public static void createDatasetsCrossValid(
            String alicanteFilename, ExecConf conf, int fold,
            int requestedPartition) throws IOException {

	Path pt = new Path(alicanteFilename);
        FileSystem fs = pt.getFileSystem();
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));

        LineNumberReader lnr = new LineNumberReader(br);
        lnr.skip(Long.MAX_VALUE);
        int datasetSize = (lnr.getLineNumber() + 1);
        int partitionSize = datasetSize / fold;

        // Throw first line
	pt = new Path(alicanteFilename);
        fs = pt.getFileSystem();
        br = new BufferedReader(new InputStreamReader(fs.open(pt)));
        String line;
        br.readLine();

        Path rPt = null;
        Path sPt = null;
        if (conf.getClassifyOrRegress() == 1) {
            rPt = new Path(conf.getHdfsPath() + "datasets/RClassification");
            sPt = new Path(conf.getHdfsPath() + "datasets/SClassification");
        }
        else if (conf.getClassifyOrRegress() == 2) {
            rPt = new Path(conf.getHdfsPath() + "datasets/RRegression");
            sPt = new Path(conf.getHdfsPath() + "datasets/SRegression");
        }

        fs = rPt.getFileSystem();
        fs.delete(rPt, true);
        BufferedWriter rBw = new BufferedWriter(new OutputStreamWriter(
                fs.create(rPt, true)));

        fs = sPt.getFileSystem();
        fs.delete(sPt, true);
        BufferedWriter sBw = new BufferedWriter(new OutputStreamWriter(
                fs.create(sPt, true)));

        int counter = 0;
        int entryPoint = requestedPartition * partitionSize;
        int exitPoint = (requestedPartition + 1) * partitionSize;
        while ((line = br.readLine()) != null) {
            if ((counter >= entryPoint) && (counter < exitPoint)) {
                rBw.write(line + "\n");
                counter++;
                continue;
            }
            sBw.write(line + "\n");
            counter++;
        }

        lnr.close();
        rBw.close();
        sBw.close();
        br.close();
    }
}
