package gnuplot.dataflow.statistics;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class DataflowMeanStdDev {
	private static List<MapperDF> mapperDFList = new ArrayList<MapperDF>();
	private static List<ReducerDF> reducerDFList = new ArrayList<ReducerDF>();
	
	public static void main(String[] args) {

		String baseDir = "G:\\MR-MEM\\CompExperiments\\";
		String[] applications = {
			"SampleToBigWiki-m36-r18",
			"SampleToBigTeraSort-36GB",
			"SampleToBigBuildInvertedIndex",
			"SampleToBig-uservisits_aggre-pig-50G",
			"SampleToBigTwitterBiDirectEdgeCount"
		};
		
		String outputDir = baseDir + "graph\\";
		String mapperOutput = "mapperDfStat.txt";
		String reducerOutput = "reducerDfStat.txt";
		
		String appNames[] = {
				"WikiWordCount",
				"TeraSort",
				"BuildInvertedIndex",
				"UserVisits_aggre-pig",
				"TwitterBiEdgeCount"
		};
		
		String mapperM = "compDataflow" + File.separator + "statisticsMappersM.txt";
		String reducerM = "compDataflow" + File.separator + "statisticsReducersM.txt";
		
		String mapperX = "compDataflow" + File.separator + "statisticsMappersX.txt";
		String reducerX = "compDataflow" + File.separator + "statisticsReducersX.txt";
		
		for(int i = 0; i < applications.length; i++) {
			String file = applications[i];
			String appName = appNames[i];
			
			File dir = new File(baseDir + file);
			File mapperMFile = new File(dir, mapperM);
			File reducerMFile = new File(dir, reducerM);
			File mapperXFile = new File(dir, mapperX);
			File reducerXFile = new File(dir, reducerX);
			
			MapperDF mapperDF = new MapperDF(appName, mapperMFile, mapperXFile);
			ReducerDF reducerDF = new ReducerDF(appName, reducerMFile, reducerXFile);
			
			mapperDFList.add(mapperDF);
			reducerDFList.add(reducerDF);
			
			displayMapper(mapperDFList, outputDir + mapperOutput);
			displayReducer(reducerDFList, outputDir + reducerOutput);
		}
	}


	private static void displayMapper(List<MapperDF> mapperDFList, String mapperOutput) {
		File output = new File(mapperOutput);
		if(!output.exists())
			output.getParentFile().mkdirs();
		
		try {
			PrintWriter writer = new PrintWriter(new FileWriter(output));
		
			writer.println("#AppName" + "\t" 
					+ "xInMBm" + "\t" + "xInMBsd" + "\t" 
					+ "xOutMBm" + "\t" + "xOutMBsd" + "\t" 
					+ "xRawBMm" + "\t" + "xRawBMsd" + "\t"
					+ "xRawAMm" + "\t" + "xRawAMsd" + "\t"
					
					+ "mInMBm" + "\t" + "mInMBsd" + "\t" 
					+ "mOutMBm" + "\t" + "mOutMBsd" + "\t" 
					+ "mRawBMm" + "\t" + "mRawBMsd" + "\t"
					+ "mRawAMm" + "\t" + "mRawAMsd");
			for(MapperDF df : mapperDFList) {
				writer.println(df.getAppName() + "\t" 
						+ df.getxInMB().getMean() + "\t" + df.getxInMB().getSd() + "\t"
						+ df.getxOutMB().getMean() + "\t" + df.getxOutMB().getSd() + "\t"
						+ df.getxRawBM().getMean() + "\t" + df.getxRawBM().getSd() + "\t"
						+ df.getxRawAM().getMean() + "\t" + df.getxRawAM().getSd() + "\t"
						
						+ df.getmInMB().getMean() + "\t" + df.getmInMB().getSd() + "\t"
						+ df.getmOutMB().getMean() + "\t" + df.getmOutMB().getSd() + "\t"
						+ df.getmRawBM().getMean() + "\t" + df.getmRawBM().getSd() + "\t"
						+ df.getmRawAM().getMean() + "\t" + df.getmRawAM().getSd());
				
			}
			writer.close();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}
	
	private static void displayReducer(List<ReducerDF> reducerDFList, String reducerOutput) {
		File output = new File(reducerOutput);
		if(!output.exists())
			output.getParentFile().mkdirs();
		
		try {
			PrintWriter writer = new PrintWriter(new FileWriter(output));
		
			writer.println("#AppName" + "\t"
					+ "xShCompMBm" + "xShCompMBsd" + "\t" 
					+ "xShRawMBm" + "\t" + "xShRawMBsd" + "\t"
					+ "xInputMBm" + "\t" + "xShRawMBsd" + "\t"
					+ "xOutMBm" + "\t" + "xOutMBsd" + "\t"
					
					+ "mShCompMBm" + "mShCompMBsd" + "\t" 
					+ "mShRawMBm" + "\t" + "mShRawMBsd" + "\t"
					+ "mInputMBm" + "\t" + "mShRawMBsd" + "\t"
					+ "mOutMBm" + "\t" + "mOutMBsd");
			for(ReducerDF df : reducerDFList) {
				writer.println(df.getAppName() + "\t" 
						+ df.getxShCompMB().getMean() + "\t" + df.getxShCompMB().getSd() + "\t"
						+ df.getxShRawMB().getMean() + "\t" + df.getxShRawMB().getSd() + "\t"
						+ df.getxInputMB().getMean() + "\t" + df.getxInputMB().getSd() + "\t"
						+ df.getxOutMB().getMean() + "\t" + df.getxOutMB().getSd() + "\t"
						
						+ df.getmShCompMB().getMean() + "\t" + df.getmShCompMB().getSd() + "\t"
						+ df.getmShRawMB().getMean() + "\t" + df.getmShRawMB().getSd() + "\t"
						+ df.getmInputMB().getMean() + "\t" + df.getmInputMB().getSd() + "\t"
						+ df.getmOutMB().getMean() + "\t" + df.getmOutMB().getSd());
				
			}
			writer.close();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}
	
}

class Statistics {
	
	private double min;
	private double firstQu;
	private double median;
	private double mean;
	private double thirdQu;
	private double max;
	private double sd;
	
	public Statistics(double[] ds) {
		min = ds[0];
		firstQu = ds[1];
		median = ds[2];
		mean = ds[3];
		thirdQu = ds[4];
		max = ds[5];
		sd = ds[6];
	}

	public double getMin() {
		return min;
	}

	public double getFirstQu() {
		return firstQu;
	}

	public double getMedian() {
		return median;
	}

	public double getMean() {
		return mean;
	}

	public double getThirdQu() {
		return thirdQu;
	}

	public double getMax() {
		return max;
	}

	public double getSd() {
		return sd;
	}
}

class MapperDF {
	
	private Statistics mInMB;
	private Statistics mOutMB;
	private Statistics mRawBM;
	private Statistics mRawAM;
	
	private Statistics xInMB;
	private Statistics xOutMB;
	private Statistics xRawBM;
	private Statistics xRawAM;
	
	private String appName;
	
	public MapperDF(String appName, File mapperMFile, File mapperXFile) {
		this.appName = appName;
//		   mInMBdf    mOutMBdf    mRawBMdf    mRawAMdf
//		   Min.   :0   Min.   :0.0000   Min.   :0.0000   Min.   :0.0000  
//		   1st Qu.:0   1st Qu.:0.3814   1st Qu.:0.4486   1st Qu.:0.4486  
//		   Median :0   Median :0.9536   Median :0.9561   Median :0.9561  
//		   Mean   :0   Mean   :1.4257   Mean   :1.4718   Mean   :1.4718  
//		   3rd Qu.:0   3rd Qu.:2.2194   3rd Qu.:2.2778   3rd Qu.:2.2778  
//		   Max.   :0   Max.   :7.5016   Max.   :7.7528   Max.   :7.7528  
//		   0 1.34355097040638 1.38052293360541 1.38052293360541
		   
		read(mapperMFile, true);
		read(mapperXFile, false);
		
		
	}

	private void read(File mapperFile, boolean mean) {
		try {
			BufferedReader reader = new BufferedReader(new FileReader(mapperFile));
			String[] title = reader.readLine().trim().split("\\s+");
			
			double[][] matrix = new double[7][4];
			
			String line;
			int i = 0;
			while((line = reader.readLine()) != null) {
				line = line.replaceAll("(:|3rd|1st|Qu\\.|Min\\.|Median|Mean|Max\\.)", " ");
				Scanner scanner = new Scanner(line);
				
				int j = 0;
				while(scanner.hasNextDouble()) 
					matrix[i][j++] = scanner.nextDouble();
						
				if(j != title.length)
					System.err.println("Error in parsing summary of statistics");
				i++;
				scanner.close();
			}
			reader.close();
			setStatistics(matrix, mean);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	private void setStatistics(double[][] matrix, boolean mean) {
		double[][] rMatrix = new double[4][7];
		for(int i = 0; i < matrix.length; i++) {
			for(int j = 0; j < matrix[i].length; j++) {
				rMatrix[j][i] = matrix[i][j];
			}
		}
		
		if(mean) {
			mInMB = new Statistics(rMatrix[0]);
			mOutMB = new Statistics(rMatrix[1]);
			mRawBM = new Statistics(rMatrix[2]);
			mRawAM = new Statistics(rMatrix[3]);
		}
		else {
			xInMB = new Statistics(rMatrix[0]);
			xOutMB = new Statistics(rMatrix[1]);
			xRawBM = new Statistics(rMatrix[2]);
			xRawAM = new Statistics(rMatrix[3]);
		}
	}

	public Statistics getmInMB() {
		return mInMB;
	}

	public Statistics getmOutMB() {
		return mOutMB;
	}

	public Statistics getmRawBM() {
		return mRawBM;
	}

	public Statistics getmRawAM() {
		return mRawAM;
	}

	public Statistics getxInMB() {
		return xInMB;
	}

	public Statistics getxOutMB() {
		return xOutMB;
	}

	public Statistics getxRawBM() {
		return xRawBM;
	}

	public Statistics getxRawAM() {
		return xRawAM;
	}

	public String getAppName() {
		return appName;
	}
	
}

class ReducerDF {
	private String appName;
	
	private Statistics mShCompMB;
	private Statistics mShRawMB;
	private Statistics mInputMB;
	private Statistics mOutMB;
	
	private Statistics xShCompMB;
	private Statistics xShRawMB;
	private Statistics xInputMB;
	private Statistics xOutMB;
	
	public ReducerDF(String appName, File reducerMFile, File reducerXFile) {
		this.appName = appName;
//		 xShCompMBdf   xShRawMBdf   xInputMBdf    xOutMBdf
//		 Min.   : 0.006571   Min.   : 2.606   Min.   : 2.606   Min.   : 0.0208  
//		 1st Qu.: 5.220545   1st Qu.: 6.676   1st Qu.: 6.676   1st Qu.: 5.3026  
//		 Median : 8.625138   Median :10.336   Median :10.336   Median : 9.2135  
//		 Mean   : 8.487951   Mean   : 9.970   Mean   : 9.970   Mean   : 8.9153  
//		 3rd Qu.:11.451038   3rd Qu.:12.837   3rd Qu.:12.837   3rd Qu.:12.3894  
//		 Max.   :22.347741   Max.   :24.481   Max.   :24.481   Max.   :23.4279  
//		 3.71607968024402 3.57003507255691 3.57003507255691 4.08188458905882
		   
		read(reducerMFile, true);
		read(reducerXFile, false);
	}

	private void read(File reducerFile, boolean mean) {
		try {
			BufferedReader reader = new BufferedReader(new FileReader(reducerFile));
			String[] title = reader.readLine().trim().split("\\s+");
			
			double[][] matrix = new double[7][4];
			
			String line;
			int i = 0;
			while((line = reader.readLine()) != null) {
				line = line.replaceAll("(:|3rd|1st|Qu\\.|Min\\.|Median|Mean|Max\\.)", " ");
				Scanner scanner = new Scanner(line);
				
				int j = 0;
				while(scanner.hasNextDouble()) 
					matrix[i][j++] = scanner.nextDouble();
						
				if(j != title.length)
					System.err.println("Error R in parsing summary of statistics");
				i++;
				scanner.close();
			}
			reader.close();
			setStatistics(matrix, mean);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private void setStatistics(double[][] matrix, boolean mean) {
		double[][] rMatrix = new double[4][7];
		for(int i = 0; i < matrix.length; i++) {
			for(int j = 0; j < matrix[i].length; j++) {
				rMatrix[j][i] = matrix[i][j];
			}
		}
		
		if(mean) {
			mShCompMB = new Statistics(rMatrix[0]);
			mShRawMB = new Statistics(rMatrix[1]);
			mInputMB = new Statistics(rMatrix[2]);
			mOutMB = new Statistics(rMatrix[3]);
		}
		else {
			xShCompMB = new Statistics(rMatrix[0]);
			xShRawMB = new Statistics(rMatrix[1]);
			xInputMB = new Statistics(rMatrix[2]);
			xOutMB = new Statistics(rMatrix[3]);
		}
		
	}

	public String getAppName() {
		return appName;
	}

	public Statistics getmShCompMB() {
		return mShCompMB;
	}

	public Statistics getmShRawMB() {
		return mShRawMB;
	}

	public Statistics getmInputMB() {
		return mInputMB;
	}

	public Statistics getmOutMB() {
		return mOutMB;
	}

	public Statistics getxShCompMB() {
		return xShCompMB;
	}

	public Statistics getxShRawMB() {
		return xShRawMB;
	}

	public Statistics getxInputMB() {
		return xInputMB;
	}

	public Statistics getxOutMB() {
		return xOutMB;
	}
	
	
	
}