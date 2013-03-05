package estimator.all;

import hadoop.split.InputSplitCalculator;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.PrintWriter;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.TreeMap;

import profile.commons.configuration.Configuration;
import profile.job.Job;
import profile.profiler.SingleJobProfiler;
import profile.task.mapper.Mapper;
import profile.task.mapper.Merge;
import profile.task.mapper.MergeInfo;
import profile.task.reducer.Reducer;

import memory.model.job.InitialJvmCapacity;
import memory.model.jvm.MapperEstimatedJvmCost;
import memory.model.jvm.ReducerEstimatedJvmCost;
import memory.model.mapper.MapperMemoryEstimator;
import memory.model.reducer.ReducerMemoryEstimator;

import data.model.mapper.MapperEstimator;
import data.model.mapper.SpillFitsInMemoryModel;
import data.model.reducer.ReducerEstimator;

/* 
 * Dataset is changed, configuration is changed too.
 * <dataset, split, Xmx, Xms, ismb, RN> ==> <new Dataflow, new Memory>
 */

public class SampleToBigDataAndMemoryEstimator {
	
	private Job job;
	private String jobId;
	private Configuration finishedConf;
	private Configuration newConf;
	
	boolean isXmxChanged;
	boolean isXmsChanged;
	boolean isSplitSizeChanged;
	boolean isMapperConfChanged;
	boolean isReducerConfChanged;
	
	private static Map<Integer, List<Long>> splitMap = new TreeMap<Integer, List<Long>>();
	
	public static void main(String[] args) {
		// -------------------------Initialize Job Information------------------------------------
		
		/*********************************Big Experiments*********************************/
		
		String bigJobName = "Big-uservisits_aggre-pig-50G";
		
		//String bigJobName = "BigBuildInvertedIndex";
		
		//String bigJobName = "BigTeraSort-36GB";
		
		//String bigJobName = "BigTwitterBiDirectEdgeCount";
		
		//String bigJobName = "BigTwitterInDegreeCount";
		
		//String bigJobName = "BigWiki-m36-r18";
		
		//String baseDir = "/home/xulijie/MR-MEM/BigExperiments/";
		//Boolean hasSplitFile = true;
		
		//int newRNBase = 9;
		//int iterateNum = 192;
		/*********************************Big Experiments*********************************/
	
		
		/*********************************Sample Experiments*********************************/
		String startJobId = "job_201301222028_0001";
		String sampleJobName = "SampleUservisits-1G";
		
		//String startJobId = "job_201301211121_0001";
		//String sampleJobName = "SampleBuildInvertedIndex-1G";
		
		//String startJobId = "job_201301191915_0001";
		//String sampleJobName = "SampleTeraSort-1G";
		
		//String startJobId = "job_201301252338_0001";
		//String sampleJobName = "SampleTwitterBiDirectEdgeCount";
			
		//String startJobId = "job_201301281455_0001";
		//String sampleJobName = "SampleTwitterInDegreeCount";
		
		//String startJobId = "job_201301192319_0001";
		//String sampleJobName = "SampleWikiWordCount-1G";
		
		String bigBaseDir = "G:\\MR-MEM\\BigExperiments\\";
		String sampleBaseDir = "G:\\MR-MEM\\SampleExperiments\\";
		String compBaseDir = "G:\\MR-MEM\\CompExperiments\\";
		//String compBaseDir = "/home/xulijie/MR-MEM/Test/";
		
		Boolean hasSplitFile = true;
				
		int newRNBase = 9;
		int iterateNum = 192;
		/*********************************Sample Experiments*********************************/
		
		int[] splitMBs = {64, 128, 256};
		boolean outputDetailedDataflow = false;
		
		String outputDir = compBaseDir + "SampleTo" + bigJobName + "\\estimatedDM\\";
		
		//boolean needMetrics = true; //going to analyze task counters/metrics/jvm?
		//int sampleMapperNum = 0; // only analyze the first sampleMapperNum mappers (0 means all the mappers)
		//int sampleReducerNum = 0; // only analyze the first sampleReducerNum reducers (0 means all the reducers)
		boolean useRuntimeMaxJvmHeap = false; //since reducers' actual JVM heap is less than mapred.child.java.opts, 
											 //this parameter determines whether to use the actual JVM heap to estimate
		
		//--------------------------Setting ends------------------------------------
		DecimalFormat nf = new DecimalFormat("0000");
		
		for(int i = 0; i < iterateNum; i++) {
			String prefix = startJobId.substring(0, startJobId.length() - 4);
			int suffix = Integer.parseInt(startJobId.substring(startJobId.length() - 4));
			String jobId = prefix + nf.format(suffix + i);		
			//--------------------------Profiling the run job-----------------------------
			SampleToBigDataAndMemoryEstimator je = new SampleToBigDataAndMemoryEstimator();
			boolean successful;

			successful = je.profile(sampleBaseDir, sampleJobName, "serialJob", jobId);
			//--------------------------Profiling ends-----------------------------
			
			if(splitMap.isEmpty())
				computeSplitMap(splitMBs, je.job.getJobConfiguration(), bigBaseDir +  bigJobName, hasSplitFile);
			//--------------------------Estimating Data Flow and Memory-----------------------------
			if(successful == false) {
				System.err.println("[" + jobId + "] is a failed job");
				continue;
			}
					
			try {
			
				je.batchEstimateDataAndMemory(useRuntimeMaxJvmHeap, outputDir + jobId, newRNBase, outputDetailedDataflow);
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}	
			//--------------------------Estimating ends-----------------------------
			System.out.println("Finish estimating " + jobId);
		}	
	}
	
	private static void computeSplitMap(int[] splitMBs, Configuration fConf, String jobNameDir, Boolean hasSplitFile) {
		if(hasSplitFile) {
			try {
				BufferedReader reader = new BufferedReader(new FileReader(jobNameDir + File.separator + "splits.txt"));
				String line;
				int splitMB;
				long splitByte;
				
				while((line = reader.readLine()) != null) {
					line = line.replaceAll("[^0-9]", " ");
					Scanner scanner = new Scanner(line);

					splitMB = scanner.nextInt();
					List<Long> splitByteList = new ArrayList<Long>();
					
					while(scanner.hasNext()) {
						splitByte = scanner.nextLong();
						splitByteList.add(splitByte);
					}
					
					splitMap.put(splitMB, splitByteList);
				}
				
				reader.close();
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
		
		else {
			try {
				PrintWriter splitWriter = new PrintWriter(new BufferedWriter(new FileWriter(jobNameDir + File.separator + "splits.txt")));
				
				// ~/MR-MEM/BigExperiments/BigBuildInvertedIndex/split.txt
				for(int i = 0; i < splitMBs.length; i++) {
					int splitMB = splitMBs[i];
					Configuration conf = fConf.copyConfiguration();
					long newSplitSize = splitMB * 1024 * 1024l;
					conf.set("split.size", String.valueOf(newSplitSize));
					conf.set("mapred.min.split.size", String.valueOf(newSplitSize));
					conf.set("mapred.max.split.size", String.valueOf(newSplitSize));
					//setNewConf(conf);
					List<Long> splitsSizeList = InputSplitCalculator.getSplitsLength(conf);
					splitMap.put(splitMB, splitsSizeList);
					splitWriter.println(splitMB + ":" + splitsSizeList);
				}
				splitWriter.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}	
	}

	private boolean profile(String baseDir, String jobName, String jobDir, String jobId) {
		File jobFile = new File(baseDir + jobName + File.separator + jobDir + File.separator + jobId + ".out");
		
		try {
			FileInputStream fis = new FileInputStream(jobFile);
			ObjectInputStream ois = new ObjectInputStream(fis);
			job = (Job)ois.readObject();
			this.jobId = jobId;
			if(job == null)
				return false;
			return true;
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
       
		return false;
	}

	private void batchEstimateDataAndMemory(boolean useRuntimeMaxJvmHeap, String outputDir, int newRNBase, boolean outputDetailedDataflow) throws IOException {
		File mDataOutputFile = new File(outputDir, "eDataMappers.txt");
		File rDataOutputFile = new File(outputDir, "eDataReducers.txt");
		
		// outputDir = /estimatedDM/jobId/
		File mJvmOutputFile = new File(outputDir, "eJvmMappers.txt");
		File rJvmOutputFile = new File(outputDir, "eJvmReducers.txt");
		
		if(!mJvmOutputFile.getParentFile().exists()) 
			mJvmOutputFile.getParentFile().mkdirs();
		
		PrintWriter mDfWriter = new PrintWriter(new BufferedWriter(new FileWriter(mDataOutputFile)));
		PrintWriter rDfWriter = new PrintWriter(new BufferedWriter(new FileWriter(rDataOutputFile)));
		
		PrintWriter mJvmWriter = new PrintWriter(new BufferedWriter(new FileWriter(mJvmOutputFile)));
		PrintWriter rJvmWriter = new PrintWriter(new BufferedWriter(new FileWriter(rJvmOutputFile)));
		
		displayAllMapDataTitle(mDfWriter);
		displayAllReduceDataTitle(rDfWriter);
		
		displayMapJvmCostTitle(mJvmWriter);
		displayReduceJvmCostTitle(rJvmWriter);
		
		for(int splitMB = 64; splitMB <= 256; splitMB = splitMB * 2) {
		
			for(int xmx = 1000; xmx <= 4000; xmx = xmx + 1000) {
				for(int ismb = 200; ismb <= 800; ismb = ismb + 200) {
					for(int reducer = newRNBase; reducer <= newRNBase * 2; reducer = reducer * 2) {
						for(int xms = 0; xms <= 1; xms++) {
							
							//--------------------------Estimate the data flow---------------------------
							//-----------------for debug-------------------------------------------------
						    //System.out.println("[split = " + splitMB + " [xmx = " + xmx + ", xms = " + xms + ", ismb = " + 
							//		ismb + ", RN = " + reducer + "]");
							//if(splitMB != 256 || xmx != 1000 || ismb != 200 || reducer != 9 || xms != 1)
							//	continue;
							//if(xmx != 4000 || xms != 1 || ismb != 1000 || reducer != 9)
							//	continue;
							//---------------------------------------------------------------------------
							
							Configuration conf = new Configuration();
							
							long newSplitSize = splitMB * 1024 * 1024l;
							conf.set("io.sort.mb", String.valueOf(ismb));
							
							if(xms == 0)
								conf.set("mapred.child.java.opts", "-Xmx" + xmx + "m");
							else
								conf.set("mapred.child.java.opts", "-Xmx" + xmx + "m" + " -Xms" + xmx + "m");
							
							conf.set("mapred.reduce.tasks", String.valueOf(reducer));
							
							
							conf.set("split.size", String.valueOf(newSplitSize));
												
							setNewConf(conf);
							
							// -------------------------Estimate the data flow-------------------------
							List<Mapper> eMappers = estimateMappers();	//don't filter the mappers with small split size
							List<Reducer> eReducers = estimateReducers(eMappers, useRuntimeMaxJvmHeap);
							
							String fileName = conf.getConf("mapred.child.java.opts").replaceAll(" ", "") + "-ismb" + ismb + "-RN" + reducer;
							
							if(outputDetailedDataflow) {
								displayMapperDataResult(eMappers, fileName , outputDir + File.separator + "eDataflow" + splitMB);
								displayReducerDataResult(eReducers, fileName, outputDir + File.separator + "eDataflow" + splitMB);
							}
							
							
							displayMapperDataResult(eMappers, mDfWriter, splitMB, xmx, xms * xmx, ismb, reducer);
							displayReducerDataResult(eReducers, rDfWriter, splitMB, xmx, xms * xmx, ismb, reducer);
							
							// -------------------------Estimate the memory cost-------------------------
							InitialJvmCapacity gcCap = computeInitalJvmCapacity();
							
							if(!gcCap.getError().isEmpty()) {
								System.err.println(gcCap.getError() + " [xmx = " + xmx + ", xms = " + xms + ", ismb = " + 
										ismb + ", RN = " + reducer + "]");
							}
							else {
								//filter the estimated mappers with low split size
								List<MapperEstimatedJvmCost> eMappersMemory = estimateMappersMemory(eMappers, gcCap);
								List<ReducerEstimatedJvmCost> eReducersMemory = estimateReducersMemory(eReducers, gcCap);
								
								
								displayMapperJvmCostResult(eMappersMemory, gcCap, mJvmWriter);
								displayReducerJvmCostResult(eReducersMemory, gcCap, rJvmWriter);
							}
							
							
						}
					}
				}
			}
			
		}
		
		mDfWriter.close();
		rDfWriter.close();
		mJvmWriter.close();
		rJvmWriter.close();
		
	}
	
	private void displayMapperDataResult(List<Mapper> eMappers, PrintWriter mDfWriter, 
			int splitMB, int xmx, int xms, int ismb, int RN) {
		//find the medium value
		int MB = 1024 * 1024;
		int size = eMappers.size();
		
		float[] InMB = new float[size];
		float[] InRec = new float[size];
		float[] OutMB = new float[size];
		float[] OutRec = new float[size];
		float[] RecBM = new float[size];
		float[] RawBM = new float[size];
		float[] CompBM = new float[size];
		float[] RecAM = new float[size];
		float[] RawAM = new float[size];
		float[] CompAM = new float[size];
		int[] SegN = new int[size];
		
		for(int i = 0; i < size; i++) {
			Mapper eMapper = eMappers.get(i);
			Merge eMerge = eMapper.getMerge();
			List<MergeInfo> mergeInfoList = eMerge.getMergeInfoList();
			
			int segmentsNum;
			double recordsBeforeMerge = 0;
			double rawLengthBeforeMerge = 0;
			double compressedLengthBeforeMerge = 0;
			
			double recordsAfterMerge = 0;
			double rawLengthAfterMerge = 0;
			double compressedLengthAfterMerge = 0;
			
			for(MergeInfo mergeInfo : mergeInfoList) {
				recordsBeforeMerge += mergeInfo.getRecordsBeforeMerge();
				rawLengthBeforeMerge += mergeInfo.getRawLengthBeforeMerge();
				compressedLengthBeforeMerge += mergeInfo.getCompressedLengthBeforeMerge();
				
				recordsAfterMerge += mergeInfo.getRecordsAfterMerge();
				rawLengthAfterMerge += mergeInfo.getRawLengthAfterMerge();
				compressedLengthAfterMerge += mergeInfo.getCompressedLengthAfterMerge();
			}
			
			segmentsNum = mergeInfoList.get(0).getSegmentsNum();
			
			double map_input_bytes = eMapper.getMapperCounters().getMap_input_bytes();
			double map_input_records = eMapper.getMapperCounters().getMap_input_records();
			double map_output_bytes = eMapper.getMapperCounters().getMap_output_bytes();
			double map_output_records = eMapper.getMapperCounters().getMap_output_records();
			
			InMB[i] = (float) (map_input_bytes / MB);
			InRec[i] = (float) (map_input_records / MB);
			OutMB[i] = (float) (map_output_bytes / MB);
			OutRec[i] = (float) (map_output_records / MB);
			RecBM[i] = (float) (recordsBeforeMerge / MB);
			RawBM[i] = (float) (rawLengthBeforeMerge / MB);
			CompBM[i] = (float) (compressedLengthBeforeMerge / MB);
			RecAM[i] = (float) (recordsAfterMerge / MB);
			RawAM[i] = (float) (rawLengthAfterMerge / MB);
			CompAM[i] = (float) (compressedLengthAfterMerge / MB);
			SegN[i] =segmentsNum;
		}
		
		Arrays.sort(InMB);
		Arrays.sort(InRec);
		Arrays.sort(OutMB);
		Arrays.sort(OutRec);
		Arrays.sort(RecBM);
		Arrays.sort(RawBM);
		Arrays.sort(CompBM);
		Arrays.sort(RecAM);
		Arrays.sort(RawAM);
		Arrays.sort(CompAM);
		Arrays.sort(SegN);
			
		int m = size / 2;
		float mInMB = InMB[m];
		float mInRec = InRec[m];
		float mOutMB = OutMB[m];
		float mOutRec = OutRec[m];
		
		float mRecBM = RecBM[m];
		float mRawBM = RawBM[m];
		float mCompBM = CompBM[m];
		float mRecAM = RecAM[m];
		float mRawAM = RawAM[m];
		float mCompAM = CompAM[m];
		int mSegN = SegN[m];
		
		int end = size - 1;
		float xInMB = InMB[end];
		float xInRec = InRec[end];
		float xOutMB = OutMB[end];
		float xOutRec = OutRec[end];
		
		float xRecBM = RecBM[end];
		float xRawBM = RawBM[end];
		float xCompBM = CompBM[end];
		float xRecAM = RecAM[end];
		float xRawAM = RawAM[end];
		float xCompAM = CompAM[end];
		int xSegN = SegN[end];
				
		String f1 = "%1$-2.1f";
		mDfWriter.println(splitMB + "\t" + xmx + "\t" + xms + "\t" + ismb + "\t" + RN + "\t"
				+ String.format(f1, mInMB) + "\t" + String.format(f1, mInRec)
				+ "\t" + String.format(f1, mOutMB) + "\t"
				+ String.format(f1, mOutRec) + "\t" + String.format(f1, mRecBM)
				+ "\t" + String.format(f1, mRawBM) + "\t"
				+ String.format(f1, mCompBM) + "\t" + String.format(f1, mRecAM)
				+ "\t" + String.format(f1, mRawAM) + "\t"
				+ String.format(f1, mCompAM) + "\t" + mSegN + "\t"
				+ String.format(f1, xInMB) + "\t" + String.format(f1, xInRec)
				+ "\t" + String.format(f1, xOutMB) + "\t"
				+ String.format(f1, xOutRec) + "\t" + String.format(f1, xRecBM)
				+ "\t" + String.format(f1, xRawBM) + "\t"
				+ String.format(f1, xCompBM) + "\t" + String.format(f1, xRecAM)
				+ "\t" + String.format(f1, xRawAM) + "\t"
				+ String.format(f1, xCompAM) + "\t" + xSegN + "\t");		
				
		
	}

	private void displayReducerDataResult(List<Reducer> eReducers, PrintWriter rDfWriter, 
			int splitMB, int xmx, int xms, int ismb, int RN) {
		
		int size = eReducers.size();

		float[] ShCompMB = new float[size];
		float[] ShRawMB = new float[size];
		float[] InRec = new float[size];
		float[] InputMB = new float[size];
		float[] OutRec = new float[size];
		float[] OutMB = new float[size];
		
		int MB = 1024 * 1024;
		for(int i = 0; i < size; i++) {
			Reducer eReducer = eReducers.get(i);
			double Reduce_shuffle_bytes = eReducer.getReducerCounters().getReduce_shuffle_bytes();
			double Reduce_shuffle_raw_bytes = eReducer.getReducerCounters().getReduce_shuffle_raw_bytes();
			double Reduce_input_records = eReducer.getReducerCounters().getReduce_input_records();
			
			double Reduce_input_bytes = eReducer.getReduce().getInputBytes();
			double Reduce_output_records = eReducer.getReduce().getOutputKeyValuePairsNum();
			double Reduce_output_bytes = eReducer.getReduce().getOutputBytes();
			
			ShCompMB[i] = (float) (Reduce_shuffle_bytes / MB);
			ShRawMB[i] = (float) (Reduce_shuffle_raw_bytes / MB);
			InRec[i] = (float) (Reduce_input_records / MB);
			InputMB[i] = (float) (Reduce_input_bytes / MB);
			OutRec[i] = (float) (Reduce_output_records / MB);
			OutMB[i] = (float) (Reduce_output_bytes / MB);
			
		}
		
		Arrays.sort(ShCompMB);
		Arrays.sort(ShRawMB);
		Arrays.sort(InRec);
		Arrays.sort(InputMB);
		Arrays.sort(OutRec);
		Arrays.sort(OutMB);
			
		int m = size / 2;
		
		float mShCompMB = ShCompMB[m];
		float mShRawMB = ShRawMB[m];
		float mInRec = InRec[m];
		float mInputMB = InputMB[m];
		float mOutRec = OutRec[m];
		float mOutMB = OutMB[m];
		
		int end = size - 1;
		float xShCompMB = ShCompMB[end];
		float xShRawMB = ShRawMB[end];
		float xInRec = InRec[end];
		float xInputMB = InputMB[end];
		float xOutRec = OutRec[end];
		float xOutMB = OutMB[end];
		
		String f1 = "%1$-2.1f";
		rDfWriter.println(splitMB + "\t" + xmx + "\t" + xms + "\t" + ismb + "\t" + RN + "\t"
				+ String.format(f1, mShCompMB) + "\t"
				+ String.format(f1, mShRawMB) + "\t"
				+ String.format(f1, mInRec) + "\t"
				+ String.format(f1, mInputMB) + "\t"
				+ String.format(f1, mOutRec) + "\t" + String.format(f1, mOutMB)
				+ "\t" + String.format(f1, xShCompMB) + "\t"
				+ String.format(f1, xShRawMB) + "\t"
				+ String.format(f1, xInRec) + "\t"
				+ String.format(f1, xInputMB) + "\t"
				+ String.format(f1, xOutRec) + "\t" + String.format(f1, xOutMB));
		
	}

	private void displayAllMapDataTitle(PrintWriter mDataWriter) {
		mDataWriter.println("split" + "\t" + "xmx" + "\t" + "xms" + "\t" + "ismb" + "\t" + "RN"
				+ "\t" + "mInMB" + "\t" + "mInRec" + "\t" + "mOutMB" + "\t"
				+ "mOutRec" + "\t" + "mRecBM" + "\t" + "mRawBM" + "\t"
				+ "mCompBM" + "\t" + "mRecAM" + "\t" + "mRawAM" + "\t"
				+ "mCompAM" + "\t" + "mSegN" + "\t" + "xInMB" + "\t" + "xInRec"
				+ "\t" + "xOutMB" + "\t" + "xOutRec" + "\t" + "xRecBM" + "\t"
				+ "xRawBM" + "\t" + "xCompBM" + "\t" + "xRecAM" + "\t"
				+ "xRawAM" + "\t" + "xCompAM" + "\t" + "xSegN");
	}

	private void displayAllReduceDataTitle(PrintWriter rDataWriter) {
		rDataWriter.println("split" + "\t" + "xmx" + "\t" + "xms" + "\t" + "ismb" + "\t" + "RN"
				+ "\t" + "mShCompMB" + "\t" + "mShRawMB" + "\t" + "mInRec"
				+ "\t" + "mInputMB" + "\t" + "mOutRec" + "\t" + "mOutMB" + "\t"
				+ "xShCompMB" + "\t" + "xShRawMB" + "\t" + "xInRec" + "\t"
				+ "xInputMB" + "\t" + "xOutRec" + "\t" + "xOutMB");
		
	}
/*
	private boolean profile(String hostname, String jobId, boolean needMetrics, int sampleMapperNum, int sampleReducerNum) {
		
		SingleJobProfiler profiler = new SingleJobProfiler(hostname, jobId, needMetrics, sampleMapperNum, sampleReducerNum);
		job = profiler.profile();
		this.jobId = jobId;
		if(job == null)
			return false;
		return true;
	}
*/
	private void setNewConf(Configuration conf) {
		finishedConf = job.getJobConfiguration();
		newConf = finishedConf.copyConfiguration();
		
		isXmxChanged = false;
		isXmsChanged = false;
		isSplitSizeChanged = false;
		isMapperConfChanged = false;
		isReducerConfChanged = false;
		
		// judge whether mapper and reducer data-model estimators is needed
		for(Entry<String, String> entry : conf.getAllConfs()) {
			String key = entry.getKey();
			String value = entry.getValue();
			
			if(key.equals("mapred.child.java.opts")) {	
				int start = value.indexOf("-Xmx") + 4;
				int end = value.indexOf('m', start + 4);	
				int xmx = Integer.parseInt(value.substring(start, end)); //4000;
				
				int xms;
				
				start = value.indexOf("-Xms") + 4;
				if(start == -1 + 4)
					xms = 0;
				else {
					end = value.indexOf('m', start);
					xms = Integer.parseInt(value.substring(start, end)); //1000;
				}
				
				if(finishedConf.isXmxChanged(xmx))
					isXmxChanged = true;
				if(finishedConf.isXmsChanged(xms))
					isXmsChanged = true;
			}
			
			if(key.equals("split.size") && finishedConf.isSpliSizeChanged(value)) {
				isSplitSizeChanged = true;		
				long splitSize = Long.parseLong(value);
				long blockSize = finishedConf.getDfs_block_size();
				long minSplit = 1L;
				long maxSplit = Long.MAX_VALUE;
				if(splitSize < blockSize) 
					maxSplit = splitSize;
				else
					minSplit = splitSize;
				
				newConf.set("mapred.min.split.size", String.valueOf(minSplit));
				newConf.set("mapred.max.split.size", String.valueOf(maxSplit));
			}
			
			if(finishedConf.isMapperConfChanged(key, value))
				isMapperConfChanged = true;	
				
			if(finishedConf.isReducerConfChanged(key, value))
				isReducerConfChanged = true;
		
			newConf.set(entry.getKey(), entry.getValue());
		}
		
	}
	
	private List<Mapper> estimateMappers() {
		List<Mapper> newMapperList = new ArrayList<Mapper>();
		List<Long> splitsSizeList = splitMap.get((int)(newConf.getSplitSize() / 1024 /1024));
		
		MapperEstimator mapperEstimator = new MapperEstimator(finishedConf, newConf);
		
		//split size is changed
		if(isSplitSizeChanged) {
			//256:[268435456, 268435456, 268435456, 268435456, 268435456, ... 268435456, 120589425]
			Map<Integer, Mapper> cacheNewMapper = new HashMap<Integer, Mapper>();
			
			for(int i = 0; i < splitsSizeList.size(); i++) {
				Long cSplitSize = splitsSizeList.get(i);
				int cSplitMB = (int) (cSplitSize / (1024 * 1024)) ; //HDFS_BYTES_READ
				
				if(!cacheNewMapper.containsKey(cSplitMB)) {
					Mapper newMapper = mapperEstimator.estimateNewMapper(job.getMapperList(), cSplitSize);
					newMapperList.add(newMapper);
					cacheNewMapper.put(cSplitMB, newMapper); 
				}
				else
					newMapperList.add(cacheNewMapper.get(cSplitMB)); //Note: multiple references of newMapper
			}	
		}
			
		//do not need to consider split size
		else {
			//assume all the sampled mappers' input split size is normal
			
			for(int i = 0; i < splitsSizeList.size(); i++) {
				Mapper newMapper = mapperEstimator.estimateNewMapper(job.getMapperList(), splitsSizeList.get(i));
				newMapperList.add(newMapper);
			}
			
		}	
		
		return newMapperList;
	}
	
	private List<Reducer> estimateReducers(List<Mapper> newMapperList, boolean useRuntimeMaxJvmHeap) {
		int fReducerNum = finishedConf.getMapred_reduce_tasks();

		if(fReducerNum == 0)
			return null;
		//if(isMapperConfChanged == false && isSplitSizeChanged == false && isReducerConfChanged == false) 
		//	return job.getReducerList();
		
		ReducerEstimator reducerEstimator = new ReducerEstimator(finishedConf, newConf, newMapperList, useRuntimeMaxJvmHeap);
		
		List<Reducer> newReducerList = new ArrayList<Reducer>();
		
		//reducerIndex means which partition of mappers' output will be received by the new reducer 
		//reducer number changes or not
		
		for(int i = 0; i < newConf.getMapred_reduce_tasks(); i++) {
			//use all the finished reducers' infos to estimate the new reducer
			Reducer newReducer = reducerEstimator.estimateNewReducer(job.getReducerList(), i);
			newReducerList.add(newReducer);
		}	
		
		return newReducerList;
	}
	
	public void estimateSpillFitsInMemory(Configuration conf) {
		long fmax_mapred_output_records = 0;
		long fmax_mapred_output_bytes = 0;
		
		for(Mapper fMapper : job.getMapperList()) {
			long records = fMapper.getMapperCounters().getMap_output_records();
			long bytes = fMapper.getMapperCounters().getMap_output_bytes();
			if(records > fmax_mapred_output_records)
				fmax_mapred_output_records = records;
			if(bytes > fmax_mapred_output_bytes)
				fmax_mapred_output_bytes = bytes;
		}
		SpillFitsInMemoryModel sfimm = new SpillFitsInMemoryModel();
		sfimm.keepInMemory(fmax_mapred_output_records, fmax_mapred_output_bytes, conf);
		
	}
	
	public void estimateSpillFitsInMemory() {
		estimateSpillFitsInMemory(null);
	}
	
	//filter the fMapper with low split size when estimating the new Mappers
	private List<MapperEstimatedJvmCost> estimateMappersMemory(List<Mapper> eMappers, InitialJvmCapacity gcCap) {
		List<MapperEstimatedJvmCost> mappersJvmCostList = new ArrayList<MapperEstimatedJvmCost>();
		
		MapperMemoryEstimator memoryEstimator = new MapperMemoryEstimator(finishedConf, newConf, gcCap);
		MapperEstimatedJvmCost jvmCost;
		
		long splitSizeMB = newConf.getSplitSize() / (1024 * 1024);
		List<Mapper> filterFMapperList = filterSmallSplitSize(job.getMapperList());
		
		//split size is changed
		//if(isSplitSizeChanged) {
		Map<Long, MapperEstimatedJvmCost> cacheJvmCost = new HashMap<Long, MapperEstimatedJvmCost>();
		
		
		for(int i = 0; i < eMappers.size(); i++) {
			Mapper eMapper = eMappers.get(i);
			
			long cSplitMB = eMapper.getInput().getSplitSize() / (1024 * 1024) ; //HDFS_BYTES_READ
			
			if(cSplitMB >= splitSizeMB) {
				if(!cacheJvmCost.containsKey(cSplitMB)) {
					// only estimate the mappers with normal split size
					jvmCost = memoryEstimator.estimateJvmCost(filterFMapperList, eMapper);	
					cacheJvmCost.put(cSplitMB, jvmCost); 
				}
				else
					jvmCost = cacheJvmCost.get(cSplitMB);
				mappersJvmCostList.add(jvmCost);
			}
			
		}	
		//}
		
		//do not need to consider split size
		/*
		else {
			int fMappersNum = job.getMapperList().size();
			
			for(int i = 0; i < eMappers.size(); i++) {
				
				Mapper eMapper = eMappers.get(i);
				if(eMapper.getInput().getSplitSize() / (1024 * 1024) >= splitSizeMB) {
					jvmCost = memoryEstimator.estimateJvmCost(job.getMapperList().get(i % fMappersNum), eMappers.get(i));
					//happen occasionally 
					if(jvmCost == null) 
						System.err.println("Error when parsing " + job.getMapperList().get(i).getTaskId() + "'s Jstat log, ignore it");
					else
						mappersJvmCostList.add(jvmCost);
				}		
			}		
		}		
		*/		

		return mappersJvmCostList;
	}
	
	//filter the finished mappers with small split, so that it use normal fMappers to estimate the new Mappers with different split size
	private List<Mapper> filterSmallSplitSize(List<Mapper> fMapperList) {
		List<Mapper> filterFMapperList = new ArrayList<Mapper>();
		long splitSizeMB = newConf.getSplitSize() / (1024 * 1024);
		
		for(Mapper fMapper : fMapperList) 
			if(fMapper.getInput().getSplitSize() / (1024 * 1024) >= splitSizeMB)
				filterFMapperList.add(fMapper);
		
		if(filterFMapperList.isEmpty())
			return fMapperList;
		else
			return filterFMapperList;
	}

	private List<ReducerEstimatedJvmCost> estimateReducersMemory(List<Reducer> eReducers, InitialJvmCapacity gcCap) {
			
		int fReducerNum = finishedConf.getMapred_reduce_tasks();

		if(fReducerNum == 0)
			return null;
		
		List<ReducerEstimatedJvmCost> reducersJvmCostList = new ArrayList<ReducerEstimatedJvmCost>();
		ReducerMemoryEstimator memoryEstimator = new ReducerMemoryEstimator(finishedConf, newConf, gcCap);
		
		ReducerEstimatedJvmCost jvmCost;
		//reducer number changes or not
	
		for(int i = 0; i < newConf.getMapred_reduce_tasks(); i++) {
			//use all the finished reducers' infos to estimate the new reducer
			jvmCost = memoryEstimator.esimateJvmCost(job.getReducerList(), eReducers.get(i));
			reducersJvmCostList.add(jvmCost);
		}
		
		return reducersJvmCostList;
	}

	

	private void displayReduceDataTitle(PrintWriter rDataWriter) {
		rDataWriter.println(
			"ShCompMB" + "\t"
			+ "ShRawMB" + "\t"
			+ "InRec" + "\t"
			+ "InputMB" + "\t"
			+ "OutRec" + "\t"
			+ "OutMB"
		);
		
	}

	private void displayReducerDataResult(List<Reducer> eReducers, String jobId, String outputDir) throws IOException {
		File rDataOutputFile = new File(outputDir + File.separator + jobId, "eDataReducers.txt");
		PrintWriter rDataWriter = new PrintWriter(new BufferedWriter(new FileWriter(rDataOutputFile)));
		
		displayReduceDataTitle(rDataWriter);
		String f1 = "%1$-3.1f";
		int MB = 1024 * 1024;
		for(Reducer eReducer : eReducers) {
			double Reduce_shuffle_bytes = eReducer.getReducerCounters().getReduce_shuffle_bytes();
			double Reduce_shuffle_raw_bytes = eReducer.getReducerCounters().getReduce_shuffle_raw_bytes();
			double Reduce_input_records = eReducer.getReducerCounters().getReduce_input_records();
			
			double Reduce_input_bytes = eReducer.getReduce().getInputBytes();
			double Reduce_output_records = eReducer.getReduce().getOutputKeyValuePairsNum();
			double Reduce_output_bytes = eReducer.getReduce().getOutputBytes();
			
			rDataWriter.println(
				String.format(f1, Reduce_shuffle_bytes / MB) + "\t"
				+ String.format(f1, Reduce_shuffle_raw_bytes / MB) + "\t"
				+ String.format(f1, Reduce_input_records / MB) + "\t"
				+ String.format(f1, Reduce_input_bytes / MB) + "\t"
				+ String.format(f1, Reduce_output_records / MB) + "\t"
				+ String.format(f1, Reduce_output_bytes / MB)
			);
		}
		rDataWriter.close();
	}

	private void displayMapDataTitle(PrintWriter mDataWriter) {
		mDataWriter.println(
			    "InMB"+ "\t"
				+ "InRec" + "\t"
				+ "OutMB" + "\t"
				+ "OutRec" + "\t"
				
				+ "RecBM" + "\t"
				+ "RawBM" + "\t"
				+ "CompBM" + "\t"
				+ "RecAM" + "\t"
				+ "RawAM" + "\t"
				+ "CompAM" + "\t"
				+ "SegN"
		);
	}

	private void displayMapperDataResult(List<Mapper> eMappers, String jobId, String outputDir) throws IOException {
		File mDataOutputFile = new File(outputDir + File.separator + jobId, "eDataMappers.txt");
		if(!mDataOutputFile.getParentFile().exists()) 
			mDataOutputFile.getParentFile().mkdirs();
		
		PrintWriter mDataWriter = new PrintWriter(new BufferedWriter(new FileWriter(mDataOutputFile)));
		displayMapDataTitle(mDataWriter);
		
		int MB = 1024 * 1024;
		String f0 = "%1$-3.0f";
		String f2 = "%1$-3.1f";
		
		for(Mapper eMapper : eMappers) {
			Merge eMerge = eMapper.getMerge();
			List<MergeInfo> mergeInfoList = eMerge.getMergeInfoList();
			
			int segmentsNum;
			double recordsBeforeMerge = 0;
			double rawLengthBeforeMerge = 0;
			double compressedLengthBeforeMerge = 0;
			
			double recordsAfterMerge = 0;
			double rawLengthAfterMerge = 0;
			double compressedLengthAfterMerge = 0;
			
			for(MergeInfo mergeInfo : mergeInfoList) {
				recordsBeforeMerge += mergeInfo.getRecordsBeforeMerge();
				rawLengthBeforeMerge += mergeInfo.getRawLengthBeforeMerge();
				compressedLengthBeforeMerge += mergeInfo.getCompressedLengthBeforeMerge();
				
				recordsAfterMerge += mergeInfo.getRecordsAfterMerge();
				rawLengthAfterMerge += mergeInfo.getRawLengthAfterMerge();
				compressedLengthAfterMerge += mergeInfo.getCompressedLengthAfterMerge();
			}
			
			segmentsNum = mergeInfoList.get(0).getSegmentsNum();
			
			double map_input_bytes = eMapper.getMapperCounters().getMap_input_bytes();
			double map_input_records = eMapper.getMapperCounters().getMap_input_records();
			double map_output_bytes = eMapper.getMapperCounters().getMap_output_bytes();
			double map_output_records = eMapper.getMapperCounters().getMap_output_records();
			
			mDataWriter.println(
					String.format(f0, map_input_bytes / MB) + "\t"
					+ String.format(f2, map_input_records / MB) + "\t"
					+ String.format(f2, map_output_bytes / MB) + "\t"
					+ String.format(f2, map_output_records / MB) + "\t"
					
					+ String.format(f2, recordsBeforeMerge / MB) + "\t"
					+ String.format(f2, rawLengthBeforeMerge / MB) + "\t"
					+ String.format(f2, compressedLengthBeforeMerge / MB) + "\t"
					+ String.format(f2, recordsAfterMerge / MB) + "\t"
					+ String.format(f2, rawLengthAfterMerge / MB) + "\t"
					+ String.format(f2, compressedLengthAfterMerge / MB) + "\t"
					+ segmentsNum
					);
		}
		
		mDataWriter.close();
	}

	private void displayMapJvmCostTitle(PrintWriter mapOutputWriter) {
		
		mapOutputWriter.write(
				"split" + "\t"
				+ "xmx" + "\t"
				+ "xms" + "\t"
				+ "ismb" + "\t"
				+ "RN" + "\t"
				+ "nOU" + "\t"
				+ "xOU" + "\t"
				+ "OGC" + "\t"
				+ "OGCMX" + "\t"
				+ "nNGU" + "\t"
				+ "xNGU" + "\t"
				+ "NGC" + "\t"
				+ "NGCMX" + "\t"
				+ "EdenC" + "\t"
				+ "S0C" + "\t"
				+ "nHeapU" + "\t"
				+ "xHeapU" + "\t"
				+ "nTempObj" + "\t"
				+ "xTempObj" + "\t"
				+ "nFix" + "\t"
				+ "xFix" + "\t"
				+ "reason" + "\n"
		);
		
	}

	private void displayMapperJvmCostResult(List<MapperEstimatedJvmCost> eMappersMemory, InitialJvmCapacity gcCap, PrintWriter mapOutputWriter) {
		String f1 = "%1$-3.0f";
		int split = (int) (newConf.getSplitSize() / 1024 / 1024);
		int xmx = gcCap.geteXmx();
		int xms = gcCap.geteXms();
		int ismb = newConf.getIo_sort_mb();
		int RN = newConf.getMapred_reduce_tasks();
		
		float nOU = Float.MAX_VALUE;
		float xOU = 0; 
		float OGC = gcCap.geteOGC();
		float OGCMX = gcCap.geteOGCMX();
		
		float nNGU = Float.MAX_VALUE;
		float xNGU = 0;
		float NGC = gcCap.geteNGC();
		float NGCMX = gcCap.geteNGCMX();
		
		float EdenC = gcCap.geteEC();
		float S0C = gcCap.geteS0C();
		
		float nHeapU = Float.MAX_VALUE;
		float xHeapU = 0;
		
		float nTempObj = Float.MAX_VALUE;
		float xTempObj = 0;
		float nFix = Float.MAX_VALUE;
		float xFix = 0;
		
		String reason = "";
		
		for(MapperEstimatedJvmCost eJvmCost : eMappersMemory) {
			float OU = eJvmCost.getOU();
			float NGU = eJvmCost.getNewUsed();
			float heapU = eJvmCost.getHeapU();
			float tempObj = eJvmCost.getTempObj();
			float fix = eJvmCost.getFix();
			
			nOU = Math.min(nOU, OU);
			xOU = Math.max(xOU, OU);
			
			nNGU = Math.min(nNGU, NGU);
			xNGU = Math.max(xNGU, NGU);
			
			nHeapU = Math.min(nHeapU, heapU);
			if(heapU > xHeapU) {
				xHeapU = heapU;
				reason = eJvmCost.getReason();
			}
			
			nTempObj = Math.min(nTempObj, tempObj);
			xTempObj = Math.max(xTempObj, tempObj);
			
			nFix = Math.min(nFix, fix);
			xFix = Math.max(xFix, fix);		
		}
		
		mapOutputWriter.write(
			split + "\t"
			+ xmx + "\t"
			+ xms + "\t"
			+ ismb + "\t"
			+ RN + "\t"
			+ String.format(f1, nOU) + "\t"
			+ String.format(f1, xOU) + "\t"
			+ String.format(f1, OGC) + "\t"
			+ String.format(f1, OGCMX) + "\t"
			+ String.format(f1, nNGU) + "\t"
			+ String.format(f1, xNGU) + "\t"
			+ String.format(f1, NGC) + "\t"
			+ String.format(f1, NGCMX) + "\t"
			+ String.format(f1, EdenC) + "\t"
			+ String.format(f1, S0C) + "\t"
			+ String.format(f1, nHeapU) + "\t"
			+ String.format(f1, xHeapU) + "\t"
			+ String.format(f1, nTempObj) + "\t"
			+ String.format(f1, xTempObj) + "\t"
			+ String.format(f1, nFix) + "\t"
			+ String.format(f1, xFix) + "\t"
			+ reason + "\n"
		);
	}
	
	private void displayReduceJvmCostTitle(PrintWriter reduceOutputWriter) {
		reduceOutputWriter.write(
				"split" + "\t"
				+ "xmx" + "\t"
				+ "xms" + "\t"
				+ "ismb" + "\t"
				+ "RN" + "\t"
				+ "nOU" + "\t"
				+ "xOU" + "\t"
				+ "OGC" + "\t"
				+ "OGCMX" + "\t"
				+ "nNGU" + "\t"
				+ "xNGU" + "\t"
				+ "NGC" + "\t"
				+ "NGCMX" + "\t"
				+ "IMSB" + "\t"
				+ "MergB" + "\t"
				+ "ShufMB" + "\t"
				+ "nRedIn" + "\t"
				+ "xRedIn" + "\t"
				+ "EdenC" + "\t"
				+ "S0C" + "\t"
				+ "nHeapU" + "\t"
				+ "xHeapU" + "\t"
				+ "nSSTObj" + "\t"
				+ "xSSTObj" + "\t"
				+ "nRTObj" + "\t"
				+ "xRTObj" + "\t"
				+ "nFix" + "\t"
				+ "xFix" + "\t"
				+ "reason" + "\n"
		);
		
	}
	
	private void displayReducerJvmCostResult(List<ReducerEstimatedJvmCost> eReducersMemory, InitialJvmCapacity gcCap, PrintWriter reduceOutputWriter) {
		String f1 = "%1$-3.0f";
		
		int split = (int) (newConf.getSplitSize() / 1024 / 1024);
		int xmx = gcCap.geteXmx();
		int xms = gcCap.geteXms();
		int ismb = newConf.getIo_sort_mb();
		int RN = newConf.getMapred_reduce_tasks();
		
		float nOU = Float.MAX_VALUE;
		float xOU = 0;
		float OGC = gcCap.geteOGC();
		float OGCMX = gcCap.geteOGCMX();
		
		float nNGU = Float.MAX_VALUE;
		float xNGU = 0;
		float NGC = gcCap.geteNGC();
		float NGCMX = gcCap.geteNGCMX();
		
		float EdenC = gcCap.geteEC();
		float S0C = gcCap.geteS0C();
		
		float nHeapU = Float.MAX_VALUE;
		float xHeapU = 0;
		
		//float nTempObj = Float.MAX_VALUE;
		//float xTempObj = 0;
		float nSSTObj = Float.MAX_VALUE;
		float xSSTObj = 0;
		float nRTObj = Float.MAX_VALUE;
		float xRTObj = 0;
		
		float nFix = Float.MAX_VALUE;
		float xFix = 0;
		
		float nRedIn = Float.MAX_VALUE;
		float xRedIn = 0;
		
		String reason = "";
		
		for(ReducerEstimatedJvmCost eJvmCost : eReducersMemory) {
			float anOU = eJvmCost.getnOU();
			float axOU = eJvmCost.getxOU();
			
			float aNGU = eJvmCost.getNewUsed();
			float anHeapU = eJvmCost.getnHeapU();
			float axHeapU = eJvmCost.getxHeapU();
			float aSSTempObj = eJvmCost.getSSTempObj();
			float aRTempObj = eJvmCost.getRedTempObj();
			float afix = eJvmCost.getFix();
			float aRedIn = eJvmCost.getReduceInputBytes();
			
			nOU = Math.min(nOU, anOU);
			xOU = Math.max(xOU, axOU);
			
			nNGU = Math.min(nNGU, aNGU);
			xNGU = Math.max(xNGU, aNGU);
			
			nHeapU = Math.min(nHeapU, anHeapU);
			if(axHeapU > xHeapU) {
				xHeapU = axHeapU;
				reason = eJvmCost.getReason();
			}
			
			//nTempObj = Math.min(nTempObj, atempObj);
			//xTempObj = Math.max(xTempObj, atempObj);
			nSSTObj = Math.min(nSSTObj, aSSTempObj);
			xSSTObj = Math.max(xSSTObj, aSSTempObj);
			nRTObj = Math.min(nRTObj, aRTempObj);
			xRTObj = Math.max(xRTObj, aRTempObj);
			
			nFix = Math.min(nFix, afix);
			xFix = Math.max(xFix, afix);
			
			nRedIn = Math.min(nRedIn, aRedIn);
			xRedIn = Math.max(xRedIn, aRedIn);
		}
		
		ReducerEstimatedJvmCost eJvmCost = eReducersMemory.get(0);
		float inMemSegBuffer = eJvmCost.getInMemSegBuffer();
		float mergeBuffer = eJvmCost.getMergeBuffer();
		float eShuffleBytesMB = eJvmCost.geteShuffleBytesMB();
		
		reduceOutputWriter.write(
				split + "\t"
				+ xmx + "\t"
				+ xms + "\t"
				+ ismb + "\t"
				+ RN + "\t"
				+ String.format(f1, nOU) + "\t"
				+ String.format(f1, xOU) + "\t"
				+ String.format(f1, OGC) + "\t"
				+ String.format(f1, OGCMX) + "\t"
				+ String.format(f1, nNGU) + "\t"
				+ String.format(f1, xNGU) + "\t"
				+ String.format(f1, NGC) + "\t"
				+ String.format(f1, NGCMX) + "\t"
				+ String.format(f1, inMemSegBuffer) + "\t"
				+ String.format(f1, mergeBuffer) + "\t"
				+ String.format(f1, eShuffleBytesMB) + "\t"
				+ String.format(f1, nRedIn) + "\t"
				+ String.format(f1, xRedIn) + "\t"
				+ String.format(f1, EdenC) + "\t"
				+ String.format(f1, S0C) + "\t"
				+ String.format(f1, nHeapU) + "\t"
				+ String.format(f1, xHeapU) + "\t"
				+ String.format(f1, nSSTObj) + "\t"
				+ String.format(f1, xSSTObj) + "\t"
				+ String.format(f1, nRTObj) + "\t"
				+ String.format(f1, xRTObj) + "\t"
				+ String.format(f1, nFix) + "\t"
				+ String.format(f1, xFix) + "\t"
				+ reason + "\n"
			);
	}

	

	

	private InitialJvmCapacity computeInitalJvmCapacity() {
		
		return new InitialJvmCapacity(finishedConf, newConf, job.getMapperList(), job.getReducerList());
	}

}
