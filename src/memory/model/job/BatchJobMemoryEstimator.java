package memory.model.job;

import hadoop.split.InputSplitCalculator;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import profile.commons.configuration.Configuration;
import profile.job.Job;
import profile.profiler.SingleJobProfiler;
import profile.task.mapper.Mapper;
import profile.task.mapper.Merge;
import profile.task.mapper.MergeInfo;
import profile.task.reducer.Reducer;

import memory.model.jvm.MapperEstimatedJvmCost;
import memory.model.jvm.ReducerEstimatedJvmCost;
import memory.model.mapper.MapperMemoryEstimator;
import memory.model.reducer.ReducerMemoryEstimator;



import data.model.mapper.MapperEstimator;
import data.model.mapper.SpillFitsInMemoryModel;
import data.model.reducer.ReducerEstimator;

public class BatchJobMemoryEstimator {
	
	private Job job;
	private String jobId;
	private Configuration finishedConf;
	private Configuration newConf;
	
	
	boolean isXmxChanged;
	boolean isXmsChanged;
	boolean isSplitSizeChanged;
	boolean isMapperConfChanged;
	boolean isReducerConfChanged;
	
	public static void main(String[] args) {
		// -------------------------Initialize Job Information------------------------------------
		String startJobId = "job_201301102246_0001";
		//String startJobId = "job_201212172252_0001";
		//String jobName = "uservisits_aggre-pig-256MB";
		//String baseDir = "/home/xulijie/MR-MEM/NewExperiments2/";
		//String baseDir = "/home/xulijie/MR-MEM/NewExperiments3/";
		String baseDir = "/home/xulijie/MR-MEM/BigExperiments/";
		String jobName = "big-uservisits_aggre-pig-256MB";
		
		String hostname = "master";
		
		
		//String outputDir = "/home/xulijie/MR-MEM/NewExperiments/Wiki-m36-r18-256MB/estimatedJvmCost/";
		//String outputDir = "/home/xulijie/MR-MEM/NewExperiments/uservisits_aggre-pig-256MB/estimatedJvmCost/";
		//String outputDir = "/home/xulijie/MR-MEM/NewExperiments/TeraSort-256MB-m36-r18/estimatedJvmCost/";
		//String outputDir = "/home/xulijie/MR-MEM/NewExperiments2/BuildCompIndex-m36-r18-256MB/estimatedJvmCost/";
		String outputDir = baseDir + jobName + "/estimatedJvmCost/";
		
		int iterateNum = 64;
		
		boolean needMetrics = true; //going to analyze task counters/metrics/jvm?
		int sampleMapperNum = 0; // only analyze the first sampleMapperNum mappers (0 means all the mappers)
		int sampleReducerNum = 0; // only analyze the first sampleReducerNum reducers (0 means all the reducers)
		boolean useRuntimeMaxJvmHeap = false; //since reducers' actual JVM heap is less than mapred.child.java.opts, 
											 //this parameter determines whether to use the actual JVM heap to estimate
		
		//--------------------------Setting ends------------------------------------
		
		DecimalFormat nf = new DecimalFormat("0000");
		
		for(int i = 0; i < iterateNum; i++) {
			String prefix = startJobId.substring(0, startJobId.length() - 4);
			int suffix = Integer.parseInt(startJobId.substring(startJobId.length() - 4));
			String jobId = prefix + nf.format(suffix + i);		
			//--------------------------Profiling the run job-----------------------------
			BatchJobMemoryEstimator je = new BatchJobMemoryEstimator();
			boolean successful = je.profile(hostname, jobId, needMetrics, sampleMapperNum, sampleReducerNum);
			//--------------------------Profiling ends-----------------------------
			
			//--------------------------Estimating Data Flow and Memory-----------------------------
			if(successful == false) {
				System.err.println("[" + jobId + "] is a failed job");
				continue;
			}
					
			try {
				je.batchEstimateDataAndMemory(useRuntimeMaxJvmHeap, outputDir + jobId);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}	
			//--------------------------Estimating ends-----------------------------
			System.out.println("Finish estimating " + jobId);
		}	
	}
	
	private void batchEstimateDataAndMemory(boolean useRuntimeMaxJvmHeap, String outputDir) throws IOException {
		//File mDataOutputFile = new File(outputDir, "eDataMappers.txt");
		//File rDataOutputFile = new File(outputDir, "eDataReducers.txt");
		
		File mJvmOutputFile = new File(outputDir, "eJvmMappers.txt");
		File rJvmOutputFile = new File(outputDir, "eJvmReducers.txt");
		
		if(!mJvmOutputFile.getParentFile().exists()) 
			mJvmOutputFile.getParentFile().mkdirs();
		
		//PrintWriter mDataWriter = new PrintWriter(new BufferedWriter(new FileWriter(mDataOutputFile)));
		//PrintWriter rDataWriter = new PrintWriter(new BufferedWriter(new FileWriter(rDataOutputFile)));
		
		PrintWriter mJvmWriter = new PrintWriter(new BufferedWriter(new FileWriter(mJvmOutputFile)));
		PrintWriter rJvmWriter = new PrintWriter(new BufferedWriter(new FileWriter(rJvmOutputFile)));
		
		//displayMapDataTitle(mDataWriter);
		//displayReduceDataTitle(rDataWriter);
		
		displayMapJvmCostTitle(mJvmWriter);
		displayReduceJvmCostTitle(rJvmWriter);
		
		for(int xmx = 1000; xmx <= 4000; xmx = xmx + 1000) {
			for(int ismb = 200; ismb <= 1000; ismb = ismb + 200) {
				for(int reducer = 9; reducer <= 18; reducer = reducer * 2) {
					for(int xms = 0; xms <= 1; xms++) {
						
						//--------------------------Estimate the data flow---------------------------
						//-----------------for debug-------------------------------------------------
					    //System.out.println("[xmx = " + xmx + ", xms = " + xms + ", ismb = " + 
						//		ismb + ", RN = " + reducer + "]");
						//if(xmx != 4000 || ismb != 400 || reducer != 16 || xms != 1)
						//	continue;
						//if(xmx != 4000 || xms != 1 || ismb != 1000 || reducer != 9)
						//	continue;
						//---------------------------------------------------------------------------
						
						Configuration conf = new Configuration();
						//long newSplitSize = 128 * 1024 * 1024l;
						conf.set("io.sort.mb", String.valueOf(ismb));
						
						if(xms == 0)
							conf.set("mapred.child.java.opts", "-Xmx" + xmx + "m");
						else
							conf.set("mapred.child.java.opts", "-Xmx" + xmx + "m" + " -Xms" + xmx + "m");
						
						conf.set("mapred.reduce.tasks", String.valueOf(reducer));
						
						//conf.set("split.size", String.valueOf(newSplitSize));
						setNewConf(conf);
						
						// -------------------------Estimate the data flow-------------------------
						List<Mapper> eMappers = estimateMappers();	//don't filter the mappers with small split size
						List<Reducer> eReducers = estimateReducers(eMappers, useRuntimeMaxJvmHeap);
						
						String fileName = conf.getConf("mapred.child.java.opts").replaceAll(" ", "") + "-ismb" + ismb + "-RN" + reducer;
						displayMapperDataResult(eMappers, fileName , outputDir + File.separator + "eDataflow");
						displayReducerDataResult(eReducers, fileName, outputDir + File.separator + "eDataflow");
						
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
		
		
		mJvmWriter.close();
		rJvmWriter.close();
		
	}
	
	private boolean profile(String hostname, String jobId, boolean needMetrics, int sampleMapperNum, int sampleReducerNum) {
		
		SingleJobProfiler profiler = new SingleJobProfiler(hostname, jobId, needMetrics, sampleMapperNum, sampleReducerNum);
		job = profiler.profile();
		this.jobId = jobId;
		if(job == null)
			return false;
		return true;
	}
	
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
		
		if(isSplitSizeChanged || isMapperConfChanged) {
			MapperEstimator mapperEstimator = new MapperEstimator(finishedConf, newConf);
					
			//split size is changed
			if(isSplitSizeChanged) {
				
				List<Long> splitsSizeList = InputSplitCalculator.getSplitsLength(newConf);
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
			else if(isMapperConfChanged) {
				for(int i = 0; i < job.getMapperList().size(); i++) {
					Mapper newMapper = mapperEstimator.estimateNewMapper(job.getMapperList().get(i));
					newMapperList.add(newMapper);
				}
				
			}
			//none mapper configuration changed
			else
				for(Mapper mapper : job.getMapperList()) 
					newMapperList.add(mapper);
		}
		//none mapper configuration changed
		else {
			for(Mapper mapper : job.getMapperList()) 
				newMapperList.add(mapper);
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
		
		//reducer number doesn't change
		//reducerIndex means which partition of mappers' output will be received by the new reducer 
		if(fReducerNum == newConf.getMapred_reduce_tasks()) {
			for(int i = 0; i < fReducerNum; i++) {
				//use the corresponding finished reducer to estimate the new reducer
				Reducer newReducer = reducerEstimator.estimateNewReducer(job.getReducerList().get(i), i);
				newReducerList.add(newReducer);
			}
		}
		//reducer number changes
		else {
			for(int i = 0; i < newConf.getMapred_reduce_tasks(); i++) {
				//use all the finished reducers' infos to estimate the new reducer
				Reducer newReducer = reducerEstimator.estimateNewReducer(job.getReducerList(), i);
				newReducerList.add(newReducer);
			}
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
		
		if(isMapperConfChanged || isSplitSizeChanged || isXmxChanged || isXmsChanged) {
			
			//split size is changed
			if(isSplitSizeChanged) {
				Map<Long, MapperEstimatedJvmCost> cacheJvmCost = new HashMap<Long, MapperEstimatedJvmCost>();
				List<Mapper> filterFMapperList = filterSmallSplitSize(job.getMapperList());
				
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
			}
			
			//do not need to consider split size
			else {
				
				for(int i = 0; i < job.getMapperList().size(); i++) {
					
					Mapper eMapper = job.getMapperList().get(i);
					if(eMapper.getInput().getSplitSize() / (1024 * 1024) >= splitSizeMB) {
						jvmCost = memoryEstimator.estimateJvmCost(job.getMapperList().get(i), eMappers.get(i));
						//happen occasionally 
						if(jvmCost == null) 
							System.err.println("Error when parsing " + job.getMapperList().get(i).getTaskId() + "'s Jstat log, ignore it");
						else
							mappersJvmCostList.add(jvmCost);
					}		
				}		
			}			
		}
		//none mapper configuration changed
		else {
			
			for(int i = 0; i <job.getMapperList().size(); i++) {
				Mapper eMapper = job.getMapperList().get(i);
				if(eMapper.getInput().getSplitSize() / (1024 * 1024) >= splitSizeMB) {
					jvmCost = memoryEstimator.copyJvmCost(job.getMapperList().get(i));
					mappersJvmCostList.add(jvmCost);
				}
			}
		}
	
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
		//reducer number doesn't change
		if(fReducerNum == newConf.getMapred_reduce_tasks()) {
			for(int i = 0; i < fReducerNum; i++) {
				//use the corresponding finished reducer to estimate the new reducer
				//if input data is changed, this model needs modification
				//currently, this model assumes the total input data is as same as the finished one
				jvmCost = memoryEstimator.esimateJvmCost(job.getReducerList().get(i), eReducers.get(i));
				reducersJvmCostList.add(jvmCost);
			}
		}
		//reducer number changes
		else {
			for(int i = 0; i < newConf.getMapred_reduce_tasks(); i++) {
				//use all the finished reducers' infos to estimate the new reducer
				jvmCost = memoryEstimator.esimateJvmCost(job.getReducerList(), eReducers.get(i));
				reducersJvmCostList.add(jvmCost);
			}
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
				"xmx" + "\t"
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
			xmx + "\t"
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
				"xmx" + "\t"
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
				xmx + "\t"
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
