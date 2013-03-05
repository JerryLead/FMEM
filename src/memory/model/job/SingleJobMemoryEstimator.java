package memory.model.job;

import hadoop.split.InputSplitCalculator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import profile.commons.configuration.Configuration;
import profile.job.Job;
import profile.profiler.SingleJobProfiler;
import profile.task.mapper.Mapper;
import profile.task.reducer.Reducer;

import memory.model.jvm.MapperEstimatedJvmCost;
import memory.model.jvm.ReducerEstimatedJvmCost;
import memory.model.mapper.MapperMemoryEstimator;
import memory.model.reducer.ReducerMemoryEstimator;



import data.model.mapper.MapperEstimator;
import data.model.mapper.SpillFitsInMemoryModel;
import data.model.reducer.ReducerEstimator;

public class SingleJobMemoryEstimator {
	
	private Job job;
	private Configuration finishedConf;
	private Configuration newConf;
	
	
	boolean isXmxChanged;
	boolean isXmsChanged;
	boolean isSplitSizeChanged;
	boolean isMapperConfChanged;
	boolean isReducerConfChanged;
	
	private void profile(String hostname, String jobId, boolean needMetrics, int sampleMapperNum, int sampleReducerNum) {
		
		SingleJobProfiler profiler = new SingleJobProfiler(hostname, jobId, needMetrics, sampleMapperNum, sampleReducerNum);
		job = profiler.profile();
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
				int end = value.indexOf('m', start);	
				int xmx = Integer.parseInt(value.substring(start, end)); //4000;
				
				start = value.indexOf("-Xms") + 4;
				end = value.indexOf('m', start);
				int xms = Integer.parseInt(value.substring(start, end)); //1000;
				
				if(finishedConf.isXmxChanged(xmx))
					isXmxChanged = true;
				if(finishedConf.isXmsChanged(xms))
					isXmsChanged = true;
			}
			
			if(key.equals("split.size") && finishedConf.isSpliSizeChanged(value)) 
				isSplitSizeChanged = true;		
			
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
				long splitSize = Long.parseLong(newConf.getConf("split.size"));
				long blockSize = finishedConf.getDfs_block_size();
				long minSplit = 1L;
				long maxSplit = Long.MAX_VALUE;
				if(splitSize < blockSize) 
					maxSplit = splitSize;
				else
					minSplit = splitSize;
				
				newConf.set("mapred.min.split.size", String.valueOf(minSplit));
				newConf.set("mapred.max.split.size", String.valueOf(maxSplit));
				
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
						newMapperList.add(cacheNewMapper.get(cSplitMB)); //note: multiple reference of newMapper
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
	
	
	private List<MapperEstimatedJvmCost> estimateMappersMemory(List<Mapper> eMappers, InitialJvmCapacity gcCap) {
		List<MapperEstimatedJvmCost> mappersJvmCostList = new ArrayList<MapperEstimatedJvmCost>();
		
		MapperMemoryEstimator memoryEstimator = new MapperMemoryEstimator(finishedConf, newConf, gcCap);
		MapperEstimatedJvmCost jvmCost;
		
		if(isMapperConfChanged || isSplitSizeChanged || isXmxChanged || isXmsChanged) {
			//split size is changed
			if(isSplitSizeChanged) {
				Map<Integer, MapperEstimatedJvmCost> cacheJvmCost = new HashMap<Integer, MapperEstimatedJvmCost>();
				
				for(int i = 0; i < eMappers.size(); i++) {
					Mapper eMapper = eMappers.get(i);
					
					int cSplitMB = (int) (eMapper.getInput().getSplitSize() / (1024 * 1024)) ; //HDFS_BYTES_READ
					
					if(!cacheJvmCost.containsKey(cSplitMB)) {
						jvmCost = memoryEstimator.estimateJvmCost(job.getMapperList(), eMapper);	
						cacheJvmCost.put(cSplitMB, jvmCost); 
					}
					else
						jvmCost = cacheJvmCost.get(cSplitMB);
					mappersJvmCostList.add(jvmCost);
				}
			}
			
			//do not need to consider split size
			else {
				
				for(int i = 0; i < job.getMapperList().size(); i++) {
					jvmCost = memoryEstimator.estimateJvmCost(job.getMapperList().get(i), eMappers.get(i));
					mappersJvmCostList.add(jvmCost);
				}
					
			}	
				
		}
		//none mapper configuration changed
		else {
			for(int i = 0; i <job.getMapperList().size(); i++) {
				jvmCost = memoryEstimator.copyJvmCost(job.getMapperList().get(i));
				mappersJvmCostList.add(jvmCost);
			}
		}
	
		return mappersJvmCostList;
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
	
	private void displayJvmCostResult(List<MapperEstimatedJvmCost> eMappersMemory, List<ReducerEstimatedJvmCost> eReducersMemory) {
		System.out.println("-----------------------------Mapper Memory Estimation-------------------------------");
		if(eMappersMemory != null)
			for(MapperEstimatedJvmCost jvmCost : eMappersMemory) {
				System.out.println(jvmCost);
			}
		
		System.out.println("-----------------------------Reducer Memory Estimation-------------------------------");
		if(eReducersMemory != null)
			for(ReducerEstimatedJvmCost jvmCost : eReducersMemory) {
				System.out.println(jvmCost);
			}
		
	}

	
	public static void main(String[] args) {
		// -------------------------Set by Users------------------------------------
		String jobId = "job_201212051641_0001";
		String hostname = "master";
		
		boolean needMetrics = true; //going to analyze task counters/metrics/jvm?
		int sampleMapperNum = 0; // only analyze the first sampleMapperNum mappers (0 means all the mappers)
		int sampleReducerNum = 0; // only analyze the first sampleReducerNum reducers (0 means all the reducers)
		boolean useRuntimeMaxJvmHeap = true; //since reducers' actual JVM heap is less than mapred.child.java.opts, 
											 //this parameter determines whether to use the actual JVM heap to estimate
		Configuration conf = new Configuration();
		//long newSplitSize = 128 * 1024 * 1024l;
		conf.set("io.sort.mb", "800");
		conf.set("mapred.child.java.opts", "-Xms3000m -Xmx3000m");
		//conf.set("split.size", String.valueOf(newSplitSize));
		//--------------------------Setting ends------------------------------------
		
		//--------------------------Estimate the data flow---------------------------
		SingleJobMemoryEstimator je = new SingleJobMemoryEstimator();
		je.profile(hostname, jobId, needMetrics, sampleMapperNum, sampleReducerNum);
		
		//je.estimateSpillFitsInMemory(conf);
		
		je.setNewConf(conf);
		
		List<Mapper> eMappers = je.estimateMappers();	
		List<Reducer> eReducers = je.estimateReducers(eMappers, useRuntimeMaxJvmHeap);
		
		// -------------------------Estimate the memory cost-------------------------
		InitialJvmCapacity gcCap = je.computeInitalJvmCapacity();
		
		List<MapperEstimatedJvmCost> eMappersMemory = je.estimateMappersMemory(eMappers, gcCap);
		List<ReducerEstimatedJvmCost> eReducersMemory = je.estimateReducersMemory(eReducers, gcCap);
		
		je.displayJvmCostResult(eMappersMemory, eReducersMemory);
	}

	private InitialJvmCapacity computeInitalJvmCapacity() {
		
		return new InitialJvmCapacity(finishedConf, newConf, job.getMapperList(), job.getReducerList());
	}

}
