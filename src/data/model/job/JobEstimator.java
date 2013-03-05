package data.model.job;

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
import profile.task.mapper.Spill;
import profile.task.reducer.Reducer;


import data.model.mapper.MapperEstimator;
import data.model.mapper.SpillFitsInMemoryModel;
import data.model.reducer.ReducerEstimator;

public class JobEstimator {
	
	private Configuration conf;
	private Job job;
	private Configuration finishedConf;
	private Configuration newConf;
	
	boolean isXmxChanged;
	boolean isXmsChanged;
	boolean isSplitSizeChanged;
	boolean isMapperConfChanged;
	boolean isReducerConfChanged;
	
	public JobEstimator(Configuration conf) {
		this.conf = conf;
	}
	
	private void profile(String hostname, String jobId, boolean needMetrics, int sampleMapperNum, int sampleReducerNum) {
		SingleJobProfiler profiler = new SingleJobProfiler(hostname, jobId, needMetrics, sampleMapperNum, sampleReducerNum);
		
		job = profiler.profile();
		
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
				long splitSize = Long.parseLong(conf.getConf("split.size"));
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
				Map<Long, Mapper> cacheNewMapper = new HashMap<Long, Mapper>();
				
				for(int i = 0; i < splitsSizeList.size(); i++) {
					Long cSplitSize = splitsSizeList.get(i);
					if(!cacheNewMapper.containsKey(cSplitSize)) {
						Mapper newMapper = mapperEstimator.estimateNewMapper(job.getMapperList(), cSplitSize);
						newMapperList.add(newMapper);
						cacheNewMapper.put(cSplitSize, newMapper); 
					}
					else
						newMapperList.add(cacheNewMapper.get(cSplitSize)); //note: multiple reference of newMapper
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
	
	private List<Reducer> estimateReducers(List<Mapper> newMapperList) {
		int fReducerNum = finishedConf.getMapred_reduce_tasks();

		if(fReducerNum == 0)
			return null;
		
		ReducerEstimator reducerEstimator = new ReducerEstimator(finishedConf, newConf, newMapperList, false);
		
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
	
	public void estimateSpillFitsInMemory() {
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
		sfimm.keepInMemory(fmax_mapred_output_records, fmax_mapred_output_bytes, null);
		
	}
	
	public void outputEstimatedMappers(List<Mapper> eMappers) {
		
		
	}
	
	public void outputEstimatedReducers(List<Reducer> eReducers) {
		// TODO Auto-generated method stub
		
	}
	public static void main(String[] args) {
		// -------------------------Set by Users----------------------------------
		String jobId = "job_201211270045_0005";
		String hostname = "m105";
		
		boolean needMetrics = false; //going to analyze task counters/metrics/jvm?
		int sampleMapperNum = 0; // only analyze the first sampleMapperNum mappers (0 means all the mappers)
		int sampleReducerNum = 0; // only analyze the first sampleReducerNum reducers (0 means all the reducers)
		
		Configuration conf = new Configuration();
		long newSplitSize = 64 * 1024 * 1024l;
		conf.set("io.sort.mb", "400");
		//conf.set("split.size", String.valueOf(newSplitSize));
		//--------------------------Setting ends----------------------------------
		
		JobEstimator je = new JobEstimator(conf);
		je.profile(hostname, jobId, needMetrics, sampleMapperNum, sampleReducerNum);
		
		List<Mapper> eMappers = je.estimateMappers();
		List<Reducer> eReducers = je.estimateReducers(eMappers);
		
		je.outputEstimatedMappers(eMappers);
		je.outputEstimatedReducers(eReducers);
		//je.estimateSpillFitsInMemory();
		
	}

	

	
}
