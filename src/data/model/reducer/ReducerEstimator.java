package data.model.reducer;

import java.util.List;

import profile.commons.configuration.Configuration;
import profile.task.mapper.Mapper;
import profile.task.reducer.MergeInShuffle;
import profile.task.reducer.Reduce;
import profile.task.reducer.Reducer;
import profile.task.reducer.ReducerBuffer;
import profile.task.reducer.Shuffle;
import profile.task.reducer.Sort;




public class ReducerEstimator {
	
	private List<Mapper> newMapperList;
	private Configuration fConf;
	private Configuration newConf;
	private int reducerIndex;
	private Reducer estimatedReducer;
	private IORatioModel ioRatioModel;
	private boolean useRuntimeMaxJvmHeap;
	private int runtimeMaxJvmHeapMB;
	
	private double mapperCombineRecIORatio;
	private double mapperCombineBytesIORatio;

	public ReducerEstimator(Configuration fConf, Configuration newConf, List<Mapper> newMapperList, boolean useRuntimeMaxJvmHeap) {
		this.fConf = fConf;
		this.newConf = newConf;
		this.newMapperList = newMapperList;
		this.useRuntimeMaxJvmHeap = useRuntimeMaxJvmHeap;
		computeMapperCombineRatio(newMapperList);
	}
	
	private void computeMapperCombineRatio(List<Mapper> newMapperList) {
		double avg_spill_combine_record_ratio = 0;
		double avg_spill_combine_bytes_ratio = 0;
		double avg_merge_combine_record_ratio = 0;
		double avg_merge_combine_bytes_ratio = 0;
		
		for(Mapper mapper : newMapperList) {
			avg_spill_combine_record_ratio += mapper.getMapperCounters().getSpill_combine_record_ratio();
			avg_spill_combine_bytes_ratio += mapper.getMapperCounters().getSpill_combine_bytes_ratio();
			avg_merge_combine_record_ratio += mapper.getMapperCounters().getMerge_combine_record_ratio();
			avg_merge_combine_bytes_ratio += mapper.getMapperCounters().getMerge_combine_bytes_ratio();
		}
		
		avg_spill_combine_record_ratio /= newMapperList.size();
		avg_spill_combine_bytes_ratio /= newMapperList.size();
		avg_merge_combine_record_ratio /= newMapperList.size();
		avg_merge_combine_bytes_ratio /= newMapperList.size();
		
		double interpolation = 0.5;
		mapperCombineRecIORatio = avg_spill_combine_record_ratio * interpolation + avg_merge_combine_record_ratio * (1 - interpolation);
		mapperCombineBytesIORatio = avg_spill_combine_bytes_ratio * interpolation + avg_merge_combine_bytes_ratio * (1 - interpolation);
	}
	
	public Reducer estimateNewReducer(Reducer finishedReducer, int reducerIndex) {
		boolean newHasCombine = newConf.getMapreduce_combine_class() != null;
		estimatedReducer = new Reducer();
		this.ioRatioModel = new IORatioModel(finishedReducer, newHasCombine, mapperCombineRecIORatio, mapperCombineBytesIORatio);
		this.reducerIndex = reducerIndex;
		
		this.runtimeMaxJvmHeapMB = computeRuntimeMaxJvmHeapMB(finishedReducer);
		computeConcretePhase();
		return estimatedReducer;
	}

	

	public Reducer estimateNewReducer(List<Reducer> finishedReducerList, int reducerIndex) {
		boolean newHasCombine = newConf.getMapreduce_combine_class() != null;
		estimatedReducer = new Reducer();
		
		if(ioRatioModel == null)
			this.ioRatioModel = new IORatioModel(finishedReducerList, newHasCombine, mapperCombineRecIORatio, mapperCombineBytesIORatio);
		
		this.reducerIndex = reducerIndex;
		
		this.runtimeMaxJvmHeapMB = computeRuntimeMaxJvmHeapMB(finishedReducerList);
		computeConcretePhase();
		return estimatedReducer;
	}
	
	
	private void computeConcretePhase() {
		//System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~Estimated Reducer~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
		
		int mapred_child_java_opts = newConf.getXmx();

		if(useRuntimeMaxJvmHeap && mapred_child_java_opts == fConf.getXmx() && runtimeMaxJvmHeapMB != 0)
			mapred_child_java_opts = runtimeMaxJvmHeapMB;
			
		//int mapred_reduce_parallel_copies = newConf.getMapred_reduce_parallel_copies();
		int io_sort_factor = newConf.getIo_sort_factor();
		int mapred_inmem_merge_threshold = newConf.getMapred_inmem_merge_threshold();
		
		float mapred_job_shuffle_merge_percent = newConf.getMapred_job_shuffle_merge_percent();
		float mapred_job_shuffle_input_buffer_percent = newConf.getMapred_job_shuffle_input_buffer_percent();
		float mapred_job_reduce_input_buffer_percent = newConf.getMapred_job_reduce_input_buffer_percent();

		ReducerBuffer buffer = ReducerBufferModel.computeBufferLimit(
				mapred_child_java_opts, 
				mapred_inmem_merge_threshold, 
				mapred_job_shuffle_merge_percent,
				mapred_job_shuffle_input_buffer_percent, 
				mapred_job_reduce_input_buffer_percent);
		
		Shuffle shuffle = ShuffleModel.computeShuffle(newMapperList, reducerIndex);	
		MergeInShuffle mergeInShuffle = MergeInShuffleModel.computeMerge(buffer, shuffle, ioRatioModel, io_sort_factor);
		Sort sort = SortModel.computeSort(buffer, mergeInShuffle, newConf);
		Reduce reduce = ReduceModel.computeReduce(sort, ioRatioModel);		
		
		estimatedReducer.setReducerBuffer(buffer);
		estimatedReducer.setShuffle(shuffle);
		estimatedReducer.setMergeInShuffle(mergeInShuffle);
		estimatedReducer.setSort(sort);
		estimatedReducer.setReduce(reduce);		

		estimatedReducer.getReducerCounters().setFinalCountersItem("Reduce shuffle bytes", shuffle.getReduce_shuffle_bytes());
		estimatedReducer.getReducerCounters().setFinalCountersItem("Reduce shuffle raw bytes", shuffle.getReduce_shuffle_raw_bytes());
		estimatedReducer.getReducerCounters().setFinalCountersItem("Reduce input records", reduce.getInputKeyValuePairsNum());
		estimatedReducer.getReducerCounters().setFinalCountersItem("Reduce input bytes", reduce.getInputBytes());
		estimatedReducer.getReducerCounters().setFinalCountersItem("Reduce output records", reduce.getOutputKeyValuePairsNum());
		estimatedReducer.getReducerCounters().setFinalCountersItem("Reduce output bytes", reduce.getOutputBytes());
	
	}
	
	private int computeRuntimeMaxJvmHeapMB(Reducer finishedReducer) {
		long memoryLimit = finishedReducer.getReducerBuffer().getMemoryLimit();
		float mapred_job_shuffle_input_buffer_percent = fConf.getMapred_job_shuffle_input_buffer_percent();
		
		long runtimeMaxJvmHeapBytes = ReducerBufferModel.computeRuntimeMaxJvmHeap(memoryLimit, mapred_job_shuffle_input_buffer_percent);
		
		return (int) (runtimeMaxJvmHeapBytes / 1024 / 1024);
	}
	
	private int computeRuntimeMaxJvmHeapMB(List<Reducer> finishedReducerList) {
		long incRuntimeMaxJvmHeapBytes = 0;
		for(Reducer reducer : finishedReducerList) 
			incRuntimeMaxJvmHeapBytes += computeRuntimeMaxJvmHeapMB(reducer);
		return (int) (incRuntimeMaxJvmHeapBytes / finishedReducerList.size());
	}

}
