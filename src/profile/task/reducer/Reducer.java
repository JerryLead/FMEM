package profile.task.reducer;

import java.io.Serializable;

import profile.commons.configuration.ReducerConfiguration;
import profile.commons.counters.ReducerCounters;
import profile.commons.metrics.GcCapacity;
import profile.commons.metrics.Metrics;
import profile.task.common.Task;

public class Reducer implements Task, Serializable {
	// basic infos
	private String taskId;
	private String machine;

	// execution time
	private long reducerStartTimeMS;
	private long shufflePhaseFinishTimeMS;
	private long sortPhaseFinishTimeMS;
	private long reducePhaseStopTimeMS;
	private long commitStartTimeMS;
	private long reducerStopTimeMS;

	// reduce phase information
	private ReducerBuffer buffer = new ReducerBuffer();
	private Shuffle shuffle = new Shuffle();
	private MergeInShuffle mergeInShuffle = new MergeInShuffle();
	private Sort sort = new Sort();
	private Reduce reduce = new Reduce();

	// other dimensions
	private ReducerConfiguration configuration = new ReducerConfiguration();
	private ReducerCounters counters = new ReducerCounters();
	private Metrics metrics = new Metrics();

	// set basic infos
	public void setTaskId(String taskId) {
		this.taskId = taskId;
	}

	public void setMachine(String machine) {
		this.machine = machine;
	}

	// set phase start/stop time
	public void setReducerStartTime(long reducerStartTime) {
		this.reducerStartTimeMS = reducerStartTime;
	}

	public void setShufflePhaseFinishTimeMS(long shufflePhaseFinishTimeMS) {
		this.shufflePhaseFinishTimeMS = shufflePhaseFinishTimeMS;
	}

	public void setSortPhaseFinishTimeMS(long sortPhaseFinishTimeMS) {
		this.sortPhaseFinishTimeMS = sortPhaseFinishTimeMS;
	}

	public void setReducePhaseStopTimeMS(long reducePhaseStopTimeMS) {
		this.reducePhaseStopTimeMS = reducePhaseStopTimeMS;
	}

	public void setCommitStartTimeMS(long commitStartTimeMS) {
		this.commitStartTimeMS = commitStartTimeMS;
	}

	public void setReducerStopTimeMS(long reducerStopTimeMS) {
		this.reducerStopTimeMS = reducerStopTimeMS;
		
		counters.setFinalCountersItem("Reduce shuffle raw bytes", shuffle.getReduce_shuffle_raw_bytes());
		
		//Note: task's final counters should be collected first
		//parseMapTaskCounters(counterLink, mapper); should be done beforehand
		//parseMapTaskLog(logLink, mapper); // set task log infos
		reduce.setInputkeyNum(counters.getReduce_input_groups());
		reduce.setInputKeyValuePairsNum(counters.getReduce_input_records());
		//reduce.setInputBytes();
		reduce.setOutputKeyValuePairsNum(counters.getReduce_output_records());
		reduce.setOutputBytes(counters.getHDFS_BYTES_WRITTEN());
		
		reduce.setInputBytes(counters.getReduce_shuffle_raw_bytes() - mergeInShuffle.getReduceInputBytesLoss());
		reduce.setComputedInputRecords(sort.getReduceInputRecords());
	}

	public ReducerBuffer getReducerBuffer() {
		return buffer;
	}
	
	public Shuffle getShuffle() {
		return shuffle;
	}
	
	public MergeInShuffle getMergeInShuffle() {
		return mergeInShuffle;
	}
	
	public Sort getSort() {
		return sort;
	}
	
	public Reduce getReduce() {
		return reduce;
	}

	public ReducerConfiguration getReducerConfiguration() {
		return configuration;
	}
	
	public ReducerCounters getReducerCounters() {
		return counters;
	}
	
	public Metrics getMetrics() {
		return metrics;
	}

	@Override
	public void addCounterItem(String[] parameters) {
		counters.addCounterItem(parameters);
		
	}

	@Override
	public void addMetricsItem(String[] parameters) {
		metrics.addMetricsItem(parameters);
		
	}

	@Override
	public void addJvmMetrics(String[] parameters) {
		metrics.addJvmMetrics(parameters);

	}
	
	@Override
	public void addJstatMetrics(String[] jstatParams) {
		metrics.addJstatMetrics(jstatParams);
	}
	
	@Override
	public void addGcCapacity(GcCapacity gcCap) {
		metrics.addGcCapacity(gcCap);
	}
	
	public void setReducerBuffer(ReducerBuffer reducerBuffer) {
		this.buffer = reducerBuffer;
	}
	
	public void setShuffle(Shuffle shuffle) {
		this.shuffle = shuffle;
	}
	
	public void setMergeInShuffle(MergeInShuffle mergeInShuffle) {
		this.mergeInShuffle = mergeInShuffle;
	}
	
	public void setSort(Sort sort) {
		this.sort = sort;
	}
	
	public void setReduce(Reduce reduce) {
		this.reduce = reduce;
	}

	public long getShufflePhaseFinishTimeMS() {
		return shufflePhaseFinishTimeMS;
	}

	public long getSortPhaseFinishTimeMS() {
		return sortPhaseFinishTimeMS;
	}

	public long getReducePhaseStopTimeMS() {
		return reducePhaseStopTimeMS;
	}



	
}
