package profile.task.mapper;

import java.io.Serializable;

import profile.commons.configuration.Configuration;
import profile.commons.configuration.MapperConfiguration;
import profile.commons.counters.MapperCounters;
import profile.commons.metrics.GcCapacity;
import profile.commons.metrics.Metrics;
import profile.task.common.Task;

//represent the map task
public class Mapper implements Task, Serializable {

	private String taskId;

	
	// execution time
	private long mapperStartTimeMS;
	private long mapperStopTimeMS;

	// important times
	private long firstSpillStartTimeMS;
	private long spillPhaseFinishTimeMS; // also start time of merge phase

	// map phase information
	private Input input = new Input();
	private Map map = new Map();

	private MapperBuffer buffer = new MapperBuffer();
	private Spill spill = new Spill();
	private Merge merge = new Merge();
	
	// other dimensions
	private Configuration configuration = new Configuration();
	private MapperCounters counters = new MapperCounters(); // contains final Counters (monitored
															// Counters are included in super class Counters)
	private Metrics metrics = new Metrics();

	// set task id
	public void setTaskId(String taskId) {
		this.taskId = taskId;
	}

	// set mapper start time
	public void setMapperStartTimeMS(long mapperStartTimeMS) {
		this.mapperStartTimeMS = mapperStartTimeMS;

	}

	// set merge phase start time
	public void setMergePhaseStartTimeMS(long startMergePhaseTimeMS) {
		this.spillPhaseFinishTimeMS = startMergePhaseTimeMS;

	}
	
	// set mapper finish time
	public void setMapperStopTimeMS(long mapperStopTimeMS) {
		this.mapperStopTimeMS = mapperStopTimeMS;
		input.setSplitSize(counters.getHDFS_BYTES_READ());
		firstSpillStartTimeMS = spill.getSpillInfoList().get(0).getStartSpillTimeMS();
	}

	public Input getInput() {
		return input;
	}

	public Map getMap() {
		return map;
	}

	public MapperBuffer getMapperBuffer() {
		return buffer;
	}

	public Spill getSpill() {
		return spill;
	}

	public Merge getMerge() {
		return merge;
	}

	public Configuration getMapperConfiguration() {
		return configuration;
	}

	public MapperCounters getMapperCounters() {
		return counters;
	}

	public Metrics getMetrics() {
		return metrics;
	}

	
	public void setInput(Input input) {
		this.input = input;
	}
	public void setMap(Map map) {
		this.map = map;
	}
	
	public void setMapperBuffer(MapperBuffer mapperBuffer) {
		this.buffer = mapperBuffer;
	}

	public void setSpill(Spill spill) {
		this.spill = spill;
	}
	
	public void setMerge(Merge merge) {
		this.merge = merge;
	}

	public void setConfiguration(Configuration jobConf) {
		this.configuration = jobConf;
		
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
	public void addJstatMetrics(String[] parameters) {
		metrics.addJstatMetrics(parameters);
		
	}
	
	@Override
	public void addGcCapacity(GcCapacity gcCap) {
		metrics.addGcCapacity(gcCap);
		
	}
	

	public long getMapperStartTimeMS() {
		return mapperStartTimeMS;
	}

	public long getMapperStopTimeMS() {
		return mapperStopTimeMS;
	}

	public long getFirstSpillStartTimeMS() {
		return firstSpillStartTimeMS;
	}

	public long getSpillPhaseFinishTimeMS() {
		return spillPhaseFinishTimeMS;
	}

	public String getTaskId() {
		return taskId;
	}

	
}
