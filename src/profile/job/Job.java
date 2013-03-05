package profile.job;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import profile.commons.configuration.Configuration;
import profile.task.mapper.Mapper;
import profile.task.reducer.Reducer;



public class Job implements Serializable {
	//job name
	private String jobName;
	
	//execution time
	private long jobStartTimeMS;
	private long jobStopTimeMS;
	
	//mappers and reducers
	private List<Mapper> mapperList = new ArrayList<Mapper>();
	private List<Reducer> reducerList = new ArrayList<Reducer>();
	
	//configuration
	private Configuration jobConfiguraiton = new Configuration();
	
	//Job Integrated Counters
	private JobCounters jobCounters = new JobCounters();
	private FileSystemCounters fileSystemCounters = new FileSystemCounters();
	private MapReduceFramework mapReduceFramework = new MapReduceFramework();
	
	//Job Phase from Details History 
	private Phase setupPhase = new Phase();
	private Phase mapPhase = new Phase();
	private Phase reducePhase = new Phase();
	private Phase cleanupPhase = new Phase();
	
	//set basic information
	public void setJobName(String jobName) {
		this.jobName = jobName;
	}
	public long getJobStartTimeMS() {
		return jobStartTimeMS;
	}

	public void setJobStartTimeMS(long jobStartTimeMS) {
		this.jobStartTimeMS = jobStartTimeMS;
	}

	public long getJobStopTimeMS() {
		return jobStopTimeMS;
	}

	public void setJobStopTimeMS(long jobStopTimeMS) {
		this.jobStopTimeMS = jobStopTimeMS;
	}
	
	//add mapper and reducer infos
	public void addMapper(Mapper newMapper) {
		mapperList.add(newMapper);
	}
	
	public void addReducer(Reducer newReducer) {
		reducerList.add(newReducer);
	}

	//add job integrated counters
	public void addJobCountersItem(String rowspanName, String counterName, long[] countersValue) {
		if(rowspanName.equals("Job Counters")) {
			jobCounters.set(counterName, countersValue);
		}
		else if(rowspanName.equals("FileSystemCounters")) {
			fileSystemCounters.set(counterName, countersValue);
		}
		else if(rowspanName.equals("Map-Reduce Framework")) {
			mapReduceFramework.set(counterName, countersValue);
		}
	}
	
	//add phases infos
	public void setPhaseItem(String kind, int totalTasksNum,
			int successfulTasksNum, int failedTasksNum, int killedTasksNum,
			long phaseStartTimeMS, long phaseStopTimeMS) {
		if(kind.equals("setup")) 
			setupPhase.set(totalTasksNum, successfulTasksNum, failedTasksNum, killedTasksNum,
					phaseStartTimeMS, phaseStopTimeMS);
		else if (kind.equals("map"))
			mapPhase.set(totalTasksNum, successfulTasksNum, failedTasksNum, killedTasksNum,
					phaseStartTimeMS, phaseStopTimeMS);
		else if (kind.equals("reduce"))
			reducePhase.set(totalTasksNum, successfulTasksNum, failedTasksNum, killedTasksNum,
					phaseStartTimeMS, phaseStopTimeMS);
		else
			cleanupPhase.set(totalTasksNum, successfulTasksNum, failedTasksNum, killedTasksNum,
					phaseStartTimeMS, phaseStopTimeMS);
		
	}
	
	//add job configuration items
	public void addConfItem(String confName, String value) {
		jobConfiguraiton.set(confName, value);
		
	}
	public List<Mapper> getMapperList() {
		return mapperList;
	}
	public List<Reducer> getReducerList() {
		return reducerList;
	}
	
	public Configuration getJobConfiguration() {
		return jobConfiguraiton;
	}
	
	public String getJobName() {
		return jobName;
	}
	
	
}

class JobCounters implements Serializable {
	private long[] Launched_reduce_tasks;
	private long[] Rack_local_map_tasks;
	private long[] Launched_map_tasks;
	private long[] Data_local_map_tasks;
	
	public void set(String counterName, long[] countersValues) {
		if(counterName.equals("Launched reduce tasks")) {
			Launched_reduce_tasks = countersValues;
		}
		else if(counterName.equals("Rack-local map tasks")) {
			Rack_local_map_tasks = countersValues;
		}
		else if(counterName.equals("Launched map tasks")) {
			Launched_map_tasks = countersValues;
		}
		else if(counterName.equals("Data-local map tasks")) {
			Data_local_map_tasks = countersValues;
		}
				
	}
	
}

class FileSystemCounters implements Serializable {
	private long[] FILE_BYTES_READ;
	private long[] HDFS_BYTES_READ;
	private long[] FILE_BYTES_WRITTEN;
	private long[] HDFS_BYTES_WRITTEN;
	
	public void set(String counterName, long[] countersValue) {
		if(counterName.equals("FILE_BYTES_READ")) {
			FILE_BYTES_READ = countersValue;
		}
		else if(counterName.equals("HDFS_BYTES_READ")) {
			HDFS_BYTES_READ = countersValue;
		}
		else if(counterName.equals("FILE_BYTES_WRITTEN")) {
			FILE_BYTES_WRITTEN = countersValue;
		}
		else if(counterName.equals("HDFS_BYTES_WRITTEN")) {
			HDFS_BYTES_WRITTEN = countersValue;
		}
	}
}

class MapReduceFramework implements Serializable {
	private long[] Reduce_input_groups;
	private long[] Combine_output_records;
	private long[] Map_input_records;
	private long[] Reduce_shuffle_bytes;
	private long[] Reduce_output_records;
	private long[] Spilled_Records;
	private long[] Map_output_bytes;
	private long[] Map_input_bytes;
	private long[] Map_output_records;
	private long[] Combine_input_records;
	private long[] Reduce_input_records;
	
	public void set(String counterName, long[] countersValue) {
		if(counterName.equals("Reduce input groups")) {
			Reduce_input_groups = countersValue;
		}
		else if(counterName.equals("Combine output records")) {
			Combine_output_records = countersValue;
		}
		else if(counterName.equals("Map input records")) {
			Map_input_records = countersValue;
		}
		else if(counterName.equals("Reduce shuffle bytes")) {
			Reduce_shuffle_bytes = countersValue;
		}
		else if(counterName.equals("Reduce output records")) {
			Reduce_output_records = countersValue;
		}
		else if(counterName.equals("Spilled Records")) {
			Spilled_Records = countersValue;
		}
		else if(counterName.equals("Map output bytes")) {
			Map_output_bytes = countersValue;
		}
		else if(counterName.equals("Map input bytes")) {
			Map_input_bytes = countersValue;
		}
		else if(counterName.equals("Map output records")) {
			Map_output_records = countersValue;
		}
		else if(counterName.equals("Combine input records")) {
			Combine_input_records = countersValue;
		}
		else if(counterName.equals("Reduce input records")) {
			Reduce_input_records = countersValue;
		}

	}
	
}

