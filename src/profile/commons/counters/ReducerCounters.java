package profile.commons.counters;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;


public class ReducerCounters extends Counters implements Serializable {
	//keeps the final counters result after reducer finishes
	private Map<String, Long> finalCounterMap = new HashMap<String, Long>();
	private String counterNames[] = {
							"FILE_BYTES_READ",
							"HDFS_BYTES_WRITTEN",
							"FILE_BYTES_WRITTEN",
							"Reduce input groups",
							"Combine output records",
							"Reduce shuffle bytes",
							"Reduce output records",
							"Spilled Records",
							"Combine input records",
							"Reduce input records",
							"Reduce shuffle raw bytes"
							};

	public ReducerCounters() {
		super();
		for(String s : counterNames)
			finalCounterMap.put(s, (long) -1);
	}
	
	public Long getFILE_BYTES_READ() {
		return finalCounterMap.get("FILE_BYTES_READ");
	}
	
	public Long getHDFS_BYTES_WRITTEN() {
		return finalCounterMap.get("HDFS_BYTES_WRITTEN");
	}
	public Long getFILE_BYTES_WRITTEN() {
		return finalCounterMap.get("FILE_BYTES_WRITTEN");
	}
	public Long getReduce_input_groups() {
		return finalCounterMap.get("Reduce input groups");
	}
	public Long getCombine_output_records() {
		return finalCounterMap.get("Combine output records");
	}
	
	public Long getSpilled_Records() {
		return finalCounterMap.get("Spilled Records");
	}
	
	public Long getReduce_shuffle_bytes() {
		return finalCounterMap.get("Reduce shuffle bytes");
	}
	
	public Long getReduce_output_records() {
		return finalCounterMap.get("Reduce output records");
	}
	public Long getCombine_input_records() {
		return finalCounterMap.get("Combine input records");
	}
	public Long getReduce_input_records() {
		return finalCounterMap.get("Reduce input records");
	}
	
	public Long getReduce_shuffle_raw_bytes() {
		return finalCounterMap.get("Reduce shuffle raw bytes");
	}
	
	public void setFinalCountersItem(String counterName, Long value) {
		//if(!finalCounterMap.containsKey(counter))
		//	;//System.err.println("finalCounterMap doesn't contain " + counter);
		//else
		finalCounterMap.put(counterName, value);
	}

}
