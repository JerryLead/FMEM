package profile.commons.counters;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;


public class MapperCounters extends Counters implements Serializable {
	//Keeps the final counters after mapper finishes
	private Map<String, Long> finalCountersMap = new HashMap<String, Long>();
	private double spill_combine_record_ratio;
	private double spill_combine_bytes_ratio;
	private double merge_combine_record_ratio;
	private double merge_combine_bytes_ratio;
	
	private String counterNames[] = {
							"FILE_BYTES_READ",
							"HDFS_BYTES_READ",
							"FILE_BYTES_WRITTEN",
							"HDFS_BYTES_WRITTEN",
							"Combine output records",
							"Map input records",
							"Spilled Records",
							"Map output bytes",
							//"Map input bytes",
							"Combine input records",
							"Map output records"};

	public MapperCounters() {
		super();
		for(String s : counterNames)
			finalCountersMap.put(s, (long) -1);	
	}
	
	public Long getFILE_BYTES_READ() {
		return finalCountersMap.get("FILE_BYTES_READ");
	}
	
	public Long getHDFS_BYTES_READ() {
		return finalCountersMap.get("HDFS_BYTES_READ");
	}
	public Long getFILE_BYTES_WRITTEN() {
		return finalCountersMap.get("FILE_BYTES_WRITTEN");
	}
	public Long getHDFS_BYTES_WRITTEN() {
		return finalCountersMap.get("HDFS_BYTES_WRITTEN");
	}
	public Long getCombine_output_records() {
		return finalCountersMap.get("Combine output records");
	}
	public Long getMap_input_records() {
		return finalCountersMap.get("Map input records");
	}
	public Long getSpilled_Records() {
		return finalCountersMap.get("Spilled Records");
	}
	public Long getMap_output_bytes() {
		return finalCountersMap.get("Map output bytes");
	}
	public Long getMap_input_bytes() {
		if(finalCountersMap.containsKey("Map input bytes"))
			return finalCountersMap.get("Map input bytes");
		else
			return finalCountersMap.get("HDFS_BYTES_READ");
	}
	public Long getCombine_input_records() {
		return finalCountersMap.get("Combine input records");
	}
	public Long getMap_output_records() {
		return finalCountersMap.get("Map output records");
	}
	
	public void setFinalCountersItem(String counter, Long value) {
		//if(!counterMap.containsKey(counter))
		//	System.err.println("counterMap doesn't contain " + counter);
		//else
		finalCountersMap.put(counter, value);
	}

	public double getSpill_combine_record_ratio() {
		return spill_combine_record_ratio;
	}

	public void setSpill_combine_record_ratio(double spill_combine_record_ratio) {
		this.spill_combine_record_ratio = spill_combine_record_ratio;
	}

	public double getSpill_combine_bytes_ratio() {
		return spill_combine_bytes_ratio;
	}

	public void setSpill_combine_bytes_ratio(double spill_combine_bytes_ratio) {
		this.spill_combine_bytes_ratio = spill_combine_bytes_ratio;
	}

	public double getMerge_combine_record_ratio() {
		return merge_combine_record_ratio;
	}

	public void setMerge_combine_record_ratio(double merge_combine_record_ratio) {
		this.merge_combine_record_ratio = merge_combine_record_ratio;
	}

	public double getMerge_combine_bytes_ratio() {
		return merge_combine_bytes_ratio;
	}

	public void setMerge_combine_bytes_ratio(double merge_combine_bytes_ratio) {
		this.merge_combine_bytes_ratio = merge_combine_bytes_ratio;
	}

}