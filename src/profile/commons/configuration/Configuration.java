package profile.commons.configuration;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;


public class Configuration implements Serializable {
	
	private static final long serialVersionUID = -3840797925044516113L;
	private static Set<String> mapperConfs = new HashSet<String>();
	private static Set<String> reducerConfs = new HashSet<String>();
	
	static {
		mapperConfs.add("io.sort.mb");
		mapperConfs.add("io.sort.spill.percent");
		mapperConfs.add("io.sort.record.percent");
		mapperConfs.add("mapred.output.compress");
		mapperConfs.add("mapred.reduce.tasks");
		//mapperConfs.add("dfs.block.size");
		//mapperConfs.add("mapreduce.combine.class");
		//mapperConfs.add("split.size");
		//mapperConfs.add("mapred.min.split.size");
		//mapperConfs.add("mapred.max.split.size");
		
		
		//reducerConfs.add("mapred.child.java.opts");
		//reducerConfs.add("io.sort.factor");
		reducerConfs.add("mapred.reduce.tasks");
		reducerConfs.add("mapred.inmem.merge.threshold");
		reducerConfs.add("mapred.job.shuffle.merge.percent");
		reducerConfs.add("mapred.job.shuffle.input.buffer.percent");
		reducerConfs.add("mapred.job.reduce.input.buffer.percent");
		//reducerConfs.add("dfs.replication");
		//reducerConfs.add("mapreduce.combine.class");
	}
	
	private Map<String, String> confMap = new HashMap<String, String>();
	
	public boolean isSpliSizeChanged(String value) {
		return getSplitSize() != Long.parseLong(value);
	}
	
	public boolean isXmxChanged(int value) {
		return getXmx() != value;
	}
	
	public boolean isXmsChanged(int value) {
		return getXms() != value;
	}
	
	public boolean isMapperConfChanged(String key, String value) {
		return mapperConfs.contains(key) && !getConf(key).equals(value);
	}
	
	public boolean isReducerConfChanged(String key, String value) {
		return reducerConfs.contains(key) && !getConf(key).equals(value);
	}
	
	public String getConf(String name) {
		return confMap.get(name);
	}

	public void set(String confName, String value) {
		confMap.put(confName, value);
	}
	
	public Configuration copyConfiguration() {
		Configuration conf = new Configuration();
		for(Entry<String, String> entry : confMap.entrySet()) {
			conf.set(entry.getKey(), entry.getValue());
		}
		return conf;
	}
	
	public Set<Entry<String, String>> getAllConfs() {
		return confMap.entrySet();
	}
	
	public String getMapreduce_combine_class() {
		return getConf("mapreduce.combine.class");
	}
		
	public long getSplitSize() {
		String minSizeStr = getConf("mapred.min.split.size");
		String maxSizeStr = getConf("mapred.max.split.size");
		
		long minSize = 1L;
		long maxSize = Long.MAX_VALUE; 
		
		if(minSizeStr != null)
			minSize = Long.parseLong(minSizeStr);
		if(maxSizeStr != null)
			maxSize = Long.parseLong(maxSizeStr);
		
		long blockSize = getDfs_block_size();
		
		return Math.max(minSize, Math.min(maxSize, blockSize));
	}
			
	// mapper configuration
	public int getXmx() {
		String jvmParameter =  confMap.get("mapred.child.java.opts"); //-Xmx4000m
		int start = jvmParameter.indexOf("-Xmx") + 4;
		int end = jvmParameter.indexOf('m', start);
		return Integer.parseInt(jvmParameter.substring(start, end)); //4000
	}
	
	public int getXms() {
		String jvmParameter =  confMap.get("mapred.child.java.opts"); //-Xms1000m
		int start = jvmParameter.indexOf("-Xms") + 4;
		if(start == -1 + 4)
			return 0;
		int end = jvmParameter.indexOf('m', start);
		return Integer.parseInt(jvmParameter.substring(start, end)); //1000
	}
	
	public int getIo_sort_mb() {
		return Integer.parseInt(getConf("io.sort.mb"));
	}
	
	public int getMapred_map_tasks() {
		return Integer.parseInt(getConf("mapred.map.tasks"));
	}

	public float getIo_sort_spill_percent() {
		return Float.parseFloat(getConf("io.sort.spill.percent"));
	}

	public float getIo_sort_record_percent() {
		return Float.parseFloat(getConf("io.sort.record.percent"));
	}

	public long getDfs_block_size() {
		return Long.parseLong(getConf("dfs.block.size"));
	}

	public boolean getMapred_output_compress() {
		return Boolean.parseBoolean(getConf("mapred.output.compress"));
	}

	public int getMapred_job_reuse_jvm_num_tasks() {
		return Integer.parseInt(getConf("mapred.job.reuse.jvm.num.tasks"));
	}
	
	public int getDfs_replication() {
		return Integer.parseInt(getConf("dfs.replication"));
	}

	public int getMapred_reduce_tasks() {	
		return Integer.parseInt(getConf("mapred.reduce.tasks"));
	}	
	
	// reducer configuration


	public int getMapred_reduce_parallel_copies() {
		return Integer.parseInt(getConf("mapred.reduce.parallel.copies"));
	}
	
	public int getIo_sort_factor() {
		return Integer.parseInt(getConf("io.sort.factor"));
	}
	
	public int getMapred_inmem_merge_threshold() {
		return Integer.parseInt(getConf("mapred.inmem.merge.threshold"));
	}
	
	public float getMapred_job_shuffle_merge_percent() {
		return Float.parseFloat(getConf("mapred.job.shuffle.merge.percent"));
	}
	
	public float getMapred_job_shuffle_input_buffer_percent() {
		return Float.parseFloat(getConf("mapred.job.shuffle.input.buffer.percent"));
	}
	
	public float getMapred_job_reduce_input_buffer_percent() {
		return Float.parseFloat(getConf("mapred.job.reduce.input.buffer.percent"));
	}
	
	public int getMin_num_spills_for_combine() {
		if(getConf("min.num.spills.for.combine") == null)
			return 3;
		else 
			return Integer.parseInt(getConf("min.num.spills.for.combine"));
	}

}



