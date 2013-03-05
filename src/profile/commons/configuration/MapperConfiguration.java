package profile.commons.configuration;


public class MapperConfiguration extends Configuration {
	
	/*
	private int mapred_child_java_opts;
	private int io_sort_mb;
	private int mapred_map_tasks;
	private float io_sort_spill_percent;
	private float io_sort_record_percent;
	private long dfs_block_size;
	private boolean mapred_output_compress;
	private int mapred_job_reuse_jvm_num_tasks;
	private int dfs_replication;
	*/
	
	public int getMapred_child_java_opts() {
		return Integer.parseInt(getConf("mapred.child.java.opts"));
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

	public long getSplitSize() {
		return Long.parseLong(getConf("split.size"));
	}
	
}