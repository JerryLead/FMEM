package profile.commons.configuration;

import java.io.Serializable;


public class ReducerConfiguration extends Configuration implements Serializable {
	/*	
	private int mapred_child_java_opts;
	private int mapred_job_reuse_jvm_num_tasks;
	private int mapred_reduce_parallel_copies;
	private int io_sort_factor;
	private float mapred_inmem_merge_threshold;
	private float mapred_job_shuffle_merge_percent;
	private float mapred_job_shuffle_input_buffer_percent;
	private float mapred_job_reduce_input_buffer_percent;
	private int mapred_reduce_tasks;
	private long dfs_block_size;
	private int dfs_replication;
	*/
	
	public int getMapred_child_java_opts() {
		return Integer.parseInt(getConf("mapred.child.java.opts"));
	}
	
	public int getMapred_job_reuse_jvm_num_tasks() {
		return Integer.parseInt(getConf("mapred.job.reuse.jvm.num.tasks"));
	}

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
	
	public int getMapred_reduce_tasks() {
		return Integer.parseInt(getConf("mapred.reduce.tasks"));
	}
	
	public long getDfs_block_size() {
		return Long.parseLong(getConf("dfs.block.size"));
	}

	public int getDfs_replication() {
		return Integer.parseInt(getConf("dfs.replication"));
	}

}

