package data.model.reducer;

import profile.task.reducer.ReducerBuffer;

public class ReducerBufferModel {

	//mapred.child.java.opts   [---------------Runtime().maxMemory()----------------------]
	//memoryLimit;             [------ShuffleBuffer MemoryLimit 70%--------][--for other--]
	//maxSingleShuffleLimit; 
	//inMemoryBufferLimit;     [--InMemoryBufferLimit 66%--][-Write Buffer-]
	
	//maxInMemReduce;          [---maxInMemReduce--]
	//maxInMemOutputs;         [maxInMemOutputs * size(KV)]
		
	public static ReducerBuffer computeBufferLimit(int mapred_child_java_opts,
			int mapred_inmem_merge_threshold,
			float mapred_job_shuffle_merge_percent,
			float mapred_job_shuffle_input_buffer_percent,
			float mapred_job_reduce_input_buffer_percent) {

		final float maxInMemCopyUse = mapred_job_shuffle_input_buffer_percent; // default 0.70f;

		// Allow unit tests to fix Runtime memory
		int maxSize = (int) (Math.min(mapred_child_java_opts * 1024 * 1024l,
				Integer.MAX_VALUE) * maxInMemCopyUse); 
														
		long maxSingleShuffleLimit = (long) (maxSize * 0.75f); 
																															
		long memoryLimit = maxSize;

	
	
	    int maxInMemOutputs = mapred_inmem_merge_threshold; // default 1000
	    float maxInMemCopyPer = mapred_job_shuffle_merge_percent; // default 0.66
	    float maxRedPer = mapred_job_reduce_input_buffer_percent;// default 0.0, i.e. no buffer
	    long maxInMemReduce = (int)Math.min(mapred_child_java_opts * 1024 * 1024l * maxRedPer, Integer.MAX_VALUE);
	    long inMemoryBufferLimit = (long) (maxInMemCopyPer * memoryLimit);
	    		
		ReducerBuffer reducerBuffer = new ReducerBuffer();
		reducerBuffer.set(memoryLimit, maxSingleShuffleLimit);
		reducerBuffer.set(inMemoryBufferLimit, maxInMemReduce, maxInMemOutputs);
		
		/*
		System.out.println("[ReducerBuffer] <memoryLimit = " + memoryLimit + ", maxSingleShuffleLimit = " + maxSingleShuffleLimit + ">");
		System.out.println("[ReducerBuffer] <inMemoryBufferLimit = " + inMemoryBufferLimit + ", bufferForReducer = " 
				+ maxInMemReduce + ", maxInMemOutputs = " + maxInMemCopyPer + ">");
		System.out.println("---------------------------------------------------------------------------------------------------------");
		*/
		
		return reducerBuffer;
	}
	
	public static long computeRuntimeMaxJvmHeap(long memoryLimit, float mapred_job_shuffle_input_buffer_percent) {
		
		float maxInMemCopyUse = mapred_job_shuffle_input_buffer_percent; // default 0.70f;
		
		//keep the mapred.child.java.opts
		if(memoryLimit == Integer.MAX_VALUE * maxInMemCopyUse) 
			return 0;
		else
			return (long) (memoryLimit / maxInMemCopyUse);
		
	}

}
