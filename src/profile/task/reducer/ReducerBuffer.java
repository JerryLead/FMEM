package profile.task.reducer;

import java.io.Serializable;

public class ReducerBuffer implements Serializable {
	private long runtime_maxMemory;     //[---------------Runtime().maxMemory()---------------------]
	private long memoryLimit;           //[--------ShuffleBuffer MemoryLimit---------][--for other--]
	private long maxSingleShuffleLimit; //
	
	private long inMemoryBufferLimit;   //[---InMemoryBufferLimit----][-Write Buffer-]
	private long maxInMemReduce;        //                                      [---maxInMemReduce--]
	private int maxInMemOutputs;        //[maxInMemOutputs * size(KV)]
	
	public void set(long memoryLimit, long maxSingleShuffleLimit) {
		this.memoryLimit = memoryLimit;
		this.maxSingleShuffleLimit = maxSingleShuffleLimit;
		
	}

	public void set(long inMemoryBufferLimit, long maxInMemReduce, int maxInMemOutputs) {
		this.inMemoryBufferLimit = inMemoryBufferLimit;
		this.maxInMemReduce = maxInMemReduce;
		this.maxInMemOutputs = maxInMemOutputs;	
	}
	
	public long getInMemoryBufferLimit() {
		return inMemoryBufferLimit;
	}
	
	public int getMaxInMemOutputs() {
		return maxInMemOutputs;
	}
	
	public long getMaxInMemReduce() {
		return maxInMemReduce;
	}

	public long getMaxSingleShuffleLimit() {
		return maxSingleShuffleLimit;
	}

	public long getRuntime_maxMemory() {
		return runtime_maxMemory;
	}

	public long getMemoryLimit() {
		return memoryLimit;
	}	
	
	
	
	
	public float getInMemoryBufferLimitMB() {
		return bytesToMB(inMemoryBufferLimit);
	}
	
	public float getMaxInMemReduceMB() {
		return bytesToMB(maxInMemReduce);
	}

	public float getMaxSingleShuffleLimitMB() {
		return bytesToMB(maxSingleShuffleLimit);
	}

	public float getRuntime_maxMemoryMB() {
		return bytesToMB(runtime_maxMemory);
	}

	public float getMemoryLimitMB() {
		return bytesToMB(memoryLimit);
	}	
	
	public float bytesToMB(long bytes) {
		return (float)((double)bytes / 1024 / 1024);
	}
	
}
