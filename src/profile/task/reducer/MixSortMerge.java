package profile.task.reducer;

import java.io.Serializable;

import profile.task.common.AbstractMerge;

/*
 *  Merger: Merging 1 sorted segments
 *  Merger: Down to the last merge-pass, with 1 segments left of total size: 82317615 bytes
 *  ReduceTask: [MixSortMerge][CountersBeforeMerge]<InMemorySegmentsNum = 0, InMemorySegmentsSize = 0, OnDiskSegmentsNum = 1, OnDiskSegmentsSize = 82317619
 */
public class MixSortMerge extends AbstractMerge implements Serializable {
	
	private int inMemorySegmentsNum;
	private long inMemorySegmentsSize;
	private long onDiskSegmentsNum;
	private long onDiskSegmentsSize;

	private long startMergeTimeMS;
	private long stopMergeTimeMS;
	
	//used in SortModel
	private long inMemoryRecords;
	private long onDiskRecords;
	
	public void set(long stopMixSortMergeTimeMS, int inMemorySegmentsNum,
			long inMemorySegmentsSize, int onDiskSegmentsNum,
			long onDiskSegmentsSize) {
		this.stopMergeTimeMS = stopMixSortMergeTimeMS;
		this.inMemorySegmentsNum = inMemorySegmentsNum;
		this.inMemorySegmentsSize = inMemorySegmentsSize;
		this.onDiskSegmentsNum = onDiskSegmentsNum;
		this.onDiskSegmentsSize = onDiskSegmentsSize;
	}
	
	public void setRecords(long inMemoryRecords, long onDiskRecords) {
		this.inMemoryRecords = inMemoryRecords;
		this.onDiskRecords = onDiskRecords;
	}

	public int getInMemorySegmentsNum() {
		return inMemorySegmentsNum;
	}

	public long getInMemorySegmentsSize() {
		return inMemorySegmentsSize;
	}

	public long getOnDiskSegmentsNum() {
		return onDiskSegmentsNum;
	}

	public long getOnDiskSegmentsSize() {
		return onDiskSegmentsSize;
	}

	public long getInMemoryRecords() {
		return inMemoryRecords;
	}

	public long getOnDiskRecords() {
		return onDiskRecords;
	}
	
	
}