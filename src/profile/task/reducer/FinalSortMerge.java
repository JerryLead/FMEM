package profile.task.reducer;

import java.io.Serializable;

import profile.task.common.AbstractMerge;

/*
 * LOG.info("[FinalSortMerge]" + "<InMemorySegmentsNum = " + inMemSegmentsNum + ", "
 *	  	 + "InMemorySegmentsSize = " + inMemBytes);
 */
public class FinalSortMerge extends AbstractMerge implements Serializable {
	
	private int inMemorySegmentsNum;
	private long inMemorySegmentsSize;
	
	private long startMergeTimeMS;
	private long stopMergeTimeMS;
	
	//used in SortModel
	private long records;
	
	public void set(long stopFinalSortMergeTimeMS,
			int inMemorySegmentsNum, long inMemBytes) {
		this.stopMergeTimeMS = stopFinalSortMergeTimeMS;
		this.inMemorySegmentsNum = inMemorySegmentsNum;
		this.inMemorySegmentsSize = inMemBytes;
	}
	
	public void setRecords(long records) {
		this.records = records;
	}

	public int getInMemorySegmentsNum() {
		return inMemorySegmentsNum;
	}

	public long getInMemorySegmentsSize() {
		return inMemorySegmentsSize;
	}

	public long getRecords() {
		return records;
	}
	
	
}