package profile.task.reducer;

import java.io.Serializable;

import profile.task.common.AbstractMerge;

/*
 * Merger: Merging 38 sorted segments
 * Merger: Down to the last merge-pass, with 38 segments left of total size: 82317689 bytes
 * ReduceTask: [InMemorySortMerge]<SegmentsNum = 38, Records = 5083386, BytesBeforeMerge = 82317689, RawLength = 82317615, CompressedLength = 82317619>
 */

public class InMemorySortMerge extends AbstractMerge implements Serializable {
	
	private int segmentsNum;
	private long records;
	private long bytesBeforeMerge;
	private long rawLength;
	private long compressedLength;
	
	private long startMergeTimeMS;
	private long stopMergeTimeMS;
	
	public InMemorySortMerge(long stopInMemorySortMergeTimeMS,
			int segmentsNum, long records, long bytesBeforeMerge,
			long rawLength, long compressedLength) {
		this.stopMergeTimeMS = stopInMemorySortMergeTimeMS;
		this.segmentsNum = segmentsNum;
		this.records = records;
		this.bytesBeforeMerge = bytesBeforeMerge;
		this.rawLength = rawLength;
		this.compressedLength = compressedLength;
	}
	
	
}
