package profile.task.reducer;

import java.io.Serializable;

import profile.task.common.AbstractMerge;

/*
 * ReduceTask: Initiating in-memory merge with 29 segments...
 * Merger: Merging 29 sorted segments
 * Merger: Down to the last merge-pass, with 29 segments left of total size: 998409412 bytes
 * ReduceTask: [InMemoryShuffleMerge]<SegmentsNum = 29, RecordsBeforeMergeAC = 9788327, BytesBeforeMergeAC = 998409412, RecordsAfterCombine = 9788327, RawLength =
 * 			   998409356, CompressedLength = 149024839>
 */
public class InMemoryShuffleMerge extends AbstractMerge implements Serializable {
	

	private int segmentsNum;
	private long recordsBeforeMergeAC;
	private long bytesBeforeMergeAC;
	private long recordsAfterCombine;
	private long rawLength;
	private long compressedLength;
	
	private long startMergeTimeMS;
	private long stopMergeTimeMS;
	
	public InMemoryShuffleMerge(long startMergeTimeMS) {
		this.startMergeTimeMS = startMergeTimeMS;
	}
	
	public void setShuffleAfterMergeItem(long stopMergeTimeMS,
			int segmentsNum, long recordsBeforeMergeAC,
			long bytesBeforeMergeAC, long recordsAfterCombine,
			long rawLength, long compressedLength) {
		this.stopMergeTimeMS = stopMergeTimeMS;
		this.segmentsNum = segmentsNum;
		this.recordsBeforeMergeAC = recordsBeforeMergeAC;
		this.bytesBeforeMergeAC = bytesBeforeMergeAC;
		this.recordsAfterCombine = recordsAfterCombine;
		this.rawLength = rawLength;
		this.compressedLength = compressedLength;
	}

	public int getSegmentsNum() {
		return segmentsNum;
	}

	public long getRecordsBeforeMergeAC() {
		return recordsBeforeMergeAC;
	}

	public long getBytesBeforeMergeAC() {
		return bytesBeforeMergeAC;
	}

	public long getRecordsAfterCombine() {
		return recordsAfterCombine;
	}

	public long getRawLength() {
		return rawLength;
	}

	public long getCompressedLength() {
		return compressedLength;
	}
	
	
}
