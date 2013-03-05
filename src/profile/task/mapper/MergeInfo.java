package profile.task.mapper;

import java.io.Serializable;

import profile.task.common.AbstractMerge;

public class MergeInfo extends AbstractMerge implements Serializable {
	/*
	 * MapTask: [BeforeMerge][Partition 1]<SegmentsNum = 32, RawLength = 4849444, CompressedLength = 4849572>
	 * Merger: Merging 32 sorted segments
	 * Merger: Merging 5 intermediate segments out of a total of 32 <WriteRecords = 49534, RawLength = 762303, CompressedLength = 762307>
	 * Merger: Merging 10 intermediate segments out of a total of 28 <WriteRecords = 95721, RawLength = 1469986, CompressedLength = 1469990>
	 * Merger: Merging 10 intermediate segments out of a total of 19 <WriteRecords = 98406, RawLength = 1520519, CompressedLength = 1520523>
	 * Merger: Down to the last merge-pass, with 10 segments left of total size: 4849400 bytes
	 * MapTask: [AfterMergeAndCombine][Partition 1]<RecordsBeforeCombine = 314862, RecordsAfterCombine = 121586, RawLength = 2010928, CompressedLength = 2010932>
	 */
	/*
	 * MapTask: [BeforeMerge][Partition 1]<SegmentsNum = 3, RawLength = 33680916, CompressedLength = 4934525>
	 * Merger: Merging 3 sorted segments
	 * Merger: Down to the last merge-pass, with 3 segments left of total size: 4934513 bytes
	 * MapTask: [AfterMergeAndCombine][Partition 1]<RecordsBeforeCombine = 330205, RecordsAfterCombine = 330205, RawLength = 33680912, CompressedLength = 4942846>
	 */

	private int partitionId;
	private int segmentsNum;
	
	private long recordsBeforeMerge;
	private long rawLengthBeforeMerge;
	private long compressedLengthBeforeMerge;
	
	private long recordsAfterMerge;
	private long rawLengthAfterMerge;
	private long compressedLengthAfterMerge;
	
	private long startMergeTimeMS;
	private long stopMergeTimeMS;
	

	public MergeInfo(long startMergeTimeMS, int partitionId, int segmentsNum,
			long rawLengthBeforeMerge, long compressedLengthBeforeMerge) {
		this.startMergeTimeMS = startMergeTimeMS;
		this.partitionId = partitionId;
		this.segmentsNum = segmentsNum;
		this.rawLengthBeforeMerge = rawLengthBeforeMerge;
		this.compressedLengthBeforeMerge = compressedLengthBeforeMerge;
	}

	public void setAfterMergeItem(long stopMergeTimeMS,
			long recordsBeforeMerge, long recordsAfterMerge,
			long rawLengthEnd, long compressedLengthEnd) {
		this.stopMergeTimeMS = stopMergeTimeMS;
		this.recordsBeforeMerge = recordsBeforeMerge;
		this.recordsAfterMerge = recordsAfterMerge;
		this.rawLengthAfterMerge = rawLengthEnd;
		this.compressedLengthAfterMerge = compressedLengthEnd;
		
	}

	public int getSegmentsNum() {
		return segmentsNum;
	}

	public void setSegmentsNum(int segmentsNum) {
		this.segmentsNum = segmentsNum;
	}

	public long getRecordsBeforeMerge() {
		return recordsBeforeMerge;
	}

	public void setRecordsBeforeMerge(long recordsBeforeMerge) {
		this.recordsBeforeMerge = recordsBeforeMerge;
	}

	public long getRawLengthBeforeMerge() {
		return rawLengthBeforeMerge;
	}

	public void setRawLengthBeforeMerge(long rawLengthBeforeMerge) {
		this.rawLengthBeforeMerge = rawLengthBeforeMerge;
	}

	public long getCompressedLengthBeforeMerge() {
		return compressedLengthBeforeMerge;
	}

	public void setCompressedLengthBeforeMerge(long compressedLengthBeforeMerge) {
		this.compressedLengthBeforeMerge = compressedLengthBeforeMerge;
	}

	public long getRecordsAfterMerge() {
		return recordsAfterMerge;
	}

	public void setRecordsAfterCombine(long recordsAfterMerge) {
		this.recordsAfterMerge = recordsAfterMerge;
	}

	public long getRawLengthAfterMerge() {
		return rawLengthAfterMerge;
	}

	public void setRawLengthAfterMerge(long rawLengthAfterMerge) {
		this.rawLengthAfterMerge = rawLengthAfterMerge;
	}

	public long getCompressedLengthAfterMerge() {
		return compressedLengthAfterMerge;
	}

	public void setCompressedLengthAfterMerge(long compressedLengthAfterMerge) {
		this.compressedLengthAfterMerge = compressedLengthAfterMerge;
	}	

	
	

}