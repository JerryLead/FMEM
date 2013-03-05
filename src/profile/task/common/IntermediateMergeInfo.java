package profile.task.common;

import java.io.Serializable;

public class IntermediateMergeInfo implements Serializable {

	private long mergeTimeMS;
	private int mergeSegmentsNum;
	private int totalSegmentsNum;
	private long writeRecords;
	private long rawLengthInter;
	private long compressedLengthInter;
	
	public IntermediateMergeInfo(long mergeTimeMS, int mergeSegmentsNum,
			int totalSegmentsNum, long writeRecords, long rawLengthInter,
			long compressedLengthInter) {
		this.mergeTimeMS = mergeTimeMS;
		this.mergeSegmentsNum = mergeSegmentsNum;
		this.totalSegmentsNum = totalSegmentsNum;
		this.writeRecords = writeRecords;
		this.rawLengthInter = rawLengthInter;
		this.compressedLengthInter = compressedLengthInter;
	}

}
