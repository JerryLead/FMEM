package profile.task.reducer;

import java.io.Serializable;
import java.util.List;

import profile.task.common.AbstractMerge;
import profile.task.common.IntermediateMergeInfo;


/*
 * LOG.info("[OnDiskShuffleMerge]<SegmentsNum = " + mapFiles.size() + ", "
 *         		  	+ "Records = " +  totalRecordsBeforeCombine + ", "
 *		  			+ "BytesBeforeMerge = " + approxOutputSize + ", "
 *		  			+ "RawLength = " + writer.getRawLength() + ", "
 *		  			+ "CompressedLength = " + writer.getCompressedLength() + ">");
 */

public class OnDiskShuffleMerge extends AbstractMerge implements Serializable {
	private int segmentsNum;
	private long records;
	private long bytesBeforeMerge;
	private long rawLength;
	private long compressedLength;
	
	private long startMergeTimeMS;
	private long stopMergeTimeMS;
	
	List<IntermediateMergeInfo> intermediateMergeList;
	
	private long lastMergePassTimeMS;
	private int lastMergePassSegmentsNum;
	private long lastMergePassTotalSize;
	
	public OnDiskShuffleMerge(long startMergeTimeMS) {
		this.startMergeTimeMS = startMergeTimeMS;
	}
	
	public void setShuffleAfterMergeItem(long stopMergeTimeMS,
			int segmentsNum, long recordsBeforeMergeAC,
			long bytesBeforeMergeAC, long rawLength, long compressedLength) {
		this.stopMergeTimeMS = stopMergeTimeMS;
		this.segmentsNum = segmentsNum;
		this.records = recordsBeforeMergeAC;
		this.bytesBeforeMerge = bytesBeforeMergeAC;
		this.rawLength = rawLength;
		this.compressedLength = compressedLength;
	}

	public int getSegmentsNum() {
		return segmentsNum;
	}

	public long getRecords() {
		return records;
	}

	public long getBytesBeforeMerge() {
		return bytesBeforeMerge;
	}

	public long getRawLength() {
		return rawLength;
	}

	public long getCompressedLength() {
		return compressedLength;
	}
	
	
}
