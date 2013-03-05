package profile.task.mapper;

import java.io.Serializable;

public class SpillInfo implements Serializable {

	private boolean hasCombine;
	private String reason; //record, buffer, flush

	private long startSpillTimeMS;
	private long stopSpillTimeMS;
	
	private long recordsBeforeCombine;
	private long bytesBeforeSpill;
	private long recordsAfterCombine;
	private long rawLength;
	private long compressedLength;
	

	//MapTask: Finished spill 1 <RecordsBeforeCombine = 1310719, BytesBeforeSpill = 13522211, RecordAfterCombine = 164046, RawLength = 2511623, CompressedLength = 2511687>
    //MapTask: Finished spill without combine 0 <Records = 1310720, BytesBeforeSpill = 131072000, RawLength = 133693456, CompressedLength = 19576859>
	
	//used in BufferEstimator just for estimation 
	public SpillInfo(String reason, long recordsBeforeCombine, long bytesBeforeSpill) {	
		this.reason = reason;
		this.recordsBeforeCombine = recordsBeforeCombine;
		this.bytesBeforeSpill = bytesBeforeSpill;
	}
	
	public SpillInfo(boolean hasCombine, long startSpillTimeMS,
			long stopSpillTimeMS, String reason, long recordsBeforeCombine,
			long bytesBeforeSpill, long recordsAfterCombine, long rawLength,
			long compressedLength) {
		this.hasCombine = hasCombine;
		this.startSpillTimeMS = startSpillTimeMS;
		this.stopSpillTimeMS = stopSpillTimeMS;
		this.reason = reason;
		this.recordsBeforeCombine = recordsBeforeCombine;
		this.bytesBeforeSpill = bytesBeforeSpill;
		this.recordsAfterCombine = recordsAfterCombine;
		this.rawLength = rawLength;
		this.compressedLength = compressedLength;
	}

	public void setAfterSpillInfo(long recordsAfterCombine, long rawLength, long compressedLength) {
		this.recordsAfterCombine = recordsAfterCombine;
		this.rawLength = rawLength;
		this.compressedLength = compressedLength;
	}
	
	public long getRecordsBeforeCombine() {
		return recordsBeforeCombine;
	}


	public long getBytesBeforeSpill() {
		return bytesBeforeSpill;
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


	public boolean isHasCombine() {
		return hasCombine;
	}


	public long getStartSpillTimeMS() {
		return startSpillTimeMS;
	}

	public long getStopSpillTimeMS() {
		return stopSpillTimeMS;
	}
	
	public String getReason() {
		return reason;
	}
}