package profile.task.reducer;

import java.io.Serializable;

public class Segment implements Serializable {

	private String storeLoc;
	private long records;
	private long rawLength;
	private long compressedLen;
	
	public Segment(String storeLoc, long records, long rawLength, long compressedLen) {
		this.storeLoc = storeLoc;
		this.records = records;
		this.rawLength = rawLength;
		this.compressedLen = compressedLen;
	}

	public long getRecords() {
		return records;
	}

	public long getRawLength() {
		return rawLength;
	}

	public long getCompressedLen() {
		return compressedLen;
	}
}
