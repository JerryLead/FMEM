package profile.task.common;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/*
 * Super class of all sub merge classes in map and reduce phase. Common intermediate and last merge info as follows is extracted here.
 * 2012-10-15 09:59:32,247 INFO org.apache.hadoop.mapred.Merger: Merging 32 sorted segments
 * 2012-10-15 09:59:32,329 INFO org.apache.hadoop.mapred.Merger: Merging 5 intermediate segments out of a total of 32 <WriteRecords = 49654, RawLength = 761352, CompressedLength = 761356>
 * 2012-10-15 09:59:32,389 INFO org.apache.hadoop.mapred.Merger: Merging 10 intermediate segments out of a total of 28 <WriteRecords = 95728, RawLength = 1465039, CompressedLength = 1465043>
 * 2012-10-15 09:59:32,444 INFO org.apache.hadoop.mapred.Merger: Merging 10 intermediate segments out of a total of 19 <WriteRecords = 98475, RawLength = 1512022, CompressedLength = 1512026>
 * 2012-10-15 09:59:32,446 INFO org.apache.hadoop.mapred.Merger: Down to the last merge-pass, with 10 segments left of total size: 4827658 bytes
 */

public abstract class AbstractMerge implements Serializable {
	private List<IntermediateMergeInfo> intermediateMergeList;
	
	private long lastMergePassTimeMS;
	private int lastMergePassSegmentsNum;
	private long lastMergePassTotalSize;

	public void addIntermediateMergeItem(long mergeTimeMS,
			int mergeSegmentsNum, int totalSegmentsNum, long writeRecords,
			long rawLengthInter, long compressedLengthInter) {
		if(intermediateMergeList == null) 
			intermediateMergeList = new ArrayList<IntermediateMergeInfo>();
		IntermediateMergeInfo info = new IntermediateMergeInfo(mergeTimeMS,mergeSegmentsNum, totalSegmentsNum, writeRecords,
				rawLengthInter, compressedLengthInter);
		intermediateMergeList.add(info);
		
	}
	public void addLastPassMergeItem(long mergeTimeMS,
			int lastMergePassSegmentsNum, long lastMergePassTotalSize) {
		this.lastMergePassTimeMS = mergeTimeMS;
		this.lastMergePassSegmentsNum = lastMergePassSegmentsNum;
		this.lastMergePassTotalSize = lastMergePassTotalSize;
		
	}
}
