package profile.task.mapper;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class Merge implements Serializable {
	private int mergeCounts;
	private List<MergeInfo> mergeInfoList = new ArrayList<MergeInfo>();
	private long mergePhaseTimeCostSec; //from first merge to the last one
	
	private double merge_combine_record_ratio;
	private double merge_combine_bytes_ratio;
	
	public void addBeforeMergeItem(long startMergeTimeMS, int partitionId,
			int segmentsNum, long rawLengthBeforeMerge,
			long compressedLengthBeforeMerge) {
		
		MergeInfo mergeInfo = new MergeInfo(startMergeTimeMS, partitionId, segmentsNum, rawLengthBeforeMerge,
				compressedLengthBeforeMerge);
		mergeInfoList.add(mergeInfo);
	}

	public void addLastMergePassItem(long mergeTimeMS, int lastMergePassSegmentsNum, long lastMergePassTotalSize) {
		mergeInfoList.get(mergeInfoList.size() - 1)
			.addLastPassMergeItem(mergeTimeMS, lastMergePassSegmentsNum, lastMergePassTotalSize);
		
	}

	public void addIntermediateMergeItem(long mergeTimeMS,
			int mergeSegmentsNum, int totalSegmentsNum, long writeRecords,
			long rawLengthInter, long compressedLengthInter) {
		mergeInfoList.get(mergeInfoList.size() - 1)
			.addIntermediateMergeItem(mergeTimeMS, mergeSegmentsNum, totalSegmentsNum, writeRecords,
					rawLengthInter, compressedLengthInter);
		
	}

	public void addAfterMergeItem(long stopMergeTimeMS, int partitionId,
			long recordsBeforeMerge, long recordsAfterMerge,
			long rawLengthEnd, long compressedLengthEnd) {
		MergeInfo mergeInfo = mergeInfoList.get(partitionId);
		mergeInfo.setAfterMergeItem(stopMergeTimeMS, recordsBeforeMerge, recordsAfterMerge,
			rawLengthEnd, compressedLengthEnd);
		
	}
	
	public List<MergeInfo> getMergeInfoList() {
		return mergeInfoList;
	}

	public void addMergeInfo(MergeInfo newInfo) {
		mergeInfoList.add(newInfo);
		
	}

	public double getMerge_combine_record_ratio() {
		return merge_combine_record_ratio;
	}

	public void setMerge_combine_record_ratio(double merge_combine_record_ratio) {
		this.merge_combine_record_ratio = merge_combine_record_ratio;
	}

	public double getMerge_combine_bytes_ratio() {
		return merge_combine_bytes_ratio;
	}

	public void setMerge_combine_bytes_ratio(double merge_combine_bytes_ratio) {
		this.merge_combine_bytes_ratio = merge_combine_bytes_ratio;
	}
	
}
