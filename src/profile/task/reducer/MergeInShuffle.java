package profile.task.reducer;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;


public class MergeInShuffle implements Serializable {
	private long reduceInputBytesLoss = 0;
	
	private List<InMemoryShuffleMerge> inMemoryShuffleMergeList = new ArrayList<InMemoryShuffleMerge>();
	private List<OnDiskShuffleMerge> onDiskShuffleMergeList;
	
	private List<Segment> inMemLeftSegments;
	private List<Segment> onDiskLeftSegments;
	
	public void addMergeInShuffleBeforeItem(String mergeLoc, long startMergeTimeMS) {
		if(mergeLoc.equals("InMemoryShuffleMerge")) {
			InMemoryShuffleMerge inMemoryShuffleMerge = new InMemoryShuffleMerge(startMergeTimeMS);
			inMemoryShuffleMergeList.add(inMemoryShuffleMerge);
			
		}
		else if(mergeLoc.equals("OnDiskShuffleMerge")) {
			if(onDiskShuffleMergeList == null)
				onDiskShuffleMergeList = new ArrayList<OnDiskShuffleMerge>();
			OnDiskShuffleMerge OnDiskShuffleMerge = new OnDiskShuffleMerge(startMergeTimeMS);
			onDiskShuffleMergeList.add(OnDiskShuffleMerge);
		}
		
	}

	public void addLastPassMergeInShuffleItem(String mergeLoc, long mergeTimeMS, int lastMergePassSegmentsNum,
			long lastMergePassTotalSize) {
		if(mergeLoc.equals("InMemoryShuffleMerge")) {
			InMemoryShuffleMerge inMemoryShuffleMerge = inMemoryShuffleMergeList.get(inMemoryShuffleMergeList.size() - 1);
			inMemoryShuffleMerge.addLastPassMergeItem(mergeTimeMS, lastMergePassSegmentsNum, lastMergePassTotalSize);
		}
		else if(mergeLoc.equals("OnDiskShuffleMerge")) {
			OnDiskShuffleMerge onDiskShuffleMerge = onDiskShuffleMergeList.get(onDiskShuffleMergeList.size() - 1);
			onDiskShuffleMerge.addLastPassMergeItem(mergeTimeMS, lastMergePassSegmentsNum, lastMergePassTotalSize);
		}
	}

	public void addShuffleIntermediateMergeItem(String mergeLoc,
			long mergeTimeMS, int mergeSegmentsNum, int totalSegmentsNum,
			long writeRecords, long rawLengthInter, long compressedLengthInter) {
		if(mergeLoc.equals("InMemoryShuffleMerge")) {
			InMemoryShuffleMerge inMemoryShuffleMerge = inMemoryShuffleMergeList.get(inMemoryShuffleMergeList.size() - 1);
			inMemoryShuffleMerge.addIntermediateMergeItem(mergeTimeMS, mergeSegmentsNum, totalSegmentsNum,
					writeRecords, rawLengthInter, compressedLengthInter);
		}
		else if(mergeLoc.equals("OnDiskShuffleMerge")) {
			OnDiskShuffleMerge onDiskShuffleMerge = onDiskShuffleMergeList.get(onDiskShuffleMergeList.size() - 1);
			onDiskShuffleMerge.addIntermediateMergeItem(mergeTimeMS, mergeSegmentsNum, totalSegmentsNum,
					writeRecords, rawLengthInter, compressedLengthInter);
		}
		
	}

	public void addShuffleAfterMergeItem(String mergeLoc, long stopMergeTimeMS,
			int segmentsNum, long recordsBeforeMergeAC,
			long bytesBeforeMergeAC, long recordsAfterCombine, long rawLength,
			long compressedLength) {
		
		if(mergeLoc.equals("InMemoryShuffleMerge")) {
			InMemoryShuffleMerge inMemoryShuffleMerge = inMemoryShuffleMergeList.get(inMemoryShuffleMergeList.size() - 1);
			inMemoryShuffleMerge.setShuffleAfterMergeItem(stopMergeTimeMS, segmentsNum, recordsBeforeMergeAC,
				bytesBeforeMergeAC, recordsAfterCombine, rawLength, compressedLength);
			
			reduceInputBytesLoss += (bytesBeforeMergeAC - rawLength);
		}
		else if(mergeLoc.equals("OnDiskShuffleMerge")) {
			OnDiskShuffleMerge onDiskShuffleMerge = onDiskShuffleMergeList.get(onDiskShuffleMergeList.size() - 1);
			onDiskShuffleMerge.setShuffleAfterMergeItem(stopMergeTimeMS, segmentsNum, recordsBeforeMergeAC, 
					bytesBeforeMergeAC, rawLength, compressedLength);
		}	
		
	}
	
	public List<InMemoryShuffleMerge> getInMemoryShuffleMergeList() {
		return inMemoryShuffleMergeList;
	}
	
	public List<OnDiskShuffleMerge> getOnDiskShuffleMergeList() {
		return onDiskShuffleMergeList;
	}
	
	
	public void addInMemoryShuffleMerge(InMemoryShuffleMerge inMemoryShuffleMerge) {
		inMemoryShuffleMergeList.add(inMemoryShuffleMerge);
	}

	public void setInMemoryShuffleMergeList(List<InMemoryShuffleMerge> inMemoryShuffleMergeList) {
		this.inMemoryShuffleMergeList = inMemoryShuffleMergeList;
	}

	public void setOnDiskShuffleMergeList(List<OnDiskShuffleMerge> onDiskShuffleMergeList) {
		this.onDiskShuffleMergeList = onDiskShuffleMergeList;
	}

	public List<Segment> getInMemLeftSegments() {
		return inMemLeftSegments;
	}

	public void setInMemLeftSegments(List<Segment> inMemLeftSegments) {
		this.inMemLeftSegments = inMemLeftSegments;
	}

	public List<Segment> getOnDiskLeftSegments() {
		return onDiskLeftSegments;
	}

	public void setOnDiskLeftSegments(List<Segment> onDiskLeftSegments) {
		this.onDiskLeftSegments = onDiskLeftSegments;
	}

	public long getReduceInputBytesLoss() {
		return reduceInputBytesLoss;
	}
}
