package data.model.reducer;

import java.util.ArrayList;
import java.util.List;

import profile.task.reducer.InMemoryShuffleMerge;
import profile.task.reducer.MergeInShuffle;
import profile.task.reducer.OnDiskShuffleMerge;
import profile.task.reducer.ReducerBuffer;
import profile.task.reducer.Segment;
import profile.task.reducer.Shuffle;
import profile.task.reducer.ShuffleInfo;


public class MergeInShuffleModel {

	public static MergeInShuffle computeMerge(ReducerBuffer eBuffer,
			Shuffle eShuffle, IORatioModel ioRatio,
			int ioSortFactor) {
		
		MergeInShuffle eMergeInShuffle = new MergeInShuffle();
		
		//buffer infos from estimated ReduceBuffer
		long inMemoryBufferLimit = eBuffer.getInMemoryBufferLimit();
		int maxInMemOutputs = eBuffer.getMaxInMemOutputs();
		long maxSingleShuffleLimit = eBuffer.getMaxSingleShuffleLimit();
		
		//keep all the estimated shuffleInfo
		List<ShuffleInfo> eShuffleInfoList = eShuffle.getShuffleInfoList();
		
		//initialize in-memory/On-disk/IoRatio model
		OnDiskSegmentsModel onDiskSegmentsModel = new OnDiskSegmentsModel(ioSortFactor);

		InMemorySegmentsModel inMemSegmentsModel = new InMemorySegmentsModel(inMemoryBufferLimit, maxInMemOutputs,
				ioRatio.getInMemShuffleMergeRecordsIoRatio(), ioRatio.getInMemShuffleMergeBytesIoRatio());
		
		//simulate the procedure of shuffle phase
		for(int i = 0; i < eShuffleInfoList.size(); i++) {
			ShuffleInfo csi = eShuffleInfoList.get(i);
	
			if(canFitInMemory(eShuffleInfoList.get(i).getRawLength(), maxSingleShuffleLimit) == false) 
				onDiskSegmentsModel.addShuffleInfo(csi);
			else
				inMemSegmentsModel.addShuffleInfo(csi, onDiskSegmentsModel);
		}
		
		//set merge in shuffle infos for MergeInfShuffle class
		eMergeInShuffle.setInMemoryShuffleMergeList(inMemSegmentsModel.getInMemoryShuffleMergeList());
		eMergeInShuffle.setOnDiskShuffleMergeList(onDiskSegmentsModel.getOnDiskShuffleMergeList());
		
		//set left segments infos after merge in shuffle
		eMergeInShuffle.setInMemLeftSegments(inMemSegmentsModel.getInMemLeftSegments());
		eMergeInShuffle.setOnDiskLeftSegments(onDiskSegmentsModel.getOnDiskLeftSegments());
		
		/*
		//display the merge in shuffle and left segments infos
		for(InMemoryShuffleMerge imsm : inMemSegmentsModel.getInMemoryShuffleMergeList()) {

			System.out.println("[InMemoryShuffleMerge] <SegmentsNum = " + imsm.getSegmentsNum() + ", RecordsBeforeMergeAC = " 
					+ imsm.getRecordsBeforeMergeAC() + ", BytesBeforeMergeAC = " + imsm.getBytesBeforeMergeAC() + ", RecordsAfterCombine = " 
					+ imsm.getRecordsAfterCombine() + ", RawLength = " + imsm.getRawLength() + ", CompressedLength = " 
					+ imsm.getCompressedLength() + ">");
		}
		
		for(OnDiskShuffleMerge odsm : onDiskSegmentsModel.getOnDiskShuffleMergeList()) {
			System.out.println("[OnDiskShuffleMerge]<SegmentsNum = " + odsm.getSegmentsNum() + ", "
					+ "Records = " +  odsm.getRecords() + ", "
					+ "BytesBeforeMerge = " + odsm.getBytesBeforeMerge() + ", "
					+ "RawLength = " + odsm.getRawLength() + ", "
					+ "CompressedLength = " + odsm.getCompressedLength() + ">");
		}
		System.out.println();
		
		int id = 0;
		for(Segment seg : inMemSegmentsModel.getInMemLeftSegments()) {
			System.out.println("[SegmentsLeftInMemory] [" + id + "] <Records = " + seg.getRecords() + ", RawLength = " + seg.getRawLength()
					+ ", CompressedLen = " + seg.getCompressedLen() + ">");
			id++;
		}
		
		for(Segment seg : onDiskSegmentsModel.getOnDiskLeftSegments()) {
			System.out.println("[SegmentsLeftOnDisk] [" + id + "] <Records = " + seg.getRecords() + ", RawLength = " + seg.getRawLength()
					+ ", CompressedLen = " + seg.getCompressedLen() + ">");
			id++;
		}
		
		System.out.println("---------------------------------------------------------------------------------------------------------");
		*/
		return eMergeInShuffle;
	}

	private static boolean canFitInMemory(long requestedSize, long maxSingleShuffleLimit) {
        return (requestedSize < Integer.MAX_VALUE && 
                requestedSize < maxSingleShuffleLimit);
    }
}



class InMemorySegmentsModel {

	private long incShuffleRecords = 0;
	private long incShuffleRawLen = 0;
	private long incShuffleCompressedLen = 0;
	
	private long inMemoryBufferLimit;
	private int maxInMemOutputs;
	private double recordsIoRatio;
	private double rawLenIoRatio;
	
	private List<ShuffleInfo> inMemShuffleList = new ArrayList<ShuffleInfo>();
	private List<InMemoryShuffleMerge> mergeList = new ArrayList<InMemoryShuffleMerge>();
	private List<Segment> inMemSegmentsList = null;
	
	public InMemorySegmentsModel(long inMemoryBufferLimit, int maxInMemOutputs, double recordsIoRatio, double rawLenIoRatio) {
		this.inMemoryBufferLimit = inMemoryBufferLimit;
		this.maxInMemOutputs = maxInMemOutputs;
		this.recordsIoRatio = recordsIoRatio;
		this.rawLenIoRatio = rawLenIoRatio;
	}
	
	public void addShuffleInfo(ShuffleInfo csi, OnDiskSegmentsModel onDiskSegmentsModel) {
		inMemShuffleList.add(csi);
		
		incShuffleRawLen += csi.getRawLength();
		incShuffleRecords += csi.getRecords();
		incShuffleCompressedLen += csi.getCompressedLen();
		
		if(incShuffleRawLen > inMemoryBufferLimit && inMemShuffleList.size() >= 2 
				|| inMemShuffleList.size() > maxInMemOutputs) {
			
			long recordsAfterCombine = (long) (incShuffleRecords * recordsIoRatio);
			long rawLength = (long) (incShuffleRawLen * rawLenIoRatio);
			long compressedLength = (long) ((double)incShuffleCompressedLen / incShuffleRawLen * rawLength);
			
			InMemoryShuffleMerge imsm = new InMemoryShuffleMerge(0);
			
			imsm.setShuffleAfterMergeItem(0, inMemShuffleList.size(), incShuffleRecords, 
					incShuffleRawLen, recordsAfterCombine, rawLength, compressedLength);
			mergeList.add(imsm);
			onDiskSegmentsModel.addInMemoryShuffleMerge(imsm);
			
			incShuffleRecords = 0;
			incShuffleRawLen = 0;
			incShuffleCompressedLen = 0;
			
			inMemShuffleList.clear();
		}
		
	}
	
	public List<InMemoryShuffleMerge> getInMemoryShuffleMergeList() {
		return mergeList;
	}
	
	public List<Segment> getInMemLeftSegments() {
		if(inMemSegmentsList == null) {
			inMemSegmentsList = new ArrayList<Segment>();
			for(ShuffleInfo si : inMemShuffleList) {
				Segment inMemSeg = new Segment("RAM", si.getRecords(), si.getRawLength(), si.getCompressedLen());
				inMemSegmentsList.add(inMemSeg);
			}
		}
		return inMemSegmentsList;
	}
}

class OnDiskSegmentsModel {
	//before merge
	private List<ShuffleInfo> shuffleInfoList = new ArrayList<ShuffleInfo>();
	private List<InMemoryShuffleMerge> inMemoryShuffleMergeList = new ArrayList<InMemoryShuffleMerge>();
	//on-disk merge
	private List<OnDiskShuffleMerge> onDiskShuffleMergeList = new ArrayList<OnDiskShuffleMerge>();
	//left files on disk 
	private List<Segment> leftSegmentsOnDisk = new ArrayList<Segment>();
	
	private int segmentsNum = 0;
	private int ioSortFactor;

	private long incRecords = 0;
	private long incRawLength = 0;
	private long incCompressedLen = 0;

	public OnDiskSegmentsModel(int ioSortFactor) {
		this.ioSortFactor = ioSortFactor;
	}
	
	public void addShuffleInfo(ShuffleInfo onDiskShuffleInfo) {
		
		incRecords += onDiskShuffleInfo.getRecords();
		incRawLength += onDiskShuffleInfo.getRawLength();
		incCompressedLen += onDiskShuffleInfo.getCompressedLen();
		shuffleInfoList.add(onDiskShuffleInfo);
		onDiskMerge();
		
	}
	public void addInMemoryShuffleMerge(InMemoryShuffleMerge inMemoryShuffleMerge) {
		
		incRecords += inMemoryShuffleMerge.getRecordsAfterCombine();
		incRawLength += inMemoryShuffleMerge.getRawLength();
		incCompressedLen += inMemoryShuffleMerge.getCompressedLength();
		inMemoryShuffleMergeList.add(inMemoryShuffleMerge);
		onDiskMerge();
	}
	
	public void onDiskMerge() {
		segmentsNum++;
		if(segmentsNum >= (2 * ioSortFactor - 1)) {
			OnDiskShuffleMerge merge = new OnDiskShuffleMerge(0);
			merge.setShuffleAfterMergeItem(0, segmentsNum, incRecords, incRawLength, incRawLength, incCompressedLen);
			
			onDiskShuffleMergeList.add(merge);
			
			Segment onDiskSeg = new Segment("Disk", incRecords, incRawLength, incCompressedLen);
			leftSegmentsOnDisk.add(onDiskSeg);
			
			segmentsNum = 0;
			incRecords = 0;
			incRawLength = 0;
			incCompressedLen = 0;
			
			shuffleInfoList.clear();
			inMemoryShuffleMergeList.clear();
		}
	}
	
	List<OnDiskShuffleMerge> getOnDiskShuffleMergeList() {
		return onDiskShuffleMergeList;
	}
	
	public List<Segment> getOnDiskLeftSegments() {
		
		for(ShuffleInfo si : shuffleInfoList) {
			Segment onDiskSeg = new Segment("Disk", si.getRecords(), si.getRawLength(), si.getCompressedLen());
			leftSegmentsOnDisk.add(onDiskSeg);
		}		
	
		for(InMemoryShuffleMerge imsm : inMemoryShuffleMergeList) {
			Segment onDiskSeg = new Segment("Disk", imsm.getRecordsAfterCombine(), imsm.getRawLength(), imsm.getCompressedLength());
			leftSegmentsOnDisk.add(onDiskSeg);
		}
		
		return leftSegmentsOnDisk;
	}
	
}