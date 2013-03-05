package data.model.reducer;

import java.util.ArrayList;
import java.util.List;

import profile.commons.configuration.Configuration;
import profile.task.reducer.FinalSortMerge;
import profile.task.reducer.MergeInShuffle;
import profile.task.reducer.MixSortMerge;
import profile.task.reducer.ReducerBuffer;
import profile.task.reducer.Segment;
import profile.task.reducer.Sort;


public class SortModel {

	public static Sort computeSort(ReducerBuffer buffer, MergeInShuffle mergeInShuffle, Configuration newConf) {
		Sort sort = new Sort();
		
		List<Segment> inMemLeftSegmentsList = mergeInShuffle.getInMemLeftSegments();
		List<Segment> onDiskLeftSegmentsList = mergeInShuffle.getOnDiskLeftSegments();
		
		long reduceInputBytes = 0; 
		
		//Because there is no combine in Sort Phase, we just add the bytes in Memory and on Disk to estimate reduceInputBytes		
		//if this algorithm is not precise, we can use shuffle bytes loss to estimate the reduce input bytes
		for(Segment imSeg : inMemLeftSegmentsList) 
			reduceInputBytes += imSeg.getRawLength();
		for(Segment diskSeg : onDiskLeftSegmentsList)
			reduceInputBytes += diskSeg.getRawLength();
		sort.setReduceInputBytes(reduceInputBytes);
		
		int ioSortFactor = newConf.getIo_sort_factor();
		
		List<Segment> stillInMemorySegmentsList = new ArrayList<Segment>();;
		List<Segment> finalSegmentsList = null; 
		
		//[InMemorySortMerge]
		if(!inMemLeftSegmentsList.isEmpty()) {
			
			int remainIndex = createInMemorySegments(inMemLeftSegmentsList, buffer.getMaxInMemReduce()); 
			
			finalSegmentsList = new ArrayList<Segment>();
			
			if (remainIndex != inMemLeftSegmentsList.size() - 1) {
				for(int i = remainIndex + 1; i < inMemLeftSegmentsList.size(); i++)
					finalSegmentsList.add(inMemLeftSegmentsList.get(i));
			}
			
			else if (ioSortFactor > onDiskLeftSegmentsList.size()) {
				int segmentsNum = 0;
				long records = 0;
				long bytesBeforeMerge = 0;
				long rawLength = 0;
				long compressedLength = 0;
				
				for(int i = 0; i <= remainIndex; i++) {
					Segment sgm = inMemLeftSegmentsList.get(i);
					segmentsNum++;
					records += sgm.getRecords();
					bytesBeforeMerge += sgm.getRawLength();
					rawLength += sgm.getRawLength();
					compressedLength += sgm.getCompressedLen();
				}
				
		
				sort.setInMemorySortMergeItem(0, segmentsNum, records, bytesBeforeMerge,
						rawLength, compressedLength);
				Segment newOnDiskSeg = new Segment("Disk", records, rawLength, compressedLength);
				onDiskLeftSegmentsList.add(newOnDiskSeg);
				
				/*
				System.out.println("[InMemorySortMerge] <SegmentsNum = " + segmentsNum
						+ ", Records = " + records + ", BytesBeforeMerge = " + bytesBeforeMerge
						+ ", RawLength = " + rawLength 
						+ ", CompressedLength = " + compressedLength + ">");
				*/
			}
			else {
				
				for(int i = 0; i <= remainIndex; i++) {
					Segment sgm = inMemLeftSegmentsList.get(i);
					stillInMemorySegmentsList.add(sgm);
				}
			}
		}
		
		//[MixSortMerge]
		if(!onDiskLeftSegmentsList.isEmpty()) {
			int inMemSegmentsNum = 0;
			long inMemRecords = 0;
			long inMemRawLength = 0;
			long inMemCompressedLength = 0;
			
			int onDiskSegmentsNum = 0;
			long onDiskRecords = 0;
			long onDiskRawLength = 0;
			long onDiskCompressedLength = 0;
			
			for(Segment seg : stillInMemorySegmentsList) {
				inMemSegmentsNum++;
				inMemRecords += seg.getRecords();
				inMemRawLength += seg.getRawLength();
				inMemCompressedLength += seg.getCompressedLen();
			}
			
			for(Segment seg : onDiskLeftSegmentsList) {
				onDiskSegmentsNum++;
				onDiskRecords += seg.getRecords();
				onDiskRawLength += seg.getRawLength();
				onDiskCompressedLength += seg.getCompressedLen();
			}
			
			MixSortMerge mixSortMerge = new MixSortMerge();
			mixSortMerge.set(0, inMemSegmentsNum, inMemRawLength, onDiskSegmentsNum, onDiskRawLength);
			mixSortMerge.setRecords(inMemRecords, onDiskRecords);
			sort.setMixSortMerge(mixSortMerge);
			
			/*
			System.out.println("[MixSortMerge] <InMemorySegmentsNum = " + inMemSegmentsNum
					+ ", InMemorySegmentsSize = " + inMemRawLength + ", OnDiskSegmentsNum = " + onDiskSegmentsNum
					+ ", OnDiskSegmentsSize = " + onDiskRawLength + ">");
			System.out.println("---------------------------------------------------------------------------------------------------------");
			*/
		}
		
		if(finalSegmentsList != null && !finalSegmentsList.isEmpty()) {		
			int segmentsNum = 0;
			long records = 0;
			long rawLength = 0;
			long compressedLength = 0;
			
			for (Segment seg : finalSegmentsList) {
				segmentsNum++;
				records += seg.getRecords();
				rawLength += seg.getRawLength();
				compressedLength += seg.getCompressedLen();
			}
			
			FinalSortMerge finalSortMerge = new FinalSortMerge();
			finalSortMerge.set(0, finalSegmentsList.size(), rawLength);
			finalSortMerge.setRecords(records);
			
			sort.setFinalSortMerge(finalSortMerge);
			
			/*
			System.out.println("[FinalSortMerge] <Records = " + finalSortMerge.getRecords() + ", inMemSegmentsNum = " 
					+ finalSortMerge.getInMemorySegmentsNum() + ", inMemSegmentsSize = " + finalSortMerge.getInMemorySegmentsSize() + ">");
			System.out.println("---------------------------------------------------------------------------------------------------------");
			System.out.println();
			*/
		}
			
		return sort;
	}
	
	private static int createInMemorySegments(List<Segment> inMemLeftSegmentsList, long leaveBytes) {
		long totalBytes = 0;
		for (int i = inMemLeftSegmentsList.size() - 1; i >= 0; i--) {
			totalBytes += inMemLeftSegmentsList.get(i).getRawLength();
			if(totalBytes > leaveBytes)
				return i;
		}
		return -1; //keeps all the InMemoryShuffleInfo in memory without merge
	}

}
