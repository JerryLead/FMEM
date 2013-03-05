package data.model.reducer;

import java.util.ArrayList;
import java.util.List;

import profile.task.reducer.InMemoryShuffleMerge;
import profile.task.reducer.MergeInShuffle;
import profile.task.reducer.Reduce;
import profile.task.reducer.Reducer;


public class IORatioModel {
	
	private InMemoryShuffleMergeIORatio mergeIoRatio;
	private ReduceIORatio reduceIoRatio;
	
	public IORatioModel(Reducer finishedReducer, boolean newHasCombine, double mapperCombineRecIORatio, double mapperCombineBytesIORatio) {
		mergeIoRatio = new InMemoryShuffleMergeIORatio(finishedReducer, newHasCombine, mapperCombineRecIORatio, mapperCombineBytesIORatio);
		reduceIoRatio = new ReduceIORatio(finishedReducer);
	}

	public IORatioModel(List<Reducer> finishedReducerList, boolean newHasCombine, double mapperCombineRecIORatio, double mapperCombineBytesIORatio) {
		mergeIoRatio = new InMemoryShuffleMergeIORatio(finishedReducerList, newHasCombine, mapperCombineRecIORatio, mapperCombineBytesIORatio);
		reduceIoRatio = new ReduceIORatio(finishedReducerList);
	}
	
	public double getInMemShuffleMergeRecordsIoRatio() {
		return mergeIoRatio.getRecordsIoRatio();
	}
	
	public double getInMemShuffleMergeBytesIoRatio() {
		return mergeIoRatio.getRawLenIoRatio();
	}
	
	public double getReduceRecordsIoRatio() {
		return reduceIoRatio.getRecordsIoRatio();
	}
	
	public double getReduceBytesIoRatio() {
		return reduceIoRatio.getBytesIoRatio();
	}
}

class InMemoryShuffleMergeIORatio {
	
	private List<Reducer> finishedReducerList;
	private double recordsIoRatio;
	private double rawLenIoRatio;
	
	private double mapperCombineRecIORatio;
	private double mapperCombineBytesIORatio;
	private boolean newHasCombine;
	
	public InMemoryShuffleMergeIORatio(Reducer finishedReducer, boolean newHasCombine,
			double mapperCombineRecIORatio, double mapperCombineBytesIORatio) {
		finishedReducerList = new ArrayList<Reducer>();
		finishedReducerList.add(finishedReducer);
		this.mapperCombineRecIORatio = mapperCombineRecIORatio;
		this.mapperCombineBytesIORatio = mapperCombineBytesIORatio;
		this.newHasCombine = newHasCombine;
		
		computeIORatio();
	}
	
	public InMemoryShuffleMergeIORatio(List<Reducer> finishedReducerList, boolean newHasCombine, 
			double mapperCombineRecIORatio, double mapperCombineBytesIORatio) {
		this.finishedReducerList = finishedReducerList;
		computeIORatio();
	}

	public void computeIORatio() {
		
		int fInMemSegmentsNum = 0;
		long fInMemRecordsBeforeMergeAC = 0;
		long fInMemBytesBeforeMergeAC = 0;
		long fInMemRecordsAfterCombine = 0;
		long fInMemRawLength = 0;
		long fInMemCompressedLength = 0;
		
		for(Reducer fr : finishedReducerList) {
			MergeInShuffle fMergeInShuffleList = fr.getMergeInShuffle();
			List<InMemoryShuffleMerge> fInMemoryShuffleMergeList = fMergeInShuffleList.getInMemoryShuffleMergeList();
			//List<OnDiskShuffleMerge> fOnDiskShuffleMergeList = fMergeInShuffleList.getOnDiskShuffleMergeList();
			
			for(InMemoryShuffleMerge imsm : fInMemoryShuffleMergeList) {
				fInMemSegmentsNum += imsm.getSegmentsNum();
				fInMemRecordsBeforeMergeAC += imsm.getRecordsBeforeMergeAC();
				fInMemBytesBeforeMergeAC += imsm.getBytesBeforeMergeAC();
				fInMemRecordsAfterCombine += imsm.getRecordsAfterCombine();
				fInMemRawLength += imsm.getRawLength();
				fInMemCompressedLength += imsm.getCompressedLength();
			}
		}
		

		if(fInMemSegmentsNum == 0) { //no InMemoryShuffleMerge in finished reducer, use reduce function's I/O ratio instead
			if(newHasCombine) {
				recordsIoRatio = mapperCombineRecIORatio;
				rawLenIoRatio = mapperCombineBytesIORatio;
			}
			else {
				recordsIoRatio = 1.0;
				rawLenIoRatio = 1.0;
			}
		}
		else {
			recordsIoRatio = (double) fInMemRecordsAfterCombine / fInMemRecordsBeforeMergeAC;
			rawLenIoRatio = (double) fInMemRawLength / fInMemBytesBeforeMergeAC;
		}
	}

	public double getRecordsIoRatio() {
		return recordsIoRatio;
	}

	public double getRawLenIoRatio() {
		return rawLenIoRatio;
	}
}

class ReduceIORatio {
	private List<Reducer> finishedReducerList;
	
	long inputkeyNum = 0; //equals "Reduce input groups" normally
	long inputKeyValuePairsNum = 0; //equals "Reduce input records"
	long inputBytes = 0; // equals rawLength after merge in Sort
	
	long outputKeyValuePairsNum; //equals "Reduce output records"
	long outputBytes; // depends on "HDFS_BYTES_WRITTEN" and dfs.replication
	
	double recordsIoRatio;
	double bytesIoRatio;
	
	public ReduceIORatio(Reducer reducer) {
		finishedReducerList = new ArrayList<Reducer>();
		finishedReducerList.add(reducer);
		computeIORatio();
	}
	
	public ReduceIORatio(List<Reducer> finishedReducerList) {
		this.finishedReducerList = finishedReducerList;
		computeIORatio();
	}
	
	public void computeIORatio() {
		
		long fInputKeyValuePairsNum = 0;
		long fOutputKeyValuePairsNum = 0;
		
		long fInputBytes = 0;
		long fOutputBytes = 0;
		
		for(Reducer fr : finishedReducerList) {
			Reduce reduce = fr.getReduce();
			fInputKeyValuePairsNum += reduce.getInputKeyValuePairsNum();
			fOutputKeyValuePairsNum += reduce.getOutputKeyValuePairsNum();
			fInputBytes += reduce.getInputBytes();
			fOutputBytes += reduce.getOutputBytes();
		}
		
		if(fInputKeyValuePairsNum == 0) {
			recordsIoRatio = 0;
			bytesIoRatio = 0;
		}
		
		else {
			recordsIoRatio = (double) fOutputKeyValuePairsNum / fInputKeyValuePairsNum;
			bytesIoRatio = (double) fOutputBytes / fInputBytes;
		}
	}
	
	public double getRecordsIoRatio() {
		return recordsIoRatio;
	}
	
	public double getBytesIoRatio() {
		return bytesIoRatio;
	}
	
}
