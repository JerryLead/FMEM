package data.model.mapper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import profile.commons.configuration.Configuration;
import profile.task.mapper.Mapper;
import profile.task.mapper.MapperBuffer;
import profile.task.mapper.Spill;
import profile.task.mapper.SpillInfo;


public class SpillModel {
	
	private static Spill generateNewSpill(long map_output_bytes, long map_output_records, MapperBuffer eBuffer) {
		Spill newSpill = new Spill();
		int spillBytesCounts =  (int) Math.ceil(((double) map_output_bytes / eBuffer.getSoftBufferLimit()));
		int spillRecordsCounts = (int) Math.ceil(((double) map_output_records / eBuffer.getSoftRecordLimit()));
		
		String reason = "record";
		int spillCounts = spillRecordsCounts;
		
		// <recordsBeforeCombine, bytesBeforeSpill> ==> Spill ==> <recordsAfterCombine, bytesAfterSpill>
		
		// total records in one spill
		long recordsBeforeCombine = eBuffer.getSoftRecordLimit();
		// (total bytes / total records) * recordsInOneSpill
		// total bytes in one spill
		long bytesBeforeSpill = (long)((double) map_output_bytes / map_output_records * recordsBeforeCombine);
		
		if(spillBytesCounts > spillRecordsCounts) {
			spillCounts = spillBytesCounts;
			reason = "buffer";
			
			bytesBeforeSpill = eBuffer.getSoftBufferLimit();
			recordsBeforeCombine = (long) (bytesBeforeSpill / ((double) map_output_bytes / map_output_records));
		}
		
		long flushRecords = map_output_records - (spillCounts - 1) * recordsBeforeCombine;
		long flushBytes = map_output_bytes - (spillCounts - 1) * bytesBeforeSpill;
		
		
		for(int i = 0; i < spillCounts - 1; i++) {
			SpillInfo spillInfo = new SpillInfo(reason, recordsBeforeCombine, bytesBeforeSpill);
			newSpill.addSpillInfo(spillInfo);
		}
		
		SpillInfo flushInfo = new SpillInfo("flush", flushRecords, flushBytes);
		newSpill.addSpillInfo(flushInfo);
		
		return newSpill;
	}
	
	//suppose io_sort_mb, io_sort_spill_percent, io_sort_record_percent have been set.
	public static Spill computeSpill(long map_output_bytes, long map_output_records, MapperBuffer eBuffer, Spill oSpill,
			Configuration fConf, Configuration newConf) {
		Spill newSpill = generateNewSpill(map_output_bytes, map_output_records, eBuffer);	
		refineSpillInfo(newSpill, oSpill, fConf, newConf);
		return newSpill;
	}	
	
	public static void refineSpillInfo(Spill newSpill, Spill oSpill, Configuration fConf, Configuration newConf) {
		//get SpillInfo list from finished map task
		List<SpillInfo> finishedSpillInfoList = oSpill.getSpillInfoList(); 
		
		List<SpillInfo> newSpillInfoList = newSpill.getSpillInfoList();

		boolean newCombine = newConf.getMapreduce_combine_class() != null ? true : false;
		int size = finishedSpillInfoList.size();
		
		
		if(size > 1 && newCombine == false) {
			SpillInfo lastInfo = finishedSpillInfoList.get(size - 1);
			double lastRatio = (double)lastInfo.getRecordsAfterCombine() / lastInfo.getRecordsBeforeCombine();
			
			SpillInfo last2ndInfo = finishedSpillInfoList.get(size - 2);
			double last2ndRatio = (double)last2ndInfo.getRecordsAfterCombine() / last2ndInfo.getRecordsBeforeCombine();
			
			if(lastRatio / last2ndRatio > 1.1 || last2ndRatio / lastRatio > 1.1) //discard the last finished spill info
				size = size - 1;		
		}
		
		
		long fRecordsBeforeCombine = 0;
		long fBytesBeforeCombine = 0;
		
		long fRecordsAfterCombine = 0;
		long fRawLength = 0;
		long fCompressedLength = 0;
		
		
		for(int i = 0; i < size; i++) {
			SpillInfo info = finishedSpillInfoList.get(i);
			fRecordsBeforeCombine += info.getRecordsBeforeCombine();
			fBytesBeforeCombine += info.getBytesBeforeSpill();
			fRecordsAfterCombine += info.getRecordsAfterCombine();
			fRawLength += info.getRawLength();
			fCompressedLength += info.getCompressedLength();
		}
		
		double spill_combine_record_ratio = (double) fRecordsAfterCombine / fRecordsBeforeCombine;
		double spill_combine_bytes_ratio = (double) fRawLength / fBytesBeforeCombine;
		
		
		// no combine() in spill phase
		if(newCombine == false) {
			newSpill.setSpill_combine_record_ratio(spill_combine_record_ratio);
			newSpill.setSpill_combine_bytes_ratio(spill_combine_bytes_ratio);
			
			for(SpillInfo newInfo : newSpillInfoList) {
				long newRecordsBeforeCombine = newInfo.getRecordsBeforeCombine();
				long newBytesBeforeCombine = newInfo.getBytesBeforeSpill();
							
				long newRecordsAfterCombine = (long) ((double)newRecordsBeforeCombine * spill_combine_record_ratio);
				long newRawLength = (long) ((double)newBytesBeforeCombine * spill_combine_bytes_ratio);
				long newCompressedLength = (long) ((double)newRawLength / fRawLength * fCompressedLength);
				
				newInfo.setAfterSpillInfo(newRecordsAfterCombine, newRawLength, newCompressedLength);
				
				/*
				System.out.println("[Spill] <RecordsBeforeCombine = " + newInfo.getRecordsBeforeCombine() + ", "
	        			+ "BytesBeforeSpill = " + newInfo.getBytesBeforeSpill() + ", "
	        			+ "RecordAfterCombine = " + newRecordsAfterCombine + ", " 
	        			+ "RawLength = " + newRawLength + ", CompressedLength = " + newCompressedLength + ">");
	        	*/
			}
			return;
		}
		
		// combine() in spill phase
		
		int fmSegN = finishedSpillInfoList.size();
		int fismb = fConf.getIo_sort_mb();
	
		int eismb = newConf.getIo_sort_mb();
		int eSegN = newSpillInfoList.size();
		
		int fSplitMB = (int) (fConf.getSplitSize() / 1024 / 1024);
		int eSplitMB = (int) (newConf.getSplitSize() / 1024 / 1024);
		
		// it is a heuristic algorithm
		if(fmSegN > 2) {
			if(eSegN > 2) {
				spill_combine_record_ratio = spill_combine_record_ratio * (1 - 0.1 * (eismb - fismb) / 200);
				spill_combine_bytes_ratio = spill_combine_bytes_ratio * (1 - 0.1 * (eismb - fismb) / 200);
			}
			//eSegN == 1,2
			else {
				int deg = ((eSplitMB / eSegN) - (fSplitMB / fmSegN)) / Math.min((eSplitMB / eSegN), (fSplitMB / fmSegN));
				spill_combine_record_ratio = spill_combine_record_ratio * (1 - 0.1 * deg);
				spill_combine_bytes_ratio = spill_combine_bytes_ratio * (1 - 0.1 * deg);
			}
			
		}
		
		else if(fmSegN == 2) {
			// Note that split size is changed in this function
			int deg = ((eSplitMB / eSegN) - (fSplitMB / fmSegN)) / Math.min((eSplitMB / eSegN), (fSplitMB / fmSegN));
			spill_combine_record_ratio = spill_combine_record_ratio * (1 - 0.1 * deg);
			spill_combine_bytes_ratio = spill_combine_bytes_ratio * (1 - 0.1 * deg);
		}
		
		else if(fmSegN == 1) {
			// Note that split size is changed in this function
			int deg = ((eSplitMB / eSegN) - (fSplitMB / fmSegN)) / Math.min((eSplitMB / eSegN), (fSplitMB / fmSegN));
			spill_combine_record_ratio = spill_combine_record_ratio * (1 - 0.1 * deg);
			spill_combine_bytes_ratio = spill_combine_bytes_ratio * (1 - 0.1 * deg);
		}		
				
		
		newSpill.setSpill_combine_record_ratio(spill_combine_record_ratio);
		newSpill.setSpill_combine_bytes_ratio(spill_combine_bytes_ratio);
		
		for(SpillInfo newInfo : newSpillInfoList) {
			long newRecordsBeforeCombine = newInfo.getRecordsBeforeCombine();
			long newBytesBeforeCombine = newInfo.getBytesBeforeSpill();
						
			long newRecordsAfterCombine = (long) ((double)newRecordsBeforeCombine * spill_combine_record_ratio);
			long newRawLength = (long) ((double)newBytesBeforeCombine * spill_combine_bytes_ratio);
			long newCompressedLength = (long) ((double)newRawLength / fRawLength * fCompressedLength);
			
			newInfo.setAfterSpillInfo(newRecordsAfterCombine, newRawLength, newCompressedLength);
			
			/*
			System.out.println("[Spill] <RecordsBeforeCombine = " + newInfo.getRecordsBeforeCombine() + ", "
        			+ "BytesBeforeSpill = " + newInfo.getBytesBeforeSpill() + ", "
        			+ "RecordAfterCombine = " + newRecordsAfterCombine + ", " 
        			+ "RawLength = " + newRawLength + ", CompressedLength = " + newCompressedLength + ">");
        	*/
		}
		
		
	}

	// fSpillList is used to compute combine I/O ratio
	public static Spill computeSpill(long map_output_bytes, long map_output_records, MapperBuffer eBuffer,
			List<Spill> fSpillList, Configuration fConf, Configuration newConf) {
		
		Spill newSpill = generateNewSpill(map_output_bytes, map_output_records, eBuffer);	
		refineSpillInfo(newSpill, fSpillList, fConf, newConf);
		return newSpill;
	}

	private static void refineSpillInfo(Spill newSpill, List<Spill> fSpillList, Configuration fConf, Configuration newConf) {
		//get SpillInfo list from finished map task
		List<SpillInfo> allFSpillInfoList = new ArrayList<SpillInfo>();
		boolean newCombine = newConf.getMapreduce_combine_class() != null ? true : false;
		
		// compute I/O record ratio of Spill (if combine is triggered, I/O != 1)
		// consider all the finished mappers' all spill infos
		int fmSegN = 0;
		for(Spill oSpill : fSpillList)  {
			List<SpillInfo> finishedSpillInfoList = oSpill.getSpillInfoList(); 
			int size = finishedSpillInfoList.size();
			fmSegN += size;
			
			if(size > 1 && newCombine == false) {
				SpillInfo lastInfo = finishedSpillInfoList.get(size - 1);
				double lastRatio = (double)lastInfo.getRecordsAfterCombine() / lastInfo.getRecordsBeforeCombine();
				
				SpillInfo last2ndInfo = finishedSpillInfoList.get(size - 2);
				double last2ndRatio = (double)last2ndInfo.getRecordsAfterCombine() / last2ndInfo.getRecordsBeforeCombine();
				
				if(lastRatio / last2ndRatio > 1.1 || last2ndRatio / lastRatio > 1.1) //discard the last flushed spill info
					size = size - 1;
				
			}
			
			for(int i = 0; i < size; i++) 
				allFSpillInfoList.add(finishedSpillInfoList.get(i));
		}
		
		
		List<SpillInfo> newSpillInfoList = newSpill.getSpillInfoList();
		
		
				
		int size = allFSpillInfoList.size();
		long fRecordsBeforeCombine = 0;
		long fBytesBeforeCombine = 0;
		
		long fRecordsAfterCombine = 0;
		long fRawLength = 0;
		long fCompressedLength = 0;
		
		//fMapper1~n, SpillInfo1~m in fMapperi
		for(int i = 0; i < size; i++) {
			SpillInfo info = allFSpillInfoList.get(i);
			fRecordsBeforeCombine += info.getRecordsBeforeCombine();
			fBytesBeforeCombine += info.getBytesBeforeSpill();
			fRecordsAfterCombine += info.getRecordsAfterCombine();
			fRawLength += info.getRawLength();
			fCompressedLength += info.getCompressedLength();
		}	
		
		double spill_combine_record_ratio = (double) fRecordsAfterCombine / fRecordsBeforeCombine;
		double spill_combine_bytes_ratio = (double) fRawLength / fBytesBeforeCombine;
		
		//no combine() in spill phase
		if(newCombine == false) {
			newSpill.setSpill_combine_record_ratio(spill_combine_record_ratio);
			newSpill.setSpill_combine_bytes_ratio(spill_combine_bytes_ratio);
			
			for(SpillInfo newInfo : newSpillInfoList) {
				long newRecordsBeforeCombine = newInfo.getRecordsBeforeCombine();
				long newBytesBeforeCombine = newInfo.getBytesBeforeSpill();
							
				long newRecordsAfterCombine = (long) ((double)newRecordsBeforeCombine * spill_combine_record_ratio);
				long newRawLength = (long) ((double)newBytesBeforeCombine * spill_combine_bytes_ratio);
				long newCompressedLength = (long) ((double)newRawLength / fRawLength * fCompressedLength);
				
				newInfo.setAfterSpillInfo(newRecordsAfterCombine, newRawLength, newCompressedLength);
				
				/*
				System.out.println("[Spill] <RecordsBeforeCombine = " + newInfo.getRecordsBeforeCombine() + ", "
	        			+ "BytesBeforeSpill = " + newInfo.getBytesBeforeSpill() + ", "
	        			+ "RecordAfterCombine = " + newRecordsAfterCombine + ", " 
	        			+ "RawLength = " + newRawLength + ", CompressedLength = " + newCompressedLength + ">");
	        	*/
			}
			return;
		}
		
		// combine() in spill phase
		int fismb = fConf.getIo_sort_mb();
		fmSegN /= fSpillList.size();
		int eismb = newConf.getIo_sort_mb();
		int eSegN = newSpillInfoList.size();
		int fSplitMB = (int) (fConf.getSplitSize() / 1024 / 1024);
		int eSplitMB = (int) (newConf.getSplitSize() / 1024 / 1024);
		
		// it is a heuristic algorithm
		if(fmSegN > 2) {
			if(eSegN >= 2) {
				spill_combine_record_ratio = spill_combine_record_ratio * (1 - 0.1 * (eismb - fismb) / 200);
				spill_combine_bytes_ratio = spill_combine_bytes_ratio * (1 - 0.1 * (eismb - fismb) / 200);
			}
			//eSegN == 1
			else {
				 int deg = ((eSplitMB / eSegN) - (fSplitMB / fmSegN)) / Math.min((eSplitMB / eSegN), (fSplitMB / fmSegN));
				spill_combine_record_ratio = spill_combine_record_ratio * (1 - 0.1 * deg);
				spill_combine_bytes_ratio = spill_combine_bytes_ratio * (1 - 0.1 * deg);
			}
			
		}
		
		else if(fmSegN == 2) {
			// Note that split size is changed in this function
			int deg = ((eSplitMB / eSegN) - (fSplitMB / fmSegN)) / Math.min((eSplitMB / eSegN), (fSplitMB / fmSegN));
			spill_combine_record_ratio = spill_combine_record_ratio * (1 - 0.1 * deg);
			spill_combine_bytes_ratio = spill_combine_bytes_ratio * (1 - 0.1 * deg);
		}
		
		else if(fmSegN == 1) {
			// Note that split size is changed in this function
			int deg = ((eSplitMB / eSegN) - (fSplitMB / fmSegN)) / Math.min((eSplitMB / eSegN), (fSplitMB / fmSegN));
			spill_combine_record_ratio = spill_combine_record_ratio * (1 - 0.1 * deg);
			spill_combine_bytes_ratio = spill_combine_bytes_ratio * (1 - 0.1 * deg);
		}
		
		
		newSpill.setSpill_combine_record_ratio(spill_combine_record_ratio);
		newSpill.setSpill_combine_bytes_ratio(spill_combine_bytes_ratio);
		
		//use the average records/bytes i/o ratio to estimate the new spill
		for(SpillInfo newInfo : newSpillInfoList) {
			long newRecordsBeforeCombine = newInfo.getRecordsBeforeCombine();
			long newBytesBeforeCombine = newInfo.getBytesBeforeSpill();
					
			long newRecordsAfterCombine = (long) ((double)newRecordsBeforeCombine * spill_combine_record_ratio);
			long newRawLength = (long) ((double)newBytesBeforeCombine * spill_combine_bytes_ratio);
			long newCompressedLength = (long) ((double)newRawLength / fRawLength * fCompressedLength);
			
			newInfo.setAfterSpillInfo(newRecordsAfterCombine, newRawLength, newCompressedLength);
				
			/*
			System.out.println("[Spill] <RecordsBeforeCombine = " + newInfo.getRecordsBeforeCombine() + ", "
        			+ "BytesBeforeSpill = " + newInfo.getBytesBeforeSpill() + ", "
        			+ "RecordAfterCombine = " + newRecordsAfterCombine + ", " 
        			+ "RawLength = " + newRawLength + ", CompressedLength = " + newCompressedLength + ">");
        	*/
		}
	}
}
