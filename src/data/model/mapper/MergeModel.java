package data.model.mapper;

import java.util.List;

import profile.commons.configuration.Configuration;
import profile.task.mapper.Mapper;
import profile.task.mapper.Merge;
import profile.task.mapper.MergeInfo;
import profile.task.mapper.Spill;
import profile.task.mapper.SpillInfo;

// sometimes without combine........
// if(eSpillInfoList.size() == 1) mergeInfo = spillInfo
//  minSpillsForCombine = job.getInt("min.num.spills.for.combine", 3);
public class MergeModel {
	
	public static Merge computeMerge(Spill eSpill, Mapper finishedMapper, Configuration fConf, Configuration newConf) {
		
		boolean newCombine = newConf.getMapreduce_combine_class() != null ? true : false;
		int mapred_reduce_tasks = newConf.getMapred_reduce_tasks();
		int min_num_spills_for_combine = newConf.getMin_num_spills_for_combine();
		
		int fMin_num_spills_for_combine = fConf.getMin_num_spills_for_combine();
		boolean fCombine = fConf.getMapreduce_combine_class() != null ? true : false;
		boolean fMergeCombine = finishedMapper.getSpill().getSpillInfoList().size() < fMin_num_spills_for_combine ? false : true;
		if(finishedMapper.getSpill().getSpillInfoList().size() == 1)
			fMergeCombine = true;
		
		Merge eMerge = new Merge();
		List<MergeInfo> finishedMergeInfoList = finishedMapper.getMerge().getMergeInfoList();
		
		List<SpillInfo> eSpillInfoList = eSpill.getSpillInfoList();
		
		boolean eMergeCombine = eSpillInfoList.size() < min_num_spills_for_combine ? false : true;
		
		long eTotalRecordsAfterCombine = 0;
		long eTotalRawLength = 0;
		long eTotalCompressedLength = 0;
		
		assert(mapred_reduce_tasks != 0);
		
		for(SpillInfo info : eSpillInfoList) {
			eTotalRecordsAfterCombine += info.getRecordsAfterCombine();
			eTotalRawLength += info.getRawLength();
			eTotalCompressedLength += info.getCompressedLength();
		}
		
		List<SpillInfo> finishedSpillInfoList = finishedMapper.getSpill().getSpillInfoList();
		
		long totalFinishedRecordsAfterCombine = 0;  //spill
		long totalFinishedRawLength = 0;
		long totalFinishedCompressedLength = 0;
		for(SpillInfo info: finishedSpillInfoList) {
			totalFinishedRecordsAfterCombine += info.getRecordsAfterCombine();
			totalFinishedRawLength += info.getRawLength();
			totalFinishedCompressedLength += info.getCompressedLength();
		}
		
		
		int segmentsNum = eSpillInfoList.size();
		int size = finishedMergeInfoList.size();
		
		long[] finishedRecordsBeforeMerge = new long[size];
		long[] finishedRawLengthBeforeMerge = new long[size];
		long[] finishedCompressedLengthBeforeMerge = new long[size];
		
		long[] finishedRecordsAfterMerge = new long[size];
		long[] finishedRawLengthAfterMerge = new long[size];
		long[] finishedCompressedLengthAfterMerge = new long[size];
		
		long totalFinishedRecordsBeforeMerge = 0;
		long totalFinishedRawLengthBeforeMerge = 0;
		long totalFinishedCompressedLengthBeforeMerge = 0;
		
		long totalFinishedRecordsAfterMerge = 0;
		long totalFinishedRawLengthAfterMerge = 0;
		long totalFinishedCompressedLengthAfterMerge = 0;

		for(int i = 0; i < size; i++) {
			MergeInfo info = finishedMergeInfoList.get(i);
			finishedRecordsBeforeMerge[i] = info.getRecordsBeforeMerge();
			finishedRawLengthBeforeMerge[i] = info.getRawLengthBeforeMerge();
			finishedCompressedLengthBeforeMerge[i] = info.getCompressedLengthBeforeMerge();
			finishedRecordsAfterMerge[i] = info.getRecordsAfterMerge();
			finishedRawLengthAfterMerge[i] = info.getRawLengthAfterMerge();
			finishedCompressedLengthAfterMerge[i] = info.getCompressedLengthAfterMerge();
			
			totalFinishedRecordsBeforeMerge += finishedRecordsBeforeMerge[i];
			totalFinishedRawLengthBeforeMerge += finishedRawLengthBeforeMerge[i];
			totalFinishedCompressedLengthBeforeMerge += finishedCompressedLengthBeforeMerge[i];
			
			totalFinishedRecordsAfterMerge += finishedRecordsAfterMerge[i];
			totalFinishedRawLengthAfterMerge += finishedRawLengthAfterMerge[i];
			totalFinishedCompressedLengthAfterMerge += finishedCompressedLengthAfterMerge[i];
		}
			
		double merge_combine_record_ratio = (double) totalFinishedRecordsAfterMerge / totalFinishedRecordsBeforeMerge;
		double merge_combine_bytes_ratio = (double) totalFinishedRawLengthAfterMerge / totalFinishedRawLengthBeforeMerge;
		
		// no combine() in merge phase
		if(newCombine == false) {
			merge_combine_record_ratio = 1;
			merge_combine_bytes_ratio = 1;
			
			eMerge.setMerge_combine_record_ratio(merge_combine_record_ratio); 
			eMerge.setMerge_combine_bytes_ratio(merge_combine_bytes_ratio); 
			
			// reducer number is not changed
			if(mapred_reduce_tasks == size) {
				for(int i = 0; i < size; i++) {
					long recordsBeforeMerge = (long) ((double)finishedRecordsBeforeMerge[i] / totalFinishedRecordsBeforeMerge * eTotalRecordsAfterCombine);
					long rawLengthBeforeMerge = (long) ((double)finishedRawLengthBeforeMerge[i] / totalFinishedRawLengthBeforeMerge * eTotalRawLength);
					long compressedLengthBeforeMerge = (long) ((double)finishedCompressedLengthBeforeMerge[i] / totalFinishedCompressedLengthBeforeMerge * eTotalCompressedLength);
					
					long recordsAfterMerge = recordsBeforeMerge;
					long rawLengthAfterMerge = rawLengthBeforeMerge;
					long compressedLengthAfterMerge = compressedLengthBeforeMerge;
					
					MergeInfo newInfo = new MergeInfo(0, i, segmentsNum, rawLengthBeforeMerge, compressedLengthBeforeMerge);
					newInfo.setAfterMergeItem(0, recordsBeforeMerge, recordsAfterMerge, rawLengthAfterMerge, compressedLengthAfterMerge);
		
					eMerge.addMergeInfo(newInfo);

					/*
					System.out.println("[BeforeMerge][Partition " + i + "]" + "<SegmentsNum = " + segmentsNum
							+ ", RawLength = " + rawLengthBeforeMerge + ", CompressedLength = " + compressedLengthBeforeMerge + ">");
				    System.out.println("[AfterMergeAndCombine][Partition " + i + "]<RecordsBeforeMerge = " 
					  		+ recordsBeforeMerge + ", "
					  		+ "RecordsAfterMerge = " + recordsAfterMerge + ", "
					  		+ "RawLength = " + rawLengthAfterMerge + ", "
					  		+ "CompressedLength = " + compressedLengthAfterMerge + ">");
				    System.out.println();
				    */
				}
			}
			else {
				long recordsBeforeMerge = eTotalRecordsAfterCombine / mapred_reduce_tasks;
				long rawLengthBeforeMerge = eTotalRawLength / mapred_reduce_tasks;
				long compressedLengthBeforeMerge = eTotalCompressedLength / mapred_reduce_tasks;
				
				long recordsAfterMerge = recordsBeforeMerge;
				long rawLengthAfterMerge = rawLengthBeforeMerge;
				long compressedLengthAfterMerge = compressedLengthBeforeMerge;
				
				for(int i = 0; i < mapred_reduce_tasks; i++) {
					MergeInfo newInfo = new MergeInfo(0, i, segmentsNum, rawLengthBeforeMerge, compressedLengthBeforeMerge);
					newInfo.setAfterMergeItem(0, recordsBeforeMerge, recordsAfterMerge, rawLengthAfterMerge, compressedLengthAfterMerge);		
					eMerge.addMergeInfo(newInfo);
					
					/*
					System.out.println("[BeforeMerge][Partition " + i + "]" + "<SegmentsNum = " + segmentsNum
							+ ", RawLength = " + rawLengthBeforeMerge + ", CompressedLength = " + compressedLengthBeforeMerge + ">");
				    System.out.println("[AfterMergeAndCombine][Partition " + i + "]<RecordsBeforeMerge = " 
					  		+ recordsBeforeMerge + ", "
					  		+ "RecordsAfterMerge = " + recordsAfterMerge + ", "
					  		+ "RawLength = " + rawLengthAfterMerge + ", "
					  		+ "CompressedLength = " + compressedLengthAfterMerge + ">");
				    System.out.println();
				    */
				}
			}
			
			return eMerge;
		}

		// combine() in merge phase
		
		
		double eMerge_combine_record_ratio;
		double eMerge_combine_bytes_ratio;
		
		if(eMergeCombine == false) {
			eMerge_combine_record_ratio = 1;
			eMerge_combine_bytes_ratio = 1;
		}
		// combine in eMerge
		else if(fMergeCombine == false){
			
			eMerge_combine_record_ratio = eSpill.getSpill_combine_record_ratio() * 0.8;
			eMerge_combine_bytes_ratio = eSpill.getSpill_combine_bytes_ratio() * 0.8;
		}
		// fMergeCombine == true
		else {
			eMerge_combine_record_ratio = 0;
			eMerge_combine_bytes_ratio = 0;
		}
		
		// reducer number is not changed
		if(mapred_reduce_tasks == size) {
			for(int i = 0; i < size; i++) {
				long recordsBeforeMerge = (long) ((double)finishedRecordsBeforeMerge[i] / totalFinishedRecordsBeforeMerge * eTotalRecordsAfterCombine);
				long rawLengthBeforeMerge = (long) ((double)finishedRawLengthBeforeMerge[i] / totalFinishedRawLengthBeforeMerge * eTotalRawLength);
				long compressedLengthBeforeMerge = (long) ((double)finishedCompressedLengthBeforeMerge[i] / totalFinishedCompressedLengthBeforeMerge * eTotalCompressedLength);
				
				long recordsAfterMerge;
				long rawLengthAfterMerge;
				long compressedLengthAfterMerge;
				
				if(eMerge_combine_record_ratio == 0 && eMerge_combine_bytes_ratio == 0) {
					recordsAfterMerge = finishedRecordsAfterMerge[i];
					rawLengthAfterMerge = finishedRawLengthAfterMerge[i];
					compressedLengthAfterMerge = finishedCompressedLengthAfterMerge[i];
				}
				else {
					recordsAfterMerge = (long) (eMerge_combine_record_ratio * recordsBeforeMerge);
					rawLengthAfterMerge = (long) (eMerge_combine_bytes_ratio * rawLengthBeforeMerge);
					compressedLengthAfterMerge = (long) (eMerge_combine_bytes_ratio * compressedLengthBeforeMerge);
				}
				
				
				MergeInfo newInfo = new MergeInfo(0, i, segmentsNum, rawLengthBeforeMerge, compressedLengthBeforeMerge);
				newInfo.setAfterMergeItem(0, recordsBeforeMerge, recordsAfterMerge, rawLengthAfterMerge, compressedLengthAfterMerge);
	
				eMerge.addMergeInfo(newInfo);

				/*
				System.out.println("[BeforeMerge][Partition " + i + "]" + "<SegmentsNum = " + segmentsNum
						+ ", RawLength = " + rawLengthBeforeMerge + ", CompressedLength = " + compressedLengthBeforeMerge + ">");
			    System.out.println("[AfterMergeAndCombine][Partition " + i + "]<RecordsBeforeMerge = " 
				  		+ recordsBeforeMerge + ", "
				  		+ "RecordsAfterMerge = " + recordsAfterMerge + ", "
				  		+ "RawLength = " + rawLengthAfterMerge + ", "
				  		+ "CompressedLength = " + compressedLengthAfterMerge + ">");
			    System.out.println();
			    */
			}
		}
		else {
			long recordsBeforeMerge = eTotalRecordsAfterCombine / mapred_reduce_tasks;
			long rawLengthBeforeMerge = eTotalRawLength / mapred_reduce_tasks;
			long compressedLengthBeforeMerge = eTotalCompressedLength / mapred_reduce_tasks;
			
			long recordsAfterMerge;
			long rawLengthAfterMerge;
			long compressedLengthAfterMerge;
			
			if(eMerge_combine_record_ratio == 0 && eMerge_combine_bytes_ratio == 0) {		
				recordsAfterMerge = totalFinishedRecordsAfterMerge / mapred_reduce_tasks;
				rawLengthAfterMerge = totalFinishedRawLengthAfterMerge / mapred_reduce_tasks;
				compressedLengthAfterMerge = totalFinishedCompressedLengthAfterMerge / mapred_reduce_tasks;
			}
			
			else {
			
				recordsAfterMerge = (long) (eMerge_combine_record_ratio * recordsBeforeMerge);
				rawLengthAfterMerge = (long) (eMerge_combine_bytes_ratio * rawLengthBeforeMerge);
				compressedLengthAfterMerge = (long) (eMerge_combine_bytes_ratio * compressedLengthBeforeMerge);
			}
			
			for(int i = 0; i < mapred_reduce_tasks; i++) {
				MergeInfo newInfo = new MergeInfo(0, i, segmentsNum, rawLengthBeforeMerge, compressedLengthBeforeMerge);
				newInfo.setAfterMergeItem(0, recordsBeforeMerge, recordsAfterMerge, rawLengthAfterMerge, compressedLengthAfterMerge);		
				eMerge.addMergeInfo(newInfo);
				
				/*
				System.out.println("[BeforeMerge][Partition " + i + "]" + "<SegmentsNum = " + segmentsNum
						+ ", RawLength = " + rawLengthBeforeMerge + ", CompressedLength = " + compressedLengthBeforeMerge + ">");
			    System.out.println("[AfterMergeAndCombine][Partition " + i + "]<RecordsBeforeMerge = " 
				  		+ recordsBeforeMerge + ", "
				  		+ "RecordsAfterMerge = " + recordsAfterMerge + ", "
				  		+ "RawLength = " + rawLengthAfterMerge + ", "
				  		+ "CompressedLength = " + compressedLengthAfterMerge + ">");
			    System.out.println();
			    */
			}
		}
		
		if(merge_combine_record_ratio == 0 && merge_combine_bytes_ratio == 0) {
			merge_combine_record_ratio = totalFinishedRecordsAfterMerge / totalFinishedRecordsBeforeMerge;
			merge_combine_bytes_ratio = totalFinishedRawLengthAfterMerge / totalFinishedRawLengthBeforeMerge;
		}
		
		eMerge.setMerge_combine_record_ratio(merge_combine_record_ratio); //need more consideration
		eMerge.setMerge_combine_bytes_ratio(merge_combine_bytes_ratio); // need more consideration
		
		return eMerge;

	}

	public static Merge computeMerge(Spill eSpill, List<Spill> fSpillList, List<Merge> fMergeList, Configuration fConf, Configuration newConf) {
		boolean newCombine = newConf.getMapreduce_combine_class() != null ? true : false;
		int mapred_reduce_tasks = newConf.getMapred_reduce_tasks();
		int min_num_spills_for_combine = newConf.getMin_num_spills_for_combine();
		int fReducerNum = fConf.getMapred_reduce_tasks();
		
		int fMin_num_spills_for_combine = fConf.getMin_num_spills_for_combine();
		boolean fCombine = fConf.getMapreduce_combine_class() != null ? true : false;
		boolean fMergeCombine = false; 
					
		Merge eMerge = new Merge();
		
		List<SpillInfo> eSpillInfoList = eSpill.getSpillInfoList();
		long eTotalRecordsAfterCombine = 0;
		long eTotalRawLength = 0;
		long eTotalCompressedLength = 0;
		
		boolean eMergeCombine = eSpillInfoList.size() < min_num_spills_for_combine ? false : true;
		assert(mapred_reduce_tasks != 0);
		
		// calculate total SpillInfos' records/bytes/compressed bytes
		for(SpillInfo info : eSpillInfoList) {
			eTotalRecordsAfterCombine += info.getRecordsAfterCombine();
			eTotalRawLength += info.getRawLength();
			eTotalCompressedLength += info.getCompressedLength();
		}
		
		long totalFinishedRecordsAfterCombine = 0;
		long totalFinishedRawLength = 0;
		long totalFinishedCompressedLength = 0;
		
		// sum(sum(spillInfo)) from all the finished mappers
		int avgfSpillInfoNum = 0;
		for(Spill fSpill : fSpillList) {
			for(SpillInfo info: fSpill.getSpillInfoList()) {
				totalFinishedRecordsAfterCombine += info.getRecordsAfterCombine();
				totalFinishedRawLength += info.getRawLength();
				totalFinishedCompressedLength += info.getCompressedLength();
			}
			avgfSpillInfoNum += fSpill.getSpillInfoList().size(); // to determine fMergeCombine
			
		}
		avgfSpillInfoNum /= fSpillList.size();
				
		if(avgfSpillInfoNum < fMin_num_spills_for_combine)
			fMergeCombine = false;
		else
			fMergeCombine = true;
		
		int segmentsNum = eSpillInfoList.size();
		int size = fReducerNum;
		
		long[] finishedRecordsBeforeMerge = new long[size];
		long[] finishedRawLengthBeforeMerge = new long[size];
		long[] finishedCompressedLengthBeforeMerge = new long[size];
		
		long[] finishedRecordsAfterMerge = new long[size];
		long[] finishedRawLengthAfterMerge = new long[size];
		long[] finishedCompressedLengthAfterMerge = new long[size];
		
		for(int i = 0; i < size; i++) {
			finishedRecordsBeforeMerge[i] = 0;
			finishedRawLengthBeforeMerge[i] = 0;
			finishedCompressedLengthBeforeMerge[i] = 0;
			finishedRecordsAfterMerge[i] = 0;
			finishedRawLengthAfterMerge[i] = 0;
			finishedCompressedLengthAfterMerge[i] = 0;
		}
		
		long totalFinishedRecordsBeforeMerge = 0;
		long totalFinishedRawLengthBeforeMerge = 0;
		long totalFinishedCompressedLengthBeforeMerge = 0;
		
		long totalFinishedRecordsAfterMerge = 0;
		long totalFinishedRawLengthAfterMerge = 0;
		long totalFinishedCompressedLengthAfterMerge = 0;

		//
		for(Merge fMerge : fMergeList) {
			assert(fMerge.getMergeInfoList().size() == size);
			
			for(int i = 0; i < size; i++) {
				MergeInfo info = fMerge.getMergeInfoList().get(i);
				finishedRecordsBeforeMerge[i] += info.getRecordsBeforeMerge();
				finishedRawLengthBeforeMerge[i] += info.getRawLengthBeforeMerge();
				finishedCompressedLengthBeforeMerge[i] += info.getCompressedLengthBeforeMerge();
				finishedRecordsAfterMerge[i] += info.getRecordsAfterMerge();
				finishedRawLengthAfterMerge[i] += info.getRawLengthAfterMerge();
				finishedCompressedLengthAfterMerge[i] += info.getCompressedLengthAfterMerge();
				
				totalFinishedRecordsBeforeMerge += info.getRecordsBeforeMerge();
				totalFinishedRawLengthBeforeMerge += info.getRawLengthBeforeMerge();
				totalFinishedCompressedLengthBeforeMerge += info.getCompressedLengthBeforeMerge();
				
				totalFinishedRecordsAfterMerge += info.getRecordsAfterMerge();
				totalFinishedRawLengthAfterMerge += info.getRawLengthAfterMerge();
				totalFinishedCompressedLengthAfterMerge += info.getCompressedLengthAfterMerge();
			}
		}
		
		double merge_combine_record_ratio = (double) totalFinishedRecordsAfterMerge / totalFinishedRecordsBeforeMerge;
		double merge_combine_bytes_ratio = (double) totalFinishedRawLengthAfterMerge / totalFinishedRawLengthBeforeMerge;
		
		// no combine() in merge phase
		if(newCombine == false) {
			merge_combine_record_ratio = 1;
			merge_combine_bytes_ratio = 1;
			
			eMerge.setMerge_combine_record_ratio(merge_combine_record_ratio); 
			eMerge.setMerge_combine_bytes_ratio(merge_combine_bytes_ratio); 
			
			// reducer number is not changed
			if(mapred_reduce_tasks == size) {
				for(int i = 0; i < size; i++) {
					long recordsBeforeMerge = (long) ((double)finishedRecordsBeforeMerge[i] / totalFinishedRecordsBeforeMerge * eTotalRecordsAfterCombine);
					long rawLengthBeforeMerge = (long) ((double)finishedRawLengthBeforeMerge[i] / totalFinishedRawLengthBeforeMerge * eTotalRawLength);
					long compressedLengthBeforeMerge = (long) ((double)finishedCompressedLengthBeforeMerge[i] / totalFinishedCompressedLengthBeforeMerge * eTotalCompressedLength);
					
					long recordsAfterMerge = recordsBeforeMerge;
					long rawLengthAfterMerge = rawLengthBeforeMerge;
					long compressedLengthAfterMerge = compressedLengthBeforeMerge;
					
					MergeInfo newInfo = new MergeInfo(0, i, segmentsNum, rawLengthBeforeMerge, compressedLengthBeforeMerge);
					newInfo.setAfterMergeItem(0, recordsBeforeMerge, recordsAfterMerge, rawLengthAfterMerge, compressedLengthAfterMerge);
		
					eMerge.addMergeInfo(newInfo);

					/*
					System.out.println("[BeforeMerge][Partition " + i + "]" + "<SegmentsNum = " + segmentsNum
							+ ", RawLength = " + rawLengthBeforeMerge + ", CompressedLength = " + compressedLengthBeforeMerge + ">");
				    System.out.println("[AfterMergeAndCombine][Partition " + i + "]<RecordsBeforeMerge = " 
					  		+ recordsBeforeMerge + ", "
					  		+ "RecordsAfterMerge = " + recordsAfterMerge + ", "
					  		+ "RawLength = " + rawLengthAfterMerge + ", "
					  		+ "CompressedLength = " + compressedLengthAfterMerge + ">");
				    System.out.println();
				    */
				}
			}
			else {
				long recordsBeforeMerge = eTotalRecordsAfterCombine / mapred_reduce_tasks;
				long rawLengthBeforeMerge = eTotalRawLength / mapred_reduce_tasks;
				long compressedLengthBeforeMerge = eTotalCompressedLength / mapred_reduce_tasks;
				
				long recordsAfterMerge = recordsBeforeMerge;
				long rawLengthAfterMerge = rawLengthBeforeMerge;
				long compressedLengthAfterMerge = compressedLengthBeforeMerge;
				
				for(int i = 0; i < mapred_reduce_tasks; i++) {
					MergeInfo newInfo = new MergeInfo(0, i, segmentsNum, rawLengthBeforeMerge, compressedLengthBeforeMerge);
					newInfo.setAfterMergeItem(0, recordsBeforeMerge, recordsAfterMerge, rawLengthAfterMerge, compressedLengthAfterMerge);		
					eMerge.addMergeInfo(newInfo);
					
					/*
					System.out.println("[BeforeMerge][Partition " + i + "]" + "<SegmentsNum = " + segmentsNum
							+ ", RawLength = " + rawLengthBeforeMerge + ", CompressedLength = " + compressedLengthBeforeMerge + ">");
				    System.out.println("[AfterMergeAndCombine][Partition " + i + "]<RecordsBeforeMerge = " 
					  		+ recordsBeforeMerge + ", "
					  		+ "RecordsAfterMerge = " + recordsAfterMerge + ", "
					  		+ "RawLength = " + rawLengthAfterMerge + ", "
					  		+ "CompressedLength = " + compressedLengthAfterMerge + ">");
				    System.out.println();
				    */
				}
			}
			
			return eMerge;
		}
		
		// combine() in merge phase
		double eMerge_combine_record_ratio;
		double eMerge_combine_bytes_ratio;
		
		int fSplitMB = (int) (fConf.getSplitSize() / 1024 / 1024);
		int eSplitMB = (int) (newConf.getSplitSize() / 1024 / 1024);
		
		if(eMergeCombine == false) {
			eMerge_combine_record_ratio = 1;
			eMerge_combine_bytes_ratio = 1;
		}
		// combine in eMerge
		else if(fMergeCombine == false){
			// We assume combine I/O ratio in spill phase is less than that in merge phase.
			//need more consideration
			eMerge_combine_record_ratio = eSpill.getSpill_combine_record_ratio() * 0.8;
			eMerge_combine_bytes_ratio = eSpill.getSpill_combine_bytes_ratio() * 0.8;
		}
		// fMergeCombine == true
		else {
			eMerge_combine_record_ratio = 0;
			eMerge_combine_bytes_ratio = 0;
		}
		
		// reducer number is not changed
		if(mapred_reduce_tasks == size) {
			for(int i = 0; i < size; i++) {
				long recordsBeforeMerge = (long) ((double)finishedRecordsBeforeMerge[i] / totalFinishedRecordsAfterCombine * eTotalRecordsAfterCombine);
				long rawLengthBeforeMerge = (long) ((double)finishedRawLengthBeforeMerge[i] / totalFinishedRawLength * eTotalRawLength);
				long compressedLengthBeforeMerge = (long) ((double)finishedCompressedLengthBeforeMerge[i] /totalFinishedCompressedLength * eTotalCompressedLength);
				
				long recordsAfterMerge;
				long rawLengthAfterMerge;
				long compressedLengthAfterMerge;
				
				if(eMerge_combine_record_ratio == 0 && eMerge_combine_bytes_ratio == 0) {
					recordsAfterMerge = finishedRecordsAfterMerge[i] / fMergeList.size() * eSplitMB / fSplitMB;
					rawLengthAfterMerge = finishedRawLengthAfterMerge[i] / fMergeList.size() * eSplitMB / fSplitMB;
					compressedLengthAfterMerge = finishedCompressedLengthAfterMerge[i] / fMergeList.size() * eSplitMB / fSplitMB;
				}
				else {
					recordsAfterMerge = (long) (eMerge_combine_record_ratio * recordsBeforeMerge);
					rawLengthAfterMerge = (long) (eMerge_combine_bytes_ratio * rawLengthBeforeMerge);
					compressedLengthAfterMerge = (long) (eMerge_combine_bytes_ratio * compressedLengthBeforeMerge);
				}

				
				MergeInfo newInfo = new MergeInfo(0, i, segmentsNum, rawLengthBeforeMerge, compressedLengthBeforeMerge);
				newInfo.setAfterMergeItem(0, recordsBeforeMerge, recordsAfterMerge, rawLengthAfterMerge, compressedLengthAfterMerge);
	
				eMerge.addMergeInfo(newInfo);

				
				/*
				System.out.println("[BeforeMerge][Partition " + i + "]" + "<SegmentsNum = " + segmentsNum
						+ ", RawLength = " + rawLengthBeforeMerge + ", CompressedLength = " + compressedLengthBeforeMerge + ">");
			    System.out.println("[AfterMergeAndCombine][Partition " + i + "]<RecordsBeforeMerge = " 
				  		+ recordsBeforeMerge + ", "
				  		+ "RecordsAfterMerge = " + recordsAfterMerge + ", "
				  		+ "RawLength = " + rawLengthAfterMerge + ", "
				  		+ "CompressedLength = " + compressedLengthAfterMerge + ">");
			    System.out.println();
			    */
			}
		}
		else {
			long recordsBeforeMerge = eTotalRecordsAfterCombine / mapred_reduce_tasks;
			long rawLengthBeforeMerge = eTotalRawLength / mapred_reduce_tasks;
			long compressedLengthBeforeMerge = eTotalCompressedLength / mapred_reduce_tasks;
			
			long recordsAfterMerge;
			long rawLengthAfterMerge;
			long compressedLengthAfterMerge;
			
			if(eMerge_combine_record_ratio == 0 && eMerge_combine_bytes_ratio == 0) {
				recordsAfterMerge = totalFinishedRecordsAfterMerge / fMergeList.size() * eSplitMB / fSplitMB / mapred_reduce_tasks;
				rawLengthAfterMerge = totalFinishedRawLengthAfterMerge / fMergeList.size() * eSplitMB / fSplitMB / mapred_reduce_tasks;
				compressedLengthAfterMerge = totalFinishedCompressedLengthAfterMerge / fMergeList.size() * eSplitMB / fSplitMB / mapred_reduce_tasks;
			}
			else {
				recordsAfterMerge = (long) (eMerge_combine_record_ratio * recordsBeforeMerge);
				rawLengthAfterMerge = (long) (eMerge_combine_bytes_ratio * rawLengthBeforeMerge);
				compressedLengthAfterMerge = (long) (eMerge_combine_bytes_ratio * compressedLengthBeforeMerge);
			}
			
			for(int i = 0; i < mapred_reduce_tasks; i++) {
				MergeInfo newInfo = new MergeInfo(0, i, segmentsNum, rawLengthBeforeMerge, compressedLengthBeforeMerge);
				newInfo.setAfterMergeItem(0, recordsBeforeMerge, recordsAfterMerge, rawLengthAfterMerge, compressedLengthAfterMerge);		
				eMerge.addMergeInfo(newInfo);
				
				/*
				System.out.println("[BeforeMerge][Partition " + i + "]" + "<SegmentsNum = " + segmentsNum
						+ ", RawLength = " + rawLengthBeforeMerge + ", CompressedLength = " + compressedLengthBeforeMerge + ">");
			    System.out.println("[AfterMergeAndCombine][Partition " + i + "]<RecordsBeforeMerge = " 
				  		+ recordsBeforeMerge + ", "
				  		+ "RecordsAfterMerge = " + recordsAfterMerge + ", "
				  		+ "RawLength = " + rawLengthAfterMerge + ", "
				  		+ "CompressedLength = " + compressedLengthAfterMerge + ">");
			    System.out.println();
			    */
			}
		}
		
		if(eMerge_combine_record_ratio == 0 && eMerge_combine_bytes_ratio == 0) {
			eMerge_combine_record_ratio = (double) totalFinishedRecordsAfterMerge / totalFinishedRecordsBeforeMerge;
			eMerge_combine_bytes_ratio = (double) totalFinishedRawLengthAfterMerge / totalFinishedRawLengthBeforeMerge;
		}
		eMerge.setMerge_combine_record_ratio(merge_combine_record_ratio); //need more consideration
		eMerge.setMerge_combine_bytes_ratio(merge_combine_bytes_ratio); // need more consideration
		return eMerge;
	}
}
