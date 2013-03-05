package data.model.reducer;

import java.util.ArrayList;
import java.util.List;

import profile.task.mapper.Mapper;
import profile.task.mapper.MergeInfo;
import profile.task.reducer.Shuffle;
import profile.task.reducer.ShuffleInfo;


public class ShuffleModel {

	public static Shuffle computeShuffle(List<Mapper> newMapperList, int reducerIndex) {
		List<ShuffleInfo> shuffleInfoList = new ArrayList<ShuffleInfo>();
		long Reduce_shuffle_bytes = 0;
		long Reduce_shuffle_raw_bytes = 0;
		
		for(int i = 0; i < newMapperList.size(); i++) {
			Mapper newMapper = newMapperList.get(i);
			//Todo: check whether newMapper has reducerNum mergeInfos
			MergeInfo mergeInfo = newMapper.getMerge().getMergeInfoList().get(reducerIndex);
			ShuffleInfo shuffleInfo = new ShuffleInfo(0, "attemp_" + i, "RAM", mergeInfo.getRawLengthAfterMerge(), mergeInfo.getCompressedLengthAfterMerge());
			shuffleInfo.setRecords(mergeInfo.getRecordsAfterMerge());
			shuffleInfoList.add(shuffleInfo);
			//System.out.println(mergeInfo.getCompressedLengthAfterMerge()/1024/1024);
			Reduce_shuffle_bytes += mergeInfo.getCompressedLengthAfterMerge();
			Reduce_shuffle_raw_bytes += mergeInfo.getRawLengthAfterMerge();
			//System.out.println("[Shuffle] [" + i + "] <loc = RAM, records = " + shuffleInfo.getRecords() + ", rawLength = " 
			//		+ shuffleInfo.getRawLength() + ", compressedLen = " + shuffleInfo.getCompressedLen() + ">");
			
		}
		
		//System.out.println("---------------------------------------------------------------------------------------------------------");
		
		Shuffle shuffle = new Shuffle();
		shuffle.setShuffleInfoList(shuffleInfoList);
		shuffle.setReduce_shuffle_bytes(Reduce_shuffle_bytes);
		shuffle.setReduce_shuffle_raw_bytes(Reduce_shuffle_raw_bytes);
		return shuffle;
	}

}
