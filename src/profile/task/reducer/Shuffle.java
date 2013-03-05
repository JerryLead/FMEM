package profile.task.reducer;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class Shuffle implements Serializable {

	private List<ShuffleInfo> shuffleInfoList = new ArrayList<ShuffleInfo>();
	private long Reduce_shuffle_bytes = 0;
	private long Reduce_shuffle_raw_bytes = 0;

	//add shuffle infos (shuffling * from map task)
	//for log parser
	public void addShuffleItem(long shuffleFinishTimeMS, String sourceTaskId,
			String storeLoc, long decompressedLen, long compressedLen) {
		ShuffleInfo shuffleInfo = new ShuffleInfo(shuffleFinishTimeMS, sourceTaskId,
			storeLoc, decompressedLen, compressedLen);
		shuffleInfoList.add(shuffleInfo);
		Reduce_shuffle_bytes += shuffleInfo.getCompressedLen();
		Reduce_shuffle_raw_bytes += shuffleInfo.getRawLength();
	}
	
	public void setShuffleInfoList(List<ShuffleInfo> shuffleInfoList) {
		this.shuffleInfoList = shuffleInfoList;
	}
	
	public List<ShuffleInfo> getShuffleInfoList() {
		return shuffleInfoList;
	}

	public long getReduce_shuffle_bytes() {
		return Reduce_shuffle_bytes;
	}

	public long getReduce_shuffle_raw_bytes() {
		return Reduce_shuffle_raw_bytes;
	}

	public void setReduce_shuffle_bytes(long reduce_shuffle_bytes) {
		Reduce_shuffle_bytes = reduce_shuffle_bytes;
	}

	public void setReduce_shuffle_raw_bytes(long reduce_shuffle_raw_bytes) {
		Reduce_shuffle_raw_bytes = reduce_shuffle_raw_bytes;
	}

	
}