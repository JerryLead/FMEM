package profile.task.reducer;

import java.io.Serializable;

public class Reduce implements Serializable {
	private long inputkeyNum; // equals "Reduce input groups" normally
	private long inputKeyValuePairsNum; // equals "Reduce input records"
	private long inputBytes; // equals rawLength after merge in Sort
	private long computedReduceInputRecords; //computed from MixSortMerge and FinalSortMerge

	private long outputKeyValuePairsNum; // equals "Reduce output records"
	private long outputBytes; // depends on "HDFS_BYTES_WRITTEN" and dfs.replication

	private long reduceStartTimeMS;
	private long reduceFinishTimeMS;

	public long getInputkeyNum() {
		return inputkeyNum;
	}

	public void setInputkeyNum(long inputkeyNum) {
		this.inputkeyNum = inputkeyNum;
	}

	public long getInputKeyValuePairsNum() {
		return inputKeyValuePairsNum;
	}

	public void setInputKeyValuePairsNum(long inputKeyValuePairsNum) {
		this.inputKeyValuePairsNum = inputKeyValuePairsNum;
	}

	public long getInputBytes() {
		return inputBytes;
	}

	public void setInputBytes(long inputBytes) {
		this.inputBytes = inputBytes;
	}

	public long getOutputKeyValuePairsNum() {
		return outputKeyValuePairsNum;
	}

	public void setOutputKeyValuePairsNum(long outputKeyValuePairsNum) {
		this.outputKeyValuePairsNum = outputKeyValuePairsNum;
	}

	public long getOutputBytes() {
		return outputBytes;
	}

	public void setOutputBytes(long outputBytes) {
		this.outputBytes = outputBytes;
	}

	public void setComputedInputRecords(long reduceInputRecords) {
		this.computedReduceInputRecords = reduceInputRecords;
	}

}
