package profile.task.mapper;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class Spill implements Serializable {
	private boolean hasCombine;
	private int spillCounts; //how many times spilling behavior occurs (includes final flush)
	private List<SpillInfo> spillInfoList = new ArrayList<SpillInfo>();
	private long spillPhaseTimeCostSec; //from first spill to last flush
	
	private double spill_combine_record_ratio; // only for estimated mappers
	private double spill_combine_bytes_ratio; // only for estimated mappers
	
	//add spill infos
	public void addSpillItem(boolean hasCombine, long startSpillTimeMS,
			long stopSpillTimeMS, String reason, 
			long recordsBeforeCombine, long bytesBeforeSpill,
			long recordAfterCombine, long rawLength, long compressedLength) {
		this.hasCombine = hasCombine;
		spillInfoList.add(new SpillInfo(hasCombine, startSpillTimeMS,
				stopSpillTimeMS, reason, 
				recordsBeforeCombine, bytesBeforeSpill,
				recordAfterCombine, rawLength, compressedLength));
		
	}
	
	public List<SpillInfo> getSpillInfoList() {
		return spillInfoList;
	}
	
	public void addSpillInfo(SpillInfo spillInfo) {
		spillInfoList.add(spillInfo);
	}

	public double getSpill_combine_record_ratio() {
		return spill_combine_record_ratio;
	}

	public void setSpill_combine_record_ratio(double spill_combine_record_ratio) {
		this.spill_combine_record_ratio = spill_combine_record_ratio;
	}

	public double getSpill_combine_bytes_ratio() {
		return spill_combine_bytes_ratio;
	}

	public void setSpill_combine_bytes_ratio(double spill_combine_bytes_ratio) {
		this.spill_combine_bytes_ratio = spill_combine_bytes_ratio;
	}
}
