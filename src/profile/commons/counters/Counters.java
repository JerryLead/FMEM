package profile.commons.counters;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class Counters implements Serializable {

	private List<CounterItem> countersList;
	
	public Counters() {
		countersList = new ArrayList<CounterItem>();	
	}
	
	//1346728352,FILE_BYTES_READ:1560,HDFS_BYTES_READ:171180032,
	//Combine output records:0,Map input records:1711800,
	//Spilled Records:0,Map output bytes:171180000,Map input bytes:171180000,
	//Combine input records:0,Map output records:1711800
	public void addCounterItem(String[] params) {
		Map<String, Long> counterMap = new HashMap<String, Long>();
		long timeMS = Long.parseLong(params[0]) * 1000; //1346728352000
		
		for(int i = 1; i < params.length; i++) {
			String kv[] = params[i].split(":");
			assert(kv.length == 2);
			counterMap.put(kv[0], Long.parseLong(kv[1]));
		}
		CounterItem item = new CounterItem(timeMS, counterMap);
		countersList.add(item);
	}
	
	//Given a timestamp (ms), return the counters at that time or the average counters between the interval counters
	public Map<String, Long> getCounterMap(long timeStampMS) {
		if(countersList == null)
			return null;
		
		int beforeIndex = 0;
		int afterIndex = countersList.size() - 1;
		
		for(int i = 0; i < countersList.size(); i++) {
			CounterItem item = countersList.get(i);
			if(item.getTimeStampMS() == timeStampMS) {
				return item.getCounterMap();
			}
			else if(timeStampMS > item.getTimeStampMS()) {
				beforeIndex = i;
			}
			else {
				afterIndex = i;
			}
		}
		
		//compute the average counters
		Map<String, Long> averageMap = new HashMap<String, Long>();
		for(Entry<String, Long> entry : countersList.get(afterIndex).getCounterMap().entrySet()) {
			Long average = countersList.get(beforeIndex).getCounterMap().get(entry.getKey());
			if(average != null) {
				average = (average + entry.getValue()) / 2;
				averageMap.put(entry.getKey(), average);
			}
			else
				averageMap.put(entry.getKey(), entry.getValue());
		}
		
		return averageMap;
		
	}
}

class CounterItem implements Serializable {
	long timeStampMS;
	Map<String, Long> counterMap;
	
	public CounterItem(long timeStampMS, Map<String, Long> counterMap) {
		this.timeStampMS = timeStampMS;
		this.counterMap = counterMap;
	}
	
	public long getTimeStampMS() {
		return timeStampMS;
	}
	
	public Map<String, Long> getCounterMap() {
		return counterMap;
	}
	
}



