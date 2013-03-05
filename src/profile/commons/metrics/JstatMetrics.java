package profile.commons.metrics;

import java.util.ArrayList;
import java.util.List;

public class JstatMetrics {
	private long startTimeSec;
	
	//used for computing timestamps
	private long elpasedTimeSec = -1;
	
	//used for computing gc infos
	private int yGC = 0;
	private int fGC = 0;
	private float yGCT = 0;
	private float fGCT = 0;
	private float gCT = 0;
	
	public JstatMetrics(String dateStrSec) {
		this.startTimeSec = Long.parseLong(dateStrSec);
	}

	/*
	 * Timestamp        S0C    S1C    S0U    S1U      EC       EU        OC         OU       PC     PU    YGC     YGCT    FGC    FGCT     GCT   
	 * 325829.7 32512.0 31872.0 23155.4  0.0   108608.0 19119.4   315584.0   149325.8  193664.0 184174.3    542   11.467  13     13.135   24.603
	 * 325831.7 32512.0 31872.0 23155.4  0.0   108608.0 47124.5   315584.0   149325.8  193664.0 184174.3    542   11.467  13     13.135   24.603
	 * 325833.7 32512.0 31872.0 23155.4  0.0   108608.0 47624.0   315584.0   149325.8  193664.0 184174.3    542   11.467  13     13.135   24.603
	 * 325835.7 32512.0 31872.0 23155.4  0.0   108608.0 47624.0   315584.0   149325.8  193664.0 184174.3    542   11.467  13     13.135   24.603
	 * 325837.7 32512.0 31872.0 23155.4  0.0   108608.0 47624.0   315584.0   149325.8  193664.0 184174.3    542   11.467  13     13.135   24.603
	 */
	//return Timestamp OGCMN OGCMX OC OU	PGCMN PGCMX PC PU	YGC YGCT FGC FGCT GCT	NGCMN NGCMX NGC S0C S1C S0U S1U EC EU
	public String[] generateArrayList(String[] params) {
		List<String> list = new ArrayList<String>();		
		// Timestamp
		long timestampSec = (long)Float.parseFloat(params[0]);
		if(elpasedTimeSec == -1)
			elpasedTimeSec = timestampSec;
		timestampSec = startTimeSec + timestampSec - elpasedTimeSec;
		
		list.add(String.valueOf(timestampSec));
		
		// S0C S1C S0U S1U EC EU NGC NGU
		list.add(kBtoMB(params[1]));
		list.add(kBtoMB(params[2]));
		list.add(kBtoMB(params[3]));
		list.add(kBtoMB(params[4]));
		list.add(kBtoMB(params[5]));
		list.add(kBtoMB(params[6]));
		
		// OC OU
		list.add(kBtoMB(params[7]));
		list.add(kBtoMB(params[8]));
		
		// PC PU
		list.add(kBtoMB(params[9]));
		list.add(kBtoMB(params[10]));
		
		/*
		// YGC YGCT FGC FGCT GCT
		list.addAll(incToSingle(Integer.parseInt(params[11]), 
				Float.parseFloat(params[12]), 
				Integer.parseInt(params[13]), 
				Float.parseFloat(params[14]), 
				Float.parseFloat(params[15])));
		*/
		// YGC YGCT FGC FGCT GCT
		list.add(params[11]);
		list.add(params[12]);
		list.add(params[13]);
		list.add(params[14]);
		list.add(params[15]);
		
		return list.toArray(new String[list.size()]);

	}
	
	private List<String> incToSingle(int nYGC, float nYGCT, int nFGC, float nFGCT, float nGCT) {
		List<String> gcList = new ArrayList<String>();
		gcList.add(String.valueOf(nYGC - yGC));
		gcList.add(String.valueOf(nYGCT - yGCT));
		gcList.add(String.valueOf(nFGC - fGC));
		gcList.add(String.valueOf(nFGCT - fGCT));
		gcList.add(String.valueOf(nGCT - gCT));
		
		this.yGC = nYGC;
		this.yGCT = nYGCT;
		this.fGC = nFGC;
		this.fGCT = nFGCT;
		this.gCT = nGCT;
		return gcList;
	}

	public String kBtoMB(String floatKB) {
		Float f = Float.parseFloat(floatKB);
		return String.valueOf(f / 1024);
	}
}