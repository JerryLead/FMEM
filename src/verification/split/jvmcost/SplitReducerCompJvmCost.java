package verification.split.jvmcost;

public class SplitReducerCompJvmCost {
	//just for JvmCostComparator, a little ugly
	private int split;
	private int xmx;
	private int xms;
	private int ismb;
	private int RN;
	
	private float OGC;
	private float OGCMX;	

	private float NGC;
	private float NGCMX;
	private float EdenC;
	private float S0C;
	private float S1C;
	
	//for ReducerRealJvmCost
	private float rnOU;
	private float rmOU;
	private float rxOU;
	private float rnNGU;
	private float rmNGU;
	private float rxNGU;
	private float rnEdenU;
	private float rxEdenU;
	
	private float rnS0U;
	private float rxS0U;
	
	private float rnS1U;
	private float rxS1U;

	private float rnHeapU;
	private float rxHeapU;
	
	private int rnRSS;
	private int rxRSS;
	
	private int rnRecords;
	private int rxRecords;
	private int mYGC;
	private int mFGC;
	private int mTime;
	
	//for ReducerPredictedJvmCost
	private float enOU;
	private float exOU;
	
	private float enNGU;
	private float exNGU;
	
	private float inMemSegBuffer;
	private float mergeBuffer;
	private float eShuffleBytesMB;
	private float nRedIn;
	private float xRedIn;
	
	private float enHeapU;
	private float exHeapU;
	private float enSSTObj;
	private float exSSTObj;
	private float enRTObj;
	private float exRTObj;
	private float enFix;
	private float exFix;
	
	private String reason;	
	private String rJobId;
	
	public SplitReducerCompJvmCost(SplitReducerRealJvmCost rJvmCost, SplitReducerPredictedJvmCost eJvmCost) {
		split = rJvmCost.getSplit();
		xmx = rJvmCost.getXmx();
		xms = rJvmCost.getXms();
		ismb = rJvmCost.getIsmb();
		RN = rJvmCost.getRN();

		OGC = rJvmCost.getOGC();
		OGCMX = rJvmCost.getOGCMX();
		NGC = rJvmCost.getNGC();
		NGCMX = rJvmCost.getNGCMX();
		EdenC = rJvmCost.getEdenC();
		S0C = rJvmCost.getS0C();
		S1C = rJvmCost.getS1C();
		
		//for MapperRealJvmCost
		rnOU = rJvmCost.getnOU();
		rmOU = rJvmCost.getmOU();
		rxOU = rJvmCost.getxOU();
		
		rnNGU = rJvmCost.getnNGU();
		rmNGU = rJvmCost.getmNGU();
		rxNGU = rJvmCost.getxNGU();
		
		rnEdenU = rJvmCost.getnEdenU();
		rxEdenU = rJvmCost.getxEdenU();
		
		rnS0U = rJvmCost.getnS0U();
		rxS0U = rJvmCost.getxS0U();
		rnS1U = rJvmCost.getnS1U();
		rxS1U = rJvmCost.getxS1U();
		
		rnHeapU = rJvmCost.getnHeapU();
		rxHeapU = rJvmCost.getxHeapU();
		
		rnRecords = rJvmCost.getnRecords();
		rxRecords = rJvmCost.getxRecords();
		mYGC = rJvmCost.getmYGC();
		mFGC = rJvmCost.getmFGC();
		mTime = rJvmCost.getmTime();
		rJobId = rJvmCost.getJobId();
		
		//for MapperPredictedJvmCost
		enOU = eJvmCost.getnOU();
		exOU = eJvmCost.getxOU();
		
		enNGU = eJvmCost.getnNGU();
		exNGU = eJvmCost.getxNGU();
		
		inMemSegBuffer = eJvmCost.getInMemSegBuffer();
		mergeBuffer = eJvmCost.getMergeBuffer();
		eShuffleBytesMB = eJvmCost.geteShuffleBytesMB();
		nRedIn = eJvmCost.getnRedIn();
		xRedIn = eJvmCost.getxRedIn();
		
		enHeapU = eJvmCost.getnHeapU();
		exHeapU = eJvmCost.getxHeapU();
		
		rnRSS = rJvmCost.getnRSS();
		rxRSS = rJvmCost.getxRSS();
		
		enSSTObj = eJvmCost.getnSSTObj();
		exSSTObj = eJvmCost.getxSSTObj();
		enRTObj = eJvmCost.getnRTObj();
		exRTObj = eJvmCost.getxRTObj();
		
		enFix = eJvmCost.getnFix();
		exFix = eJvmCost.getxFix();
		
		reason = eJvmCost.getReason();		
	}
	
	@Override
	public String toString() {
		String f1 = "%1$-3.0f";
		
		return split + "\t" + xmx + "\t" + xms + "\t" + ismb + "\t" + RN + "\t"
				+ String.format(f1, rmOU) + "\t" + String.format(f1, rxOU) + "\t" + String.format(f1, exOU) + "\t"
				+ String.format(f1, exOU - rmOU) + "\t" + String.format(f1, exOU - rxOU) + "\t"
				
				+ String.format(f1, rmNGU) + "\t" + String.format(f1, rxNGU) + "\t" + String.format(f1, exNGU) + "\t"
				+ String.format(f1, exNGU - rmNGU) + "\t" + String.format(f1, exNGU - rxNGU) + "\t"
				
				+ String.format(f1, rnOU) + "\t" + String.format(f1, enOU) + "\t" + String.format(f1, enOU - rnOU) + "\t"
				+ String.format(f1, rnNGU) + "\t" + String.format(f1, enNGU) + "\t" + String.format(f1, enNGU - rnNGU) + "\t"
				+ String.format(f1, rnHeapU) + "\t" + String.format(f1, enHeapU) + "\t" + String.format(f1, enHeapU - rnHeapU) + "\t"
				+ String.format(f1, rxHeapU) + "\t" + String.format(f1, exHeapU) + "\t" + String.format(f1, exHeapU - rxHeapU) + "\t"
				+ rnRSS + "\t" + rxRSS + "\t" + String.format(f1, exHeapU - rxRSS) + "\t"
				
				+ inMemSegBuffer + "\t" + mergeBuffer + "\t" + eShuffleBytesMB + "\t"
				+ String.format(f1, nRedIn) + "\t" + String.format(f1, xRedIn) + "\t"
		
				+ rnEdenU + "\t" + rxEdenU + "\t"
				+ rnS0U + "\t" + rxS0U + "\t" + rnS1U + "\t" + rxS1U + "\t" 
				+ enSSTObj + "\t" + exSSTObj + "\t" + enRTObj + "\t" + exRTObj + "\t" + enFix + "\t" + exFix + "\t"
				+ OGC + "\t" + OGCMX + "\t" + NGC + "\t" + NGCMX + "\t" + EdenC + "\t" + S0C + "\t" 
				+ rnRecords + "\t" + rxRecords + "\t" + mYGC + "\t" + mFGC + "\t" + mTime + "\t" + reason;
	}

	public String toDiffString(String jobId) {
		String f1 = "%1$-3.0f";
		String f2 = "%1$-3.1f";
		
		float mOUdf = exOU - rmOU;
		float xOUdf = exOU - rxOU;
		float mNGUdf = exNGU - rmNGU;
		float xNGUdf = exNGU - rxNGU;
		float xHeapUdf = exHeapU - rxHeapU;
		float RSSdf = exHeapU - rxRSS;
		
		float mOUrt = rmOU == 0 ? 100 : Math.abs(mOUdf) * 100 / rmOU;
		float xOUrt = rxOU == 0 ? 100 : Math.abs(xOUdf) * 100 / rxOU;
		float mNGUrt = rmNGU == 0 ? 100 : Math.abs(mNGUdf) * 100 / rmNGU;
		float xNGUrt = rxNGU == 0 ? 100 : Math.abs(xNGUdf) * 100 / rxNGU;
		float xHeapUrt = rxHeapU == 0 ? 100 : Math.abs(xHeapUdf) * 100 / rxHeapU;
		float RSSrt = rxRSS == 0 ? 100 : Math.abs(RSSdf) * 100 / rxRSS;
		
		return split + "\t" + xmx + "\t" + xms + "\t" + ismb + "\t" + RN + "\t"
			+ String.format(f1, mOUdf) + "\t" + String.format(f1, xOUdf) + "\t" 
		    + String.format(f1, mNGUdf) + "\t" + String.format(f1, xNGUdf) + "\t"
			+ String.format(f1, xHeapUdf) + "\t" + String.format(f1, RSSdf) + "\t"
			+ String.format(f2, mOUrt) + "\t" + String.format(f2, xOUrt) + "\t" 
			+ String.format(f2, mNGUrt) + "\t" + String.format(f2, xNGUrt) + "\t" 
			+ String.format(f2, xHeapUrt) + "\t" + String.format(f2, RSSrt) + "\t"
			+ jobId + "\t" + rJobId + "\t"
			+ String.format(f1, rmOU) + "\t" + String.format(f1, rxOU) + "\t" + String.format(f1, exOU) + "\t"
			+ String.format(f1, rmNGU) + "\t" + String.format(f1, rxNGU) + "\t" + String.format(f1, exNGU) + "\t"
			+ String.format(f1, rxHeapU) + "\t" + String.format("%1$-3d", rxRSS) + "\t" + String.format(f1, exHeapU);
	}

}
