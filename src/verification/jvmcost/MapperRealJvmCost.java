package verification.jvmcost;

public class MapperRealJvmCost {

	private int xmx;
	private int xms;
	private int ismb;
	private int RN;
	private int Bytes;
	
	private float nOU;
	private float mOU;
	private float xOU;
	private float OGC;
	private float OGCMX;
	
	private float nNGU;
	private float mNGU;
	private float xNGU;
	private float NGC;
	private float NGCMX;
	
	private float nEdenU;
	private float xEdenU;
	private float EdenC;
	private float xS0U;
	private float xS1U;
	private float S0C;
	
	private float nHeapU;
	private float xHeapU;
	
	private int nRSS;
	private int xRSS;
	
	private int mYGC;
	private int mFGC;
	private float mYGCT;
	private float mFGCT;
	private int mTime;
	
	private String jobId;

	//xmx	xms	ismb	nOU	xOU	OGC	OGCMX	nNGU	xNGU	NGC	NGCMX	nEdenU	xEdenU	
	// EdenC	xS0U	xS1U	S0C	nHeapU	xHeapU	RN	Bytes	mYGC	mFGC	mTime
	public MapperRealJvmCost(String[] title, String[] value) {
		for(int i = 0; i < title.length; i++) {
			if(title[i].equals("xmx"))
				xmx = Integer.parseInt(value[i]);
			else if(title[i].equals("xms"))
				xms = Integer.parseInt(value[i]);
			else if(title[i].equals("ismb"))
				ismb = Integer.parseInt(value[i]);
			else if(title[i].equals("RN") || title[i].equals("reducers"))
				RN = Integer.parseInt(value[i]);
			else if(title[i].equals("Bytes"))
				Bytes = Integer.parseInt(value[i]);
			
			else if(title[i].equals("nOU"))	
				nOU = Float.parseFloat(value[i]);
			else if(title[i].equals("mOU"))	
				mOU = Float.parseFloat(value[i]);
			else if(title[i].equals("xOU"))
				xOU = Float.parseFloat(value[i]);
			else if(title[i].equals("OGC"))
				OGC = Float.parseFloat(value[i]);
			else if(title[i].equals("OGCMX"))
				OGCMX = Float.parseFloat(value[i]);
			
			else if(title[i].equals("nNGU"))
				nNGU = Float.parseFloat(value[i]);
			else if(title[i].equals("mNGU"))
				mNGU = Float.parseFloat(value[i]);
			else if(title[i].equals("xNGU"))
				xNGU = Float.parseFloat(value[i]);
			else if(title[i].equals("NGC"))
				NGC = Float.parseFloat(value[i]);
			else if(title[i].equals("NGCMX"))
				NGCMX = Float.parseFloat(value[i]);
			
			else if(title[i].equals("nEdenU"))
				nEdenU = Float.parseFloat(value[i]);
			else if(title[i].equals("xEdenU"))
				xEdenU = Float.parseFloat(value[i]);
			else if(title[i].equals("EdenC"))
				EdenC = Float.parseFloat(value[i]);
			else if(title[i].equals("xS0U"))
				xS0U = Float.parseFloat(value[i]);
			else if(title[i].equals("xS1U"))
				xS1U = Float.parseFloat(value[i]);
			else if(title[i].equals("S0C"))
				S0C = Float.parseFloat(value[i]);
			
			else if(title[i].equals("nHeapU")) 
				nHeapU = Float.parseFloat(value[i]);
			else if(title[i].equals("xHeapU"))
				xHeapU = Float.parseFloat(value[i]);
			
			else if(title[i].equals("nRSS"))
				nRSS = Integer.parseInt(value[i]);
			else if(title[i].equals("xRSS"))
				xRSS = Integer.parseInt(value[i]);
			
			else if(title[i].equals("mYGC"))
				mYGC = Integer.parseInt(value[i]);
			else if(title[i].equals("mFGC"))
				mFGC = Integer.parseInt(value[i]);
			else if(title[i].equals("mYGCT"))
				mYGCT =Float.parseFloat(value[i]);
			else if(title[i].equals("mFGCT"))
				mFGCT = Float.parseFloat(value[i]);
			else if(title[i].equals("mTime"))
				mTime = Integer.parseInt(value[i]);
			else if(title[i].equalsIgnoreCase("jobId"))
				jobId = value[i];
		}
	}

	public int getXmx() {
		return xmx;
	}

	public int getXms() {
		return xms;
	}

	public int getIsmb() {
		return ismb;
	}

	public int getRN() {
		return RN;
	}

	public float getnOU() {
		return nOU;
	}

	public float getxOU() {
		return xOU;
	}

	public float getOGC() {
		return OGC;
	}

	public float getOGCMX() {
		return OGCMX;
	}

	public float getnNGU() {
		return nNGU;
	}

	public float getxNGU() {
		return xNGU;
	}

	public float getNGC() {
		return NGC;
	}

	public float getNGCMX() {
		return NGCMX;
	}

	public float getnEdenU() {
		return nEdenU;
	}

	public float getxEdenU() {
		return xEdenU;
	}

	public float getEdenC() {
		return EdenC;
	}

	public float getxS0U() {
		return xS0U;
	}

	public float getxS1U() {
		return xS1U;
	}

	public float getS0C() {
		return S0C;
	}

	public float getnHeapU() {
		return nHeapU;
	}

	public float getxHeapU() {
		return xHeapU;
	}

	public int getBytes() {
		return Bytes;
	}

	public int getmYGC() {
		return mYGC;
	}

	public int getmFGC() {
		return mFGC;
	}

	public int getmTime() {
		return mTime;
	}

	public float getmOU() {
		return mOU;
	}

	public float getmNGU() {
		return mNGU;
	}

	public int getnRSS() {
		return nRSS;
	}

	public int getxRSS() {
		return xRSS;
	}

	public float getmYGCT() {
		return mYGCT;
	}

	public float getmFGCT() {
		return mFGCT;
	}

	public String getJobId() {
		return jobId;
	}

	
}
