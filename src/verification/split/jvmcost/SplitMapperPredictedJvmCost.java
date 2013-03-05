package verification.split.jvmcost;

public class SplitMapperPredictedJvmCost {

	//just for JvmCostComparator, a little ugly
	private int split;
	private int xmx;
	private int xms;
	private int ismb;
	private int RN;
	
	private float nOU;
	private float xOU;
	private float OGC;
	private float OGCMX;
	
	private float nNGU;
	private float xNGU;
	private float NGC;
	private float NGCMX;
	private float EdenC;
	private float S0C;
	
	private float nHeapU;
	private float xHeapU;
	private float nTempObj;
	private float xTempObj;
	private float nFix;
	private float xFix;
	
	private String reason;
	
	//for JvmCostComparator
	//xmx	xms	ismb	RN	nOU	xOU	OGC	OGCMX	nNGU	xNGU	NGC	NGCMX	EdenC	
	//S0C	nHeapU	xHeapU	nTempObj	xTempObj	nFix	xFix	reason
	public SplitMapperPredictedJvmCost(String[] title, String[] value) {
		for(int i = 0; i < title.length; i++) {
			if(title[i].equals("xmx"))
				xmx = Integer.parseInt(value[i]);
			else if(title[i].equals("xms"))
				xms = Integer.parseInt(value[i]);
			else if(title[i].equals("ismb"))
				ismb = Integer.parseInt(value[i]);
			else if(title[i].equals("RN"))
				RN = Integer.parseInt(value[i]);
			
			else if(title[i].equals("nOU"))	
				nOU = Float.parseFloat(value[i]);
			else if(title[i].equals("xOU"))
				xOU = Float.parseFloat(value[i]);
			else if(title[i].equals("OGC"))
				OGC = Float.parseFloat(value[i]);
			else if(title[i].equals("OGCMX"))
				OGCMX = Float.parseFloat(value[i]);
			
			else if(title[i].equals("nNGU"))
				nNGU = Float.parseFloat(value[i]);
			else if(title[i].equals("xNGU"))
				xNGU = Float.parseFloat(value[i]);
			else if(title[i].equals("NGC"))
				NGC = Float.parseFloat(value[i]);
			else if(title[i].equals("NGCMX"))
				NGCMX = Float.parseFloat(value[i]);
			
			else if(title[i].equals("EdenC"))
				EdenC = Float.parseFloat(value[i]);
			else if(title[i].equals("S0C"))
				S0C = Float.parseFloat(value[i]);
			
			else if(title[i].equals("nHeapU")) 
				nHeapU = Float.parseFloat(value[i]);
			else if(title[i].equals("xHeapU"))
				xHeapU = Float.parseFloat(value[i]);
			
			else if(title[i].equals("nTempObj"))
				nTempObj = Float.parseFloat(value[i]);
			else if(title[i].equals("xTempObj"))
				xTempObj = Float.parseFloat(value[i]);
			else if(title[i].equals("nFix"))
				nFix = Float.parseFloat(value[i]);
			else if(title[i].equals("xFix"))
				xFix = Float.parseFloat(value[i]);
			else if(title[i].equals("split"))
				split = Integer.parseInt(value[i]);
			else if(title[i].equals("reason"))
				reason = value[i];
		}
	}

	public int getXmx() {
		return xmx;
	}

	public void setXmx(int xmx) {
		this.xmx = xmx;
	}

	public int getXms() {
		return xms;
	}

	public void setXms(int xms) {
		this.xms = xms;
	}

	public int getIsmb() {
		return ismb;
	}

	public void setIsmb(int ismb) {
		this.ismb = ismb;
	}

	public int getRN() {
		return RN;
	}

	public void setRN(int rN) {
		RN = rN;
	}

	public float getnOU() {
		return nOU;
	}

	public void setnOU(float nOU) {
		this.nOU = nOU;
	}

	public float getxOU() {
		return xOU;
	}

	public void setxOU(float xOU) {
		this.xOU = xOU;
	}

	public float getOGC() {
		return OGC;
	}

	public void setOGC(float oGC) {
		OGC = oGC;
	}

	public float getOGCMX() {
		return OGCMX;
	}

	public void setOGCMX(float oGCMX) {
		OGCMX = oGCMX;
	}

	public float getnNGU() {
		return nNGU;
	}

	public void setnNGU(float nNGU) {
		this.nNGU = nNGU;
	}

	public float getxNGU() {
		return xNGU;
	}

	public void setxNGU(float xNGU) {
		this.xNGU = xNGU;
	}

	public float getNGC() {
		return NGC;
	}

	public void setNGC(float nGC) {
		NGC = nGC;
	}

	public float getNGCMX() {
		return NGCMX;
	}

	public void setNGCMX(float nGCMX) {
		NGCMX = nGCMX;
	}

	public float getEdenC() {
		return EdenC;
	}

	public void setEdenC(float edenC) {
		EdenC = edenC;
	}

	public float getS0C() {
		return S0C;
	}

	public void setS0C(float s0c) {
		S0C = s0c;
	}

	public float getnHeapU() {
		return nHeapU;
	}

	public void setnHeapU(float nHeapU) {
		this.nHeapU = nHeapU;
	}

	public float getxHeapU() {
		return xHeapU;
	}

	public void setxHeapU(float xHeapU) {
		this.xHeapU = xHeapU;
	}

	public float getnTempObj() {
		return nTempObj;
	}

	public void setnTempObj(float nTempObj) {
		this.nTempObj = nTempObj;
	}

	public float getxTempObj() {
		return xTempObj;
	}

	public void setxTempObj(float xTempObj) {
		this.xTempObj = xTempObj;
	}

	public float getnFix() {
		return nFix;
	}

	public void setnFix(float nFix) {
		this.nFix = nFix;
	}

	public float getxFix() {
		return xFix;
	}

	public void setxFix(float xFix) {
		this.xFix = xFix;
	}

	public String getReason() {
		return reason;
	}

	public void setReason(String reason) {
		this.reason = reason;
	}
	
	public int getSplit() {
		return split;
	}
	

}
