package memory.model.jvm;

public class ReducerEstimatedJvmCost {
	// All values are MB
	private float inMemSegBuffer;
	private float mergeBuffer;
	private float eShuffleBytesMB;
	private float reduceInputBytes;
	
	private String SSReason;
	private float SSPermUsed;
	private float SSNewUsed;
	private float SSnOU;
	private float SSxOU;
	private float SSnHeapU;
	private float SSxHeapU;
	private float SSTempObj;
	private float SSFix;
	
	private String redReason;
	private float redPermUsed;
	private float redNewUsed;
	private float rednOU;
	private float redxOU;
	private float rednHeapU;
	private float redxHeapU;
	private float redTempObj;
	private float redFix;
	
	private String reason;
	private float permUsed;
	private float newUsed;
	private float nOU;
	private float xOU;
	private float nHeapU;
	private float xHeapU;
	//private float tempObj;
	private float fix;
	
	public void updateReducerJvmCost() {
		
		permUsed = Math.max(SSPermUsed, redPermUsed);
		newUsed = Math.max(SSNewUsed, redNewUsed);
		nOU = Math.max(SSnOU, rednOU);
		xOU = Math.max(SSxOU, redxOU);
		nHeapU = Math.max(SSnHeapU, rednHeapU);
		xHeapU = Math.max(SSxHeapU, redxHeapU);
		//tempObj = Math.max(SSTempObj, redTempObj);
		fix = Math.max(SSFix, redFix);	
		if(SSxOU >= redxOU) 
			reason = SSReason;
		else
			reason = redReason;
		/*	
		permUsed = Math.max(SSPermUsed, redPermUsed);
		newUsed = SSNewUsed;
		nOU = SSnOU;
		xOU = SSxOU;
		nHeapU = SSnHeapU;
		xHeapU = SSxHeapU;
		tempObj = SSTempObj;
		fix = SSFix;	
		
		reason = SSReason;	
		*/
	}
	
	@Override
	public String toString() {
		String f1 = "%1$-3.1f";
		return "PermU" + "\t" + "NewU" + "\t" 
				+ "nOU" + "\t" + "xOU" + "\t"
				+ "nHeapU" + "\t" + "xHeapU" + "\n"
				+ String.format(f1, SSPermUsed) + "\t"
				+ String.format(f1, newUsed) + "\t"
				+ String.format(f1, nOU) + "\t"
				+ String.format(f1, xOU) + "\t"
				+ String.format(f1, nHeapU) + "\t"
				+ String.format(f1, xHeapU);
	}

	public float getSSPermUsed() {
		return SSPermUsed;
	}

	public void setSSPermUsed(float sSPermUsed) {
		SSPermUsed = sSPermUsed;
	}

	public float getSSNewUsed() {
		return SSNewUsed;
	}

	public void setSSNewUsed(float sSNewUsed) {
		SSNewUsed = sSNewUsed;
	}

	public float getSSnOU() {
		return SSnOU;
	}

	public void setSSnOU(float sSnOU) {
		SSnOU = sSnOU;
	}

	public float getSSxOU() {
		return SSxOU;
	}

	public void setSSxOU(float sSxOU) {
		SSxOU = sSxOU;
	}

	public float getSSnHeapU() {
		return SSnHeapU;
	}

	public void setSSnHeapU(float sSnHeapU) {
		SSnHeapU = sSnHeapU;
	}

	public float getSSxHeapU() {
		return SSxHeapU;
	}

	public void setSSxHeapU(float sSxHeapU) {
		SSxHeapU = sSxHeapU;
	}

	public float getSSTempObj() {
		return SSTempObj;
	}

	public void setSSTempObj(float sSTempObj) {
		SSTempObj = sSTempObj;
	}

	public float getSSFix() {
		return SSFix;
	}

	public void setSSFix(float sSFix) {
		SSFix = sSFix;
	}

	public float getRedPermUsed() {
		return redPermUsed;
	}

	public void setRedPermUsed(float redPermUsed) {
		this.redPermUsed = redPermUsed;
	}

	public float getRedNewUsed() {
		return redNewUsed;
	}

	public void setRedNewUsed(float redNewUsed) {
		this.redNewUsed = redNewUsed;
	}

	public float getRednOU() {
		return rednOU;
	}

	public void setRednOU(float rednOU) {
		this.rednOU = rednOU;
	}

	public float getRedxOU() {
		return redxOU;
	}

	public void setRedxOU(float redxOU) {
		this.redxOU = redxOU;
	}

	public float getRednHeapU() {
		return rednHeapU;
	}

	public void setRednHeapU(float rednHeapU) {
		this.rednHeapU = rednHeapU;
	}

	public float getRedxHeapU() {
		return redxHeapU;
	}

	public void setRedxHeapU(float redxHeapU) {
		this.redxHeapU = redxHeapU;
	}

	public float getRedTempObj() {
		return redTempObj;
	}

	public void setRedTempObj(float redTempObj) {
		this.redTempObj = redTempObj;
	}

	public float getRedFix() {
		return redFix;
	}

	public void setRedFix(float redFix) {
		this.redFix = redFix;
	}

	public float getPermUsed() {
		return permUsed;
	}

	public void setPermUsed(float permUsed) {
		this.permUsed = permUsed;
	}

	public float getNewUsed() {
		return newUsed;
	}

	public void setNewUsed(float newUsed) {
		this.newUsed = newUsed;
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
/*
	public float getTempObj() {
		return tempObj;
	}

	public void setTempObj(float tempObj) {
		this.tempObj = tempObj;
	}
*/
	public float getFix() {
		return fix;
	}

	public void setFix(float fix) {
		this.fix = fix;
	}

	public String getReason() {
		return reason;
	}

	public void setReason(String reason) {
		this.reason = reason;
	}

	public String getSSReason() {
		return SSReason;
	}

	public void setSSReason(String sSReason) {
		SSReason = sSReason;
	}

	public String getRedReason() {
		return redReason;
	}

	public void setRedReason(String redReason) {
		this.redReason = redReason;
	}


	public float getInMemSegBuffer() {
		return inMemSegBuffer;
	}

	public void setInMemSegBuffer(float inMemSegBuffer) {
		this.inMemSegBuffer = inMemSegBuffer;
	}

	public float getMergeBuffer() {
		return mergeBuffer;
	}

	public void setMergeBuffer(float mergeBuffer) {
		this.mergeBuffer = mergeBuffer;
	}

	public float geteShuffleBytesMB() {
		return eShuffleBytesMB;
	}

	public void seteShuffleBytesMB(float eShuffleBytesMB) {
		this.eShuffleBytesMB = eShuffleBytesMB;
	}
	
	public float getReduceInputBytes() {
		return reduceInputBytes;
	}

	public void setReduceInputBytes(float reduceInputBytes) {
		this.reduceInputBytes = reduceInputBytes;
	}


}
