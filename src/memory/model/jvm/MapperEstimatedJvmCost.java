package memory.model.jvm;

public class MapperEstimatedJvmCost {
	// All values are MB
	private String reason;
	
	private float permUsed;
	private float newUsed;

	private float OU;
	private float heapU;

	private float tempObj;
	private float fix;

	

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

	public float getOU() {
		return OU;
	}

	public void setOU(float oU) {
		OU = oU;
	}

	public float getHeapU() {
		return heapU;
	}

	public void setHeapU(float heapU) {
		this.heapU = heapU;
	}

	public float getTempObj() {
		return tempObj;
	}

	public void setTempObj(float tempObj) {
		this.tempObj = tempObj;
	}

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
	
	@Override
	public String toString() {
		String f1 = "%1$-3.1f";
		return  String.format(f1, permUsed) + "\t"
				+ String.format(f1, newUsed) + "\t" 
				+ String.format(f1, OU) + "\t" 
				+ String.format(f1, heapU) + "\t" 
				+ String.format(f1, tempObj) + "\t"
				+ String.format(f1, fix);
	}

	

}
