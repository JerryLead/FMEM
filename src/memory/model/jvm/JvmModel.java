package memory.model.jvm;

import profile.commons.metrics.JstatItem;
import profile.commons.metrics.JvmItem;

public class JvmModel {
	//from JVM Memory Metrics
	private int jvmUsed;
	private int jvmTotal;
	private int jvmMax;
	
	//from Pidstat
	private int RSS;
	//Heap
	private float heapCommitted;
	private float heapUsed;
	
	//New Generation
	private float s0Committed;
	private float s0Used;
	private float s1Committed;
	private float s1Used;
	private float edenCommitted;
	private float edenUsed;
	
	private float newCommitted;
	private float newUsed;
	
	//Old Generation
	private float oldCommitted;
	private float oldUsed;
	
	//Perm Generation
	private float permCommitted;
	private float permUsed;
	
	//used for computing gc infos
	private int youngGC;
	private int fullGC;
	private float YGCT;
	private float FGCT;
	private float GCT;
	
	public JvmModel() {
		//from JVM Memory Metrics
		jvmUsed = -1;
		jvmTotal = -1;
		jvmMax = -1;
		
		RSS = -1;
		
		//Heap
		heapCommitted = -1;
		heapUsed = -1;
		
		//New Generation
		s0Committed = -1;
		s0Used = -1;
		s1Committed = -1;
		s1Used = -1;
		edenCommitted = -1;
		edenUsed = -1;
		
		newCommitted = -1;
		newUsed = -1;
		
		//Old Generation
		oldCommitted = -1;
		oldUsed = -1;
		
		//Perm Generation
		permCommitted = -1;
		permUsed = -1;
		
		//used for computing gc infos
		youngGC = -1;
		fullGC = -1;
		YGCT = -1;
		FGCT = -1;
		GCT = -1;
	}
	
	public void selectMaxValue(JstatItem item) {
		if(s0Committed < item.getS0C())
			s0Committed = item.getS0C();
		if(s0Used < item.getS0U())
			s0Used = item.getS0U();
		if(s1Committed < item.getS1C())
			s1Committed = item.getS1C();
		if(s1Used < item.getS1U())
			s1Used = item.getS1U();
		if(edenCommitted < item.getEC())
			edenCommitted = item.getEC();
		if(edenUsed < item.getEU())
			edenUsed = item.getEU();
		
		if(newCommitted < item.getS0C() + item.getS1C() + item.getEC())
			newCommitted = item.getS0C() + item.getS1C() + item.getEC();
		if(newUsed < item.getS0U() + item.getS1U() + item.getEU())
			newUsed = item.getS0U() + item.getS1U() + item.getEU();
		
		if(oldCommitted < item.getOC())
			oldCommitted = item.getOC();
		if(oldUsed < item.getOU())
			oldUsed = item.getOU();
		
		if(permCommitted < item.getPC())
			permCommitted = item.getPC();
		if(permUsed < item.getPU())
			permUsed = item.getPU();
		
		if(heapUsed < item.getS0U() + item.getS1U() + item.getEU() + item.getOU())
			heapUsed = item.getS0U() + item.getS1U() + item.getEU() + item.getOU();
		
		if(youngGC < item.getYGC())
			youngGC = item.getYGC();
		if(fullGC < item.getFGC())
			fullGC = item.getFGC();
		if(YGCT < item.getYGCT())
			YGCT = item.getYGCT();
		if(FGCT < item.getFGCT())
			FGCT = item.getFGCT();
		if(GCT < item.getGCT())
			GCT = item.getGCT();
	}
	
	public void selectMaxValue(JvmItem item) {
		if(item.getJvmUsed() > this.jvmUsed)
			this.jvmUsed = item.getJvmUsed();
		if(item.getJvmMax() > this.jvmMax)
			this.jvmMax = item.getJvmMax();
		if(item.getJvmTotal() > this.jvmTotal)
			this.jvmTotal = item.getJvmTotal();
	}

	@Override
	public String toString() {
		return "jvmUsed" + "\t" + "jvmTotal" + "\t" + "jvmMax" + "\t"
				+ "heapCommitted" + "\t" + "heapUsed" + "\t" + "avgUsed" + "\t"
				+ "s0C" + "\t" + "s0U" + "\t" + "s1C" + "\t" + "s1U" + "\t"
				+ "edenC" + "\t" + "edenU" + "\t" + "ngnC" + "\t" + "ngnU" + "\t"
				+ "oldC" + "\t" + "oldU" + "\t" + "permC" + "\t" + "permU" + "\t"
				+ "yGC" + "\t" + "fGC" + "\n"
			    + jvmUsed + "\t" + jvmTotal + "\t" + jvmMax + "\t"
			    + heapCommitted + "\t" + heapUsed + "\t" 
			    + s0Committed + "\t" + s0Used + "\t" + s1Committed + "\t" + s1Used + "\t"
			    + edenCommitted + "\t" + edenUsed + "\t" + newCommitted + "\t" + newUsed + "\t"
			    + oldCommitted + "\t" + oldUsed + "\t" + permCommitted + "\t" + permUsed + "\t"
			    + youngGC + "\t" + fullGC;
			    		
	}

	
	public void setJvmUsed(int jvmUsed) {
		this.jvmUsed = jvmUsed;
	}

	public void setJvmTotal(int jvmTotal) {
		this.jvmTotal = jvmTotal;
	}

	public void setJvmMax(int jvmMax) {
		this.jvmMax = jvmMax;
	}

	public void setCommitted(float heapCommitted) {
		this.heapCommitted = heapCommitted;
	}

	public void setHeapUsed(float heapUsed) {
		this.heapUsed = heapUsed;
	}

	public void setS0Committed(float s0Committed) {
		this.s0Committed = s0Committed;
	}

	public void setS0Used(float s0Used) {
		this.s0Used = s0Used;
	}

	public void setS1Committed(float s1Committed) {
		this.s1Committed = s1Committed;
	}

	public void setS1Used(float s1Used) {
		this.s1Used = s1Used;
	}

	public void setEdenCommitted(float edenCommitted) {
		this.edenCommitted = edenCommitted;
	}

	public void setEdenUsed(float edenUsed) {
		this.edenUsed = edenUsed;
	}

	public void setNewCommitted(float newCommitted) {
		this.newCommitted = newCommitted;
	}

	public void setNewUsed(float newUsed) {
		this.newUsed = newUsed;
	}

	public void setOldCommitted(float oldCommitted) {
		this.oldCommitted = oldCommitted;
	}

	public void setOldUsed(float oldUsed) {
		this.oldUsed = oldUsed;
	}

	public void setPermCommitted(float permCommitted) {
		this.permCommitted = permCommitted;
	}

	public void setPermUsed(float permUsed) {
		this.permUsed = permUsed;
	}

	public void setYoungGC(int youngGC) {
		this.youngGC = youngGC;
	}

	public void setFullGC(int fullGC) {
		this.fullGC = fullGC;
	}

	public int getJvmUsed() {
		return jvmUsed;
	}

	public int getJvmTotal() {
		return jvmTotal;
	}

	public int getJvmMax() {
		return jvmMax;
	}

	public float getHeapCommitted() {
		return heapCommitted;
	}

	public float getHeapUsed() {
		return heapUsed;
	}

	public float getS0Committed() {
		return s0Committed;
	}

	public float getS0Used() {
		return s0Used;
	}

	public float getS1Committed() {
		return s1Committed;
	}

	public float getS1Used() {
		return s1Used;
	}

	public float getEdenCommitted() {
		return edenCommitted;
	}

	public float getEdenUsed() {
		return edenUsed;
	}

	public float getNewCommitted() {
		return newCommitted;
	}

	public float getNewUsed() {
		return newUsed;
	}

	public float getOldCommitted() {
		return oldCommitted;
	}

	public float getOldUsed() {
		return oldUsed;
	}

	public float getPermCommitted() {
		return permCommitted;
	}

	public float getPermUsed() {
		return permUsed;
	}

	public int getYoungGC() {
		return youngGC;
	}

	public int getFullGC() {
		return fullGC;
	}

	public float getYGCT() {
		return YGCT;
	}

	public float getFGCT() {
		return FGCT;
	}

	public float getGCT() {
		return GCT;
	}

	public int getRSS() {
		return RSS;
	}

	public void setRSS(int rSS) {
		RSS = rSS;
	}
	
	
	
}
