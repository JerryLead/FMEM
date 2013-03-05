package profile.commons.metrics;

import java.io.Serializable;

public class GcCapacity implements Serializable {

	private float NGCMN;
	private float NGCMX;
	private float NGC;
	private float S0C;
	private float S1C;
	private float EC;
	private float OGCMN;
	private float OGCMX;
	private float OGC;
	private float OC;
	private float PGCMN;
	private float PGCMX;
	private float PGC;
	private float PC;
	private int YGC;
	private int FGC;
	
	public GcCapacity(String[] gccapacity) {
		NGCMN = Float.parseFloat(gccapacity[0]) / 1024;
		NGCMX = Float.parseFloat(gccapacity[1]) / 1024;
		NGC = Float.parseFloat(gccapacity[2]) / 1024;
		S0C = Float.parseFloat(gccapacity[3]) / 1024;
		S1C = Float.parseFloat(gccapacity[4]) / 1024;
		EC = Float.parseFloat(gccapacity[5]) / 1024;
		OGCMN = Float.parseFloat(gccapacity[6]) / 1024;
		OGCMX = Float.parseFloat(gccapacity[7]) / 1024;
		OGC = Float.parseFloat(gccapacity[8]) / 1024;
		OC = Float.parseFloat(gccapacity[9]) / 1024;
		PGCMN = Float.parseFloat(gccapacity[10]) / 1024;
		PGCMX = Float.parseFloat(gccapacity[11]) / 1024;
		PGC = Float.parseFloat(gccapacity[12]) / 1024;
		PC = Float.parseFloat(gccapacity[13]) / 1024;
		YGC = Integer.parseInt(gccapacity[14]);
		FGC = Integer.parseInt(gccapacity[15]);
	}

	public float getNGCMN() {
		return NGCMN;
	}

	public float getNGCMX() {
		return NGCMX;
	}

	public float getNGC() {
		return NGC;
	}

	public float getS0C() {
		return S0C;
	}

	public float getS1C() {
		return S1C;
	}

	public float getEC() {
		return EC;
	}

	public float getOGCMN() {
		return OGCMN;
	}

	public float getOGCMX() {
		return OGCMX;
	}

	public float getOGC() {
		return OGC;
	}

	public float getOC() {
		return OC;
	}

	public float getPGCMN() {
		return PGCMN;
	}

	public float getPGCMX() {
		return PGCMX;
	}

	public float getPGC() {
		return PGC;
	}

	public float getPC() {
		return PC;
	}

	public int getYGC() {
		return YGC;
	}

	public int getFGC() {
		return FGC;
	}

	public void setNGCMN(float nGCMN) {
		NGCMN = nGCMN;
	}

	public void setNGCMX(float nGCMX) {
		NGCMX = nGCMX;
	}

	public void setNGC(float nGC) {
		NGC = nGC;
	}

	public void setS0C(float s0c) {
		S0C = s0c;
	}

	public void setS1C(float s1c) {
		S1C = s1c;
	}

	public void setEC(float eC) {
		EC = eC;
	}

	public void setOGCMN(float oGCMN) {
		OGCMN = oGCMN;
	}

	public void setOGCMX(float oGCMX) {
		OGCMX = oGCMX;
	}

	public void setOGC(float oGC) {
		OGC = oGC;
	}

	public void setOC(float oC) {
		OC = oC;
	}

	public void setPGCMN(float pGCMN) {
		PGCMN = pGCMN;
	}

	public void setPGCMX(float pGCMX) {
		PGCMX = pGCMX;
	}

	public void setPGC(float pGC) {
		PGC = pGC;
	}

	public void setPC(float pC) {
		PC = pC;
	}

	public void setYGC(int yGC) {
		YGC = yGC;
	}

	public void setFGC(int fGC) {
		FGC = fGC;
	}

	public String toString() {
		
		String f1 = "%1$-3.1f";
		
		return  String.format("%1$-3.0f", (NGCMX + OGCMX)) + "\t" + 
				//String.format(f1, PGCMX) + "\t" 
				String.format(f1, NGCMX) + "\t" + String.format(f1, NGC)
				+ "\t" + String.format(f1, S0C) + "\t" + String.format(f1, S1C) + "\t" + String.format(f1, EC) + 
				"\t" + String.format(f1, OGCMX) + "\t" + String.format(f1, OGC) + "\t" + String.format(f1, PGCMX)
				+ "\t" + String.format(f1, PGC);
		
		
		
	}
	
}
