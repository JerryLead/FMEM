package profile.commons.metrics;

import java.io.Serializable;

public class JstatItem implements Serializable {
	//All values are changed into MB
	private long timestampMS;
	
	private float S0C;
	private float S1C;
	private float S0U;
	private float S1U;
	private float EC;
	private float EU;
	private float NGC;
	private float NGU;
	
	private float OC;
	private float OU;
	
	private float PC;
	private float PU;
	
	private int YGC;
	private float YGCT;
	private int FGC;
	private float FGCT;
	private float GCT;

	public JstatItem(String[] params) {
		// Time
		timestampMS = Long.parseLong(params[0]) * 1000;;

		S0C = Float.parseFloat(params[1]);
		S1C = Float.parseFloat(params[2]);
		S0U = Float.parseFloat(params[3]);
		S1U = Float.parseFloat(params[4]);
		EC = Float.parseFloat(params[5]);
		EU = Float.parseFloat(params[6]);
		
		OC = Float.parseFloat(params[7]);
		OU = Float.parseFloat(params[8]);
		
		PC = Float.parseFloat(params[9]);
		PU = Float.parseFloat(params[10]);
		
		YGC = Integer.parseInt(params[11]);
		YGCT = Float.parseFloat(params[12]);
		FGC = Integer.parseInt(params[13]);
		FGCT = Float.parseFloat(params[14]);
		GCT = Float.parseFloat(params[15]);
		
		NGC = S0C + S1C + EC;
		NGU = S0U + S1U + EU;
		
	}
	
	public String toString() {
		StringBuilder sb = new StringBuilder();
		String f1 = "%1$,-3.0f";

		sb.append(LongToTime.longToMMSS(timestampMS));
		sb.append(' ');
		
		sb.append(String.format(f1, S0U)); sb.append(' ');
		sb.append(String.format(f1, S1U)); sb.append(' ');
		sb.append(String.format(f1, EU)); sb.append(' ');
		sb.append(String.format(f1, OU)); sb.append(' ');
		sb.append(String.format(f1, PU)); sb.append(' ');
		
		sb.append(String.format(f1, S0C)); sb.append(' ');
		sb.append(String.format(f1, S1C)); sb.append(' ');
		sb.append(String.format(f1, EC)); sb.append(' ');
		sb.append(String.format(f1, OC)); sb.append(' ');
		sb.append(String.format(f1, PC)); sb.append(' ');
		sb.append(String.format(f1, NGC)); sb.append(' ');
		
		sb.append(String.format("%1$-2d", YGC)); sb.append(' ');
		sb.append(String.format("%1$-2d", FGC)); sb.append(' ');
		sb.append(String.format("%1$,-3.2f", YGCT)); sb.append(' ');
		sb.append(String.format("%1$,-3.2f", FGCT)); sb.append(' ');
		sb.append(String.format("%1$,-3.2f", GCT)); 
	
		return sb.toString();
	}

	public long getTimeStampMS() {
		return timestampMS;
	}

	public float getOC() {
		return OC;
	}

	public float getOU() {
		return OU;
	}

	public float getPC() {
		return PC;
	}

	public float getPU() {
		return PU;
	}

	public int getYGC() {
		return YGC;
	}

	public float getYGCT() {
		return YGCT;
	}

	public int getFGC() {
		return FGC;
	}

	public float getFGCT() {
		return FGCT;
	}

	public float getGCT() {
		return GCT;
	}

	public float getNGC() {
		return NGC;
	}
	
	public float getNGU() {
		return NGU;
	}

	public float getS0C() {
		return S0C;
	}

	public float getS1C() {
		return S1C;
	}

	public float getS0U() {
		return S0U;
	}

	public float getS1U() {
		return S1U;
	}

	public float getEC() {
		return EC;
	}

	public float getEU() {
		return EU;
	}

	
}