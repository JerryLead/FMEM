package profile.commons.metrics;

import java.io.Serializable;

public class JvmItem implements Serializable {
	private long timeStampMS;
	private int JVMUsed;
	private int Total;
	private int Max;

	public JvmItem(String[] params) {
		// Time
		timeStampMS = Long.parseLong(params[0]) * 1000;
		JVMUsed = Integer.parseInt(params[1]);
		Total = Integer.parseInt(params[2]);
		Max = Integer.parseInt(params[3]);
	}

	public long getTimeStampMS() {
		return timeStampMS;
	}

	public int getJvmUsed() {
		return JVMUsed;
	}

	public int getJvmTotal() {
		return Total;
	}

	public int getJvmMax() {
		return Max;
	}
	
	public String toString() {
		return timeStampMS + "\t" + String.format("%1$,-5d", JVMUsed) + "\t" + String.format("%1$,-5d", Total) + "\t" + String.format("%1$,-5d", Max);
	}
}