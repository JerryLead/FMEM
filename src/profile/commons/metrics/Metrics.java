package profile.commons.metrics;

import java.io.Serializable;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;


public class Metrics implements Serializable {
	// Since the time is different between metrics and jvmMetrics, we divide
	// them into two separated lists
	private List<MetricsItem> metricsList;
	private List<JvmItem> jvmMetricsList;
	private List<JstatItem> jstatMetricsList;
	private GcCapacity gcCap;

	public Metrics() {
		metricsList = new ArrayList<MetricsItem>();
		jvmMetricsList = new ArrayList<JvmItem>();
		jstatMetricsList = new ArrayList<JstatItem>();
	}

	public void addMetricsItem(String[] metricsParams) {
		MetricsItem item = new MetricsItem(metricsParams);
		metricsList.add(item);
	}

	public void addJvmMetrics(String[] jvmParams) {
		JvmItem item = new JvmItem(jvmParams);
		jvmMetricsList.add(item);
	}

	public void addJstatMetrics(String[] jstatParams) {
		JstatItem item = new JstatItem(jstatParams);
		jstatMetricsList.add(item);
	}
	
	// Given a timestamp (ms), return the metrics at that time or the average
	// metrics between the interval metrics
	public MetricsItem getMetricsItem(long timeStampMS) {
		if (metricsList == null)
			return null;

		int beforeIndex = 0;
		int afterIndex = metricsList.size() - 1;

		for (int i = 0; i < metricsList.size(); i++) {
			MetricsItem item = metricsList.get(i);
			if (item.getTimeStampMS() == timeStampMS) {
				return item;
			} else if (timeStampMS > item.getTimeStampMS()) {
				beforeIndex = i;
			} else {
				afterIndex = i;
			}
		}

		return metricsList.get(afterIndex);

	}

	// Given a timestamp (ms), return the jvm metrics at that time or the last jvm metrics
	public JvmItem getJvmItem(long timeStampMS) {
		if (jvmMetricsList == null)
			return null;

		int beforeIndex = 0;
		int afterIndex = jvmMetricsList.size() - 1;

		for (int i = 0; i < jvmMetricsList.size(); i++) {
			JvmItem item = jvmMetricsList.get(i);
			if (item.getTimeStampMS() == timeStampMS) {
				return item;
			} else if (timeStampMS > item.getTimeStampMS()) {
				beforeIndex = i;
			} else {
				afterIndex = i;
			}
		}

		return jvmMetricsList.get(afterIndex);

	}
	/*
	public List<String> getMetricsList() {
		List<String> list = new ArrayList<String>();
		for(MetricsItem item : metricsList) {
			list.add(item.toString());
		}
		return list;
	}
	*/
	public List<JvmItem> getJvmMetricsList() {
		return jvmMetricsList;
	}

	public List<JstatItem> getJstatMetricsList() {
		return jstatMetricsList;
	}
	
	public GcCapacity getGcCapacity() {
		return gcCap;
	}
	
	public List<String> getJvmMetricsStringList() {
		List<String> list = new ArrayList<String>();
		String header = "Timestamp\tJVMUsed\tTotal\tMax";
		list.add(header);
		for(JvmItem item : jvmMetricsList) {
			list.add(item.toString());
		}
		return list;
	}

	public List<String> getJstatMetricsStringList() {
		
		List<String> list = new ArrayList<String>();
		String header = "TimeS S0U S1U EU OU PU S0C S1C EC OC PC NGC YGC FGC YGCT FGCT GCT";
		list.add(header);
		for(JstatItem item : jstatMetricsList) {
			list.add(item.toString());
		}
		return list;
	}

	public void addGcCapacity(GcCapacity gcCap) {
		this.gcCap = gcCap;
	}

}

class MetricsItem implements Serializable {
	long timeStampMS;
	CPUMetrics cpuMetrics;
	MemMetrics memMetrics;
	IoMetrics ioMetrics;

	public MetricsItem(String[] params) {

		timeStampMS = Long.parseLong(params[0]) * 1000;
		int PID = Integer.parseInt(params[1]);
		float usr = Float.parseFloat(params[2]);
		float system = Float.parseFloat(params[3]);
		float guest = Float.parseFloat(params[4]);
		float CPU = Float.parseFloat(params[5]);
		int CPUID = Integer.parseInt(params[6]);

		cpuMetrics = new CPUMetrics(PID, usr, system, guest, CPU, CPUID);

		float minflt = Float.parseFloat(params[7]);
		float majflt = Float.parseFloat(params[8]);
		long VSZ = Long.parseLong(params[9]) / 1024;
		long RSS = Long.parseLong(params[10]) / 1024;
		float MEM = Float.parseFloat(params[11]);

		memMetrics = new MemMetrics(minflt, majflt, VSZ, RSS, MEM);

		float kB_rd = Float.parseFloat(params[12]);
		float kB_wr = Float.parseFloat(params[13]);
		float kB_ccwr = Float.parseFloat(params[14]);

		ioMetrics = new IoMetrics(kB_rd, kB_wr, kB_ccwr);
	}

	public long getTimeStampMS() {
		return timeStampMS;
	}
	
	
}

class CPUMetrics implements Serializable {
	private int PID;
	private float usr;
	private float system;
	private float guest;
	private float CPU;
	private int CPUID;

	public CPUMetrics(int PID, float usr, float system, float guest, float CPU,
			int CPUID) {
		this.PID = PID;
		this.usr = usr;
		this.system = system;
		this.guest = guest;
		this.CPU = CPU;
		this.CPUID = CPUID;
	}
}

class MemMetrics implements Serializable {
	private float minflt;
	private float majflt;
	private long VSZ;
	private long RSS;
	private float MEM;

	public MemMetrics(float minflt, float majflt, long VSZ, long RSS, float MEM) {
		this.minflt = minflt;
		this.majflt = majflt;
		this.VSZ = VSZ;
		this.RSS = RSS;
		this.MEM = MEM;
	}
}

class IoMetrics implements Serializable {
	private float kB_rd;
	private float kB_wr;
	private float kB_ccwr;

	public IoMetrics(float kB_rd, float kB_wr, float kB_ccwr) {
		this.kB_rd = kB_rd;
		this.kB_wr = kB_wr;
		this.kB_ccwr = kB_ccwr;
	}
}

class LongToTime {

	private static DateFormat f = new SimpleDateFormat("mm:ss");
	
	public static String longToMMSS(long time) {
		return f.format(time);
	}

}
