package profile.task.common;

import profile.commons.metrics.GcCapacity;

/*
 * used in TaskCountersMetricsJvmParser.java, mapper and reducer do the real work.
 */
public interface Task {

	public void addCounterItem(String[] parameters);

	public void addMetricsItem(String[] parameters);

	public void addJvmMetrics(String[] parameters);

	public void addJstatMetrics(String[] generateArrayList);

	public void addGcCapacity(GcCapacity gcCap);
	
}
