package profile.job;

import java.io.Serializable;

public class Phase implements Serializable {

	private static final long serialVersionUID = -1593235419689687114L;
	//execution time
	private long phaseStartTimeMS;
	private long phaseStopTimeMS;
	
	//tasks information
	private int totalTasksNum;
	private int successfulTasksNum;
	private int failedTasksNum;
	private int killedTasksNum;
	
	public void set(int totalTasksNum, int successfulTasksNum,
			int failedTasksNum, int killedTasksNum, long phaseStartTimeMS,
			long phaseStopTimeMS) {
		this.totalTasksNum = totalTasksNum;
		this.successfulTasksNum = successfulTasksNum;
		this.failedTasksNum = failedTasksNum;
		this.killedTasksNum = killedTasksNum;
		this.phaseStartTimeMS = phaseStartTimeMS;
		this.phaseStopTimeMS = phaseStopTimeMS;	
	}
	
}
