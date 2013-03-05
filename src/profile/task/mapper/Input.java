package profile.task.mapper;

import java.io.Serializable;

public class Input implements Serializable {
	private long splitSize;
	private String[] splitLocations;
	private String machine;

	public long getSplitSize() {
		return splitSize;
	}

	public void setSplitSize(long splitSize) {
		this.splitSize = splitSize;
	}

	public String[] getSplitLocations() {
		return splitLocations;
	}

	public void setSplitLocations(String[] splitLocations) {
		this.splitLocations = splitLocations;
	}

	public String getMachine() {
		return machine;
	}

	public void setMachine(String machine) {
		this.machine = machine;
	}

	public void setInputItems(String machine, String[] splitLocations) {
		setMachine(machine);
		setSplitLocations(splitLocations);
	}
}
