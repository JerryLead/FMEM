package profile.task.mapper;

import java.io.Serializable;

public class Map implements Serializable  {
	private long timeBetweenStartAndSpillMS;
	private long inputKeyValuePairsNum; //equals "Map input records"
	private long inputBytes; //equals "HDFS_BYTES_READ"
	
	private long outputKeyValuePairsNum; //equals "Map output records"
	private long outputBytes; // equals "Map output bytes"
}
