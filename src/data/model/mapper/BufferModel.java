package data.model.mapper;

import profile.task.mapper.MapperBuffer;

public class BufferModel {
	public static MapperBuffer computeBufferLimit(int io_sort_mb, float io_sort_spill_percent, 
			float io_sort_record_percent) {
		
		MapperBuffer buffer = new MapperBuffer();
		float spillper = io_sort_spill_percent;
		float recper = io_sort_record_percent;
		int sortmb = io_sort_mb;

		// buffers and accounting
		long maxMemUsage = sortmb << 20;
		long recordCapacity = (long) (maxMemUsage * recper);
		recordCapacity -= recordCapacity % 16;
		long kvbufferBytes = maxMemUsage - recordCapacity;

		recordCapacity /= 16;
		//each kvoffsets/kvindices is a integer, kvindices has three elements while kvoffsets has only one
		long kvoffsetsLen = recordCapacity; 
		long kvindicesLen = recordCapacity * 3;

		long softBufferLimit = (long) (kvbufferBytes * spillper);
		long softRecordLimit = (long) (kvoffsetsLen * spillper);

		/*
		System.out.println("data buffer = " + softBufferLimit + "/"
				+ kvbufferBytes);
		System.out.println("record buffer = " + softRecordLimit + "/"
				+ kvoffsetsLen);
		*/
		
		buffer.setDataBuffer(softBufferLimit, kvbufferBytes);
		buffer.setRecordBuffer(softRecordLimit, kvoffsetsLen);
	
		return buffer;
	}
}
