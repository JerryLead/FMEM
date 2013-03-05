package data.model.mapper;

import profile.commons.configuration.Configuration;

public class SpillFitsInMemoryModel {
	private int io_sort_mb;
	private float io_sort_spill_percent;
	private float io_sort_record_percent;

	
	public void keepInMemory(long fmax_mapred_output_records, long fmax_mapred_output_bytes, Configuration conf) {

		float alpha = 1.02f;
		long recordsCapacity = fmax_mapred_output_records * 16;
		recordsCapacity = (long)(recordsCapacity * alpha) + 16;
		
		long kvbufferBytes = (long)(fmax_mapred_output_bytes * alpha);
		long sortmb = recordsCapacity + kvbufferBytes;
		
		io_sort_record_percent = (float) ((double)recordsCapacity / sortmb);
		io_sort_spill_percent = 1.0f;
		io_sort_mb = (int) Math.ceil((double)sortmb / 1024 / 1024);
		

		System.out.println("set io.sort.mb = " + io_sort_mb);
		System.out.println("set io.sort.record.percent = " + io_sort_record_percent);
		System.out.println("set io.sort.spill.percent = " + io_sort_spill_percent);
		
		if(conf != null) {
			conf.set("io.sort.mb", String.valueOf(io_sort_mb));
			conf.set("io.sort.record.percent", String.valueOf(io_sort_record_percent));
			conf.set("io.sort.spill.percent", String.valueOf(io_sort_spill_percent));
		}
	}


	public int getIo_sort_mb() {
		return io_sort_mb;
	}


	public float getIo_sort_spill_percent() {
		return io_sort_spill_percent;
	}


	public float getIo_sort_record_percent() {
		return io_sort_record_percent;
	}
	
	
}
