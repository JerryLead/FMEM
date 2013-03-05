package verification.split.dataflow;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SplitMapperDataflow {
	private int rMapNum;

	private float mInMB = -1;
	private float mInRec = -1;
	private float mOutMB = -1;
	private float mOutRec = -1;
	
	private float mRecBM = -1;
	private float mRawBM = -1;
	private float mCompBM = -1;
	private float mRecAM = -1;
	private float mRawAM = -1;
	private float mCompAM = -1;
	private int mSegN = -1;
	
	private float xInMB = -1;
	private float xInRec = -1;
	private float xOutMB = -1;
	private float xOutRec = -1;
	
	private float xRecBM = -1;
	private float xRawBM = -1;
	private float xCompBM = -1;
	private float xRecAM = -1;
	private float xRawAM = -1;
	private float xCompAM = -1;
	private int xSegN = -1;
	
	private int split = 0;
	private int xmx = -1;
	private int xms = -1;
	private int ismb = -1;
	private int RN = -1;
	private String jobId = "";
	
	public SplitMapperDataflow(String file) {	
		try {
			BufferedReader reader = new BufferedReader(new FileReader(new File(file)));	
			String[] title = reader.readLine().split("\t");
			String line;
			List<MapperDataflowItem> list = new ArrayList<MapperDataflowItem>();
			while((line = reader.readLine()) != null) {
				String[] values = line.split("\t");
				list.add(new MapperDataflowItem(title, values));
			}
			computeStatistics(list);
			reader.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public SplitMapperDataflow(String[] titles, String[] params) {	
		for(int i = 0; i < titles.length; i++) {
			String title = titles[i];
			if(title.equals("xmx"))
				xmx = Integer.parseInt(params[i]);
			else if(title.equals("xms"))
				xms = Integer.parseInt(params[i]);
			else if(title.equals("ismb"))
				ismb = Integer.parseInt(params[i]);
			else if(title.equals("RN"))
				RN = Integer.parseInt(params[i]);
			else if(title.equals("mInMB"))
				mInMB = Float.parseFloat(params[i]);
			else if(title.equals("mInRec"))
				mInRec = Float.parseFloat(params[i]);
			else if(title.equals("mOutMB"))
				mOutMB = Float.parseFloat(params[i]);
			else if(title.equals("mOutRec"))
				mOutRec = Float.parseFloat(params[i]);
			else if(title.equals("mRecBM"))
				mRecBM = Float.parseFloat(params[i]);
			else if(title.equals("mRawBM"))
				mRawBM = Float.parseFloat(params[i]);
			else if(title.equals("mCompBM"))
				mCompBM = Float.parseFloat(params[i]);
			else if(title.equals("mRecAM"))
				mRecAM = Float.parseFloat(params[i]);
			else if(title.equals("mRawAM"))
				mRawAM = Float.parseFloat(params[i]);
			else if(title.equals("mCompAM"))
				mCompAM = Float.parseFloat(params[i]);
			else if(title.equals("mSegN"))
				mSegN = Integer.parseInt(params[i]);
			else if(title.equals("xInMB"))
				xInMB = Float.parseFloat(params[i]);
			else if(title.equals("xInRec"))
				xInRec = Float.parseFloat(params[i]);
			else if(title.equals("xOutMB"))
				xOutMB = Float.parseFloat(params[i]);
			else if(title.equals("xOutRec"))
				xOutRec = Float.parseFloat(params[i]);
			else if(title.equals("xRecBM"))
				xRecBM = Float.parseFloat(params[i]);
			else if(title.equals("xRawBM"))
				xRawBM = Float.parseFloat(params[i]);
			else if(title.equals("xCompBM"))
				xCompBM = Float.parseFloat(params[i]);
			else if(title.equals("xRecAM"))
				xRecAM = Float.parseFloat(params[i]);
			else if(title.equals("xRawAM"))
				xRawAM = Float.parseFloat(params[i]);
			else if(title.equals("xCompAM"))
				xCompAM = Float.parseFloat(params[i]);
			else if(title.equals("xSegN"))
				xSegN = Integer.parseInt(params[i]);
			else if(title.equals("jobId"))
				jobId = params[i];
			else if(title.equals("split"))
				split = Integer.parseInt(params[i]);

		}
	}

	private void computeStatistics(List<MapperDataflowItem> list) {
		//find the medium value
		int size = list.size();
		rMapNum = size;
		float[] InMB = new float[size];
		float[] InRec = new float[size];
		float[] OutMB = new float[size];
		float[] OutRec = new float[size];
		float[] RecBM = new float[size];
		float[] RawBM = new float[size];
		float[] CompBM = new float[size];
		float[] RecAM = new float[size];
		float[] RawAM = new float[size];
		float[] CompAM = new float[size];
		int[] SegN = new int[size];
		
		for(int i = 0; i < size; i++) {
			InMB[i] = list.get(i).getInMB();
			InRec[i] = list.get(i).getInRec();
			OutMB[i] = list.get(i).getOutMB();
			OutRec[i] = list.get(i).getOutRec();
			RecBM[i] = list.get(i).getRecBM();
			RawBM[i] = list.get(i).getRawBM();
			CompBM[i] = list.get(i).getCompBM();
			RecAM[i] = list.get(i).getRecAM();
			RawAM[i] = list.get(i).getRawAM();
			CompAM[i] = list.get(i).getCompAM();
			SegN[i] = list.get(i).getSegN();
		}
		
		Arrays.sort(InMB);
		Arrays.sort(InRec);
		Arrays.sort(OutMB);
		Arrays.sort(OutRec);
		Arrays.sort(RecBM);
		Arrays.sort(RawBM);
		Arrays.sort(CompBM);
		Arrays.sort(RecAM);
		Arrays.sort(RawAM);
		Arrays.sort(CompAM);
		Arrays.sort(SegN);
			
		int m = size / 2;
		mInMB = InMB[m];
		mInRec = InRec[m];
		mOutMB = OutMB[m];
		mOutRec = OutRec[m];
		
		mRecBM = RecBM[m];
		mRawBM = RawBM[m];
		mCompBM = CompBM[m];
		mRecAM = RecAM[m];
		mRawAM = RawAM[m];
		mCompAM = CompAM[m];
		mSegN = SegN[m];
		
		int end = size - 1;
		xInMB = InMB[end];
		xInRec = InRec[end];
		xOutMB = OutMB[end];
		xOutRec = OutRec[end];
		
		xRecBM = RecBM[end];
		xRawBM = RawBM[end];
		xCompBM = CompBM[end];
		xRecAM = RecAM[end];
		xRawAM = RawAM[end];
		xCompAM = CompAM[end];
		xSegN = SegN[end];
	}

	public int getrMapNum() {
		return rMapNum;
	}

	public float getmInMB() {
		return mInMB;
	}

	public float getmInRec() {
		return mInRec;
	}

	public float getmOutMB() {
		return mOutMB;
	}

	public float getmOutRec() {
		return mOutRec;
	}

	public float getmRecBM() {
		return mRecBM;
	}

	public float getmRawBM() {
		return mRawBM;
	}

	public float getmCompBM() {
		return mCompBM;
	}

	public float getmRecAM() {
		return mRecAM;
	}

	public float getmRawAM() {
		return mRawAM;
	}

	public float getmCompAM() {
		return mCompAM;
	}

	public int getmSegN() {
		return mSegN;
	}

	public float getxInMB() {
		return xInMB;
	}

	public float getxInRec() {
		return xInRec;
	}

	public float getxOutMB() {
		return xOutMB;
	}

	public float getxOutRec() {
		return xOutRec;
	}

	public float getxRecBM() {
		return xRecBM;
	}

	public float getxRawBM() {
		return xRawBM;
	}

	public float getxCompBM() {
		return xCompBM;
	}

	public float getxRecAM() {
		return xRecAM;
	}

	public float getxRawAM() {
		return xRawAM;
	}

	public float getxCompAM() {
		return xCompAM;
	}

	public int getxSegN() {
		return xSegN;
	}
	
	public int getXmx() {
		return xmx;
	}

	public int getXms() {
		return xms;
	}

	public int getIsmb() {
		return ismb;
	}

	public int getRN() {
		return RN;
	}

	public String getJobId() {
		return jobId;
	}

	public void setJobId(String jobId) {
		this.jobId = jobId;
	}
	
	public int getSplit() {
		return split;
	}

	@Override
	public String toString() {
		String f1 = "%1$-2.1f";
		return String.format(f1, mInMB) + "\t" + String.format(f1, mInRec) + "\t" 
				+ String.format(f1, mOutMB) + "\t" + String.format(f1, mOutRec) + "\t"
				+ String.format(f1, mRecBM)  + "\t" + String.format(f1, mRawBM) + "\t" 
				+ String.format(f1, mCompBM) + "\t" +  String.format(f1, mRecAM) + "\t" 
				+ String.format(f1, mRawAM) + "\t" + String.format(f1, mCompAM) + "\t"
				+ mSegN + "\t" 
				+ String.format(f1, xInMB) + "\t" + String.format(f1, xInRec) + "\t" 
				+ String.format(f1, xOutMB) + "\t" + String.format(f1, xOutRec) + "\t" 
				+ String.format(f1, xRecBM) + "\t" 
				+ String.format(f1, xRawBM) + "\t" + String.format(f1, xCompBM) + "\t" 
				+ String.format(f1, xRecAM) + "\t" + String.format(f1, xRawAM) + "\t" 
				+ String.format(f1, xCompAM) + "\t" 
				+ xSegN;
	}
}

class MapperDataflowItem {
	private float InMB; // Map input bytes
	private float InRec; // Map input records
	private float OutMB; // Map output bytes
	private float OutRec; // Map output records
	
	private float RecBM; // records before merge
	private float RawBM; // raw bytes before merge
	private float CompBM; // compressed bytes before merge
	private float RecAM; // records after merge
	private float RawAM; // raw bytes after merge
	private float CompAM; // compressed bytes after merge
	private int SegN; // merged number of segments
	
	public MapperDataflowItem(String[] title, String[] values) {
		for(int i = 0; i < title.length; i++) {
			if(title[i].equals("SegN"))
				SegN = Integer.parseInt(values[i]);
			else {
				float v = Float.parseFloat(values[i]);
				if(title[i].equals("InMB"))
					InMB = v;
				else if(title[i].equals("InRec"))
					InRec = v;
				else if(title[i].equals("OutMB"))
					OutMB = v;
				else if(title[i].equals("OutRec"))
					OutRec = v;
				else if(title[i].equals("RecBM"))
					RecBM = v;
				else if(title[i].equals("RawBM"))
					RawBM = v;
				else if(title[i].equals("CompBM"))
					CompBM = v;
				else if(title[i].equals("RecAM"))
					RecAM = v;
				else if(title[i].equals("RawAM"))
					RawAM = v;
				else if(title[i].equals("CompAM"))
					CompAM = v;
					
			}
		}	
	}

	public float getInMB() {
		return InMB;
	}

	public float getInRec() {
		return InRec;
	}

	public float getOutMB() {
		return OutMB;
	}

	public float getOutRec() {
		return OutRec;
	}

	public float getRecBM() {
		return RecBM;
	}

	public float getRawBM() {
		return RawBM;
	}

	public float getCompBM() {
		return CompBM;
	}

	public float getRecAM() {
		return RecAM;
	}

	public float getRawAM() {
		return RawAM;
	}

	public float getCompAM() {
		return CompAM;
	}

	public int getSegN() {
		return SegN;
	}
}
