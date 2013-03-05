package verification.dataflow;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ReducerDataflow {

	private float mShCompMB = -1;
	private float mShRawMB = -1;
	private float mInRec = -1;
	private float mInputMB = -1;
	private float mOutRec = -1;
	private float mOutMB = -1;
	
	private float xShCompMB = -1;
	private float xShRawMB = -1;
	private float xInRec = -1;
	private float xInputMB = -1;
	private float xOutRec = -1;
	private float xOutMB = -1;
	
	private int xmx = -1;
	private int xms = -1;
	private int ismb = -1;
	private int RN = -1;
	private String jobId;
	
	public ReducerDataflow(String file) {	
		try {
			BufferedReader reader = new BufferedReader(new FileReader(new File(file)));	
			String[] title = reader.readLine().split("\t");
			String line;
			List<ReducerDataflowItem> list = new ArrayList<ReducerDataflowItem>();
			while((line = reader.readLine()) != null) {
				String[] values = line.split("\t");
				list.add(new ReducerDataflowItem(title, values));
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

	public ReducerDataflow(String[] titles, String[] params) {
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
			
			else if(title.equals("mShCompMB"))
				mShCompMB = Float.parseFloat(params[i]);
			else if(title.equals("mShRawMB"))
				mShRawMB = Float.parseFloat(params[i]);
			else if(title.equals("mInRec"))
				mInRec = Float.parseFloat(params[i]);
			else if(title.equals("mInputMB"))
				mInputMB = Float.parseFloat(params[i]);
			else if(title.equals("mOutRec"))
				mOutRec = Float.parseFloat(params[i]);
			else if(title.equals("mOutMB"))
				mOutMB = Float.parseFloat(params[i]);
			else if(title.equals("xShCompMB"))
				xShCompMB = Float.parseFloat(params[i]);
			else if(title.equals("xShRawMB"))
				xShRawMB = Float.parseFloat(params[i]);
			else if(title.equals("xInRec"))
				xInRec = Float.parseFloat(params[i]);
			else if(title.equals("xInputMB"))
				xInputMB = Float.parseFloat(params[i]);
			else if(title.equals("xOutRec"))
				xOutRec = Float.parseFloat(params[i]);
			else if(title.equals("xOutMB"))
				xOutMB = Float.parseFloat(params[i]);
			else if(title.equals("jobId"))
				jobId = params[i];

		}
	}
	
	private void computeStatistics(List<ReducerDataflowItem> list) {
		//find the medium value
		int size = list.size();

		float[] ShCompMB = new float[size];
		float[] ShRawMB = new float[size];
		float[] InRec = new float[size];
		float[] InputMB = new float[size];
		float[] OutRec = new float[size];
		float[] OutMB = new float[size];
		
		for(int i = 0; i < size; i++) {
			ShCompMB[i] = list.get(i).getShCompMB();
			ShRawMB[i] = list.get(i).getShRawMB();
			InRec[i] = list.get(i).getInRec();
			InputMB[i] = list.get(i).getInputMB();
			OutRec[i] = list.get(i).getOutRec();
			OutMB[i] = list.get(i).getOutMB();
		}
		
		Arrays.sort(ShCompMB);
		Arrays.sort(ShRawMB);
		Arrays.sort(InRec);
		Arrays.sort(InputMB);
		Arrays.sort(OutRec);
		Arrays.sort(OutMB);
			
		int m = size / 2;
		
		mShCompMB = ShCompMB[m];
		mShRawMB = ShRawMB[m];
		mInRec = InRec[m];
		mInputMB = InputMB[m];
		mOutRec = OutRec[m];
		mOutMB = OutMB[m];
		
		int end = size - 1;
		xShCompMB = ShCompMB[end];
		xShRawMB = ShRawMB[end];
		xInRec = InRec[end];
		xInputMB = InputMB[end];
		xOutRec = OutRec[end];
		xOutMB = OutMB[end];
	}

	public float getmShCompMB() {
		return mShCompMB;
	}

	public float getmShRawMB() {
		return mShRawMB;
	}

	public float getmInRec() {
		return mInRec;
	}

	public float getmInputMB() {
		return mInputMB;
	}

	public float getmOutRec() {
		return mOutRec;
	}

	public float getmOutMB() {
		return mOutMB;
	}

	public float getxShCompMB() {
		return xShCompMB;
	}

	public float getxShRawMB() {
		return xShRawMB;
	}

	public float getxInRec() {
		return xInRec;
	}

	public float getxInputMB() {
		return xInputMB;
	}

	public float getxOutRec() {
		return xOutRec;
	}

	public float getxOutMB() {
		return xOutMB;
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

	@Override
	public String toString() {
		String f1 = "%1$-2.1f";
		return String.format(f1, mShCompMB) + "\t" + String.format(f1, mShRawMB) + "\t" 
				+ String.format(f1, mInRec) + "\t" + String.format(f1, mInputMB) + "\t" 
				+ String.format(f1, mOutRec) + "\t" + String.format(f1, mOutMB) + "\t"
				+ String.format(f1, xShCompMB) + "\t" + String.format(f1, xShRawMB) + "\t" 
				+ String.format(f1, xInRec) + "\t" + String.format(f1, xInputMB) + "\t" 
				+ String.format(f1, xOutRec) + "\t" + String.format(f1, xOutMB);
	}
	
}

class ReducerDataflowItem {
	private float ShCompMB; // shuffle compressed bytes
	private float ShRawMB; // shuffle raw bytes
	private float InRec; // reduce input records
	private float InputMB; // reduce input bytes
	private float OutRec; // reduce output records
	private float OutMB; // reduce output bytes
	
	public ReducerDataflowItem(String[] title, String[] values) {
		for(int i = 0; i < title.length; i++) {
			float v = Float.parseFloat(values[i]);
			if(title[i].equals("ShCompMB"))
				ShCompMB = v;
			else if(title[i].equals("ShRawMB"))
				ShRawMB = v;
			else if(title[i].equals("InRec"))
				InRec = v;
			else if(title[i].equals("InputMB"))
				InputMB = v;
			else if(title[i].equals("OutRec"))
				OutRec = v;
			else if(title[i].equals("OutMB"))
				OutMB = v;
		}	
	}

	public float getShCompMB() {
		return ShCompMB;
	}

	public float getShRawMB() {
		return ShRawMB;
	}

	public float getInRec() {
		return InRec;
	}

	public float getInputMB() {
		return InputMB;
	}

	public float getOutRec() {
		return OutRec;
	}

	public float getOutMB() {
		return OutMB;
	}

	
}