package verification.gnuplot;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class MapperDataDiff {

	private List<Float> mInMB = new ArrayList<Float>();
	private List<Float> mInRec = new ArrayList<Float>();
	private List<Float> mOutMB = new ArrayList<Float>();
	private List<Float> mOutRec = new ArrayList<Float>();
	private List<Float> mRecBM = new ArrayList<Float>();
	private List<Float> mRawBM = new ArrayList<Float>();
	private List<Float> mCompBM = new ArrayList<Float>();
	private List<Float> mRecAM = new ArrayList<Float>();
	private List<Float> mRawAM = new ArrayList<Float>();
	private List<Float> mCompAM = new ArrayList<Float>();
	private List<Integer> mSegN = new ArrayList<Integer>();
	private List<Float> xInMB = new ArrayList<Float>();
	private List<Float> xInRec = new ArrayList<Float>();
	private List<Float> xOutMB = new ArrayList<Float>();
	private List<Float> xOutRec = new ArrayList<Float>();
	
	private List<Float> xRecBM = new ArrayList<Float>();
	private List<Float> xRawBM = new ArrayList<Float>();
	private List<Float> xCompBM = new ArrayList<Float>();
	private List<Float> xRecAM = new ArrayList<Float>();
	private List<Float> xRawAM = new ArrayList<Float>();
	private List<Float> xCompAM = new ArrayList<Float>();
	private List<Integer> xSegN = new ArrayList<Integer>();
	
	private List<String> eJobId = new ArrayList<String>();
	private List<String> rJobId = new ArrayList<String>();
	
	
	public MapperDataDiff(File diffMapperFile) {
		readFile(diffMapperFile);
		sortList();
	}

	private void sortList() {
								
							
		Collections.sort(mInMB);
		Collections.reverse(mInMB);
			
		Collections.sort(mInRec);
		Collections.reverse(mInRec);
		
		Collections.sort(mOutMB);
		Collections.reverse(mOutMB);
		
		Collections.sort(mOutRec);
		Collections.reverse(mOutRec);
		
		Collections.sort(mRecBM);
		Collections.reverse(mRecBM);
		
		Collections.sort(mRawBM);
		Collections.reverse(mRawBM);
		
		Collections.sort(mCompBM);
		Collections.reverse(mCompBM);
		
		Collections.sort(mRecAM);
		Collections.reverse(mRecAM);
		
		Collections.sort(mRawAM);
		Collections.reverse(mRawAM);
		
		Collections.sort(mCompAM);
		Collections.reverse(mCompAM);
		
		Collections.sort(xInMB);
		Collections.reverse(xInMB);
		
						
						
	
		Collections.sort(xInRec);
		Collections.reverse(xInRec);
		
		Collections.sort(xOutMB);
		Collections.reverse(xOutMB);
		
		Collections.sort(xOutRec);
		Collections.reverse(xOutRec);
		
		Collections.sort(xRecBM);
		Collections.reverse(xRecBM);
		
		Collections.sort(xRawBM);
		Collections.reverse(xRawBM);
		
		Collections.sort(xCompBM);
		Collections.reverse(xCompBM);
		
		Collections.sort(xRecAM);
		Collections.reverse(xRecAM);
		
		Collections.sort(xRawAM);
		Collections.reverse(xRawAM);
		
		Collections.sort(xCompAM);
		Collections.reverse(xCompAM);
		
	}

	private void readFile(File diffMapperFile) {
		try {
			BufferedReader reader = new BufferedReader(new FileReader(diffMapperFile));
			String[] title = reader.readLine().split("\t");
			String line;
			
			while((line = reader.readLine()) != null) {
				String[] value = line.split("\t");
				for(int i = 0; i < title.length; i++) 
					addValue(title[i], value[i]);
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	private void addValue(String title, String value) {
		if(title.equalsIgnoreCase("eJobId"))
			eJobId.add(value);
		else if(title.equalsIgnoreCase("rJobId"))
			rJobId.add(value);
		else if(title.equals("mSegN"))
			mSegN.add(Integer.parseInt(value));
		else if(title.equals("xSegN"))
			xSegN.add(Integer.parseInt(value));
		else {
			float v = Float.parseFloat(value);
			v = Math.abs(v); // we want to get the absolute difference
			if(title.equals("mInMB"))
				mInMB.add(v);
			else if(title.equals("mInRec"))
				mInRec.add(v);
			else if(title.equals("mOutMB"))
				mOutMB.add(v);
			else if(title.equals("mOutRec"))
				mOutRec.add(v);
			else if(title.equals("mRecBM"))
				mRecBM.add(v);
			else if(title.equals("mRawBM"))
				mRawBM.add(v);
			else if(title.equals("mCompBM"))
				mCompBM.add(v);
			else if(title.equals("mRecAM"))
				mRecAM.add(v);
			else if(title.equals("mRawAM"))
				mRawAM.add(v);
			else if(title.equals("mCompAM"))
				mCompAM.add(v);
			else if(title.equals("xInMB"))
				xInMB.add(v);
			else if(title.equals("xInRec"))
				xInRec.add(v);
			else if(title.equals("xOutMB"))
				xOutMB.add(v);
			else if(title.equals("xOutRec"))
				xOutRec.add(v);
			else if(title.equals("xRecBM"))
				xRecBM.add(v);
			else if(title.equals("xRawBM"))
				xRawBM.add(v);
			else if(title.equals("xCompBM"))
				xCompBM.add(v);
			else if(title.equals("xRecAM"))
				xRecAM.add(v);
			else if(title.equals("xRawAM"))
				xRawAM.add(v);
			else if(title.equals("xCompAM"))
				xCompAM.add(v);;
		}
	}

	public List<Float> getmInMB() {
		return mInMB;
	}

	public List<Float> getmInRec() {
		return mInRec;
	}

	public List<Float> getmOutMB() {
		return mOutMB;
	}

	public List<Float> getmOutRec() {
		return mOutRec;
	}

	public List<Float> getmRecBM() {
		return mRecBM;
	}

	public List<Float> getmRawBM() {
		return mRawBM;
	}

	public List<Float> getmCompBM() {
		return mCompBM;
	}

	public List<Float> getmRecAM() {
		return mRecAM;
	}

	public List<Float> getmRawAM() {
		return mRawAM;
	}

	public List<Float> getmCompAM() {
		return mCompAM;
	}

	public List<Integer> getmSegN() {
		return mSegN;
	}

	public List<Float> getxInMB() {
		return xInMB;
	}

	public List<Float> getxInRec() {
		return xInRec;
	}

	public List<Float> getxOutMB() {
		return xOutMB;
	}

	public List<Float> getxOutRec() {
		return xOutRec;
	}

	public List<Float> getxRecBM() {
		return xRecBM;
	}

	public List<Float> getxRawBM() {
		return xRawBM;
	}

	public List<Float> getxCompBM() {
		return xCompBM;
	}

	public List<Float> getxRecAM() {
		return xRecAM;
	}

	public List<Float> getxRawAM() {
		return xRawAM;
	}

	public List<Float> getxCompAM() {
		return xCompAM;
	}

	public List<Integer> getxSegN() {
		return xSegN;
	}

	public List<String> geteJobId() {
		return eJobId;
	}

	public List<String> getrJobId() {
		return rJobId;
	}
	
	
}
