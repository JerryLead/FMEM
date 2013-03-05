package verification.gnuplot;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ReducerDataDiff {

	private List<Float> mShCompMB = new ArrayList<Float>();
	private List<Float> mShRawMB = new ArrayList<Float>();
	private List<Float> mInRec = new ArrayList<Float>();
	private List<Float> mInputMB = new ArrayList<Float>();
	private List<Float> mOutRec	= new ArrayList<Float>();
	private List<Float> mOutMB = new ArrayList<Float>();
	private List<Float> xShCompMB = new ArrayList<Float>();
	private List<Float> xShRawMB = new ArrayList<Float>();
	private List<Float> xInRec = new ArrayList<Float>();
	private List<Float> xInputMB = new ArrayList<Float>();
	private List<Float> xOutRec = new ArrayList<Float>();
	private List<Float> xOutMB = new ArrayList<Float>();
	private List<String> eJobId = new ArrayList<String>();
	private List<String> rJobId = new ArrayList<String>();
	
	public ReducerDataDiff(File diffReducerFile) {
		readFile(diffReducerFile);
		sortList();
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
	
		else {
			float v = Float.parseFloat(value);
			v = Math.abs(v); // we want the absolute value
			if(title.equals("mShCompMB"))
				mShCompMB.add(v);
			else if(title.equals("mShRawMB"))
				mShRawMB.add(v);
			else if(title.equals("mInRec"))
				mInRec.add(v);
			else if(title.equals("mInputMB"))
				mInputMB.add(v);
			else if(title.equals("mOutRec"))
				mOutRec.add(v);
			else if(title.equals("mOutMB"))
				mOutMB.add(v);
			else if(title.equals("xShCompMB"))
				xShCompMB.add(v);
			else if(title.equals("xShRawMB"))
				xShRawMB.add(v);
			else if(title.equals("xInRec"))
				xInRec.add(v);
			else if(title.equals("xInputMB"))
				xInputMB.add(v);
			else if(title.equals("xOutRec"))
				xOutRec.add(v);
			else if(title.equals("xOutMB"))
				xOutMB.add(v);
		}
	}
	
	private void sortList() {
		
		Collections.sort(mShCompMB);
		Collections.reverse(mShCompMB);
			
		Collections.sort(mShRawMB);
		Collections.reverse(mShRawMB);
		
		Collections.sort(mInRec);
		Collections.reverse(mInRec);
		
		Collections.sort(mInRec);
		Collections.reverse(mInRec);
		
		Collections.sort(mInputMB);
		Collections.reverse(mInputMB);
		
		Collections.sort(mOutRec);
		Collections.reverse(mOutRec);
		
		Collections.sort(mOutMB);
		Collections.reverse(mOutMB);
		
		Collections.sort(xShCompMB);
		Collections.reverse(xShCompMB);
		
		Collections.sort(xShRawMB);
		Collections.reverse(xShRawMB);
		
		Collections.sort(xInRec);
		Collections.reverse(xInRec);
		
		Collections.sort(xInputMB);
		Collections.reverse(xInputMB);
		
		Collections.sort(xOutRec);
		Collections.reverse(xOutRec);
		
		Collections.sort(xOutMB);
		Collections.reverse(xOutMB);
	}

	public List<Float> getmShCompMB() {
		return mShCompMB;
	}

	public List<Float> getmShRawMB() {
		return mShRawMB;
	}

	public List<Float> getmInRec() {
		return mInRec;
	}

	public List<Float> getmInputMB() {
		return mInputMB;
	}

	public List<Float> getmOutRec() {
		return mOutRec;
	}

	public List<Float> getmOutMB() {
		return mOutMB;
	}

	public List<Float> getxShCompMB() {
		return xShCompMB;
	}

	public List<Float> getxShRawMB() {
		return xShRawMB;
	}

	public List<Float> getxInRec() {
		return xInRec;
	}

	public List<Float> getxInputMB() {
		return xInputMB;
	}

	public List<Float> getxOutRec() {
		return xOutRec;
	}

	public List<Float> getxOutMB() {
		return xOutMB;
	}

	public List<String> geteJobId() {
		return eJobId;
	}

	public List<String> getrJobId() {
		return rJobId;
	}
	
	
}
