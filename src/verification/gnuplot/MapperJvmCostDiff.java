package verification.gnuplot;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class MapperJvmCostDiff {
			
	private List<Float> xOUdfList = new ArrayList<Float>();
	private List<Float> mNGUdfList = new ArrayList<Float>();
	private List<Float> xNGUdfList = new ArrayList<Float>();
	private List<Float> xHeapUdfList = new ArrayList<Float>();
	private List<Float> RSSdfList = new ArrayList<Float>();
	
	private List<Float> xOUrtList = new ArrayList<Float>();
	private List<Float> mNGUrtList = new ArrayList<Float>();
	private List<Float> xNGUrtList = new ArrayList<Float>();
	private List<Float> xHeapUrtList = new ArrayList<Float>();
	private List<Float> RSSrtList = new ArrayList<Float>();
	
	/*
	private float rxOU;
	private float exOU;
	private float rmNGU;
	private float rxNGU;
	private float exNGU;
	private float rxHeapU;
	private float rxRSS;
	private float exHeapU;
	*/

	public MapperJvmCostDiff(File diffMapperFile) {
		readFile(diffMapperFile);
		sortList();
	}


	private void sortList() {
		Collections.sort(xOUdfList);
		Collections.reverse(xOUdfList);
			
		Collections.sort(mNGUdfList);
		Collections.reverse(mNGUdfList);
		
		Collections.sort(xNGUdfList);
		Collections.reverse(xNGUdfList);
		
		Collections.sort(xHeapUdfList);
		Collections.reverse(xHeapUdfList);
		
		Collections.sort(RSSdfList);
		Collections.reverse(RSSdfList);
		
		Collections.sort(xOUrtList);
		Collections.reverse(xOUrtList);
		
		Collections.sort(mNGUrtList);
		Collections.reverse(mNGUrtList);
		
		Collections.sort(xNGUrtList);
		Collections.reverse(xNGUrtList);
		
		Collections.sort(xHeapUrtList);
		Collections.reverse(xHeapUrtList);
		
		Collections.sort(RSSrtList);
		Collections.reverse(RSSrtList);
		
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
			
			reader.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}


	private void addValue(String title, String value) {
		if(title.equalsIgnoreCase("eJobId") || title.equalsIgnoreCase("rJobId"))
			return;
		float v = Float.parseFloat(value);
		if(title.equals("xOUdf"))
			xOUdfList.add(v);
		else if(title.equals("mNGUdf"))
			mNGUdfList.add(v);
		else if(title.equals("xNGUdf"))
			xNGUdfList.add(v);
		else if(title.equals("xHeapUdf"))
			xHeapUdfList.add(v);
		else if(title.equals("RSSdf"))
			RSSdfList.add(v);
		
		else if(title.equals("xOUrt"))
			xOUrtList.add(v);
		else if(title.equals("mNGUrt"))
			mNGUrtList.add(v);
		else if(title.equals("xNGUrt"))
			xNGUrtList.add(v);
		else if(title.equals("xHeapUrt"))
			xHeapUrtList.add(v);
		else if(title.equals("RSSrt"))
			RSSrtList.add(v);
		
	}


	public List<Float> getxOUdfList() {
		return xOUdfList;
	}


	public List<Float> getmNGUdfList() {
		return mNGUdfList;
	}


	public List<Float> getxNGUdfList() {
		return xNGUdfList;
	}


	public List<Float> getxHeapUdfList() {
		return xHeapUdfList;
	}


	public List<Float> getRSSdfList() {
		return RSSdfList;
	}


	public List<Float> getxOUrtList() {
		return xOUrtList;
	}


	public List<Float> getmNGUrtList() {
		return mNGUrtList;
	}


	public List<Float> getxNGUrtList() {
		return xNGUrtList;
	}


	public List<Float> getxHeapUrtList() {
		return xHeapUrtList;
	}


	public List<Float> getRSSrtList() {
		return RSSrtList;
	}

	
}
