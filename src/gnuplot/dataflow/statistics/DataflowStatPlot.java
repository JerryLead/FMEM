package gnuplot.dataflow.statistics;

import java.awt.image.BufferedImage;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.imageio.ImageIO;

import verification.gnuplot.PNGTerminal;

import com.panayotis.gnuplot.JavaPlot;
import com.panayotis.gnuplot.dataset.GenericDataSet;
import com.panayotis.gnuplot.plot.DataSetPlot;
import com.panayotis.gnuplot.style.Style;
import com.panayotis.gnuplot.terminal.PostscriptTerminal;
import com.panayotis.iodebug.Debug;

public class DataflowStatPlot {

	private List<ArrayList<String>> mapperMatrix = new ArrayList<ArrayList<String>>();
	private List<ArrayList<String>> reducerMatrix = new ArrayList<ArrayList<String>>();
	
	private String[] mTitles;
	private String[] rTitles;
	private String mImage;
	private String rImage;
	
	public DataflowStatPlot(String mapperDf, String reducerDf, String mImage,
			String rImage) {
		readMapper(mapperDf);
		readReducer(reducerDf);
		
		this.mImage = mImage;
		this.rImage = rImage;
	}

	
	private void readMapper(String mapperDf) {
		File file = new File(mapperDf);
		try {
			BufferedReader reader = new BufferedReader(new FileReader(file));
			String title = reader.readLine();
			mTitles = title.trim().split("\\s+");
			
			String line;
			while((line = reader.readLine()) != null) {
				String[] s = line.trim().split("\\s+");
				ArrayList<String> list = new ArrayList<String>();
				for(String str : s)
					list.add(str);
				mapperMatrix.add(list);
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
	
	public void plot() {
		//plotMapperPNG();
		//plotReducerPNG();
		
		plotMapperEPS();
		plotReducerEPS();
	}
	
	private void plotMapperEPS() {
		plotDfEPS(mapperMatrix, mTitles, true, mImage + ".eps");
		
	}


	private void plotReducerEPS() {
		plotDfEPS(reducerMatrix, rTitles, false, rImage + ".eps");
		
	}


	private void plotMapperPNG() {
		BufferedImage image = plotDfPNG(mapperMatrix, mTitles, true);
		outputImage(image, mImage + ".png");
	}


	private void plotReducerPNG() {
		BufferedImage image = plotDfPNG(reducerMatrix, rTitles, false);
		outputImage(image, rImage + ".png");
	}
	
	private void outputImage(BufferedImage plotPNG, String file) {
		
		try {
			ImageIO.write(plotPNG, "PNG", new File(file));
			plotPNG.flush();  
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}  
	}

	private void readReducer(String reducerDf) {
		File file = new File(reducerDf);
		try {
			BufferedReader reader = new BufferedReader(new FileReader(file));
			String title = reader.readLine();
			rTitles = title.trim().split("\\s+");
			
			String line;
			while((line = reader.readLine()) != null) {
				String[] s = line.trim().split("\\s+");
				ArrayList<String> list = new ArrayList<String>();
				
				for(String str : s)
					list.add(str);
				reducerMatrix.add(list);
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


	public static BufferedImage plotDfPNG(List<ArrayList<String>> list, String[] titles, 
			boolean mapper) {
		JavaPlot p = new JavaPlot();
      
		JavaPlot.getDebugger().setLevel(Debug.VERBOSE);
		
        p.set("xtics", "rotate by -45");
		p.set("grid", "ytics");
		p.set("style", "histogram errorbars gap 2 lw 1");
		p.set("style fill", "solid 1.00 border 0");
		//p.set("style", "data histogram");
		
		if(mapper)
			p.setTitle("Mapper Dataflow Absolute Error");
		else
			p.setTitle("Reducer Dataflow Absolute Error");
		p.getAxis("x").setLabel("Applications");//, "Arial", 20);
        p.getAxis("y").setLabel("Absolute Error (100%)");
        p.set("xrange", "[-0.5:]");
        p.set("yrange", "[0:]");
        //p.getAxis("x").setBoundaries(0, list.get(list.size() - 1).getTime());
        p.setKey(JavaPlot.Key.TOP_LEFT);

        GenericDataSet dataSet = new GenericDataSet(true);
        dataSet.addAll(list);

        
        DataSetPlot plot = new DataSetPlot(dataSet);
        plot.set("using", "2:3:xtic(1)");
        plot.setTitle(titles[1]);
        plot.getPlotStyle().setStyle(Style.HISTOGRAMS);
        //plot.getPlotStyle().set("lc", "rgb 'red'");
        p.addPlot(plot);
        
       
        plot = new DataSetPlot(dataSet);
        plot.set("using", "4:5:xtic(1)");
        plot.setTitle(titles[3]);
        plot.getPlotStyle().setStyle(Style.HISTOGRAMS);
        p.addPlot(plot);
       
        if(mapper) {
        	plot = new DataSetPlot(dataSet);
            plot.set("using", "6:7:xtic(1)");
            plot.setTitle(titles[5]);
            plot.getPlotStyle().setStyle(Style.HISTOGRAMS);
            p.addPlot(plot);
            
            plot = new DataSetPlot(dataSet);
            plot.set("using", "8:9:xtic(1)");
            plot.setTitle(titles[7]);
            plot.getPlotStyle().setStyle(Style.HISTOGRAMS);
            p.addPlot(plot);
           
        }
        
      
        PNGTerminal t = new PNGTerminal();  
        p.setTerminal(t);   
        p.setPersist(false);
        p.plot();
        
        return t.getImage();

	}
	
	public void plotDfEPS(List<ArrayList<String>> list, String[] titles, 
			boolean mapper, String imageFile) {
		JavaPlot p = new JavaPlot();
      
		JavaPlot.getDebugger().setLevel(Debug.VERBOSE);
		
        p.set("xtics", "rotate by -45");
		p.set("grid", "ytics");
		p.set("style", "histogram errorbars gap 2 lw 1");
		p.set("style fill", "solid 1.00 border 0");
		//p.set("style", "data histogram");
		
		if(mapper)
			p.setTitle("Mapper Dataflow Absolute Error");
		else
			p.setTitle("Reducer Dataflow Absolute Error");
		p.getAxis("x").setLabel("Applications");//, "Arial", 20);
        p.getAxis("y").setLabel("Absolute Error (100%)");
        p.set("xrange", "[-0.5:]");
        p.set("yrange", "[0:]");
        //p.getAxis("x").setBoundaries(0, list.get(list.size() - 1).getTime());
        p.setKey(JavaPlot.Key.TOP_LEFT);

        GenericDataSet dataSet = new GenericDataSet(true);
        dataSet.addAll(list);

        
        DataSetPlot plot = new DataSetPlot(dataSet);
        plot.set("using", "2:3:xtic(1)");
        plot.setTitle(titles[1]);
        plot.getPlotStyle().setStyle(Style.HISTOGRAMS);
        //plot.getPlotStyle().set("lc", "rgb 'red'");
        p.addPlot(plot);
        
       
        plot = new DataSetPlot(dataSet);
        plot.set("using", "4:5:xtic(1)");
        plot.setTitle(titles[3]);
        plot.getPlotStyle().setStyle(Style.HISTOGRAMS);
        p.addPlot(plot);
       
        if(mapper) {
        	plot = new DataSetPlot(dataSet);
            plot.set("using", "6:7:xtic(1)");
            plot.setTitle(titles[5]);
            plot.getPlotStyle().setStyle(Style.HISTOGRAMS);
            p.addPlot(plot);
            
            plot = new DataSetPlot(dataSet);
            plot.set("using", "8:9:xtic(1)");
            plot.setTitle(titles[7]);
            plot.getPlotStyle().setStyle(Style.HISTOGRAMS);
            p.addPlot(plot);
           
        }
        
      
        PostscriptTerminal epsf = new PostscriptTerminal(imageFile);
        epsf.setColor(true);
        p.setTerminal(epsf);

        p.plot();
	}
	
	public static void main(String[] args) {
		String baseDir = "G:\\MR-MEM\\CompExperiments\\graph\\";
		String mapperDf = baseDir + "mapperDfStat.txt";
		String reducerDf = baseDir + "reducerDfStat.txt";
		
		String mImage = baseDir + "mDfImage";
		String rImage = baseDir + "rDfImage";
		
		DataflowStatPlot p = new DataflowStatPlot(mapperDf, reducerDf, mImage, rImage);
		p.plot();
	}
}
