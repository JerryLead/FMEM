package verification.gnuplot;

import java.util.List;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;

import javax.imageio.ImageIO;

import com.panayotis.gnuplot.JavaPlot;
import com.panayotis.gnuplot.plot.DataSetPlot;
import com.panayotis.gnuplot.style.Style;

public class DiffJvmCostPlot {
	private String jobName;
	private String baseDir;
	
	public DiffJvmCostPlot(String jobName, String baseDir) {
		this.jobName = jobName;
		this.baseDir = baseDir;
	}
	
	public static void main(String[] args) {
		String jobName = "Wiki-m36-r18-256MB";
		String baseDir = "/home/xulijie/MR-MEM/NewExperiments/";
		//String compJvmCostDir = "/home/xulijie/MR-MEM/NewExperiments/uservisits_aggre-pig-256MB/compJvmCost/";
		//String absolErrorImageDir = "/home/xulijie/MR-MEM/NewExperiments/uservisits_aggre-pig-256MB/AbsolErrorImage/";
		
		//String compJvmCostDir = "/home/xulijie/MR-MEM/NewExperiments/Wiki-m36-r18-256MB/compJvmCost/";
		//String absolErrorImageDir = "/home/xulijie/MR-MEM/NewExperiments/Wiki-m36-r18-256MB/AbsolErrorImage/";
		DiffJvmCostPlot diffJvmPlot = new DiffJvmCostPlot(jobName, baseDir);
		diffJvmPlot.visualize();	
	}

	public void visualize() {
		String compJvmCostDir = baseDir + jobName + "/compJvmCost/";
		String diffImageDir = baseDir + jobName + "/ErrorImage/jvmCostError/";
		
		String diffMapper = "diffMappers.txt";
		String diffReducer = "diffReducers.txt";
		
		
		File diffMapperFile = new File(compJvmCostDir + diffMapper);
		File diffReducerFile = new File(compJvmCostDir + diffReducer);
		
	
		MapperJvmCostDiff mDiff = readMapperArray(diffMapperFile);
		ReducerJvmCostDiff rDiff = readReducerArray(diffReducerFile);
		
		plotMapperDiff(mDiff, diffImageDir);
		plotReducerDiff(rDiff, diffImageDir);	
		
		System.out.println("Finish ploting Jvm Cost Difference");
		
	}

	private void plotMapperDiff(MapperJvmCostDiff mDiff, String diffImageDir) {
		List<Float> xOUdfList = mDiff.getxOUdfList();
		List<Float> xNGUdfList = mDiff.getxNGUdfList();
		List<Float> RSSdfList = mDiff.getRSSdfList();
		List<Float> xHeapUdfList = mDiff.getxHeapUdfList();
		
		String absImgDir = diffImageDir + "AbsoluteError/";
		File absImgFile = new File(absImgDir);
		if(!absImgFile.exists())
			absImgFile.mkdirs();
		
		BufferedImage image = plotPNG(xOUdfList, "Mapper xOU Absolute Error", "xOU", "Difference (MB)");
		outputImage(image, absImgDir + "Mapper-abs-xOU.png");
		image = plotPNG(xNGUdfList, "Mapper xNGU Absolute Error", "xNGU", "Difference (MB)");
		outputImage(image, absImgDir + "Mapper-abs-xNGU.png");
		image = plotPNG(RSSdfList, "Mapper RSS Absolute Error", "RSS", "Difference (MB)");
		outputImage(image, absImgDir + "Mapper-abs-RSS.png");
		image = plotPNG(xHeapUdfList, "Mapper xHeapU Absolute Error", "xHeapU", "Difference (MB)");
		outputImage(image, absImgDir + "Mapper-abs-xHeapU.png");
		
		
		List<Float> xOUrtList = mDiff.getxOUrtList();
		List<Float> xNGUrtList = mDiff.getxNGUrtList();
		List<Float> RSSrtList = mDiff.getRSSrtList();
		List<Float> xHeapUrtList = mDiff.getxHeapUrtList();
		
		String relImgDir = diffImageDir + "RelativeError/";
		File relImgDirFile = new File(relImgDir);
		if(!relImgDirFile.exists())
			relImgDirFile.mkdirs();
		
		image = plotPNG(xOUrtList, "Mapper xOU Relative Error", "xOU", "Percentage Error (100%)");
		outputImage(image, relImgDir + "Mapper-rel-xOU.png");
		image = plotPNG(xNGUrtList, "Mapper xNGU Relative Error", "xNGU", "Percentage Error (100%)");
		outputImage(image, relImgDir + "Mapper-rel-xNGU.png");
		image = plotPNG(RSSrtList, "Mapper RSS Relative Error", "RSS", "Percentage Error (100%)");
		outputImage(image, relImgDir + "Mapper-rel-RSS.png");
		image = plotPNG(xHeapUrtList, "Mapper xHeapU Relative Error", "xHeapU", "Percentage Error (100%)");
		outputImage(image, relImgDir + "Mapper-rel-xHeapU.png");
	}
	
	private void outputImage(BufferedImage plotPNG, String title) {
		
		try {
			ImageIO.write(plotPNG, "PNG", new File(title));
			plotPNG.flush();  
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}  
	}

	private void plotReducerDiff(ReducerJvmCostDiff rDiff, String diffImageDir) {
		List<Float> xOUdfList = rDiff.getxOUdfList();
		List<Float> xNGUdfList = rDiff.getxNGUdfList();
		List<Float> RSSdfList = rDiff.getRSSdfList();
		List<Float> xHeapUdfList = rDiff.getxHeapUdfList();
		
		String absImgDir = diffImageDir + "AbsoluteError/";
		File absImgFile = new File(absImgDir);
		if(!absImgFile.exists())
			absImgFile.mkdirs();
		
		BufferedImage image = plotPNG(xOUdfList, "Reducer xOU Absolute Error", "xOU", "Difference (MB)");
		outputImage(image, absImgDir + "Reducer-abs-xOU.png");
		image = plotPNG(xNGUdfList, "Reducer xNGU Absolute Error", "xNGU", "Difference (MB)");
		outputImage(image, absImgDir + "Reducer-abs-xNGU.png");
		image = plotPNG(RSSdfList, "Reducer RSS Absolute Error", "RSS", "Difference (MB)");
		outputImage(image, absImgDir + "Reducer-abs-RSS.png");
		image = plotPNG(xHeapUdfList, "Reducer xHeapU Absolute Error", "xHeapU", "Difference (MB)");
		outputImage(image, absImgDir + "Reducer-abs-xHeapU.png");
		
		
		
		List<Float> xOUrtList = rDiff.getxOUrtList();
		List<Float> xNGUrtList = rDiff.getxNGUrtList();
		List<Float> RSSrtList = rDiff.getRSSrtList();
		List<Float> xHeapUrtList = rDiff.getxHeapUrtList();
		
		String relImgDir = diffImageDir + "RelativeError/";
		File relImgDirFile = new File(relImgDir);
		if(!relImgDirFile.exists())
			relImgDirFile.mkdirs();
		
		image = plotPNG(xOUrtList, "Reducer xOU Relative Error", "xOU", "Percentage Error (100%)");
		outputImage(image, relImgDir + "Reducer-rel-xOU.png");
		image = plotPNG(xNGUrtList, "Reducer xNGU Relative Error", "xNGU", "Percentage Error (100%)");
		outputImage(image, relImgDir + "Reducer-rel-xNGU.png");
		image = plotPNG(RSSrtList, "Reducer RSS Relative Error", "RSS", "Percentage Error (100%)");
		outputImage(image, relImgDir + "Reducer-rel-RSS.png");
		image = plotPNG(xHeapUrtList, "Reducer xHeapU Relative Error", "xHeapU", "Percentage Error (100%)");
		outputImage(image, relImgDir + "Reducer-rel-xHeapU.png");
	}

	private BufferedImage plotPNG(List<Float> rtList, String title, String explain, String yTitle) {
		double[][] dataSet = new double[rtList.size()][1];
		for(int i = 0; i < rtList.size(); i++)
			dataSet[i][0] = rtList.get(i);
		
		
		JavaPlot p = new JavaPlot();
            
        p.setTitle(title);
        p.getAxis("x").setLabel("Job Number");//, "Arial", 20);
        //p.getAxis("y").setLabel("Percentage Error (100%)"); 
        p.getAxis("y").setLabel(yTitle); 
        //p.getAxis("x").setBoundaries(0, list.get(list.size() - 1).getTime());
        p.setKey(JavaPlot.Key.TOP_RIGHT);
       
        p.set("style", "histogram clustered");
		p.set("style", "data histogram");
		p.set("style", "fill solid 0.4 border");
		p.set("xrange", "[0:]");
		
        DataSetPlot plot = new DataSetPlot(dataSet);
        plot.setTitle(explain);
        plot.getPlotStyle().setStyle(Style.BOXES);
        plot.getPlotStyle().setLineType(3);
        plot.set("using", "1");
        p.addPlot(plot);

        PNGTerminal t = new PNGTerminal();  
        p.setTerminal(t);   
        p.setPersist(false);
        p.plot();
        
        return t.getImage();	
	}

	private MapperJvmCostDiff readMapperArray(File diffMapperFile) {
		MapperJvmCostDiff mDiff = new MapperJvmCostDiff(diffMapperFile);
		return mDiff;
		
	}

	private ReducerJvmCostDiff readReducerArray(File diffReducerFile) {	
		ReducerJvmCostDiff rDiff = new ReducerJvmCostDiff(diffReducerFile);
		return rDiff;
	}

	
}
