package verification.split.gnuplot;

import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.util.List;

import javax.imageio.ImageIO;

import com.panayotis.gnuplot.JavaPlot;
import com.panayotis.gnuplot.plot.DataSetPlot;
import com.panayotis.gnuplot.style.Style;

public class SplitDiffDataflowPlot {

	private String jobName;
	private String baseDir;
	
	public SplitDiffDataflowPlot(String jobName, String baseDir) {
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
		SplitDiffDataflowPlot diffDataPlot = new SplitDiffDataflowPlot(jobName, baseDir);
		diffDataPlot.visualize();	
	}

	public void visualize() {
		String compJvmCostDir = baseDir + jobName + "/compDataflow/";
		String absolErrorImageDir = baseDir + jobName + "/ErrorImage/dataError/";
		
		String diffMapper = "compMappers.txt";
		String diffReducer = "compReducers.txt";
		
		
		File diffMapperFile = new File(compJvmCostDir + diffMapper);
		File diffReducerFile = new File(compJvmCostDir + diffReducer);
		
	
		SplitMapperDataDiff mDiff = readMapperArray(diffMapperFile);
		SplitReducerDataDiff rDiff = readReducerArray(diffReducerFile);
		
		plotMapperDiff(mDiff, absolErrorImageDir);
		plotReducerDiff(rDiff, absolErrorImageDir);	
		
		System.out.println("Finish ploting Dataflow Error");
		
	}

	private void plotMapperDiff(SplitMapperDataDiff mDiff, String absolErrorImageDir) {
	
		List<Pair> xRecBMList = mDiff.sortList("xRecBM");
		List<Pair> xBytesBMList = mDiff.sortList("xRawBM");
		List<Pair> xRecAMList = mDiff.sortList("xRecAM");
		List<Pair> xBytesAMList = mDiff.sortList("xRawAM");
		
		File imgDir = new File(absolErrorImageDir);
		if(!imgDir.exists())
			imgDir.mkdirs();
		
		BufferedImage image = plotPNG(xRecBMList, "Mapper xRecords Before Merge", "xRecBM", "Count (M)");
		outputImage(image, absolErrorImageDir + "Mapper-xRecBM.png");
		image = plotPNG(xBytesBMList, "Mapper xBytes Before Merge", "xBytesBM", "Bytes (MB)");
		outputImage(image, absolErrorImageDir + "Mapper-xBytesBM.png");
		image = plotPNG(xRecAMList, "Mapper xRecords After Merge", "xRecAM", "Count (M)");
		outputImage(image, absolErrorImageDir + "Mapper-xRecAM.png");
		image = plotPNG(xBytesAMList, "Mapper xBytes After Merge", "xBytesAM", "Bytes (MB)");
		outputImage(image, absolErrorImageDir + "Mapper-xBytesAM.png");
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

	private void plotReducerDiff(SplitReducerDataDiff rDiff, String absolErrorImageDir) {
						
		List<Pair> xShCompMBList = rDiff.sortList("xShCompMB");
		List<Pair> xShRawMBList = rDiff.sortList("xShRawMB");
		List<Pair> xInRecList = rDiff.sortList("xInRec");
		List<Pair> xInputMBList = rDiff.sortList("xInputMB");
		List<Pair> xOutRecList = rDiff.sortList("xOutRec");
		List<Pair> xOutMBList = rDiff.sortList("xOutMB");
		
		BufferedImage image = plotPNG(xShCompMBList, "Max Shuffle Compressed Bytes", "xShCompMB", "Count (M)");
		outputImage(image, absolErrorImageDir + "Reducer-abs-xShCompMB.png");
		image = plotPNG(xShRawMBList, "Max Shuffle Raw Bytes", "xShRawMB", "Bytes (MB)");
		outputImage(image, absolErrorImageDir + "Reducer-abs-xShRawMB.png");
		image = plotPNG(xInRecList, "Max Reduce Input Records", "xInRec", "Count (M)");
		outputImage(image, absolErrorImageDir + "Reducer-abs-xInRec.png");
		image = plotPNG(xInputMBList, "Max Reduce Input Bytes", "xInputMB", "Bytes (MB)");
		outputImage(image, absolErrorImageDir + "Reducer-abs-xInputMB.png");
		image = plotPNG(xOutRecList, "Max Reduce Output Records", "xOutRec", "Count (M)");
		outputImage(image, absolErrorImageDir + "Reducer-abs-xOutRec.png");
		image = plotPNG(xOutMBList, "Max Reduce Output Bytes", "xOutMB", "Bytes (MB)");
		outputImage(image, absolErrorImageDir + "Reducer-abs-xOutMB.png");
	}

	private BufferedImage plotPNG(List<Pair> list, String title, String explain, String yTitle) {
		double[][] dataSet = new double[list.size()][2];
		for(int i = 0; i < list.size(); i++) {
			dataSet[i][0] = list.get(i).getReal();
			dataSet[i][1] = list.get(i).getEstimated();
			
			//if(dataSet[i][1] - dataSet[i][0] > 100)
			//	System.out.println(dataSet[i][0] + " : " + dataSet[i][1]);
		}
		
		JavaPlot p = new JavaPlot();
            
        p.setTitle(title);
        p.getAxis("x").setLabel("Job Number");//, "Arial", 20);
        p.getAxis("y").setLabel(yTitle); 
        //p.getAxis("x").setBoundaries(0, list.get(list.size() - 1).getTime());
        p.setKey(JavaPlot.Key.TOP_LEFT);
       
        //p.set("style", "histogram clustered");
		//p.set("style", "data histogram");
		//p.set("style", "fill solid 0.4 border");
		p.set("xrange", "[0:]");
		
        DataSetPlot plot = new DataSetPlot(dataSet);
        plot.setTitle("real " + explain);
        plot.getPlotStyle().setStyle(Style.LINES);
        //plot.getPlotStyle().setLineType(3);
        plot.set("using", "1");
        p.addPlot(plot);
        
        plot = new DataSetPlot(dataSet);
        plot.setTitle("estimated " + explain);
        plot.getPlotStyle().setStyle(Style.LINES);
        //plot.getPlotStyle().setLineType(3);
        plot.set("using", "2");
        p.addPlot(plot);

        PNGTerminal t = new PNGTerminal();  
        p.setTerminal(t);   
        p.setPersist(false);
        p.plot();
        
        return t.getImage();	
	}

	private SplitMapperDataDiff readMapperArray(File diffMapperFile) {
		SplitMapperDataDiff mDiff = new SplitMapperDataDiff(diffMapperFile);
		return mDiff;
		
	}

	private SplitReducerDataDiff readReducerArray(File diffReducerFile) {	
		SplitReducerDataDiff rDiff = new SplitReducerDataDiff(diffReducerFile);
		return rDiff;
	}

}
