package verification.all;

import verification.dataflow.DataflowComparator;
import verification.gnuplot.DiffDataflowPlot;
import verification.gnuplot.DiffJvmCostPlot;
import verification.jvmcost.JvmCostComparator;

public class DataJvmCompAndVisualizer {

	public static void main(String[] args) {
		//String jobName = "BuildCompIndex-m36-r18-256MB";
		//String jobName = "BuildCompIndex-m36-r18-256MB";
		//String jobName = "uservisits_aggre-pig-256MB";
		//String jobName = "BuildCompIndex-m36-r18";
		//String jobName = "Wiki-m36-r18";
		//String jobName = "BigTwitterInDegreeCount";
		//String jobName = "big-uservisits_aggre-pig-256MB";
		//String jobName = "BigTwitterBiDirectEdgeCount";
		//String jobName = "BigTeraSort";
		//String jobName = "BigTeraSort-36GB";
		//String jobName = "SampleTeraSort-1G";
		String jobName = "Big-uservisits_aggre-pig-50G";
		//String jobName = "big-uservisits_aggre-pig-50G-256MB";
		
		String baseDir = "/home/xulijie/MR-MEM/BigExperiments/";
		//String baseDir = "/home/xulijie/MR-MEM/SampleExperiments/";
		//String baseDir = "/home/xulijie/MR-MEM/Test/";

		int splitSizeMB = 256; //set the split size to filter the finished mappers
		boolean isJobIdInEstiDir = true;
		
		DataflowComparator dataComp = new DataflowComparator(jobName, baseDir);
		dataComp.compareDataflow();
		
		JvmCostComparator jvmComp = new JvmCostComparator(jobName, baseDir, splitSizeMB, isJobIdInEstiDir);
		jvmComp.compareJvmCost();
		
		DiffJvmCostPlot jvmPlot = new DiffJvmCostPlot(jobName, baseDir);
		jvmPlot.visualize();
		
		DiffDataflowPlot dataPlot = new DiffDataflowPlot(jobName, baseDir);
		dataPlot.visualize();
		
		System.out.println("Finished!");	
	}
}
