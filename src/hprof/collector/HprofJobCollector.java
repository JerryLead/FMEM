package hprof.collector;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import profile.job.JobTasksParser;

import html.parser.link.LinksSaver;
import html.util.HtmlFetcher;

public class HprofJobCollector {

	private static Map<String, String> map;
	private static List<String[]> memList;
	private static List<String[]> cpuList;

	public static void main(String[] args) {
		String startJobId = "job_201302022034_0052";
		String hostname = "master";
		
		int iterateNum = 2;
		int mapperProfNum = 3;
		int reducerProfNum = 3;
		
		boolean hasCPU = false;
		
		String jobDir = "/home/xulijie/MR-MEM/HprofBigExperiments/";
		//String jobName = "HprofBigWiki-m36-r18";
		//String jobName = "HprofBigTeraSort-36GB";
		//String jobName = "Standard-Big-uservisits_aggre-pig-50G";
		String jobName = "HprofBig-uservisits_aggre-pig-50G";
		
		String hprofDir = jobDir + jobName + "/hprofLog/";
		String simpleDir = jobDir + jobName + "/simpleLog/";

		String prefix = startJobId.substring(0, startJobId.length() - 4);
		int suffix = Integer.parseInt(startJobId.substring(startJobId.length() - 4));

		DecimalFormat nf = new DecimalFormat("0000");

		String jobId;

		for (int i = 0; i < iterateNum; i++) {
			jobId = prefix + nf.format(suffix + i);
			
			LinksSaver linksSaver = new LinksSaver(hostname, jobId);
			boolean jobFailed = initLinksSaver(linksSaver);
			if(jobFailed == false) {
				System.err.println("[" + jobId + "] is a failed job");
				continue;
			}
			
			hprof(linksSaver, mapperProfNum, reducerProfNum, hprofDir + jobId, simpleDir + jobId, hasCPU);
			
			System.out.println("Finish profiling " + jobId);
		}

		

	}

	private static void hprof(LinksSaver linksSaver, int mapperProfNum, int reducerProfNum, 
			String hprofDir, String simpleDir, boolean hasCPU) {
		String mapperUrl;
		String reducerUrl;
		
		// /home/xulijie/MR-MEM/HprofBigExperiments/HprofBigWiki-m36-r18/hprofLog/jobId/
		File jobIdHprofFile = new File(hprofDir);
		if(!jobIdHprofFile.exists())
			jobIdHprofFile.mkdirs();
		
		File jobIdSimpleFile = new File(simpleDir);
		if(!jobIdSimpleFile.exists())
			jobIdSimpleFile.mkdirs();
		
		for(int i = 0; i < mapperProfNum; i++) {
			List<String> countersList = new ArrayList<String>();
			mapperUrl = getMapperLogUrl(linksSaver.getMap_tasks_list().get(i), countersList);
			//System.out.println(mapperUrl);
			String taskLink = linksSaver.getMap_tasks_list().get(i);
			genMemAndCPUInfo(mapperUrl, countersList, jobIdHprofFile, 
					jobIdSimpleFile, taskLink.substring(taskLink.lastIndexOf('=') + 1), hasCPU);
		}
		
		for(int i = 0; i < reducerProfNum; i++) {
			List<String> countersList = new ArrayList<String>();
			reducerUrl = getReducerLogUrl(linksSaver.getReduce_tasks_list().get(i), countersList);
			//System.out.println(reducerUrl);
			String taskLink = linksSaver.getReduce_tasks_list().get(i);
			genMemAndCPUInfo(reducerUrl, countersList, jobIdHprofFile, 
					jobIdSimpleFile, taskLink.substring(taskLink.lastIndexOf('=') + 1), hasCPU);
		}
	}

	private static String getReducerLogUrl(String reduceTaskDetailsJsp, List<String> countersList) {
		Document reduceDetails = HtmlFetcher.getHtml(reduceTaskDetailsJsp);
		Element tr = null;
		for(Element elem : reduceDetails.getElementsByTag("tbody").first()
				.children()) {
			if(elem.child(2).text().equals("SUCCEEDED")) {
				tr = elem;
				break;
			}
		}
		
		//String taskId = tr.child(0).text();
		
		String countersLink = tr.child(10).child(0).absUrl("href");
		Document countersDoc = HtmlFetcher.getHtml(countersLink);
		Elements countersTrs = countersDoc.getElementsByTag("tbody").first().children();
		
		for(Element elem : countersTrs) {
			
			if(elem.getElementsByTag("td").size() == 1) {
				countersList.add(" ");
				String title = elem.text();
				
				countersList.add(title);
				countersList.add(" ");
				
			}
			if(elem.getElementsByTag("td").size() == 3) {
				String valueStr = elem.child(2).text();
				String name = elem.child(1).text();
				//Long value = Long.parseLong(valueStr.replaceAll(",", ""));
				countersList.add(name + "\t" + valueStr);
			}
		}

		String logLink = tr.child(9).child(4).absUrl("href");
		
		return logLink;
	}

	private static String getMapperLogUrl(String mapTaskDetailsJsp, List<String> countersList) {
		Document mapDetails = HtmlFetcher.getHtml(mapTaskDetailsJsp);
		
		Element tr = null;
		for(Element elem : mapDetails.getElementsByTag("tbody").first().children()) {
			if(elem.child(2).text().equals("SUCCEEDED")) {
				tr = elem;
				break;
			}
		}
		
		String counterLink = tr.child(8).child(0).absUrl("href");
		Document countersDoc = HtmlFetcher.getHtml(counterLink);
		Elements countersTrs = countersDoc.getElementsByTag("tbody").first().children();
		
		for(Element elem : countersTrs) {
			
			if(elem.getElementsByTag("td").size() == 1) {
				String title = elem.text();
				countersList.add(" ");
				countersList.add(title);
				countersList.add(" ");
			}
			if(elem.getElementsByTag("td").size() == 3) {
				String valueStr = elem.child(2).text();
				String name = elem.child(1).text();
				//Long value = Long.parseLong(valueStr.replaceAll(",", ""));
				countersList.add(name + "\t" + valueStr);
			}
		}
		
		String logLink = tr.child(7).child(4).absUrl("href");
		return logLink;
	}

	private static boolean initLinksSaver(LinksSaver linksSaver) {
		String jobdetaisUrl = linksSaver.getJobdetails_jsp();
		Document wholeJspDoc = html.util.HtmlFetcher.getHtml(jobdetaisUrl);
		Element body = wholeJspDoc.getElementsByTag("body").first();
		
		Element statusElem = body.select(":containsOwn(Status:)").first();
		String status = statusElem.nextSibling().toString().trim();
		if(status.equals("Failed"))
			return false;		

		JobTasksParser.parseJobTasks(linksSaver); //initiate map/reduce tasks link list
		return true;
		
	}

	private static void genMemAndCPUInfo(String url, List<String> countersList, 
			File jobIdHprofFile, File jobIdSimpleFile, String taskId, boolean hasCPU) {
		
		map = new HashMap<String, String>();
		memList = new ArrayList<String[]>();
		cpuList = new ArrayList<String[]>();
		
		try {
			PrintWriter profWriter = new PrintWriter(new BufferedWriter(new FileWriter(new File(jobIdHprofFile, taskId + ".txt"))));
			PrintWriter simpleWriter = new PrintWriter(new BufferedWriter(new FileWriter(new File(jobIdSimpleFile, taskId + ".txt"))));
			
			for(String line : countersList) 
				profWriter.println(line);
			profWriter.println();
			
			Document doc = HtmlFetcher.getHtml(url);
			Element profile = doc.getElementsByTag("pre").last();
			String[] lines = profile.text().split("\\n");
			// System.out.println(lines.length);

			for (int i = 0; i < lines.length; i++) {
				if(hasCPU) {
					if (lines[i].startsWith("TRACE") && lines[i].contains("thread=")) {
						String traceId = lines[i].substring(lines[i].indexOf(' ') + 1,
								lines[i].indexOf(':'));
						int j;
						for (j = i + 1; j < lines.length; j++) {
							if (lines[j].startsWith("SITES BEGIN")
									|| lines[j].startsWith("TRACE")
									&& lines[j].contains("thread="))
								break;
						}

						int start = i;
						int end = j;

						map.put(traceId, start + " " + end);
						i = end - 1;
					}
				}
				
				else {
					if (lines[i].startsWith("TRACE") && lines[i].endsWith(":")) {
						String traceId = lines[i].substring(lines[i].indexOf(' ') + 1,
								lines[i].indexOf(':'));
						int j;
						for (j = i + 1; j < lines.length; j++) {
							if (lines[j].startsWith("SITES BEGIN")
									|| lines[j].startsWith("TRACE")
									&& lines[j].endsWith(":"))
								break;
						}

						int start = i;
						int end = j;

						map.put(traceId, start + " " + end);
						i = end - 1;
					}
				}

				if (lines[i].startsWith(" rank")) {
					int j;
					for (j = i; j < lines.length; j++) {
						if (lines[j].startsWith("SITES END"))
							break;
						String[] items = lines[j].trim().split("\\s+");
						assert (items.length == 9);

						memList.add(items);

					}
					i = j;
				}

				if (lines[i].startsWith("CPU SAMPLES BEGIN")) {
					int j;
					for (j = i + 1; j < lines.length; j++) {
						String[] items = lines[j].trim().split("\\s+");
						assert (items.length == 6);

						cpuList.add(items);
						if (lines[j].startsWith("CPU SAMPLES END"))
							break;
					}
					i = j;
				}

			}
			outputInfos(lines, profWriter, false, hasCPU);
			outputInfos(lines, simpleWriter, true, hasCPU);
		
			profWriter.close();
			simpleWriter.close();
		
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}

	private static void outputInfos(String lines[], PrintWriter writer, boolean simple, boolean hasCPU) {
		if(simple == false)
			writer.println("Memory Samples:");
		
		for(int i = 0; i < memList.get(0).length; i++)
			writer.print(memList.get(0)[i] + "\t");
		
		writer.println("currMB" + "\t" + "currObjM" + "\t" + "totalMB" + "\t" + "totalObjM");
		
		String f1 = "%1$-3.2f";
		for (int t = 1; t < memList.size(); t++) {
			String[] memItems = memList.get(t);
			writer.print(memItems[0]);
			for (int i = 1; i < memItems.length; i++)
				writer.print("\t" + memItems[i]);
			
			double currMB = ((double) Long.parseLong(memItems[3])) / 1024 / 1024;
			double currObjM = ((double) Long.parseLong(memItems[4])) / 1000 / 1000;
			double totalMB = ((double) Long.parseLong(memItems[5])) / 1024 / 1024;
			double totalObjM = ((double) Long.parseLong(memItems[6])) / 1000 / 1000;
			
			writer.print("\t" + String.format(f1, currMB) + "\t" + String.format(f1, currObjM) + "\t" 
					+ String.format(f1, totalMB) + "\t" + String.format(f1, totalObjM));
			writer.println();
		}

		if(simple == true)
			return;
		
		writer.println("Memory Trace:");

		for (int i = 1; i < memList.size(); i++) {
			String[] memItems = memList.get(i);
			String threadId = memItems[7];
			String value = map.get(threadId);
			assert (value != null);

			String[] index = value.split("\\s+");
			int start = Integer.parseInt(index[0]);
			int end = Integer.parseInt(index[1]);

			for (int j = start; j < end; j++) {
				writer.println(lines[j]);
			}
			writer.println();
		}

		if(hasCPU) {
			writer.println("CPU Samples:");

			for (int i = 0; i <= 20 && i < cpuList.size(); i++) {
				String[] cpuItems = cpuList.get(i);
				writer.print(cpuItems[0]);
				for (int j = 1; j < cpuItems.length; j++)
					writer.print("\t" + cpuItems[j]);
				writer.println();
			}

			writer.println("CPU Trace:");

			for (int i = 1; i <= 10 && i < cpuList.size(); i++) {
				String[] cpuItems = cpuList.get(i);
				String threadId = cpuItems[4];
				String value = map.get(threadId);
				assert (value != null);

				String[] index = value.split("\\s");
				int start = Integer.parseInt(index[0]);
				int end = Integer.parseInt(index[1]);

				for (int j = start; j < end; j++) {
					writer.println(lines[j]);
				}
				writer.println();
			}
		}
		
	}

}
