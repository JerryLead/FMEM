package collector.jvmcost;

import html.parser.link.LinksSaver;
import html.util.HtmlFetcher;

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
import java.util.Map.Entry;


import org.jsoup.Connection;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import profile.commons.configuration.Configuration;
import profile.commons.metrics.GcCapacity;
import profile.commons.metrics.JstatItem;
import profile.commons.metrics.JstatMetrics;
import profile.job.JobTasksParser;

import memory.model.job.InitialJvmCapacity;
import memory.model.jvm.JvmModel;



public class JvmCostCollector {
	
	private String jobName;
	private GcCapacity gcCap;
	private InitialJvmCapacity jvmCap;
	private Configuration fConf;
	private boolean useHistory;
	
	private List<JvmModel> mappersMaxJM;
	private List<JvmModel> reducersMaxJM;;
	
	private List<Map<String, Long>> mappersCountersList;
	private List<Map<String, Long>> reducersCountersList;
	
	private List<MapperTime> mapperTimeList;
	private List<ReducerTime> reducerTimeList;
	
	private boolean computeTaskLinksNormal(LinksSaver linksSaver) {
		//dispose configuration
		String jobConfLink = linksSaver.getJobconf_jsp();
		Document wholeJspDoc = HtmlFetcher.getHtml(jobConfLink);
		Element body = wholeJspDoc.getElementsByTag("body").first();

		Element confTable = body.getElementsByTag("tbody").first();
		for (Element elem : confTable.children()) {
			if (!elem.child(0).text().equals("name"))
				fConf.set(elem.child(0).text(), elem.child(1).text());
		}
		
		String jobdetaisUrl = linksSaver.getJobdetails_jsp();
		
		Document jobDetails = HtmlFetcher.getHtml(jobdetaisUrl);
		Element countersTable = jobDetails.getElementsByTag("p").get(0).nextElementSibling();
		
		Element jobNameElem = jobDetails.getElementsContainingOwnText("Job Name:").first();
		jobName = jobNameElem.nextSibling().toString().trim();
		
		Element jobStatusElem = jobDetails.getElementsContainingOwnText("Status:").first();
		String status = jobStatusElem.nextSibling().toString();
		
		if(status.trim().equals("Failed"))
			return false;
		
		JobTasksParser.parseJobTasks(linksSaver); //initiate map/reduce tasks link list
		return true;
	}
	
	
	private boolean computeTaskLinksUseHistory(LinksSaver linksSaver) {
		String jobdetaisUrl = linksSaver.getJobdetailshistory_jsp();
		
		Connection conn = Jsoup.connect(jobdetaisUrl);
		Document jobDetailsHistory = HtmlFetcher.getHtml(conn);
		String sessionId = conn.response().cookie("JSESSIONID");

		Element jobConfElem = jobDetailsHistory.getElementsContainingOwnText("JobConf:").first();
		String jobConfLink = jobConfElem.nextElementSibling().absUrl("href");
		
		Element jobNameElem = jobDetailsHistory.getElementsContainingOwnText("JobName:").first();
		jobName = jobNameElem.nextSibling().toString().trim();
		
		Element jobStatusElem = jobDetailsHistory.getElementsContainingOwnText("Status:").first();
		String status = jobStatusElem.nextSibling().toString();
		
		if(status.trim().equals("FAILED"))
			return false;
		
		conn = Jsoup.connect(jobConfLink);
		conn.cookie("JSESSIONID", sessionId);
		
		Document wholeJspDoc = HtmlFetcher.getHtml(conn);
		Element body = wholeJspDoc.getElementsByTag("body").first();

		Element confTable = body.getElementsByTag("tbody").first();
		for (Element elem : confTable.children()) {
			if (!elem.child(0).text().equals("name"))
				fConf.set(elem.child(0).text(), elem.child(1).text());
		}
		
		//fetchJobConfFromHDFS(jobConfLink);
		
		Elements trs = jobDetailsHistory.getElementsByTag("tbody").first().children();

		Element tr = trs.get(2);
		String mappersLink = tr.child(2).child(0).absUrl("href");

		tr = trs.get(3);
		String reducersLink = tr.child(2).child(0).absUrl("href");
		
		linksSaver.setMap_jobtasks_jsp(mappersLink);
		linksSaver.setReduce_jobtasks_jsp(reducersLink);
		
		JobTasksParser.parseJobTasks(linksSaver); //initiate map/reduce tasks link list
		
		return true;
	}
	
	private String getMetricsLink(String taskDetailsJsp, boolean isMapper) {
		Document mapDetails = HtmlFetcher.getHtml(taskDetailsJsp);
		Element tr = null;
		
		if(useHistory) {
			tr = mapDetails.getElementsByTag("tbody").first().child(1);
			
		}
		else {
			for(Element elem : mapDetails.getElementsByTag("tbody").first().children()) {
				if(elem.child(2).text().equals("SUCCEEDED")) {
					tr = elem;
					break;
				}
			}		
		}	
		
		if(isMapper) {
			String finishTime;
			if(useHistory) 
				finishTime = tr.child(2).text();
			else
				finishTime = tr.child(5).text();
			int mapperTimeCostSec = stringToTime(finishTime.split("\\(")[1].replace(')', ' ').trim());
			MapperTime time = new MapperTime();
			time.setMapperTimeCostSec(mapperTimeCostSec);
			
			mapperTimeList.add(time);
		}
		else {
			String shuffleTimeStr;
			String sortTimeStr;
			String finishTimeStr;
			
			if(useHistory) {
				shuffleTimeStr = tr.child(2).text().trim();
				sortTimeStr = tr.child(3).text().trim();
				finishTimeStr = tr.child(4).text().trim();
			}
			else {
				shuffleTimeStr = tr.child(5).text().trim();
				sortTimeStr = tr.child(6).text().trim();
				finishTimeStr = tr.child(7).text().trim();
			}
			
			ReducerTime time = new ReducerTime();
			int reducerShuffleTimeCostSec = stringToTime(shuffleTimeStr.split("\\(")[1].replace(')', ' ').trim());
			int reducerSortTimeCostSec = stringToTime(sortTimeStr.split("\\(")[1].replace(')', ' ').trim());
			int reducerTimeCostSec = stringToTime(finishTimeStr.split("\\(")[1].replace(')', ' ').trim());
			int reducerReduceTimeCostSec = reducerTimeCostSec - reducerShuffleTimeCostSec - reducerSortTimeCostSec;
			if(reducerReduceTimeCostSec < 0)
				reducerReduceTimeCostSec = 0;
			
			time.setReducerShuffleTimeCostSec(reducerShuffleTimeCostSec);
			time.setReducerSortTimeCostSec(reducerSortTimeCostSec);
			time.setReducerReduceTimeCostSec(reducerReduceTimeCostSec);
			time.setReducerTimeCostSec(reducerTimeCostSec);
			
			reducerTimeList.add(time);
		}
		
		
		String metricsLink = tr.child(0).child(0).absUrl("href") + "&text=true";	

		return metricsLink;
	}
	
	//convert **mins, **sec into ** sec
	public int stringToTime(String lastTime) {

		StringBuilder sb = new StringBuilder();
		for(int i = 0; i < lastTime.length(); i++) {
			char c = lastTime.charAt(i);
			if(c >= '0' && c <= '9')
				sb.append(c);
			else 
				sb.append(' ');
			if(c == '-')
				System.err.println(lastTime);
		}
		
		String ints[] = sb.toString().split("\\s+");
		int sec = 0;
		for(int i = 0; i < ints.length; i++) {
			if(!ints[i].isEmpty())
				sec += sec * 60 + Integer.parseInt(ints[i]);
		}
		return sec;
	}
	
	private void updateGcCapacity(GcCapacity gcCapacity) {
		
		if(gcCap == null)
			this.gcCap = gcCapacity;
		else {
			if(gcCap.getEC() > gcCapacity.getEC())
				gcCap.setEC(gcCapacity.getEC());
			if(gcCap.getNGC() > gcCapacity.getNGC()) 
				gcCap.setNGC(gcCapacity.getNGC());	
			if(gcCap.getOGC() > gcCapacity.getOGC()) {
				gcCap.setOGC(gcCapacity.getOGC());
				gcCap.setOC(gcCapacity.getOGC());
			}
			if(gcCap.getPGC() > gcCapacity.getPGC()) {
				gcCap.setPGC(gcCapacity.getPGC());
				gcCap.setPC(gcCapacity.getPGC());
			}	
			if(gcCap.getS0C() > gcCapacity.getS0C())
				gcCap.setS0C(gcCapacity.getS0C());
			if(gcCap.getS1C() > gcCapacity.getS1C())
				gcCap.setS1C(gcCapacity.getS1C());
		}
	}

	private void parseJstatMetrics(String wholeJstatText, List<JvmModel> list) {
		String[] lines = wholeJstatText.split("\\n");
		if(lines.length < 5)
			return;
		
		String dateStrSec;
	
		if(!lines[0].trim().startsWith("NGCMN"))
			return;
		
		String[] gccapacity = lines[1].trim().split("\\s+");
		if(gccapacity.length != 16)
			return;

		GcCapacity gcCap = new GcCapacity(gccapacity);
		updateGcCapacity(gcCap);
		
		dateStrSec = lines[2].trim(); //(Date s) 1351667183
		
		JstatMetrics jsm = new JstatMetrics(dateStrSec);
		
		JvmModel jvmModel = new JvmModel();
		for (int i = 4; i < lines.length; i++) {
			String parameters[] = lines[i].trim().split("\\s+");
			if (parameters.length == 16) { // 16 parameters totally
				JstatItem item = new JstatItem(jsm.generateArrayList(parameters));
				jvmModel.selectMaxValue(item);
			}
		}	
		
		list.add(jvmModel);
	}

	private void parseCounters(String wholeCountersText, List<Map<String, Long>> countersList) {
		String lines[] = wholeCountersText.split("\\n");
		String line = lines[lines.length - 1];
		String params[] = line.trim().split(",");
		
		Map<String, Long> counterMap = new HashMap<String, Long>();
		//long timeMS = Long.parseLong(params[0]) * 1000; //1346728352000
		
		for(int i = 1; i < params.length; i++) {
			String kv[] = params[i].split(":");
			assert(kv.length == 2);
			counterMap.put(kv[0], Long.parseLong(kv[1]));
		}
		countersList.add(counterMap);
		
	}
	
	private void getCountersAndJstatInfo(LinksSaver linksSaver) {
		for(int i = 0; i < linksSaver.getMap_tasks_list().size(); i++) {
			String metricsLink = getMetricsLink(linksSaver.getMap_tasks_list().get(i), true);

			Document doc = HtmlFetcher.getHtml(metricsLink);
			Elements pElems = doc.getElementsByTag("pre");
			
			Element pidstatElem = pElems.get(1);
			Element jstatElem = pElems.get(3);
			
			String countersLink = getCountersLink(linksSaver.getMap_tasks_list().get(i), true);
			
			
			parseJstatMetrics(jstatElem.text(), mappersMaxJM);		
			parsePidstatMetrics(pidstatElem.text(), mappersMaxJM);
			//parseCounters(countersElem.text(), mappersCountersList);
			parseCounters2(countersLink, mappersCountersList);
		}
		
		
		for(int i = 0; i < linksSaver.getReduce_tasks_list().size(); i++) {
			String metricsLink = getMetricsLink(linksSaver.getReduce_tasks_list().get(i), false);
			
			Document doc = HtmlFetcher.getHtml(metricsLink);
			Elements pElems = doc.getElementsByTag("pre");
			
			Element pidstatElem = pElems.get(1);
			Element jstatElem = pElems.get(3);
			
			
			parseJstatMetrics(jstatElem.text(), reducersMaxJM);
			parsePidstatMetrics(pidstatElem.text(), reducersMaxJM);
			
			String countersLink = getCountersLink(linksSaver.getReduce_tasks_list().get(i), false);
			//parseCounters(countersElem.text(), reducersCountersList);
			parseCounters2(countersLink, reducersCountersList);
			
		}	
		
	}
	
	private void parsePidstatMetrics(String wholeMetricsText, List<JvmModel> list) {
		JvmModel cjm = list.get(list.size() - 1);
		
		String[] lines = wholeMetricsText.split("\\n");
		int RSS = 0;
		
		for (String line : lines) {
			if (line.length() == 0 || line.charAt(0) == '#')
				continue;
			String parameters[] = line.trim().split("\\s+");
			if (parameters.length == 16) { // 16 parameters totally
				//long VSZ = Long.parseLong(parameters[9]) / 1024;
				RSS = (int) Math.max(RSS, Long.parseLong(parameters[10]) / 1024);
			}
		}
		
		cjm.setRSS(RSS);		
	}


	private void parseCounters2(String countersLink, List<Map<String, Long>> countersList) {
		Document countersDoc = HtmlFetcher.getHtml(countersLink);
		Elements countersTrs = countersDoc.getElementsByTag("tbody").first()
				.children();
		
		Map<String, Long> counterMap = new HashMap<String, Long>();
		for(Element elem : countersTrs) {
			if(elem.getElementsByTag("td").size() == 3) {
				counterMap.put(elem.child(1).text().trim(), Long.parseLong(elem.child(2).text().trim().replaceAll(",", "")));
			}
		}
		
		countersList.add(counterMap);
	}


	private String getCountersLink(String taskDetailsJsp, boolean isMapper) {
		Document mapDetails = HtmlFetcher.getHtml(taskDetailsJsp);
		Element tr = null;
		
		if(useHistory) {
			tr = mapDetails.getElementsByTag("tbody").first().child(1);
			
		}
		else {
			for(Element elem : mapDetails.getElementsByTag("tbody").first().children()) {
				if(elem.child(2).text().equals("SUCCEEDED")) {
					tr = elem;
					break;
				}
			}		
		}	
		
		String countersLink;
		if(isMapper) {
			if(useHistory) {
				countersLink = tr.child(8).child(0).absUrl("href");
			}
			else {
				countersLink = tr.child(8).child(0).absUrl("href");
			}
		}
		else {
			if(useHistory) {
				countersLink = tr.child(10).child(0).absUrl("href");
			}
			else {
				countersLink = tr.child(10).child(0).absUrl("href");
			}
			
		}
		
		
		return countersLink;
	}


	public void profile(String hostname, String jobId, boolean useHistory) {
		fConf = new Configuration();
		mappersMaxJM = new ArrayList<JvmModel>();
		reducersMaxJM = new ArrayList<JvmModel>();
		gcCap = null;
		this.useHistory = useHistory;
		
		mappersCountersList = new ArrayList<Map<String, Long>>();
		reducersCountersList = new ArrayList<Map<String, Long>>();
		
		mapperTimeList = new ArrayList<MapperTime>();
		reducerTimeList = new ArrayList<ReducerTime>();
		
		LinksSaver linksSaver = new LinksSaver(hostname, jobId);
		
		boolean succeed;
		if(useHistory)
			succeed = computeTaskLinksUseHistory(linksSaver);
		else
			succeed = computeTaskLinksNormal(linksSaver);
		if(succeed == false)
			return;
		
		getCountersAndJstatInfo(linksSaver);
		
		if(gcCap == null)
			return;
		
		jvmCap = new InitialJvmCapacity(fConf, gcCap);
		
	}
	
	
	public void outputPerTaskJvmMaxMinValues(String perTaskJvmMaxMinDir, String jobId, String logName) throws IOException {
		//write jobid.conf into 
		
		File path = new File(perTaskJvmMaxMinDir);
		/*
		File logDir = new File(path, "conf");
		if(!logDir.exists())
			logDir.mkdirs();
		File confFile = new File(logDir, jobId + ".conf");
		PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(confFile)));
		for(Entry<String, String> entry : fConf.getAllConfs()) {
			writer.write(entry.getKey() + "\t" + entry.getValue());
			writer.write("\r\n");
		}
		writer.close();
		*/
		File details = new File(path, logName);
		if(!details.getParentFile().exists())
			details.getParentFile().mkdirs();
		
		PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(details, true)));
		
		writer.println("*************" + jobId + " " + jobName + "*************");
		writer.println("[Xmx]" + fConf.getXmx() + "m [Xms]" + jvmCap.getfXms() + "m [ismb]"
				+ fConf.getIo_sort_mb() + "m" + " [Reuse]" + (fConf.getMapred_job_reuse_jvm_num_tasks() == -1)
				+ " [reducers]" + fConf.getMapred_reduce_tasks() + " [split]" + fConf.getSplitSize()/1024/1024
				);
		
		
		String f1 = "%1$-3.1f";
		String f2 = "%1$-1d";
		String f3 = "%1$-3.3f";
		writer.println("--------------------------Estimated/Real Capacity--------------------------");
		writer.println("MX" + "\t" + "NGCMX" + "\t" + "NGC" + "\t" + "S0C" + "\t" + "S1C" + "\t" + "EC" + "\t" + 
				"OGCMX" + "\t" + "OGC" + "\t" + "PGCMX" + "\t" + "PGC");
		String br = "\t";
		writer.println(String.format("%1$-3d", jvmCap.geteXmx()) + br
				//+ String.format(f1, jvmCap.getePGCMX()) + br
				+ String.format(f1, jvmCap.geteNGCMX()) + br 
				+ String.format(f1, jvmCap.geteNGC()) + br 
				+ String.format(f1, jvmCap.geteS0C()) + br 
				+ String.format(f1, jvmCap.geteS1C()) + br 
				+ String.format(f1, jvmCap.geteEC()) + br 
				+ String.format(f1, jvmCap.geteOGCMX()) + br 
				+ String.format(f1, jvmCap.geteOGC()) + br 
				+ String.format(f1, jvmCap.getePGCMX()) + br 
				+ String.format(f1, jvmCap.getePGC()));
		writer.println(gcCap);
		writer.println("--------------------------mapper statistics--------------------------");
		writer.println("OU\tEdenU\tNGU\tS0U\tS1U\tHeapU\tBytes\tYGC\tYGCT\tFGC\tFGCT\tGCT\tIndex\tTimeS\tRSS");
		for(int i = 0; i < mappersMaxJM.size(); i++) {
			JvmModel jm = mappersMaxJM.get(i);
			
			writer.println(String.format(f1, jm.getOldUsed()) + "\t" + 
					String.format(f1, jm.getEdenUsed()) + "\t" 
					+ String.format(f1, jm.getNewUsed()) + "\t"
					+ String.format(f1, jm.getS0Used()) + "\t" 
					+ String.format(f1, jm.getS1Used()) + "\t" 
					+ String.format(f1, jm.getHeapUsed()) + "\t" 
					+ mappersCountersList.get(i).get("HDFS_BYTES_READ") /1024/1024 + "\t"
					+ String.format(f2, jm.getYoungGC()) + "\t"
					+ String.format(f3, jm.getYGCT()) + "\t"
					+ String.format(f2, jm.getFullGC()) + "\t"
					+ String.format(f3, jm.getFGCT()) + "\t"
					+ String.format(f3, jm.getGCT()) + "\t"
					+ i + "\t"
					+ mapperTimeList.get(i).getMapperTimeCostSec() + "\t"
					+ jm.getRSS());
			
		}
		writer.println("--------------------------reducer statistics--------------------------");
		writer.println("OU\tEdenU\tNGU\tS0U\tS1U\tHeapU\tRecords\tYGC\tYGCT\tFGC\tFGCT\tGCT\tIndex\tShuffle\tSort\tReduce\tTimeS\tRSS");
		for(int i = 0; i < reducersMaxJM.size(); i++) {
			JvmModel jm = reducersMaxJM.get(i);
			ReducerTime t = reducerTimeList.get(i);
			
			writer.println(String.format(f1, jm.getOldUsed()) + "\t" 
					+ String.format(f1, jm.getEdenUsed()) + "\t" 
					+ String.format(f1, jm.getNewUsed()) + "\t"
					+ String.format(f1, jm.getS0Used()) + "\t" 
					+ String.format(f1, jm.getS1Used()) + "\t" 
					+ String.format(f1, jm.getHeapUsed()) + "\t" 
					+ reducersCountersList.get(i).get("Reduce input records") /1000 + "\t"
					+ String.format(f2, jm.getYoungGC()) + "\t"
					+ String.format(f3, jm.getYGCT()) + "\t"
					+ String.format(f2, jm.getFullGC()) + "\t"
					+ String.format(f3, jm.getFGCT()) + "\t"
					+ String.format(f3, jm.getGCT()) + "\t"
					+ i + "\t"
					+ t.getReducerShuffleTimeCostSec() + "\t"
					+ t.getReducerSortTimeCostSec() + "\t"
					+ t.getReducerReduceTimeCostSec() + "\t"
					+ t.getReducerTimeCostSec() + "\t"
					+ jm.getRSS());
		}
		writer.write("\r\n");
		writer.close();
	}
	
	public static void main(String[] args) {
		String startJobId = "job_201212252123_0001";
		String hostname = "master";
		int iterateNum = 80;
		
		//String perTaskJvmMaxMinDir = "/home/xulijie/MR-MEM/NewExperiments/uservisits_aggre-pig-256MB/RealJvmCost/";
		//String perTaskJvmMaxMinDir = "/home/xulijie/MR-MEM/Experiments/Wiki-m36-r18-256MB/RealJvmCost/";
		//String perTaskJvmMaxMinDir = "/home/xulijie/MR-MEM/Experiments/TeraSort-256MB-m36-r18/RealJvmCost/";
		//String perTaskJvmMaxMinDir = "/home/xulijie/MR-MEM/Experiments/TeraSort-single/RealJvmCost/";
		//String perTaskJvmMaxMinDir = "/home/xulijie/MR-MEM/NewExperiments/TeraSort-256MB-m36-r18/RealJvmCost/";
		//String perTaskJvmMaxMinDir = "/home/xulijie/MR-MEM/NewExperiments/Wiki-m36-r18-256MB/RealJvmCost/";
		String perTaskJvmMaxMinDir = "/home/xulijie/MR-MEM/NewExperiments/BuildCompIndex-m36-r18-256MB/RealJvmCost/";
		
		
		boolean useHistory = false;
		String logName = "taskJvmDetails.txt";
		
		
		String prefix = startJobId.substring(0, startJobId.length() - 4);
		int suffix = Integer.parseInt(startJobId.substring(startJobId.length() - 4));
		
		DecimalFormat nf = new DecimalFormat("0000");
		JvmCostCollector analyzer = new JvmCostCollector();
		
		String jobId;
		
		for(int i = 0; i < iterateNum; i++) {
			jobId = prefix + nf.format(suffix + i);		
			analyzer.profile(hostname, jobId, useHistory);
			try {
				analyzer.outputPerTaskJvmMaxMinValues(perTaskJvmMaxMinDir, jobId, logName);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			System.out.println("[" + jobId + "]" + "'s jvm max-min usage has been writen into " + perTaskJvmMaxMinDir + logName);
		}
			
	}
	
}

class MapperTime {
	private int mapperTimeCostSec;

	public int getMapperTimeCostSec() {
		return mapperTimeCostSec;
	}

	public void setMapperTimeCostSec(int mapperTimeCostSec) {
		this.mapperTimeCostSec = mapperTimeCostSec;
	}
}

class ReducerTime {
	
	private int reducerShuffleTimeCostSec;
	private int reducerSortTimeCostSec;
	private int reducerReduceTimeCostSec;
	private int reducerTimeCostSec;
	public int getReducerShuffleTimeCostSec() {
		return reducerShuffleTimeCostSec;
	}
	public void setReducerShuffleTimeCostSec(int reducerShuffleTimeCostSec) {
		this.reducerShuffleTimeCostSec = reducerShuffleTimeCostSec;
	}
	public int getReducerSortTimeCostSec() {
		return reducerSortTimeCostSec;
	}
	public void setReducerSortTimeCostSec(int reducerSortTimeCostSec) {
		this.reducerSortTimeCostSec = reducerSortTimeCostSec;
	}
	public int getReducerReduceTimeCostSec() {
		return reducerReduceTimeCostSec;
	}
	public void setReducerReduceTimeCostSec(int reducerReduceTimeCostSec) {
		this.reducerReduceTimeCostSec = reducerReduceTimeCostSec;
	}
	public int getReducerTimeCostSec() {
		return reducerTimeCostSec;
	}
	public void setReducerTimeCostSec(int reducerTimeCostSec) {
		this.reducerTimeCostSec = reducerTimeCostSec;
	}

}