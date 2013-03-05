package html.parser.task;

import java.util.ArrayList;
import java.util.List;

import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import profile.commons.metrics.GcCapacity;
import profile.commons.metrics.JstatMetrics;
import profile.task.common.Task;


import html.util.HtmlFetcher;

public class TaskCountersMetricsJvmParser {
	public static void parseCountersMetricsJvm (String metricsLink, Task task) {
		Document doc = HtmlFetcher.getHtml(metricsLink);
		Elements pElems = doc.getElementsByTag("pre");
		Element countersElem = pElems.get(0);
		Element metricsElem = pElems.get(1);
		Element jvmElem = pElems.get(2);
		Element jstatElem = pElems.get(3);
		
		//parseCounters(countersElem.text(), task);
		parseMetrics(metricsElem.text(), task);
		parseJvmMetrics(jvmElem.text(), task);	
		parseJstatMetrics(jstatElem.text(), task);
	}

	private static void parseCounters(String wholeCountersText, Task task) {
		String lines[] = wholeCountersText.split("\\n");
		for (String line : lines) {			
			if (line.length() == 0 || line.charAt(0) == '#')
				continue;
			String parameters[] = line.trim().split(",");
			task.addCounterItem(parameters);
		}
		
	}
	
	private static void parseMetrics(String wholeMetricsText, Task task) {
		
		String[] lines = wholeMetricsText.split("\\n");
		for (String line : lines) {
			if (line.length() == 0 || line.charAt(0) == '#')
				continue;
			String parameters[] = line.trim().split("\\s+");
			if (parameters.length == 16) // 16 parameters totally
				task.addMetricsItem(parameters);
		}
		
	}


	private static void parseJvmMetrics(String wholeJVMText, Task task) {
		String[] lines = wholeJVMText.split("\\n");

		for (int i = 1; i < lines.length; i++) {
			String parameters[] = lines[i].trim().split("\t");
			if (parameters.length == 4) // 16 parameters totally
				task.addJvmMetrics(parameters);
		}
		
	}
	
	private static void parseJstatMetrics(String wholeJstatText, Task task) {
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
		task.addGcCapacity(gcCap);
		
		dateStrSec = lines[2].trim(); //(Date s) 1351667183
		
		JstatMetrics jsm = new JstatMetrics(dateStrSec);
		
		for (int i = 4; i < lines.length; i++) {
			String parameters[] = lines[i].trim().split("\\s+");
			if (parameters.length == 16) // 16 parameters totally
				task.addJstatMetrics(jsm.generateArrayList(parameters));
			
		}	
	}
	
	public static String getGcCapacity(String metricsLink) {
		Document doc = HtmlFetcher.getHtml(metricsLink);
		Elements pElems = doc.getElementsByTag("pre");
		
		Element jstatElem = pElems.get(3);
		
		String[] lines = jstatElem.text().split("\\n");
		if(lines.length < 2)
			return null;
		if(!lines[0].trim().startsWith("NGCMN"))
			return null;
		return lines[1].trim();
	}

}


