package data.analysis.hprof;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import html.util.HtmlFetcher;

public class HprofAnalyzer {
	private static Map<String, String> map = new HashMap<String, String>();
	private static List<String[]> memList = new ArrayList<String[]>();
	private static List<String[]> cpuList = new ArrayList<String[]>();
	
	private static void genMemAndCPUInfo(String url) {
		Document doc = HtmlFetcher.getHtml(url);
		Element profile = doc.getElementsByTag("pre").last();
		String[] lines = profile.text().split("\\n");
		//System.out.println(lines.length);
		
		
		for(int i = 0; i < lines.length; i++) {
			if(lines[i].startsWith("TRACE") && lines[i].contains("thread=")) {
				String traceId = lines[i].substring(lines[i].indexOf(' ') + 1, lines[i].indexOf(':'));
				int j;
				for(j = i + 1; j < lines.length; j++)  {
					if (lines[j].startsWith("SITES BEGIN") || lines[j].startsWith("TRACE") && lines[j].contains("thread=")) 
						break;
				}
					
				int start = i;
				int end = j;
				
				map.put(traceId, start + " " + end);
				i = end - 1;
			}
			
			if(lines[i].startsWith(" rank")) {
				int j;
				for(j = i; j < lines.length; j++) {
					if(lines[j].startsWith("SITES END"))
						break;
					String[] items = lines[j].trim().split("\\s+");
					assert(items.length == 9);
					
					memList.add(items);
					
				}
				i = j;
			}
			
			if(lines[i].startsWith("CPU SAMPLES BEGIN")) {
				int j;
				for(j = i + 1; j < lines.length; j++) {
					String[] items = lines[j].trim().split("\\s+");
					assert(items.length == 6);
					
					cpuList.add(items);
					if(lines[j].startsWith("CPU SAMPLES END"))
						break;
				}
				i = j;
			}
			
			
		}
		outputInfos(lines);
	}
	
	private static void outputInfos(String lines[]) {
		System.out.println("Memory Samples:");
		for(String[] memItems : memList) {
			System.out.print(memItems[0]);
			for(int i = 1; i < memItems.length; i++)
				System.out.print("\t" + memItems[i]);
			System.out.println();
		}
		
		System.out.println("Memory Trace:");
		
		for(int i = 1; i < memList.size(); i++) {
			String[] memItems = memList.get(i);
			String threadId = memItems[7];
			String value = map.get(threadId);
			assert(value != null);
			
			String[] index = value.split("\\s+");
			int start = Integer.parseInt(index[0]);
			int end = Integer.parseInt(index[1]);
			
			for(int j = start; j < end; j++) {
				System.out.println(lines[j]);
			}
			System.out.println();
		}
		
		System.out.println("CPU Samples:");
		

		for(int i = 0; i <= 20 && i < cpuList.size(); i++) {
			String[] cpuItems = cpuList.get(i);
			System.out.print(cpuItems[0]);
			for(int j = 1; j < cpuItems.length; j++)
				System.out.print("\t" + cpuItems[j]);
			System.out.println();
		}
		
		System.out.println("CPU Trace:");
		
		for(int i = 1; i <= 10 && i < cpuList.size(); i++) {
			String[] cpuItems = cpuList.get(i);
			String threadId = cpuItems[4];
			String value = map.get(threadId);
			assert(value != null);
			
			String[] index = value.split("\\s");
			int start = Integer.parseInt(index[0]);
			int end = Integer.parseInt(index[1]);
			
			for(int j = start; j < end; j++) {
				System.out.println(lines[j]);
			}
			System.out.println();
		}
	}

	public static void main(String[] args) {
		String url = "http://slave4:50060/tasklog?taskid=attempt_201211211439_0084_m_000000_0&all=true";
		genMemAndCPUInfo(url);
		
	}

	

}
