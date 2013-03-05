package html.parser.task;

import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import profile.task.reducer.Reducer;

import html.util.HtmlFetcher;


public class ReduceTaskParser {

	public static Reducer parseReduceTask(String reduceTaskDetailsJsp, boolean needMetrics) {
		Document reduceDetails = HtmlFetcher.getHtml(reduceTaskDetailsJsp);
		Element tr = null;
		for(Element elem : reduceDetails.getElementsByTag("tbody").first()
				.children()) {
			if(elem.child(2).text().equals("SUCCEEDED")) {
				tr = elem;
				break;
			}
		}
		
		Reducer reducer = new Reducer();
		
		String taskId = tr.child(0).text();
		String machine = tr.child(1).text();
		machine = machine.substring(machine.lastIndexOf('/')+1);
		
		reducer.setTaskId(taskId); // set task id
		reducer.setMachine(machine); // set machine
		
		
		String countersLink = tr.child(10).child(0).absUrl("href");
		parseReduceTaskCounters(countersLink, reducer);
		
		String logLink = tr.child(9).child(4).absUrl("href");
		parseReduceTaskLog(logLink, reducer);
		
		String metricsLink = tr.child(0).child(0).absUrl("href") + "&text=true";	
		if(needMetrics)
			parseReduceTaskMetrics(metricsLink, reducer);
		
		return reducer;
	}

	private static void parseReduceTaskCounters(String countersLink,
			Reducer reducer) {
		Document countersDoc = HtmlFetcher.getHtml(countersLink);
		Elements countersTrs = countersDoc.getElementsByTag("tbody").first().children();
		for(Element elem : countersTrs) {
			if(elem.getElementsByTag("td").size() == 3) {
				String value = elem.child(2).text();
				reducer.getReducerCounters().setFinalCountersItem(elem.child(1).text(), Long.parseLong(value.replaceAll(",", "")));
			}
		}
	}

	private static void parseReduceTaskLog(String logLink, Reducer reducer) {
		TaskLogParser.parseReducerLog(logLink, reducer);	
	}

	private static void parseReduceTaskMetrics(String metricsLink, Reducer reducer) {
		TaskCountersMetricsJvmParser.parseCountersMetricsJvm(metricsLink, reducer);	
	}
}
