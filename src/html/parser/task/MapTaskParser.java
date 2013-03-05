package html.parser.task;

import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import profile.task.mapper.Mapper;


import html.util.HtmlFetcher;



public class MapTaskParser {

	public static Mapper parseMapTask(String mapTaskDetailsJsp, boolean needMetrics) {
		Document mapDetails = HtmlFetcher.getHtml(mapTaskDetailsJsp);
		
		Element tr = null;
		for(Element elem : mapDetails.getElementsByTag("tbody").first().children()) {
			if(elem.child(2).text().equals("SUCCEEDED")) {
				tr = elem;
				break;
			}
		}
		
		Mapper mapper = new Mapper();
		
		String taskId = tr.child(0).text();
		mapper.setTaskId(taskId); // set TaskId
		
		String machine = tr.child(1).text();
		machine = machine.substring(machine.lastIndexOf('/')+1);
		Element locTr = mapDetails.getElementsByTag("tbody").last();
		String[] splitLocations = new String[locTr.children().size()];
		int i = 0;
		for(Element elem : locTr.children()) {
			String location = elem.text();
			splitLocations[i++] = location.substring(location.lastIndexOf('/') + 1);
		}
		
		mapper.getInput().setInputItems(machine, splitLocations); // set machine and splitLocations
		
		String counterLink = tr.child(8).child(0).absUrl("href");
		parseMapTaskCounters(counterLink, mapper); //set task counters
		
		String logLink = tr.child(7).child(4).absUrl("href");
		parseMapTaskLog(logLink, mapper); // set task log infos

		String metricsLink = tr.child(0).child(0).absUrl("href") + "&text=true";	
		if(needMetrics)
			parseMapTaskMetrics(metricsLink, mapper); // set task metrics
		
		return mapper;
	}

	private static void parseMapTaskCounters(String counterLink, Mapper mapper) {
		Document countersDoc = HtmlFetcher.getHtml(counterLink);
		Elements countersTrs = countersDoc.getElementsByTag("tbody").first().children();
		for(Element elem : countersTrs) {
			if(elem.getElementsByTag("td").size() == 3) {
				String value = elem.child(2).text();
				mapper.getMapperCounters().setFinalCountersItem(elem.child(1).text(), Long.parseLong(value.replaceAll(",", "")));
			}
		}
	}
	

	private static void parseMapTaskLog(String logLink, Mapper mapper) {
		TaskLogParser.parseMapperLog(logLink, mapper);	
	}
	

	private static void parseMapTaskMetrics(String metricsLink, Mapper mapper) {
		TaskCountersMetricsJvmParser.parseCountersMetricsJvm(metricsLink, mapper);
		
	}


}
