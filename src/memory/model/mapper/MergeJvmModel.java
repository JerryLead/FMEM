package memory.model.mapper;

import java.util.List;

import profile.commons.metrics.JstatItem;
import profile.commons.metrics.JvmItem;
import profile.task.mapper.Mapper;



public class MergeJvmModel {

	private Mapper fMapper;
	private Mapper eMapper;
	
	private List<JstatItem> jstatItemList = fMapper.getMetrics().getJstatMetricsList();
	private List<JvmItem> jvmItemList = fMapper.getMetrics().getJvmMetricsList();
	
	public MergeJvmModel(Mapper fMapper, Mapper eMapper) {
		fMapper = fMapper;
		eMapper = eMapper;
		
		jstatItemList = fMapper.getMetrics().getJstatMetricsList();
		jvmItemList = fMapper.getMetrics().getJvmMetricsList();
		
		long mapperStopMS = fMapper.getMapperStopTimeMS();
	}

}
