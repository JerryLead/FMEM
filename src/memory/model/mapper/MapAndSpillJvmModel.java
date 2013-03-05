package memory.model.mapper;

import java.util.List;

import profile.commons.configuration.Configuration;
import profile.commons.metrics.JstatItem;
import profile.commons.metrics.JvmItem;
import profile.task.mapper.Mapper;

import memory.model.jvm.JvmMetricsUtil;
import memory.model.jvm.JvmModel;



public class MapAndSpillJvmModel {
	private Configuration fConf;
	private Configuration newConf; 
	private Mapper fMapper;
	private Mapper eMapper;
	
	private List<JstatItem> jstatItemList;
	private List<JvmItem> jvmItemList;
	
	private int firstSpillJstatIndex; //map() -> buffer full -> first spill
	private int spillFinishedJstatIndex; //last spill (flush) -> merge begin
	
	private int firstSpillJvmIndex; //map() -> buffer full -> first spill
	private int spillFinishedJvmIndex; //last spill (flush) -> merge begin
	
	private JvmModel eJvmModel; //put the estimated JVM info into eJvmModel;
	
	public MapAndSpillJvmModel(Configuration finishedConf, Configuration newConf, Mapper fMapper, Mapper eMapper) {
		this.fConf = finishedConf;
		this.newConf = newConf;
		this.fMapper = fMapper;
		this.eMapper = eMapper;
		
		jstatItemList = fMapper.getMetrics().getJstatMetricsList();
		jvmItemList = fMapper.getMetrics().getJvmMetricsList();
		
		long firstSpillMS = fMapper.getFirstSpillStartTimeMS();
		long spillFinishedMS = fMapper.getSpillPhaseFinishTimeMS();
		
		firstSpillJstatIndex = JvmMetricsUtil.getJstatItem(jstatItemList, firstSpillMS);
		spillFinishedJstatIndex = JvmMetricsUtil.getJstatItem(jstatItemList, spillFinishedMS);
		
		firstSpillJvmIndex = JvmMetricsUtil.getJvmItem(jvmItemList, firstSpillMS);
		spillFinishedJvmIndex = JvmMetricsUtil.getJvmItem(jvmItemList, spillFinishedMS);
	}

	private void estimateJvmCost() {
		//find the max value in map-first-spill phase and spill-finished phase
		JvmModel fMapToSpill = JvmMetricsUtil.maxJstatValue(jstatItemList, 0, firstSpillJstatIndex);
		JvmModel fSpillToFinish = JvmMetricsUtil.maxJstatValue(jstatItemList, firstSpillJstatIndex, spillFinishedJstatIndex);
		
		JvmMetricsUtil.updateMaxJvmValue(fMapToSpill, jvmItemList, firstSpillJvmIndex, spillFinishedJvmIndex);
		JvmMetricsUtil.updateMaxJvmValue(fSpillToFinish, jvmItemList, firstSpillJvmIndex, spillFinishedJvmIndex);
		
		//estimate the Old Generation
		float fOldUsed = fMapToSpill.getOldUsed();
		int fBuffer = fConf.getIo_sort_mb();
		float eOldUsed = fOldUsed - fBuffer + newConf.getIo_sort_mb(); 
		
		//estimate the Perm Generation
		float ePermUsed = fMapToSpill.getPermUsed();
		
		//estimate the New Generation
		float fEdenUsed = fMapToSpill.getEdenUsed();

		
		
	}
	
	
	
	

}
