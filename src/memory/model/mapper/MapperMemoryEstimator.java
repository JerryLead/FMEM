package memory.model.mapper;

import java.util.List;

import profile.commons.configuration.Configuration;
import profile.commons.metrics.JstatItem;
import profile.commons.metrics.JvmItem;
import profile.task.mapper.Mapper;

import memory.model.job.InitialJvmCapacity;
import memory.model.jvm.MapperEstimatedJvmCost;
import memory.model.jvm.JvmMetricsUtil;
import memory.model.jvm.JvmModel;


public class MapperMemoryEstimator {
	private Configuration finishedConf;
	private Configuration newConf;
	private InitialJvmCapacity gcCap;
	
	private MapperJvmModel mapperJvmModel;
	
	public MapperMemoryEstimator(Configuration finishedConf, Configuration newConf, InitialJvmCapacity gcCap) {
		this.finishedConf = finishedConf;
		this.newConf = newConf;
		this.gcCap = gcCap;
		this.mapperJvmModel = new MapperJvmModel(finishedConf, newConf, gcCap);
	}

	
	//when spilt.size is not changed
	//estimate the JVM cost according to the JVM info of finished Mapper (fMapper)
	public MapperEstimatedJvmCost estimateJvmCost(Mapper fMapper, Mapper eMapper) {
	
		MapperEstimatedJvmCost eJvmModel = mapperJvmModel.estimateJvmCost(fMapper, eMapper);
		//System.out.println(eJvmModel);
		return eJvmModel;
	}
	
	
	//when split.size is changed
	//estimate the JVM cost according to the JVM info of finished Mappers (fMapperList)
	public MapperEstimatedJvmCost estimateJvmCost(List<Mapper> fMapperList, Mapper eMapper) {
		
		MapperEstimatedJvmCost eJvmModel = mapperJvmModel.estimateJvmCost(fMapperList, eMapper);
		//System.out.println(eJvmModel);
		return eJvmModel;
	}
	
	//just copy the JVM info from finishedMapperList to eMapperList(estimated)
	public MapperEstimatedJvmCost copyJvmCost(Mapper fMapper) {
		
		List<JstatItem> jstatItemList = fMapper.getMetrics().getJstatMetricsList();
		List<JvmItem> jvmItemList = fMapper.getMetrics().getJvmMetricsList();
		
		//find the max value in mapper phase
		JvmModel fMapPhaseMax = JvmMetricsUtil.maxJstatValue(jstatItemList, 0, jstatItemList.size() - 1);
		JvmMetricsUtil.updateMaxJvmValue(fMapPhaseMax, jvmItemList, 0, jvmItemList.size() - 1);
		
		// estimate the Old Generation
		float eOldUsed = fMapPhaseMax.getOldUsed();

		// estimate the Perm Generation
		float ePermUsed = fMapPhaseMax.getPermUsed();

		// estimate the New Generation
		float newUsed = fMapPhaseMax.getNewUsed();
		float heapUsed = fMapPhaseMax.getHeapUsed();
		
				
		MapperEstimatedJvmCost jvmCost = new MapperEstimatedJvmCost();
		
		jvmCost.setPermUsed(ePermUsed);
		jvmCost.setNewUsed(newUsed);
		jvmCost.setOU(eOldUsed);
		jvmCost.setHeapU(heapUsed);
				
		return jvmCost;
	}
	
	
}
