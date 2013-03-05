package memory.model.reducer;

import java.util.ArrayList;
import java.util.List;

import profile.commons.configuration.Configuration;
import profile.task.reducer.Reducer;

import memory.model.job.InitialJvmCapacity;
import memory.model.jvm.ReducerEstimatedJvmCost;

public class ReducerMemoryEstimator {

	private Configuration fConf;
	private Configuration newConf;
	private InitialJvmCapacity gcCap;
	
	private ShuffleSortJvmModel shuffleModel;
	private ReduceJvmModel reduceModel;
	
	public ReducerMemoryEstimator(Configuration fConf, Configuration newConf, InitialJvmCapacity gcCap) {
		this.fConf = fConf;
		this.newConf = newConf;
		this.gcCap = gcCap;
		
		this.shuffleModel = new ShuffleSortJvmModel(fConf, newConf, gcCap);
		this.reduceModel = new ReduceJvmModel(fConf, newConf, gcCap);
	}

	public ReducerEstimatedJvmCost esimateJvmCost(Reducer fReducer, Reducer eReducer) {
		//TO DO: solve the problem of runtime.maxmemory and child.java.opts
		
		//Shuffle & Sort phase
		ReducerEstimatedJvmCost eSSJvmModel = shuffleModel.estimateJvmCost(fReducer, eReducer);
		//System.out.println(eSSJvmModel);
	
		//Reduce Phase	
		reduceModel.estimateJvmCost(fReducer, eReducer, eSSJvmModel);
		//System.out.println(eRJvmModel);
		eSSJvmModel.updateReducerJvmCost();
		
		return eSSJvmModel;
	}	

	public ReducerEstimatedJvmCost esimateJvmCost(List<Reducer> fReducerList, Reducer eReducer) {
		List<ReducerEstimatedJvmCost> list = new ArrayList<ReducerEstimatedJvmCost>();
		for(Reducer fReducer : fReducerList) 
			list.add(esimateJvmCost(fReducer, eReducer));
		
		return findMaxValue(list);
	}
	
	
	//just select the ReducerEstimatedJvmCost with the largest xHeapU
	private ReducerEstimatedJvmCost findMaxValue(List<ReducerEstimatedJvmCost> list) {
		float xHeapU = 0;
		int index = 0;
		
		for(int i = 0; i < list.size(); i++) {
			if(list.get(i).getxHeapU() > xHeapU) {
				xHeapU = list.get(i).getxHeapU();
				index = i;
			}
		}
		return list.get(index);
		/*
		ReducerEstimatedJvmCost jvmCost = new ReducerEstimatedJvmCost();
		
		float permUsed = 0;
		float newUsed = 0;

		float nOU = 0;
		float xOU = 0;
		
		float nHeapU = 0;
		float xHeapU = 0;

		float tempObj = 0;
		float fix = 0;
		
		for(ReducerEstimatedJvmCost jc : list) {
			if(jc.getPermUsed() > permUsed)
				permUsed = jvmCost.getPermUsed();
			if(jc.getNewUsed() > newUsed)
				newUsed = jvmCost.getNewUsed();
			if(jc.getnOU() > nOU)
				nOU = jvmCost.getnOU();
			if(jc.getxOU() > xOU)
				xOU = jvmCost.getxOU();
			if(jc.getnHeapU() > nHeapU)
				nHeapU = jvmCost.getnHeapU();
			if(jc.getxHeapU() > xHeapU)
				xHeapU = jvmCost.getxHeapU();
			if(jc.getTempObj() > tempObj)
				tempObj = jvmCost.getTempObj();
			if(jc.getFix() > fix)
				fix = jvmCost.getFix();
		}
		
		jvmCost.setPermUsed(permUsed);
		jvmCost.setNewUsed(newUsed);
		jvmCost.setnOU(nOU);
		jvmCost.setxOU(xOU);
		jvmCost.setnHeapU(nHeapU);
		jvmCost.setxHeapU(xHeapU);
		
		return jvmCost;
		*/
	}
	
}
