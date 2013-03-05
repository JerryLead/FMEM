package memory.model.jvm;

import java.util.List;

import profile.commons.metrics.JstatItem;
import profile.commons.metrics.JvmItem;


public class JvmMetricsUtil {
	// get the JvmItem at given time
	public static int getJvmItem(List<JvmItem> jvmItemList, long timeStampMS) {
		if (jvmItemList == null)
			return -1;

		int beforeIndex = 0;
		int afterIndex = jvmItemList.size() - 1;

		for (int i = 0; i < jvmItemList.size(); i++) {
			JvmItem item = jvmItemList.get(i);
			if (item.getTimeStampMS() == timeStampMS) {
				return i;
			} else if (timeStampMS > item.getTimeStampMS()) {
				beforeIndex = i;
			} else {
				afterIndex = i;
			}
		}

		return afterIndex;
	}	
		
	// get the JvmItem at given time
	public static int getJstatItem(List<JstatItem> jstatItemList, long timeStampMS) {
		if (jstatItemList == null)
			return -1;

		int beforeIndex = 0;
		int afterIndex = jstatItemList.size() - 1;

		for (int i = 0; i < jstatItemList.size(); i++) {
			JstatItem item = jstatItemList.get(i);
			if (item.getTimeStampMS() == timeStampMS) {
				return i;
			} else if (timeStampMS > item.getTimeStampMS()) {
				beforeIndex = i;
			} else {
				afterIndex = i;
			}
		}

		return afterIndex;
	}

	public static JvmModel maxJstatValue(List<JstatItem> jstatItemList, int start, int end) {
		JvmModel jvmModel = new JvmModel();
		for(int i = start; i <= end; i++) {
			JstatItem item = jstatItemList.get(i);
			jvmModel.selectMaxValue(item);
		}
		return jvmModel;
	}

	public static void updateMaxJvmValue(JvmModel jvmModel, List<JvmItem> jvmItemList, int start, int end) {
		for(int i = start; i <= end; i++) 
			jvmModel.selectMaxValue(jvmItemList.get(i));	
	}		
}
