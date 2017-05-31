package com.yahoo.ycsb.db;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public class RandomUtility {
	public static List<String> randomSampling(Set<String> sample, int k) {
		List<String> sampleList = new ArrayList<>(sample);
		Collections.shuffle(sampleList);
		
		if (k >= sampleList.size()) {
			return sampleList;
		}
		
		return sampleList.subList(0, k);
	}
}
