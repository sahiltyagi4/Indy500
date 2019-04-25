package com.dsc.iu.utils;

import java.util.LinkedList;
import java.util.List;

public class ExternalThreadSyncRun {
	
	public static void main(String[] args) {
		
		List<String> carlist = new LinkedList<String>();
		carlist.add("22");
		
//		carlist.add("20");carlist.add("21");carlist.add("13");carlist.add("98");carlist.add("19");carlist.add("33");carlist.add("24");carlist.add("26");carlist.add("7");carlist.add("6");
//	    carlist.add("60");carlist.add("27");carlist.add("22");carlist.add("18");carlist.add("3");carlist.add("4");carlist.add("28");carlist.add("32");carlist.add("59");carlist.add("25");
//	    carlist.add("64");carlist.add("10");carlist.add("15");carlist.add("17");carlist.add("12");carlist.add("1");carlist.add("9");carlist.add("14");carlist.add("23");carlist.add("30");
//	    carlist.add("29");carlist.add("88");carlist.add("66");
		
		int thread=0;
        for(String carnum : carlist) {
        		ThreemetricSynchronization ob = new ThreemetricSynchronization();
        		ob.threadrun(carnum, thread);
        		thread++;
        		System.out.println("launched HTM SEQUENTIAL EXECUTION fo car:" + carnum);
        }
		
	}
}
