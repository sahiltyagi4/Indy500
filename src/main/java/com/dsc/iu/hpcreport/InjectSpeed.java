package com.dsc.iu.hpcreport;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

public class InjectSpeed {
	public static void main(String[] args) {
		try {
			BufferedReader rdr = new BufferedReader(new InputStreamReader(new FileInputStream(new File("C:\\Users\\styagi\\Desktop\\hpcreport\\input11.log"))));
			String record;
			List<Double> list = new LinkedList<Double>();
			while((record=rdr.readLine()) != null) {
				list.add(Double.parseDouble(record));
			}
			rdr.close();
			
			System.out.println("initial size of list:"+list.size());
			Random seed = new Random(7);
			for(int i=0;i<533;i++) {
				int record_num = seed.nextInt(106512);
				System.out.println(record_num);
				list.add(record_num, 0.00);
			}
			
			System.out.println("final list size:"+list.size());
			BufferedWriter wrtr = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File("C:\\Users\\styagi\\Desktop\\hpcreport\\injected11.log"))));
			Iterator<Double> itr = list.iterator();
			while(itr.hasNext()) {
				wrtr.write(String.valueOf(itr.next())+"\n");
			}
			wrtr.close();
			
		} catch (IOException e) {	e.printStackTrace(); }
	}
}
