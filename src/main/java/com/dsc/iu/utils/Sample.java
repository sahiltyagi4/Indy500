package com.dsc.iu.utils;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

public class Sample {
	public static void main(String[] args) {
		
		JSONObject data = new JSONObject();
		data.put("carnum", 23);
		data.put("score", 0.5);
		System.out.println(data.toJSONString());
		int x=3;
		int y = 3;
		while(true) {
			System.out.println(x+y);
			x++;
			y--;
		}
	}
}
