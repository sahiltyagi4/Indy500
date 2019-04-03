package com.dsc.iu.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.joda.time.DateTimeZone;
import org.numenta.nupic.algorithms.AnomalyLikelihood;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

public class ParallelHTM99threads {
	static List<String> carlist = new ArrayList<String>();
	public static void main(String[] args) {
		carlist.add("20");carlist.add("21");carlist.add("13");carlist.add("98");carlist.add("19");carlist.add("6");carlist.add("33");carlist.add("24");carlist.add("26");carlist.add("7");carlist.add("60");carlist.add("27");
		carlist.add("22");carlist.add("18");carlist.add("3");carlist.add("4");carlist.add("28");carlist.add("32");carlist.add("59");carlist.add("25");carlist.add("64");carlist.add("10");carlist.add("15");carlist.add("17");
		carlist.add("12");carlist.add("1");carlist.add("9");carlist.add("14");carlist.add("23");carlist.add("30");carlist.add("29");carlist.add("88");carlist.add("66");
		
		
	}
	
	private void runHTM(String carnum, String metric, int threadnum) {
		new Thread("thread-"+threadnum+"-for car-"+carnum) {
			public void run() {
				System.out.println("thread name:"+Thread.currentThread().getName());
				String arg1 = "{\"input\":\"/Users/sahiltyagi/Desktop/numenta_indy2018-13-vspeed.csv\", \"output\":\"/Users/sahiltyagi/Desktop/javaNAB.csv\", \"aggregationInfo\": {\"seconds\": 0, \"fields\": [], \"months\": 0, \"days\": 0, \"years\": 0, \"hours\": 0, \"microseconds\": 0, \"weeks\": 0, \"minutes\": 0, \"milliseconds\": 0}, \"model\": \"HTMPrediction\", \"version\": 1, \"predictAheadTime\": null, \"modelParams\": {\"sensorParams\": {\"sensorAutoReset\": null, \"encoders\": {\"value\": {\"name\": \"value\", \"resolution\": 2.5143999999999997, \"seed\": 42, \"fieldname\": \"value\", \"type\": \"RandomDistributedScalarEncoder\"}, \"timestamp_dayOfWeek\": null, \"timestamp_timeOfDay\": {\"fieldname\": \"timestamp\", \"timeOfDay\": [21, 9.49], \"type\": \"DateEncoder\", \"name\": \"timestamp\"}, \"timestamp_weekend\": null}, \"verbosity\": 0}, \"anomalyParams\": {\"anomalyCacheRecords\": null, \"autoDetectThreshold\": null, \"autoDetectWaitRecords\": 5030}, \"spParams\": {\"columnCount\": 2048, \"synPermInactiveDec\": 0.0005, \"spatialImp\": \"cpp\", \"inputWidth\": 0, \"spVerbosity\": 0, \"synPermConnected\": 0.2, \"synPermActiveInc\": 0.003, \"potentialPct\": 0.8, \"numActiveColumnsPerInhArea\": 40, \"boostStrength\": 0.0, \"globalInhibition\": 1, \"seed\": 1956}, \"trainSPNetOnlyIfRequested\": false, \"clParams\": {\"alpha\": 0.035828933612158, \"verbosity\": 0, \"steps\": \"1\", \"regionName\": \"SDRClassifierRegion\"}, \"tmParams\": {\"columnCount\": 2048, \"activationThreshold\": 20, \"cellsPerColumn\": 32, \"permanenceDec\": 0.008, \"minThreshold\": 13, \"inputWidth\": 2048, \"maxSynapsesPerSegment\": 128, \"outputType\": \"normal\", \"initialPerm\": 0.24, \"globalDecay\": 0.0, \"maxAge\": 0, \"newSynapseCount\": 31, \"maxSegmentsPerCell\": 128, \"permanenceInc\": 0.04, \"temporalImp\": \"tm_cpp\", \"seed\": 1960, \"verbosity\": 0, \"predictedSegmentDecrement\": 0.001}, \"tmEnable\": true, \"clEnable\": false, \"spEnable\": true, \"inferenceType\": \"TemporalAnomaly\"}}";
	        	
	            // Parse command line args
	            OptionParser parser = new OptionParser();
	            parser.nonOptions("OPF parameters object (JSON)");
	            parser.acceptsAll(Arrays.asList("p", "params"), "OPF parameters file (JSON).\n(default: first non-option argument)")
	                .withOptionalArg()
	                .ofType(File.class);
	            parser.acceptsAll(Arrays.asList("i", "input"), "Input data file (csv).\n(default: stdin)")
	                .withOptionalArg()
	                .ofType(File.class);
	            parser.acceptsAll(Arrays.asList("o", "output"), "Output results file (csv).\n(default: stdout)")
	                .withOptionalArg()
	                .ofType(File.class);
	            parser.acceptsAll(Arrays.asList("s", "skip"), "Header lines to skip")
	                .withOptionalArg()
	                .ofType(Integer.class)
	                .defaultsTo(0);
	            parser.acceptsAll(Arrays.asList("h", "?", "help"), "Help");
	            OptionSet options = parser.parse(arg1);
	            
	            try {
	            		FileInputStream inp = new FileInputStream(new File("/Users/sahiltyagi/Desktop/benchmarks/HTMjava/modifiedHTMparams/htmjava_indy2018-13-vspeed-inout.csv"));
	            		JsonNode params;
	                    ObjectMapper mapper = new ObjectMapper();
	                    if (options.has("p")) {
	                        params = mapper.readTree((File)options.valueOf("p"));
	                    } 
	                    else if (options.nonOptionArguments().isEmpty()) {
	                        try { inp.close(); }catch(Exception ignore) {}
	                        if(options.has("o")) {
	                            try {}catch(Exception ignore) {}
	                        }
	                        throw new IllegalArgumentException("Expecting OPF parameters. See 'help' for more information");
	                    } else {
	                        params = mapper.readTree((String)options.nonOptionArguments().get(0));
	                    }
	                    
	                    int skip = (int) options.valueOf("s");

	                    // Force timezone to UTC
	                 DateTimeZone.setDefault(DateTimeZone.UTC);
	                 AnomalyLikelihood likelihood = new AnomalyLikelihood(true, 8640, false, 375, 375);
	                    
	                 
	            } catch(IOException e) {
	            		e.printStackTrace();
	            }
			}
		}.start();
	}
}
