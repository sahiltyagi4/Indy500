package com.dsc.iu.utils;

/* ---------------------------------------------------------------------
 * Numenta Platform for Intelligent Computing (NuPIC)
 * Copyright (C) 2014, Numenta, Inc.  Unless you have an agreement
 * with Numenta, Inc., for a separate license for this software code, the
 * following terms and conditions apply:
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero Public License version 3 as
 * published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero Public License for more details.
 *
 * You should have received a copy of the GNU Affero Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 *
 * http://numenta.org/licenses/
 * ---------------------------------------------------------------------
 */

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.json.simple.JSONObject;
import org.numenta.nupic.Parameters;
import org.numenta.nupic.Parameters.KEY;
import org.numenta.nupic.algorithms.Anomaly;
import org.numenta.nupic.algorithms.AnomalyLikelihood;
import org.numenta.nupic.algorithms.SpatialPooler;
import org.numenta.nupic.algorithms.TemporalMemory;
import org.numenta.nupic.network.Inference;
import org.numenta.nupic.network.Layer;
import org.numenta.nupic.network.Network;
import org.numenta.nupic.network.PublisherSupplier;
import org.numenta.nupic.network.Region;
import org.numenta.nupic.network.sensor.ObservableSensor;
import org.numenta.nupic.network.sensor.Publisher;
import org.numenta.nupic.network.sensor.Sensor;
import org.numenta.nupic.network.sensor.SensorParams;
import org.numenta.nupic.util.Tuple;
import org.numenta.nupic.util.UniversalRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

public class ThreemetricSynchronization {
    protected static final Logger LOGGER = LoggerFactory.getLogger(ThreemetricSynchronization.class);
   
    private double SPATIAL_TOLERANCE = 0.05;
    public ConcurrentHashMap<String, Queue<Double>> anomalyScoreouts = new ConcurrentHashMap<>();
    private String[] metrics = {"vehicleSpeed", "engineSpeed", "throttle"};
    private Queue<JSONObject> jsonq = new LinkedList<>();

    /**
     * Create HTM Model to be used by NAB
     * @param modelParams OPF Model parameters to parameters from
     */

    /**
     * Update encoders parameters
     * @param modelParams OPF Model parameters to get encoder parameters from
     * @return Updated Encoder parameters suitable for {@link Parameters.KEY.FIELD_ENCODING_MAP}
     */
    public Map<String, Map<String, Object>> getFieldEncodingMap(JsonNode modelParams) {
        Map<String, Map<String, Object>> fieldEncodings = new HashMap<>();
        String fieldName;
        Map<String, Object> fieldMap;
        JsonNode encoders = modelParams.path("encoders");
        LOGGER.trace("getFieldEncodingMap({})", encoders);
        for (JsonNode node : encoders) {
            if (node.isNull())
                continue;

            fieldName = node.path("fieldname").textValue();
            fieldMap = fieldEncodings.get(fieldName);
            if (fieldMap == null) {
                fieldMap = new HashMap<>();
                fieldMap.put("fieldName", fieldName);
                fieldEncodings.put(fieldName, fieldMap);
            }
            fieldMap.put("encoderType", node.path("type").textValue());
            if (node.has("timeOfDay")) {
                JsonNode timeOfDay = node.get("timeOfDay");
                fieldMap.put("fieldType", "datetime");
                fieldMap.put(KEY.DATEFIELD_PATTERN.getFieldName(), "YYYY-MM-dd HH:mm:ss.SSS");
                fieldMap.put(KEY.DATEFIELD_TOFD.getFieldName(),
                        new Tuple(timeOfDay.get(0).asInt(), timeOfDay.get(1).asDouble()));
            } else {
                fieldMap.put("fieldType", "float");
            }
            if (node.has("resolution")) {
                fieldMap.put("resolution", node.get("resolution").asDouble());
            }
        }
        LOGGER.trace("getFieldEncodingMap => {}", fieldEncodings);
        return fieldEncodings;
    }

    /**
     * Update Spatial Pooler parameters
     * @param modelParams OPF Model parameters to get spatial pooler parameters from
     * @return Updated Spatial Pooler parameters
     */
    public Parameters getSpatialPoolerParams(JsonNode modelParams) {
        Parameters p = Parameters.getSpatialDefaultParameters();
        JsonNode spParams = modelParams.path("spParams");
        LOGGER.trace("getSpatialPoolerParams({})", spParams);
        if (spParams.has("columnCount")) {
            p.set(KEY.COLUMN_DIMENSIONS, new int[]{spParams.get("columnCount").asInt()});
        }
        if (spParams.has("maxBoost")) {
            p.set(KEY.MAX_BOOST, spParams.get("maxBoost").asDouble());
        }
        if (spParams.has("synPermInactiveDec")) {
            p.set(KEY.SYN_PERM_INACTIVE_DEC, spParams.get("synPermInactiveDec").asDouble());
        }
        if (spParams.has("synPermConnected")) {
            p.set(KEY.SYN_PERM_CONNECTED, spParams.get("synPermConnected").asDouble());
        }
        if (spParams.has("synPermActiveInc")) {
            p.set(KEY.SYN_PERM_ACTIVE_INC, spParams.get("synPermActiveInc").asDouble());
        }
        if (spParams.has("numActiveColumnsPerInhArea")) {
            p.set(KEY.NUM_ACTIVE_COLUMNS_PER_INH_AREA, spParams.get("numActiveColumnsPerInhArea").asDouble());
        }
        if (spParams.has("globalInhibition")) {
            p.set(KEY.GLOBAL_INHIBITION, spParams.get("globalInhibition").asBoolean());
        }
        if (spParams.has("potentialPct")) {
            p.set(KEY.POTENTIAL_PCT, spParams.get("potentialPct").asDouble());
        }

        LOGGER.trace("getSpatialPoolerParams => {}", p);
        return p;
    }

    /**
     * Update Temporal Memory parameters
     * @param modelParams OPF Model parameters to get Temporal Memory parameters from
     * @return Updated Temporal Memory parameters
     */
    public Parameters getTemporalMemoryParams(JsonNode modelParams) {
        Parameters p = Parameters.getTemporalDefaultParameters();
        JsonNode tpParams = modelParams.path("tpParams");
        LOGGER.trace("getTemporalMemoryParams({})", tpParams);
        if (tpParams.has("columnCount")) {
            p.set(KEY.COLUMN_DIMENSIONS, new int[]{tpParams.get("columnCount").asInt()});
        }
        if (tpParams.has("activationThreshold")) {
            p.set(KEY.ACTIVATION_THRESHOLD, tpParams.get("activationThreshold").asInt());
        }
        if (tpParams.has("cellsPerColumn")) {
            p.set(KEY.CELLS_PER_COLUMN, tpParams.get("cellsPerColumn").asInt());
        }
        if (tpParams.has("permanenceInc")) {
            p.set(KEY.PERMANENCE_INCREMENT, tpParams.get("permanenceInc").asDouble());
        }
        if (tpParams.has("minThreshold")) {
            p.set(KEY.MIN_THRESHOLD, tpParams.get("minThreshold").asInt());
        }
        if (tpParams.has("initialPerm")) {
            p.set(KEY.INITIAL_PERMANENCE, tpParams.get("initialPerm").asDouble());
        }
        if(tpParams.has("maxSegmentsPerCell")) {
            p.set(KEY.MAX_SEGMENTS_PER_CELL, tpParams.get("maxSegmentsPerCell").asInt());
        }
        if(tpParams.has("maxSynapsesPerSegment")) {
            p.set(KEY.MAX_SYNAPSES_PER_SEGMENT, tpParams.get("maxSynapsesPerSegment").asInt());
        }
        if (tpParams.has("permanenceDec")) {
            p.set(KEY.PERMANENCE_DECREMENT, tpParams.get("permanenceDec").asDouble());
        }
        if (tpParams.has("predictedSegmentDecrement")) {
            p.set(KEY.PREDICTED_SEGMENT_DECREMENT, tpParams.get("predictedSegmentDecrement").asDouble());
        }
        if (tpParams.has("newSynapseCount")) {
            p.set(KEY.MAX_NEW_SYNAPSE_COUNT, tpParams.get("newSynapseCount").intValue());
        }

        LOGGER.trace("getTemporalMemoryParams => {}", p);
        return p;
    }

    /**
     * Update Sensor parameters
     * @param modelParams OPF Model parameters to get Sensor parameters from
     * @return Updated Sensor parameters
     */
    public Parameters getSensorParams(JsonNode modelParams) {
        JsonNode sensorParams = modelParams.path("sensorParams");
        LOGGER.trace("getSensorParams({})", sensorParams);
        Map<String, Map<String, Object>> fieldEncodings = getFieldEncodingMap(sensorParams);
        Parameters p = Parameters.empty();
        p.set(KEY.CLIP_INPUT, true);
        p.set(KEY.FIELD_ENCODING_MAP, fieldEncodings);

        LOGGER.trace("getSensorParams => {}", p);
        return p;
    }

    /**
     * Update NAB parameters
     * @param params OPF parameters to get NAB model parameters from
     * @return Updated Model parameters
     */
    public Parameters getModelParameters(JsonNode params) {
        JsonNode modelParams = params.path("modelParams");
        LOGGER.trace("getModelParameters({})", modelParams);
        Parameters p = Parameters.getAllDefaultParameters()
            .union(getSpatialPoolerParams(modelParams))
            .union(getTemporalMemoryParams(modelParams))
            .union(getSensorParams(modelParams));
        
        p.set(KEY.RANDOM, new UniversalRandom(42));
        LOGGER.trace("getModelParameters => {}", p);
        return p;
    }
    
    protected void threadrun(String carnum, int threadnum) {
    		new Thread("thread-"+threadnum+"-for car-"+carnum) {
    			public void run() {
    				ThreemetricSynchronization model = new ThreemetricSynchronization();
    				
    				for(int i=0;i<metrics.length;i++) {
    					anomalyScoreouts.put(metrics[i], new ConcurrentLinkedQueue<>());
    				}
    				
    				Network speed_network, rpm_network, throttle_network;
    				PublisherSupplier speed_supplier, rpm_supplier, throttle_supplier;
    				
    				String arg1 = "{\"aggregationInfo\": {\"seconds\": 0, \"fields\": [], \"months\": 0, \"days\": 0, \"years\": 0, \"hours\": 0, \"microseconds\": 0, \"weeks\": 0, \"minutes\": 0, \"milliseconds\": 0}, "
    	    				+ "\"model\": \"HTMPrediction\", \"version\": 1, \"predictAheadTime\": null, \"modelParams\": {\"sensorParams\": {\"sensorAutoReset\": null, \"encoders\": {\"value\": {\"name\": \"value\", "
    	    				+ "\"resolution\": 2.5143999999999997, \"seed\": 42, \"fieldname\": \"value\", \"type\": \"RandomDistributedScalarEncoder\"}, \"timestamp_dayOfWeek\": null, "
    	    				+ "\"timestamp_timeOfDay\": {\"fieldname\": \"timestamp\", \"timeOfDay\": [21, 9.49], \"type\": \"DateEncoder\", \"name\": \"timestamp\"}, \"timestamp_weekend\": null}, \"verbosity\": 0}, "
    	    				+ "\"anomalyParams\": {\"anomalyCacheRecords\": null, \"autoDetectThreshold\": null, \"autoDetectWaitRecords\": 5030}, \"spParams\": {\"columnCount\": 2048, \"synPermInactiveDec\": 0.0005, "
    	    				+ "\"spatialImp\": \"cpp\", \"inputWidth\": 0, \"spVerbosity\": 0, \"synPermConnected\": 0.2, \"synPermActiveInc\": 0.003, \"potentialPct\": 0.8, \"numActiveColumnsPerInhArea\": 40, "
    	    				+ "\"boostStrength\": 0.0, \"globalInhibition\": 1, \"seed\": 1956}, \"trainSPNetOnlyIfRequested\": false, \"clParams\": {\"alpha\": 0.035828933612158, \"verbosity\": 0, \"steps\": \"1\", "
    	    				+ "\"regionName\": \"SDRClassifierRegion\"}, \"tmParams\": {\"columnCount\": 2048, \"activationThreshold\": 20, \"cellsPerColumn\": 32, \"permanenceDec\": 0.008, \"minThreshold\": 13, "
    	    				+ "\"inputWidth\": 2048, \"maxSynapsesPerSegment\": 128, \"outputType\": \"normal\", \"initialPerm\": 0.24, \"globalDecay\": 0.0, \"maxAge\": 0, \"newSynapseCount\": 31, "
    	    				+ "\"maxSegmentsPerCell\": 128, \"permanenceInc\": 0.04, \"temporalImp\": \"tm_cpp\", \"seed\": 1960, \"verbosity\": 0, \"predictedSegmentDecrement\": 0.001}, \"tmEnable\": true, "
    	    				+ "\"clEnable\": false, \"spEnable\": true, \"inferenceType\": \"TemporalAnomaly\"}}";
    	      	
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
    	          
    	       // Parse OPF Model Parameters
    	          JsonNode params=null;
    	          ObjectMapper mapper = new ObjectMapper();
    	          try {
    	        	  		if (options.has("p")) {
    	        	  			params = mapper.readTree((File)options.valueOf("p"));
    	        	  		}	 
    	        	  		else if (options.nonOptionArguments().isEmpty()) {
    	        	  			//try { inp.close(); }catch(Exception ignore) {}
    	        	  			if(options.has("o")) {
    	        	  				//try { outpw.flush(); outpw.close(); }catch(Exception ignore) {}
    	        	  			}
    	        	  			throw new IllegalArgumentException("Expecting OPF parameters. See 'help' for more information");
    	        	  		} else {
    	        	  			params = mapper.readTree((String)options.nonOptionArguments().get(0));
    	        	  		}
    	          } catch(Exception e) {}
    	          
    	          
    	          // Create Sensor publisher to push NAB input data to network
  		        speed_supplier = PublisherSupplier.builder()
  		                .addHeader("timestamp,value")
  		                .addHeader("datetime,float")
  		                .addHeader("T,B")
  		                .build();
  		        
  		        rpm_supplier = PublisherSupplier.builder()
  		                .addHeader("timestamp,value")
  		                .addHeader("datetime,float")
  		                .addHeader("T,B")
  		                .build();
  		        
  		        throttle_supplier = PublisherSupplier.builder()
  		                .addHeader("timestamp,value")
  		                .addHeader("datetime,float")
  		                .addHeader("T,B")
  		                .build();

  		        // Get updated model parameters
  		        Parameters parameters = model.getModelParameters(params);
  		        
  		        LOGGER.info("RUNNING WITH NO EXPLICIT P_RADIUS SET");

  		        // Create NAB Networks
  		        speed_network = Network.create("speed Network", parameters)
  		            .add(Network.createRegion("speed Region")
  		                .add(Network.createLayer("speed Layer", parameters)
  		                    .add(Anomaly.create())
  		                    .add(new TemporalMemory())
  		                    .add(new SpatialPooler())
  		                    .add(Sensor.create(ObservableSensor::create,
  		                            SensorParams.create(SensorParams.Keys::obs, "Manual Input speed", speed_supplier)))));
  		        
  		        rpm_network = Network.create("rpm Network", parameters)
  		                .add(Network.createRegion("rpm Region")
  		                    .add(Network.createLayer("rpm Layer", parameters)
  		                        .add(Anomaly.create())
  		                        .add(new TemporalMemory())
  		                        .add(new SpatialPooler())
  		                        .add(Sensor.create(ObservableSensor::create,
  		                                SensorParams.create(SensorParams.Keys::obs, "Manual Input rpm", rpm_supplier)))));
  		        
  		        throttle_network = Network.create("throttle Network", parameters)
  		                .add(Network.createRegion("throttle Region")
  		                    .add(Network.createLayer("throttle Layer", parameters)
  		                        .add(Anomaly.create())
  		                        .add(new TemporalMemory())
  		                        .add(new SpatialPooler())
  		                        .add(Sensor.create(ObservableSensor::create,
  		                                SensorParams.create(SensorParams.Keys::obs, "Manual Input throttle", throttle_supplier)))));
    	          
  		        
  		      Publisher speed_publisher = speed_supplier.get();
  	          Publisher rpm_publisher = rpm_supplier.get();
  	          Publisher throttle_publisher = throttle_supplier.get();
  		        
    	          // Force timezone to UTC
    	          DateTimeZone.setDefault(DateTimeZone.UTC);
    	          
    			  model.callhtm(carnum, threadnum, params, speed_network, rpm_network, throttle_network, 
    					  	speed_publisher, rpm_publisher, throttle_publisher);
    			}
    		}.start();
    }
    
    private void callhtm(String carnum, int thread, JsonNode params, Network speed_network, Network rpm_network, 
    					Network throttle_network, Publisher speed_publisher, Publisher rpm_publisher, Publisher throttle_publisher) {
    	try {
    		AnomalyLikelihood speed_likelihood, rpm_likelihood, throttle_likelihood;
    		
    		File logfile = new File("/scratch_ssd/sahil/IPBroadcaster_Input_2018-05-27_0.log");
//    		File logfile = new File("/Users/sahiltyagi/Downloads/Indy_500_2018/IPBroadcaster_Input_2018-05-27_0.log");
    		FileInputStream inp = new FileInputStream(logfile);
    		
          PrintWriter inpw, outpw;
          String fileloc = "/scratch_ssd/sahil/parallelsync";
//          String fileloc = "/Users/sahiltyagi/Desktop/parallelsync";
  		  
          File infile = new File(fileloc + "/input-" + carnum + "-" + thread + ".csv");
  		  inpw = new PrintWriter(infile);
  		  
  		  File outfile = new File(fileloc + "/output-" + carnum + "-" + thread + ".csv");
  		  outpw = new PrintWriter(outfile);
  		  
  		  boolean speed_spatialAnomaly = false, rpm_spatialAnomaly = false, throttle_spatialAnomaly = false;
  		  double speed_prev_likelihood=0., rpm_prev_likelihood=0., throttle_prev_likelihood=0.;
          
          //anomaly likelihood calculation
          speed_likelihood = new AnomalyLikelihood(true, 8640, false, 375, 375);
          rpm_likelihood = new AnomalyLikelihood(true, 8640, false, 375, 375);
          throttle_likelihood = new AnomalyLikelihood(true, 8640, false, 375, 375);
          
          // Create NAB Network Models
          starthtmnetworks(speed_network, speed_spatialAnomaly, params, speed_likelihood, carnum, metrics[0], thread, outpw);
          starthtmnetworks(rpm_network, rpm_spatialAnomaly, params, rpm_likelihood, carnum, metrics[1], thread, outpw);
          starthtmnetworks(throttle_network, throttle_spatialAnomaly, params, throttle_likelihood, carnum, metrics[2], thread, outpw);
          
          BufferedReader in = new BufferedReader(new InputStreamReader(inp));
          String line;
          double speed_init_min=0L, speed_init_max=0L, speed_maxExpected=0L, speed_minExpected=0L;
          double rpm_init_min=0L, rpm_init_max=0L, rpm_maxExpected=0L, rpm_minExpected=0L;
          double throttle_init_min=0L, throttle_init_max=0L, throttle_maxExpected=0L, throttle_minExpected=0L;
          
          SimpleDateFormat df = new SimpleDateFormat("YYYY-mm-dd HH:mm:ss.SSS");
          Date dt=null;
          try {
        	  		dt = df.parse("2018-05-27 16:23:00.000");
          } catch(ParseException p) {}
          
          while ((line = in.readLine()) != null && line.trim().length() > 0) {
        	  		if(line.startsWith("$P") && line.split("�")[2].matches("\\d+:\\d+:\\d+.\\d+") && line.split("�")[1].equalsIgnoreCase(carnum)) {
        	  			
        	  			String racetime = "2018-05-27 " + line.split("�")[2].trim();
        	  			
        	  			Date dtformat = null;
        	  			try {
        	  				dtformat = df.parse(racetime);
        	  			} catch(ParseException p) {}
        	  			
        	  			if(dtformat.getTime() > dt.getTime()) {
        	  				
        	  				JSONObject obj = new JSONObject();
        	  				obj.put(metrics[0], Double.parseDouble(line.split("�")[4]));
        	  				obj.put(metrics[1], Double.parseDouble(line.split("�")[5]));
        	  				obj.put(metrics[2], Double.parseDouble(line.split("�")[6]));
        	  				obj.put("timeOfDay", racetime);
          	              
        	  				jsonq.add(obj);
        	  				System.out.println("json queue is: " + jsonq.peek().toJSONString());
        	  				
          	            //SPEED
          	              //spatial anomaly logic. if spatialAnomaly is TRUE, then logscore = 1.0
          	              double speed  = Double.parseDouble(line.split("�")[4]);
          	              speed_spatialAnomaly = false;
          	              if(speed_init_min != speed_init_max) {
          	              		double tolerance = (speed_init_max - speed_init_min) * SPATIAL_TOLERANCE;
          	              		speed_maxExpected = speed_init_max + tolerance;
          	              		speed_minExpected = speed_init_min - tolerance;
          	              }
          	              
          	              if(speed > speed_maxExpected || speed < speed_minExpected) {
          	            	  		speed_spatialAnomaly = true;
          	              }
          	              
          	              if(speed > speed_init_max || speed_init_max == 0L) {
          	            	  		speed_init_max = speed;
          	              }
          	              
          	              if(speed < speed_init_min || speed_init_min == 0L) {
          	            	  		speed_init_min = speed;
          	              }
          	              
          	              long speed_timestamp = System.currentTimeMillis();
          	              speed_publisher.onNext(racetime + "," + speed);
          	              //System.out.println("publisher thread:" + Thread.currentThread().getName() + "," + Thread.currentThread().getId());
          	              
          	            //RPM
          	            double rpm  = Double.parseDouble(line.split("�")[5]);
        	              	rpm_spatialAnomaly = false;
        	              	if(rpm_init_min != rpm_init_max) {
        	              		double tolerance = (rpm_init_max - rpm_init_min) * SPATIAL_TOLERANCE;
        	              		rpm_maxExpected = rpm_init_max + tolerance;
        	              		rpm_minExpected = rpm_init_min - tolerance;
        	              	}
        	              
        	              	if(rpm > rpm_maxExpected || rpm < rpm_minExpected) {
        	            	  		rpm_spatialAnomaly = true;
        	              	}
        	              
        	              	if(rpm > rpm_init_max || rpm_init_max == 0L) {
        	            	  		rpm_init_max = rpm;
        	              	}
        	              
        	              	if(rpm < rpm_init_min || rpm_init_min == 0L) {
        	            	  		rpm_init_min = rpm;
        	              	}
        	              
        	              	long rpm_timestamp = System.currentTimeMillis();
        	              	rpm_publisher.onNext(racetime + "," + rpm);
          	              
        	              //THROTTLE
        	              	double throttle  = Double.parseDouble(line.split("�")[6]);
        	              	throttle_spatialAnomaly = false;
        	              	if(throttle_init_min != throttle_init_max) {
        	              		double tolerance = (throttle_init_max - throttle_init_min) * SPATIAL_TOLERANCE;
        	              		throttle_maxExpected = throttle_init_max + tolerance;
        	              		throttle_minExpected = throttle_init_min - tolerance;
        	              	}
        	              
        	              	if(throttle > throttle_maxExpected || throttle < throttle_minExpected) {
        	              		throttle_spatialAnomaly = true;
        	              	}
        	              
        	              	if(throttle > throttle_init_max || throttle_init_max == 0L) {
        	              		throttle_init_max = throttle;
        	              	}
        	              
        	              	if(throttle < throttle_init_min || throttle_init_min == 0L) {
        	              		throttle_init_min = throttle;
        	              	}
        	              
        	              	long throttle_timestamp = System.currentTimeMillis();
        	              	throttle_publisher.onNext(racetime + "," + throttle);
        	              	
        	              	//FILE WRITE --> INPUT DATA FOR POST-PROCESSING
        	              	inpw.println(carnum + "_" + racetime + "," + speed + "," + speed_timestamp + "," + rpm + "," + rpm_timestamp 
        	              				+ "," + throttle + "," + throttle_timestamp);
          	              
          	            try {
          	            	 	Thread.sleep(30000);
          	            } catch(InterruptedException i) {}
        	  			}
        	  		}
          }
      
          speed_publisher.onComplete();
          rpm_publisher.onComplete();
          throttle_publisher.onComplete();
          in.close();
          LOGGER.trace("Done publishing data");
      } catch (IOException e) {
          e.printStackTrace();
      }
    }
    
    private void starthtmnetworks(Network htmnetwork, boolean spatialAnomaly, JsonNode params, AnomalyLikelihood likelihood, 
    								String carnum, String metric, int thread_num, PrintWriter pw) {
    		
    		System.out.println("going to start htm..");
    		
    		if(metric.equalsIgnoreCase(metrics[0])) {
    			htmnetwork.observe().subscribe((Inference inference) -> {
    	        	
    	            double score = inference.getAnomalyScore();
    	            double value = (Double)inference.getClassifierInput().get("value").get("inputValue");
    	            DateTime timestamp = (DateTime)inference.getClassifierInput().get("timestamp").get("inputValue");
    	            
    	            double anomaly_likelihood = likelihood.anomalyProbability(value, score, timestamp);
    	            
    	            double logscore=0L;
    	            if(spatialAnomaly) {
    	            		logscore = 1.0;
    	            } else {
    	            		logscore = AnomalyLikelihood.computeLogLikelihood(anomaly_likelihood);
    	            }
    	            
    	            System.out.println("speed logscore:"+logscore);
    	            anomalyScoreouts.get(metrics[0]).add(logscore);
    	            System.out.println("in speed: " + anomalyScoreouts.get(metrics[0]).peek());
    	            boolean hasRecords = true;
    	            for (int i = 0; i < metrics.length; i++) {
    	            		hasRecords &= !this.anomalyScoreouts.get(metrics[i]).isEmpty();
    	            		System.out.println("speed hasrec loop");
                }
    	            System.out.println("hasrecords condition is:"+hasRecords + " in metric " + metrics[0]);
                    
                if (hasRecords) {
                		JSONObject obj = this.jsonq.poll();
                		System.out.println("has records speed");
                		for (int i = 0; i < metrics.length; i++) {
                			obj.put(metric+"_Anomaly", anomalyScoreouts.get(metrics[i]).poll());
                		}
                		
                		pw.println(obj.get("carnum") + "," + obj.get("timeOfDay") + "," + obj.get(metrics[0]) + "," + obj.get(metrics[1]) 
							+ "," + obj.get(metrics[2]) + "," + obj.get(metrics[0] + "_Anomaly") + "," + obj.get(metrics[1] + "_Anomaly") 
							+ "," + obj.get(metrics[2] + "_Anomaly") + System.currentTimeMillis());
                }
    	            
    	        }, (error) -> {
    	            LOGGER.error("Error processing data", error);
    	        }, () -> {
    	            LOGGER.trace("Done processing data");
    	        });
    		} else if(metric.equalsIgnoreCase(metrics[1])) {
    			htmnetwork.observe().subscribe((Inference inference) -> {
    	        	
    	            double score = inference.getAnomalyScore();
    	            double value = (Double)inference.getClassifierInput().get("value").get("inputValue");
    	            DateTime timestamp = (DateTime)inference.getClassifierInput().get("timestamp").get("inputValue");
    	            
    	            double anomaly_likelihood = likelihood.anomalyProbability(value, score, timestamp);
    	            
    	            double logscore=0L;
    	            if(spatialAnomaly) {
    	            		logscore = 1.0;
    	            } else {
    	            		logscore = AnomalyLikelihood.computeLogLikelihood(anomaly_likelihood);
    	            }
    	            
    	            System.out.println("RPM logscore:"+logscore);
    	            anomalyScoreouts.get(metrics[1]).add(logscore);
    	            System.out.println("in rpm: " + anomalyScoreouts.get(metrics[1]).peek());
    	            boolean hasRecords = true;
    	            for (int i = 0; i < metrics.length; i++) {
    	            		hasRecords &= !this.anomalyScoreouts.get(metrics[i]).isEmpty();
    	            		System.out.println("rpm hasrec loop");
                }
    	            System.out.println("hasrecords condition is:"+hasRecords + " in metric " + metrics[1]);
                    
                if (hasRecords) {
                		System.out.println("has records RPM");
                		JSONObject obj = this.jsonq.poll();
                		for (int i = 0; i < metrics.length; i++) {
                			obj.put(metric+"_Anomaly", anomalyScoreouts.get(metrics[i]).poll());
                		}
                		
                		pw.println(obj.get("carnum") + "," + obj.get("timeOfDay") + "," + obj.get(metrics[0]) + "," + obj.get(metrics[1]) 
        						+ "," + obj.get(metrics[2]) + "," + obj.get(metrics[0] + "_Anomaly") + "," + obj.get(metrics[1] + "_Anomaly") 
        						+ "," + obj.get(metrics[2] + "_Anomaly") + System.currentTimeMillis());
                }
    	           
    	        }, (error) -> {
    	            LOGGER.error("Error processing data", error);
    	        }, () -> {
    	            LOGGER.trace("Done processing data");
    	        });
    		} else if(metric.equalsIgnoreCase(metrics[2])) {
    			htmnetwork.observe().subscribe((Inference inference) -> {
    	        	
    	            double score = inference.getAnomalyScore();
    	            double value = (Double)inference.getClassifierInput().get("value").get("inputValue");
    	            DateTime timestamp = (DateTime)inference.getClassifierInput().get("timestamp").get("inputValue");
    	            
    	            double anomaly_likelihood = likelihood.anomalyProbability(value, score, timestamp);
    	            
    	            double logscore=0L;
    	            if(spatialAnomaly) {
    	            		logscore = 1.0;
    	            } else {
    	            		logscore = AnomalyLikelihood.computeLogLikelihood(anomaly_likelihood);
    	            }
    	            
    	            System.out.println("throttle logscore:"+logscore);
    	            anomalyScoreouts.get(metrics[2]).add(logscore);
    	            System.out.println("in throttle: " + anomalyScoreouts.get(metrics[2]).peek());
    	            boolean hasRecords = true;
    	            for (int i = 0; i < metrics.length; i++) {
    	            		hasRecords &= !this.anomalyScoreouts.get(metrics[i]).isEmpty();
    	            		System.out.println("throttle hasrec loop");
                }
    	            System.out.println("hasrecords condition is:"+hasRecords + " in metric " + metrics[2]);
                    
                if (hasRecords) {
                		System.out.println("has records throttle");
                		JSONObject obj = this.jsonq.poll();
                		for (int i = 0; i < metrics.length; i++) {
                			obj.put(metric+"_Anomaly", anomalyScoreouts.get(metrics[i]).poll());
                		}
                		
                		pw.println(obj.get("carnum") + "," + obj.get("timeOfDay") + "," + obj.get(metrics[0]) + "," + obj.get(metrics[1]) 
							+ "," + obj.get(metrics[2]) + "," + obj.get(metrics[0] + "_Anomaly") + "," + obj.get(metrics[1] + "_Anomaly") 
							+ "," + obj.get(metrics[2] + "_Anomaly") + System.currentTimeMillis());
                }
    	           
    	        }, (error) -> {
    	            LOGGER.error("Error processing data", error);
    	        }, () -> {
    	            LOGGER.trace("Done processing data");
    	        });
    		}
        
        htmnetwork.start();
    }
}
