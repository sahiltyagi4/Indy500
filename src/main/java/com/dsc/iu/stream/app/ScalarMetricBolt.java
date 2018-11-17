package com.dsc.iu.stream.app;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.numenta.nupic.Parameters;
import org.numenta.nupic.Parameters.KEY;
import org.numenta.nupic.algorithms.Anomaly;
import org.numenta.nupic.algorithms.Classifier;
import org.numenta.nupic.algorithms.SDRClassifier;
import org.numenta.nupic.algorithms.SpatialPooler;
import org.numenta.nupic.algorithms.TemporalMemory;
import org.numenta.nupic.network.Inference;
import org.numenta.nupic.network.Network;
import org.numenta.nupic.network.sensor.ObservableSensor;
import org.numenta.nupic.network.sensor.Publisher;
import org.numenta.nupic.network.sensor.Sensor;
import org.numenta.nupic.network.sensor.SensorParams;
import org.numenta.nupic.network.sensor.SensorParams.Keys;

import rx.Subscriber;

public class ScalarMetricBolt extends BaseRichBolt {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static OutputCollector collector;
	public static String carnum, metric;
	public static int min, max;
	Publisher manualpublish;
	Network network;
	
	public ScalarMetricBolt(String carnum, String metric, String min, String max) {
		ScalarMetricBolt.carnum = carnum;
		ScalarMetricBolt.metric = metric;
		ScalarMetricBolt.min = Integer.parseInt(min);
		ScalarMetricBolt.max = Integer.parseInt(max);
	}

	@Override
	public void execute(Tuple arg0) {
		//pushing to publisher values: telemtry_log_data,*metric from constructor*
		manualpublish.onNext(arg0.getStringByField("telemetry_log_time")+","+arg0.getStringByField(metric));
	}

	@Override
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector arg2) {
		collector = arg2;
		manualpublish = Publisher.builder()
						.addHeader(metric)
						.addHeader("float")
						.addHeader("B")
						.build();
		Sensor<ObservableSensor<String[]>> sensor = Sensor.create(ObservableSensor::create, SensorParams.create(Keys::obs, new Object[] {"singlemetricStreaming", manualpublish }));
		Parameters params = getParams();
		params = params.union(getNetworkLearningEncoderParams());
		Network network = Network.create("single_metric_anomaly_detection", params)
						.add(Network.createRegion("region1")
						.add(Network.createLayer("layer2/3", params)
						.alterParameter(KEY.AUTO_CLASSIFY, Boolean.TRUE)
						.add(Anomaly.create())
						.add(new TemporalMemory())
						.add(new SpatialPooler())
						.add(sensor)));
		
		network.observe().subscribe(getSubscriber());
		network.start();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		arg0.declare(new Fields("carnum","metric","dataval","score","timestamp"));
	}
	
	private static Parameters getParams() {
		Parameters parameters = Parameters.getAllDefaultParameters();
        parameters.set(Parameters.KEY.INPUT_DIMENSIONS, new int[] { 8 });
        parameters.set(KEY.COLUMN_DIMENSIONS, new int[] { 20 });
        parameters.set(KEY.CELLS_PER_COLUMN, 6);
        
        //SpatialPooler specific
        parameters.set(KEY.POTENTIAL_RADIUS, 12);//3
        parameters.set(KEY.POTENTIAL_PCT, 0.5);//0.5
        parameters.set(KEY.GLOBAL_INHIBITION, false);
        parameters.set(KEY.LOCAL_AREA_DENSITY, -1.0);
        parameters.set(KEY.NUM_ACTIVE_COLUMNS_PER_INH_AREA, 5.0);
        parameters.set(KEY.STIMULUS_THRESHOLD, 1.0);
        parameters.set(KEY.SYN_PERM_INACTIVE_DEC, 0.01);
        parameters.set(KEY.SYN_PERM_ACTIVE_INC, 0.1);
        parameters.set(KEY.SYN_PERM_TRIM_THRESHOLD, 0.05);
        parameters.set(KEY.SYN_PERM_CONNECTED, 0.1);
        parameters.set(KEY.MIN_PCT_OVERLAP_DUTY_CYCLES, 0.1);
        parameters.set(KEY.MIN_PCT_ACTIVE_DUTY_CYCLES, 0.1);
        parameters.set(KEY.DUTY_CYCLE_PERIOD, 10);
        parameters.set(KEY.MAX_BOOST, 10.0);
        parameters.set(KEY.SEED, 42);
        
        //Temporal Memory specific
        parameters.set(KEY.INITIAL_PERMANENCE, 0.2);
        parameters.set(KEY.CONNECTED_PERMANENCE, 0.8);
        parameters.set(KEY.MIN_THRESHOLD, 5);
        parameters.set(KEY.MAX_NEW_SYNAPSE_COUNT, 6);
        parameters.set(KEY.PERMANENCE_INCREMENT, 0.05);
        parameters.set(KEY.PERMANENCE_DECREMENT, 0.05);
        parameters.set(KEY.ACTIVATION_THRESHOLD, 4);
        
        return parameters;
	}
	
	private static Parameters getNetworkLearningEncoderParams() {
        Map<String, Map<String, Object>> fieldEncodings = getNetworkDemoFieldEncodingMap();

        Parameters p = Parameters.getEncoderDefaultParameters();
        p.set(KEY.GLOBAL_INHIBITION, true);
        p.set(KEY.COLUMN_DIMENSIONS, new int[] { 2048 });
        p.set(KEY.CELLS_PER_COLUMN, 32);
        p.set(KEY.NUM_ACTIVE_COLUMNS_PER_INH_AREA, 40.0);
        p.set(KEY.POTENTIAL_PCT, 0.8);
        p.set(KEY.SYN_PERM_CONNECTED,0.1);
        p.set(KEY.SYN_PERM_ACTIVE_INC, 0.0001);
        p.set(KEY.SYN_PERM_INACTIVE_DEC, 0.0005);
        p.set(KEY.MAX_BOOST, 1.0);
        p.set(KEY.INFERRED_FIELDS, getInferredFieldsMap(metric, SDRClassifier.class));
        
        p.set(KEY.MAX_NEW_SYNAPSE_COUNT, 20);
        p.set(KEY.INITIAL_PERMANENCE, 0.21);
        p.set(KEY.PERMANENCE_INCREMENT, 0.1);
        p.set(KEY.PERMANENCE_DECREMENT, 0.1);
        p.set(KEY.MIN_THRESHOLD, 9);
        p.set(KEY.ACTIVATION_THRESHOLD, 12);
        
        p.set(KEY.CLIP_INPUT, true);
        p.set(KEY.FIELD_ENCODING_MAP, fieldEncodings);

        return p;
    }
	
	private static Map<String, Class<? extends Classifier>> getInferredFieldsMap(String field, Class<? extends Classifier> classifier) {
        Map<String, Class<? extends Classifier>> inferredFieldsMap = new HashMap<>();
        inferredFieldsMap.put(field, classifier);
        return inferredFieldsMap;
    }
	
	private static Map<String, Map<String, Object>> getNetworkDemoFieldEncodingMap() {
        Map<String, Map<String, Object>> fieldEncodings = setupMap(null, 50, 21, min, max, 0, 0.1, null, Boolean.TRUE, null, metric, "float", "ScalarEncoder");
        return fieldEncodings;
    }
	
	private static Map<String, Map<String, Object>> setupMap(
            Map<String, Map<String, Object>> map,
            int n, int w, double min, double max, double radius, double resolution, Boolean periodic,
            Boolean clip, Boolean forced, String fieldName, String fieldType, String encoderType) {

        if(map == null) {
            map = new HashMap<String, Map<String, Object>>();
        }
        Map<String, Object> inner = null;
        if((inner = map.get(fieldName)) == null) {
            map.put(fieldName, inner = new HashMap<String, Object>());
        }

        inner.put("n", n);
        inner.put("w", w);
        inner.put("minVal", min);
        inner.put("maxVal", max);
        inner.put("radius", radius);
        inner.put("resolution", resolution);

        if(periodic != null) inner.put("periodic", periodic);
        if(clip != null) inner.put("clipInput", clip);
        if(forced != null) inner.put("forced", forced);
        if(fieldName != null) inner.put("fieldName", fieldName);
        if(fieldType != null) inner.put("fieldType", fieldType);
        if(encoderType != null) inner.put("encoderType", encoderType);

        return map;
    }
	
	private static Subscriber<Inference> getSubscriber() {
        return new Subscriber<Inference>() {
            @Override public void onCompleted() {}
            @Override public void onError(Throwable e) { e.printStackTrace(); }
            @Override public void onNext(Inference infer) {
            	//removing record num >0 condition
            	double actual_val = (Double)infer.getClassifierInput().get(metric).get("inputValue");
//            	 StringBuilder sb = new StringBuilder()
//            			 			.append(infer.getRecordNum())
//            			 			.append(",")
//            			 			.append(String.format("%3.2f", actual_val))
//            			 			.append(",")
//            			 			.append(infer.getAnomalyScore())
//            			 			.append(",")
//            			 			.append(System.currentTimeMillis());
            	
//            	StringBuilder sb = new StringBuilder()
//			 					.append(carnum)
//			 					.append(",")
//			 					.append(metric)
//			 					.append(",")
//			 					.append(String.format("%3.2f", actual_val))
//			 					.append(",")
//			 					.append(infer.getAnomalyScore())
//			 					.append(",")
//			 					.append(System.currentTimeMillis());
            	
            	collector.emit(new Values(carnum, metric, String.format("%3.2f", actual_val), infer.getAnomalyScore(), System.currentTimeMillis()));
            	
            }
        };
    }

}
