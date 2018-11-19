package com.dsc.iu.streaming;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;

public class BrokerTestApp implements MqttCallback {
	public static void main(String[] args) {
		BrokerTestApp app = new BrokerTestApp();
		app.subscribetobroker();
	}
	
	private void subscribetobroker() {
		MqttConnectOptions conn = new MqttConnectOptions();
		conn.setAutomaticReconnect(true);
		conn.setCleanSession(true);
		conn.setConnectionTimeout(30);
		conn.setKeepAliveInterval(30);
		conn.setUserName("admin");
		conn.setPassword("password".toCharArray());
		
		try {
			MqttClient mqttClient = new MqttClient("tcp://127.0.0.1:61613", MqttClient.generateClientId());
			mqttClient.setCallback(this);
			mqttClient.connect(conn);
			mqttClient.subscribe("test1", 2);
		} catch(MqttException m) {m.printStackTrace();}
	}

	@Override
	public void connectionLost(Throwable arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void deliveryComplete(IMqttDeliveryToken arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void messageArrived(String arg0, MqttMessage arg1) throws Exception {
		// TODO Auto-generated method stub
		System.out.println("Broker message:" +new String(arg1.getPayload()));
	}
}
