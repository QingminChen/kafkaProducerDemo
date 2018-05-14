package com.qingmin.kafka.demo;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import com.google.gson.Gson;

public class KafkaProducerTest {
    
	public static void main(String[] args){
		Gson gson = new Gson();
		
		Properties props = new Properties();
        props.put("bootstrap.servers", "hdpvrtvw9010:6667");
        props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<Integer, String> producer = new KafkaProducer<Integer, String>(props);
        
        String topic = "test.appdynamic.alert.qingmin";
        String count = "10";
        int messageCount = Integer.parseInt(count);
        System.out.println("Topic Name - " + topic);
        System.out.println("Message Count - " + messageCount);
        
        for (int mCount = 0; mCount < messageCount; mCount++) {
            String message = "{'name':'Business Transaction error rate is high','startTimeInMillis':"+System.currentTimeMillis()+"-"+mCount+",'severity':'CRITICAL','priority':'P1','incidentStatus':'OPEN','airId':'2700'}";
            //ProducerRecord<Integer,String> record = new ProducerRecord<Integer,String>(topic, mCount, message);   
            //ProducerRecord<Integer,String> record = new ProducerRecord<Integer,String>(topic,0,22, message);    
            ProducerRecord<Integer,String> record = new ProducerRecord<Integer,String>(topic,message);
            try {
				RecordMetadata recordMetadata=producer.send(record).get();
				System.out.println("Partition:"+recordMetadata.partition()+";Timestamp:"+recordMetadata.timestamp()+";Topic:"+recordMetadata.topic()+";RecordMetadata String:"+gson.toJson(recordMetadata).toString());
				
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (ExecutionException e) {
				e.printStackTrace();
			}
        }
        producer.close();
	}
}
