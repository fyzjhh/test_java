/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.test.kafka;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

public class Consumer extends Thread {
	Map<String, Integer> topics = new HashMap<String, Integer>();

	public Consumer(String[] ts) {
		for (int i = 0; i < ts.length; i++) {
			topics.put(ts[i], i + 1);
		}
	}

	private static ConsumerConfig createConsumerConfig() {
		Properties props = new Properties();
		props.put("zookeeper.connect", KafkaProperties.zkConnect);
		props.put("group.id", KafkaProperties.groupId);
		return new ConsumerConfig(props);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void run() {

		ConsumerConnector consumer = kafka.consumer.Consumer
				.createJavaConsumerConnector(createConsumerConfig());

		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer
				.createMessageStreams(topics);
		Set names = consumerMap.keySet();
		Iterator it = names.iterator();
		while (it.hasNext()) {
			String tn = (String) it.next();
			List<KafkaStream<byte[], byte[]>> lists = consumerMap.get(tn);
			for (Iterator iterator = lists.iterator(); iterator.hasNext();) {
				KafkaStream<byte[], byte[]> kafkaStream = (KafkaStream<byte[], byte[]>) iterator
						.next();
				ConsumerIterator<byte[], byte[]> ksit = kafkaStream.iterator();
				while (ksit.hasNext())

					System.out.println("Consumer\ttopic:" + tn + " msg:"
							+ new String(ksit.next().message()));

			}
		}
	}
}
