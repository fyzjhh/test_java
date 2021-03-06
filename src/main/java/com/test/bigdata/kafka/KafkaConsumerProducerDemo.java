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

public class KafkaConsumerProducerDemo implements KafkaProperties {
	public static void main(String[] args) {

		// String[] topics = new String[] { "jt1", "jt2", "jt3" };
		// for (int i = 0; i < topics.length; i++) {
		// Producer p = new Producer(topics[i]);
		// p.start();
		// }
		//
		// Consumer consumerThread = new Consumer(topics);
		// consumerThread.start();

		Producer p = new Producer("t1");
		p.start();
	}
}
