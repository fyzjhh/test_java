package com.test.redis;

import redis.clients.jedis.JedisPubSub;

public class JhhPubSub extends JedisPubSub {

	@Override
	public void onMessage(String channel, String message) {
		System.out.println(channel + "=" + message);

	}

	@Override
	public void onPMessage(String pattern, String channel, String message) {

	}

	@Override
	public void onSubscribe(String channel, int subscribedChannels) {
		System.out.println(channel + "=" + subscribedChannels);
	}

	@Override
	public void onUnsubscribe(String channel, int subscribedChannels) {
		// TODO Auto-generated method stub

	}

	@Override
	public void onPUnsubscribe(String pattern, int subscribedChannels) {
		// TODO Auto-generated method stub

	}

	@Override
	public void onPSubscribe(String pattern, int subscribedChannels) {
		// TODO Auto-generated method stub

	}

}