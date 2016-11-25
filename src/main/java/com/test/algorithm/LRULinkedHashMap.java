package com.test.algorithm;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

@SuppressWarnings("serial")
public class LRULinkedHashMap<K, V> extends LinkedHashMap<K, V> {
	private final int maxCapacity;
	private static final float DEFAULT_LOAD_FACTOR = 0.75f;
	private final Lock lock = new ReentrantLock();

	public LRULinkedHashMap(int maxCapacity) {
		super(maxCapacity, DEFAULT_LOAD_FACTOR, true);
		this.maxCapacity = maxCapacity;
	}

	@Override
	protected boolean removeEldestEntry(java.util.Map.Entry<K, V> eldest) {
		return size() > maxCapacity;
	}

	@Override
	public V get(Object key) {
		try {
			lock.lock();
			return super.get(key);
		} finally {
			lock.unlock();
		}
	}

	@Override
	public V put(K key, V value) {
		try {
			lock.lock();
			return super.put(key, value);
		} finally {
			lock.unlock();
		}
	}

	@Override
	public V remove(Object key) {
		try {
			lock.lock();
			return super.remove(key);
		} finally {
			lock.unlock();
		}
	}

	@Override
	public void clear() {
		try {
			lock.lock();
			super.clear();
		} finally {
			lock.unlock();
		}
	}

	public static void main(String[] args) throws Exception {
		Map<Integer, String> map = new LRULinkedHashMap<Integer, String>(
				118);
		for (int i = 0; i < 6; i++) {
			String s = String.valueOf(i);
			map.put(i, s);
		}

		map.get(3);

		for (int i = 10; i < 20; i++) {

			map.put(i, String.valueOf(i));
		}
		map.get(10);
		for (int i = 20; i < 30; i++) {

			map.put(i, String.valueOf(i));
		}
		map.get(22);
		map.get(3);
		map.get(3);
		for (int i = 30; i < 40; i++) {

			map.put(i, String.valueOf(i));
		}
		map.get(12);
		map.get(31);
		map.get(22);
		for (Map.Entry<Integer, String> e : map.entrySet()) {
			System.out.println(e.getKey());
		}
	}
}