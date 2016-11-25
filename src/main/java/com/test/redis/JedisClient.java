package com.showjoy.footprint.utils;

import java.net.MalformedURLException;


import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * redis配置
 * @author jiujie
 * @version $Id: RedisConfig.java, v 0.1 2015年7月15日 下午6:47:36 jiujie Exp $
 */
public class JedisClient {

    /** 主机 */
    public static final String        HOST      = getHost();

    /** 端口 */
    public static final int           PORT      = getPort();

    private static final String       PASSWORD  = getPassword();

    private volatile static JedisPool jedisPool = getJedisPool();

    private static final int          DB_INDEX  = 4;


    private static JedisPool getJedisPool() {
        //控制一个pool可分配多少个jedis实例，通过pool.getResource()来获取；
        //如果赋值为-1，则表示不限制；如果pool已经分配了maxActive个jedis实例，则此时pool的状态为exhausted(耗尽)。
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        //设置最大实例总数
        jedisPoolConfig.setMaxTotal(100000);
        //控制一个pool最多有多少个状态为idle(空闲的)的jedis实例。
        jedisPoolConfig.setMaxIdle(100);
        //表示当borrow(引入)一个jedis实例时，最大的等待时间，如果超过等待时间，则直接抛出JedisConnectionException；
        jedisPoolConfig.setMaxWaitMillis(1 * 1000);
        // 在borrow一个jedis实例时，是否提前进行alidate操作；如果为true，则得到的jedis实例均是可用的；  
        jedisPoolConfig.setTestOnBorrow(true);
        // 在还会给pool时，是否提前进行validate操作  
        jedisPoolConfig.setTestOnReturn(true);
        jedisPoolConfig.setTestWhileIdle(true);
        jedisPoolConfig.setMinEvictableIdleTimeMillis(30000);
        jedisPoolConfig.setTimeBetweenEvictionRunsMillis(5000);
        jedisPoolConfig.setNumTestsPerEvictionRun(-1);
        return new JedisPool(jedisPoolConfig, HOST, PORT, 5000, PASSWORD, DB_INDEX);
    }

    /**
     * 获取Jedis实例
     * @return
     */
    public static Jedis getJedis() {
        //jeids instance
        return jedisPool.getResource();
    }

    private static String getHost() {
        Configuration config = getConfig();
        if (config == null) {
            return null;
        }
        return config.getString("redis.host");
    }

    private static int getPort() {
        Configuration config = getConfig();
        if (config == null) {
            return 0;
        }
        return Integer.valueOf(config.getString("redis.port"));
    }

    private static String getPassword() {
        Configuration config = getConfig();
        if (config == null) {
            return "";
        }
        return config.getString("redis.password");
    }

    /**
     * 把jedis实例归还给连接池
     * @param jedis
     */
    public static void returnResource(Jedis jedis) {
        if (jedisPool == null || jedis == null) {
            return;
        }
        jedisPool.returnResourceObject(jedis);
    }

}
