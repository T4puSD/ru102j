package com.redislabs.university.RU102J.dao;

import com.redislabs.university.RU102J.core.KeyHelper;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.Random;

public class RateLimiterSlidingDaoRedisImpl implements RateLimiter {

    private final JedisPool jedisPool;
    private final long windowSizeMS;
    private final long maxHits;

    public RateLimiterSlidingDaoRedisImpl(JedisPool pool, long windowSizeMS,
                                          long maxHits) {
        this.jedisPool = pool;
        this.windowSizeMS = windowSizeMS;
        this.maxHits = maxHits;
    }

    // Challenge #7
    @Override
    public void hit(String name) throws RateLimitExceededException {
        // START CHALLENGE #7
        try(Jedis jedis = jedisPool.getResource();
            Transaction tx = jedis.multi()) {
            final String slidingWindowKey = KeyHelper.getKey("limiter:" + windowSizeMS + ":" + name + ":" + maxHits);
            final long timestamp = ZonedDateTime.now().toInstant().toEpochMilli();
            final String member = timestamp + "-" + Math.random();
            tx.zadd(slidingWindowKey, timestamp, member);
            tx.zremrangeByScore(slidingWindowKey, 0,  (timestamp - windowSizeMS));
            final Response<Long> hit = tx.zcard(slidingWindowKey);
            tx.exec();

            if (hit.get() > maxHits) {
                throw new RateLimitExceededException();
            }
        }
        // END CHALLENGE #7
    }
}
