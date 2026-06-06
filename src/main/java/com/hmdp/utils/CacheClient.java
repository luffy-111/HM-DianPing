package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.entity.Shop;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

@Component
@Slf4j
public class CacheClient {

    private final StringRedisTemplate stringRedisTemplate;

    public CacheClient(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    public void set(String key, Object value, Long time, TimeUnit unit) {
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value), time, unit);
    }

    public void setWithLogicalExpire(String key, Object value, Long time, TimeUnit unit) {
        // 封装逻辑过期时间
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));
        // 写入Redis
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
    }

    /**
     * 缓存穿透
     *
     * @param keyPrefix
     * @param id
     * @param type
     * @param dbFallback
     * @param time
     * @param unit
     * @param <R>
     * @param <ID>
     * @return
     */
    public <R, ID> R queryWithCachePenetration(String keyPrefix,
                                               ID id,
                                               Class<R> type,
                                               Function<ID, R> dbFallback,
                                               Long time,
                                               TimeUnit unit) {
        String key = keyPrefix + id;
        // 1. 从redis中查询
        String json = stringRedisTemplate.opsForValue().get(key);
        // 2. 判断redis中是否存在
        if (StrUtil.isNotBlank(json)) {
            // 3. 存在，返回
            return JSONUtil.toBean(json, type);
        }
        // 判断是否命中空值
        if (json != null) {
            return null;
        }
        // 4. 不存在，从数据库中查询
        R r = dbFallback.apply(id);
        // 5. 判断数据库中是否存在
        if (r == null) {
            // 将空值写入缓存
            stringRedisTemplate.opsForValue().set(key, "", RedisConstants.CACHE_NULL_TTL, TimeUnit.MINUTES);
            // 6. 不存在，返回错误
            return null;
        }
        // 6. 存在，储到redis中
        this.set(key, r, time, unit);

        // 7. 返回
        return r;
    }

    // 创建线程池
    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    /**
     * 逻辑过期解决缓存击穿
     *
     * @param keyPrefix
     * @param id
     * @param type
     * @param dbFallback
     * @param time
     * @param unit
     * @param <R>
     * @param <ID>
     * @return
     */
    public <R, ID> R queryWithLogicalExpire(String keyPrefix,
                                            ID id,
                                            Class<R> type,
                                            Function<ID, R> dbFallback,
                                            Long time,
                                            TimeUnit unit) {
        String key = keyPrefix + id;
        // 1. 从redis中查询
        String json = stringRedisTemplate.opsForValue().get(key);
        // 2. 判断redis中是否存在
        if (StrUtil.isBlank(json)) {
            // 3. 不存在，返回
            return null;
        }
        // 4. 存在，需要先把json转换成对象
        RedisData redisData = JSONUtil.toBean(json, RedisData.class);
        R r = JSONUtil.toBean((JSONObject) redisData.getData(), type);
        LocalDateTime expireTime = redisData.getExpireTime();
        // 5. 判断逻辑过期时间
        if (expireTime.isAfter(LocalDateTime.now())) {
            // 5.1 未过期, 直接返回
            return r;
        }
        // 5.2 已过期, 需要缓存重建
        // 6. 缓存重建
        // 6.1 尝试获取锁
        String lockKey = RedisConstants.LOCK_SHOP_KEY + id;
        boolean isLock = tryLock(lockKey);
        // 6.2 判断是否获取锁成功
        if (isLock) {
            // double check
            String shopJson2 = stringRedisTemplate.opsForValue().get(key);
            RedisData redisData2 = JSONUtil.toBean(shopJson2, RedisData.class);
            if (redisData2.getExpireTime().isAfter(LocalDateTime.now())) {
                unLock(lockKey);
                return JSONUtil.toBean((JSONObject) redisData2.getData(), type);
            }
            // 6.3 获取锁成功, 创建一个线程，实现缓存重建
            CACHE_REBUILD_EXECUTOR.submit(() -> {
                try {
                    // 6.4 重建缓存
                    this.setWithLogicalExpire(key, dbFallback.apply(id), time, unit);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    // 6.5 释放锁
                    unLock(lockKey);
                }
            });
        }
        // 6.6 获取锁失败, 返回原始数据
        return r;
    }

    /**
     * 尝试获取锁
     *
     * @param key
     * @return
     */
    private boolean tryLock(String key) {
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", RedisConstants.LOCK_SHOP_TTL, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }

    /**
     * 释放锁
     *
     * @param key
     */
    private void unLock(String key) {
        stringRedisTemplate.delete(key);
    }
}
