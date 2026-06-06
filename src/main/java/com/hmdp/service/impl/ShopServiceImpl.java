package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisConstants;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result queryById(Long id) {
        // 缓存穿透
        // Shop shop = queryWithCachePenetration(id);

        // 缓存击穿(用互斥锁)
        Shop shop = queryWithMutex(id);
        if (shop == null) {
            return Result.fail("店铺不存在");
        }
        // 6. 返回
        return Result.ok(shop);
    }

    public Shop queryWithMutex(Long id) {
        String key = RedisConstants.CACHE_SHOP_KEY + id;
        // 1. 从redis中查询
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        // 2. 判断redis中是否存在
        if (StrUtil.isNotBlank(shopJson)) {
            // 3. 存在，返回
            return JSONUtil.toBean(shopJson, Shop.class);
        }
        // 判断是否命中空值
        if (shopJson != null) {
            return null;
        }

        // 实现缓存重建
        // a. 获取互斥锁
        String lockKey = RedisConstants.LOCK_SHOP_KEY + id;
        Shop shop = null;
        try {
            boolean isLock = tryLock(lockKey);
            // b. 判断是否获取成功
            if (!isLock) {
                // c. 获取锁失败，休眠并重试
                Thread.sleep(50); // 休眠50毫秒，防止CPU占用过高
                return queryWithMutex(id);
            }
            // 4(d. 获取锁成功). 从数据库中查询
            shop = getById(id);
            // 5. 判断数据库中是否存在
            if (shop == null) {
                // 将空值写入缓存
                stringRedisTemplate.opsForValue().set(key, "", RedisConstants.CACHE_NULL_TTL, TimeUnit.MINUTES);
                // 6. 不存在，返回错误
                return null;
            }
            // 6. 存在，存储到redis中
            stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shop), RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            // e. 释放锁
            unLock(lockKey);
        }
        // 7. 返回
        return shop;
    }

    public Shop queryWithCachePenetration(Long id) {
        String key = RedisConstants.CACHE_SHOP_KEY + id;
        // 1. 从redis中查询
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        // 2. 判断redis中是否存在
        if (StrUtil.isNotBlank(shopJson)) {
            // 3. 存在，返回
            return JSONUtil.toBean(shopJson, Shop.class);
        }
        // 判断是否命中空值
        if (shopJson != null) {
            return null;
        }
        // 4. 不存在，从数据库中查询
        Shop shop = getById(id);
        // 5. 判断数据库中是否存在
        if (shop == null) {
            // 将空值写入缓存
            stringRedisTemplate.opsForValue().set(key, "", RedisConstants.CACHE_NULL_TTL, TimeUnit.MINUTES);
            // 6. 不存在，返回错误
            return null;
        }
        // 6. 存在，返回并存储到redis中
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shop), RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);
        return shop;
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

    @Override
    @Transactional
    public Result update(Shop shop) {
        Long id = shop.getId();
        if (id == null) {
            return Result.fail("店铺id不能为空");
        }
        //1. 更新数据库
        updateById(shop);
        //2. 删除缓存
        stringRedisTemplate.delete(RedisConstants.CACHE_SHOP_KEY + shop.getId());

        return Result.ok();
    }
}
