package com.hmdp.service.impl;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;

import static com.hmdp.utils.RedisConstants.SHOP_TYPE_KEY;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result queryTypeList() {
        // 1. 先从 Redis 查询
        String shopTypeJson = stringRedisTemplate.opsForValue().get(SHOP_TYPE_KEY);

        // 2. Redis 中存在，直接返回
        if (StrUtil.isNotBlank(shopTypeJson)) {
            List<ShopType> typeList = JSONUtil.toList(shopTypeJson, ShopType.class);
            return Result.ok(typeList);
        }

        // 3. Redis 中不存在，查询数据库
        List<ShopType> typeList = query().orderByAsc("sort").list();

        // 4. 数据库也没有，返回失败
        if (typeList == null || typeList.isEmpty()) {
            return Result.fail("店铺类型不存在！");
        }

        // 5. 数据库存在，写入 Redis 并返回
        stringRedisTemplate.opsForValue().set(SHOP_TYPE_KEY, JSONUtil.toJsonStr(typeList));

        return Result.ok(typeList);
    }
}

