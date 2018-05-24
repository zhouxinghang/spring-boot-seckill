package com.itstyle.seckill.common.guava;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.Lists;

import com.itstyle.seckill.common.dynamicquery.DynamicQuery;
import com.itstyle.seckill.common.entity.SuccessKilled;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

/**
 * Created by zhouxinghang on 2018/5/22.
 *  guava cache实现本地缓存
 */
@Repository
public class GuavaCache {

    private static Logger LOG = LoggerFactory.getLogger(GuavaCache.class);
    @Resource
    private DynamicQuery dynamicQuery;

    private static LoadingCache<Long, Long> itemCountCache;

    private static Cache<Long, List<Object>> listCache;

    @PostConstruct
    private void init() {
        itemCountCache = CacheBuilder.newBuilder()
            .expireAfterAccess(5, TimeUnit.MINUTES)
            .maximumSize(1000)
            .removalListener((RemovalListener<Long, Long>) notification -> {
                //LOG.info("商品数量缓存key:{},value:{}，已过期", notification.getKey(), notification.getValue(), notification.getCause());
                //当缓存更新时候也会执行sing这个方法

            })
            .build(new CacheLoader<Long, Long>() {
                @Override
                public Long load(Long itemId) throws Exception {
                    //LOG.info("不存在该商品缓存key:{}，重新加载",itemId);
                    String nativeSql = "SELECT number FROM seckill WHERE seckill_id=?";
                    Object object =  dynamicQuery.nativeQueryObject(nativeSql, new Object[]{itemId});
                    Long number =  ((Number) object).longValue();
                    return number;
                }
            });

        listCache = CacheBuilder.newBuilder()
            .expireAfterAccess(5, TimeUnit.MINUTES)
            .maximumSize(1000)
            .removalListener(new RemovalListener<Long, List<Object>>() {
                @Override
                public void onRemoval(RemovalNotification<Long, List<Object>> notification) {
                    //LOG.info("商品秒杀结果缓存key:{},value:{}，已过期", notification.getKey(), notification.getValue(), notification.getCause());

                }
            })
            .build();
    }

    public Long getCount(Long itemId) {
        Long count = new Long(0);
        try {
            count = itemCountCache.get(itemId);
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        return count;
    }

    public void setCount(Long itemId, Long count) {
        itemCountCache.put(itemId, count);
    }

    public void setList(Long itemId, Object obj) {
        try {
            List<Object> objects = listCache.get(itemId, new Callable<List<Object>>() {

                @Override
                public List<Object> call() throws Exception {
                    return Lists.newArrayList();
                }
            });
            objects.add(obj);
            listCache.put(itemId, objects);
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    public List<Object>  getList(Long itemId) {
        try {
            List<Object> objects = listCache.get(itemId, new Callable<List<Object>>() {

                @Override
                public List<Object> call() throws Exception {
                    return Lists.newArrayList();
                }
            });
            return objects;
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        return Lists.newArrayList();
    }


}
