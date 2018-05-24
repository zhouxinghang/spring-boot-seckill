package com.itstyle.seckill.service.impl;

import java.sql.Timestamp;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.itstyle.seckill.common.dynamicquery.DynamicQuery;
import com.itstyle.seckill.common.entity.Result;
import com.itstyle.seckill.common.entity.Seckill;
import com.itstyle.seckill.common.entity.SuccessKilled;
import com.itstyle.seckill.common.enums.SeckillStatEnum;
import com.itstyle.seckill.common.guava.GuavaCache;
import com.itstyle.seckill.distributedlock.redis.RedissLockUtil;
import com.itstyle.seckill.distributedlock.zookeeper.ZkLockUtil;
import com.itstyle.seckill.service.ISeckillDistributedService;

import javax.annotation.Resource;

@Service
public class SeckillDistributedServiceImpl implements ISeckillDistributedService {
	
	@Autowired
	private DynamicQuery dynamicQuery;
	@Resource
	private GuavaCache guavaCache;

	@Override
	@Transactional
	public Result startSeckilRedisLock(long seckillId,long userId) {
		boolean res=false;
		try {
			//尝试获取锁，最多等待3秒，上锁以后20秒自动解锁（实际项目中推荐这种，以防出现死锁）、这里根据预估秒杀人数，设定自动释放锁时间
			res = RedissLockUtil.tryLock(seckillId+"", TimeUnit.SECONDS, 3, 20);
			String nativeSql = "SELECT number FROM seckill WHERE seckill_id=?";
			Object object =  dynamicQuery.nativeQueryObject(nativeSql, new Object[]{seckillId});
			Long number =  ((Number) object).longValue();
			if(number>0){
				SuccessKilled killed = new SuccessKilled();
				killed.setSeckillId(seckillId);
				killed.setUserId(userId);
				killed.setState((short)0);
				killed.setCreateTime(new Timestamp(new Date().getTime()));
				dynamicQuery.save(killed);
				nativeSql = "UPDATE seckill  SET number=number-1 WHERE seckill_id=? AND number>0";
				dynamicQuery.nativeExecuteUpdate(nativeSql, new Object[]{seckillId});
			}else{
				return Result.error(SeckillStatEnum.END);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally{
			if(res){//释放锁
				RedissLockUtil.unlock(seckillId+"");
			}
		}
		return Result.ok(SeckillStatEnum.SUCCESS);
	}
	@Override
	@Transactional
	public Result startSeckilZksLock(long seckillId, long userId) {
		try {
			ZkLockUtil.acquire(seckillId+"");
			String nativeSql = "SELECT number FROM seckill WHERE seckill_id=?";
			Object object =  dynamicQuery.nativeQueryObject(nativeSql, new Object[]{seckillId});
			Long number =  ((Number) object).longValue();
			if(number>0){
				SuccessKilled killed = new SuccessKilled();
				killed.setSeckillId(seckillId);
				killed.setUserId(userId);
				killed.setState((short)0);
				killed.setCreateTime(new Timestamp(new Date().getTime()));
				dynamicQuery.save(killed);
				nativeSql = "UPDATE seckill  SET number=number-1 WHERE seckill_id=? AND number>0";
				dynamicQuery.nativeExecuteUpdate(nativeSql, new Object[]{seckillId});
			}else{
				return Result.error(SeckillStatEnum.END);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally{
			ZkLockUtil.release(seckillId+"");
		}
		return Result.ok(SeckillStatEnum.SUCCESS);
	}

	/**
	 * 将商品的数量
	 * 查询数据库，将数量放入缓存(假设是单机服务，使用本地缓存guava)，然后修改缓存，等秒杀结束在写入数据库，而不是频繁的会写
	 * @param itemId
	 * @param userId
	 * @return
	 */
	@Override
	public Result startSeckilRedisLock2(long itemId, long userId) {
		boolean res = false;
		try{
			res = RedissLockUtil.tryLock(itemId+"", TimeUnit.SECONDS, 3, 20);
			Long count = guavaCache.getCount(itemId);
			if(count > 0) {
				SuccessKilled killed = new SuccessKilled();
				killed.setSeckillId(itemId);
				killed.setUserId(userId);
				killed.setState((short)0);
				killed.setCreateTime(new Timestamp(System.currentTimeMillis()));
				//将kill保存到缓存中，可以使本地缓存，也可以是Redis list。然后等秒杀结束，通过统一的任务来将其会写到db
				guavaCache.setList(itemId, killed);
				// 商品数量更新
				guavaCache.setCount(itemId, --count);
			} else {
				return Result.error(SeckillStatEnum.END);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if(res) {
				RedissLockUtil.unlock(itemId+"");
			}
		}



		return null;
	}

	@Override
	public Result startSeckilLock(long seckillId, long userId, long number) {
		boolean res=false;
		try {
			//尝试获取锁，最多等待3秒，上锁以后10秒自动解锁（实际项目中推荐这种，以防出现死锁）
			res = RedissLockUtil.tryLock(seckillId+"", TimeUnit.SECONDS, 3, 10);
			String nativeSql = "SELECT number FROM seckill WHERE seckill_id=?";
			Object object =  dynamicQuery.nativeQueryObject(nativeSql, new Object[]{seckillId});
			Long count =  ((Number) object).longValue();
			if(count>=number){
				SuccessKilled killed = new SuccessKilled();
				killed.setSeckillId(seckillId);
				killed.setUserId(userId);
				killed.setState((short)0);
				killed.setCreateTime(new Timestamp(new Date().getTime()));
				dynamicQuery.save(killed);
				nativeSql = "UPDATE seckill  SET number=number-? WHERE seckill_id=? AND number>0";
				dynamicQuery.nativeExecuteUpdate(nativeSql, new Object[]{number,seckillId});
			}else{
				return Result.error(SeckillStatEnum.END);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally{
			if(res){//释放锁
				RedissLockUtil.unlock(seckillId+"");
			}
		}
		return Result.ok(SeckillStatEnum.SUCCESS);
	}

}
