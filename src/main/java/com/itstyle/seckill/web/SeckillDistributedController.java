package com.itstyle.seckill.web;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.itstyle.seckill.common.dynamicquery.DynamicQuery;
import com.itstyle.seckill.common.entity.Result;
import com.itstyle.seckill.common.entity.SuccessKilled;
import com.itstyle.seckill.common.guava.GuavaCache;
import com.itstyle.seckill.queue.kafka.KafkaSender;
import com.itstyle.seckill.queue.redis.RedisSender;
import com.itstyle.seckill.service.ISeckillDistributedService;
import com.itstyle.seckill.service.ISeckillService;

import javax.annotation.Resource;

@Api(tags ="分布式秒杀")
@RestController
@RequestMapping("/seckillDistributed")
public class SeckillDistributedController {
	private final static Logger LOGGER = LoggerFactory.getLogger(SeckillDistributedController.class);
	
	private static int corePoolSize = Runtime.getRuntime().availableProcessors();
	//调整队列数 拒绝服务
	private static ThreadPoolExecutor executor  = new ThreadPoolExecutor(corePoolSize, corePoolSize+1, 10l, TimeUnit.SECONDS,
			new LinkedBlockingQueue<Runnable>(10000));
	
	@Autowired
	private ISeckillService seckillService;
	@Autowired
	private ISeckillDistributedService seckillDistributedService;
	@Autowired
	private RedisSender redisSender;
	@Autowired
	private KafkaSender kafkaSender;
	@Resource
	private GuavaCache guavaCache;
	@Resource
	private DynamicQuery dynamicQuery;
	
	@ApiOperation(value="秒杀一(Rediss分布式锁)",nickname="科帮网")
	@PostMapping("/startRedisLock")
	public Result startRedisLock(long seckillId){
		seckillService.deleteSeckill(seckillId);
		final long killId =  seckillId;
		LOGGER.info("开始秒杀一");
		for(int i=0;i<1000;i++){
			final long userId = i;
			Runnable task = new Runnable() {
				@Override
				public void run() {
					Result result = seckillDistributedService.startSeckilRedisLock(killId, userId);
					LOGGER.info("用户:{}{}",userId,result.get("msg"));
				}
			};
			executor.execute(task);
		}
		try {
			Thread.sleep(15000);
			Long  seckillCount = seckillService.getSeckillCount(seckillId);
			LOGGER.info("一共秒杀出{}件商品",seckillCount);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return Result.ok();
	}
	@ApiOperation(value="秒杀二(zookeeper分布式锁)",nickname="科帮网")
	@PostMapping("/startZkLock")
	public Result startZkLock(long seckillId){
		seckillService.deleteSeckill(seckillId);
		final long killId =  seckillId;
		LOGGER.info("开始秒杀二");
		for(int i=0;i<1000;i++){
			final long userId = i;
			Runnable task = new Runnable() {
				@Override
				public void run() {
					Result result = seckillDistributedService.startSeckilZksLock(killId, userId);
					LOGGER.info("用户:{}{}",userId,result.get("msg"));
				}
			};
			executor.execute(task);
		}
		try {
			Thread.sleep(10000);
			Long  seckillCount = seckillService.getSeckillCount(seckillId);
			LOGGER.info("一共秒杀出{}件商品",seckillCount);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return Result.ok();
	}
	@ApiOperation(value="秒杀三(Redis分布式队列-订阅监听)",nickname="科帮网")
	@PostMapping("/startRedisQueue")
	public Result startRedisQueue(long seckillId){
		seckillService.deleteSeckill(seckillId);
		final long killId =  seckillId;
		LOGGER.info("开始秒杀三");
		for(int i=0;i<1000;i++){
			final long userId = i;
			Runnable task = new Runnable() {
				@Override
				public void run() {
					//思考如何返回给用户信息ws
					redisSender.sendChannelMess("seckill",killId+";"+userId);
				}
			};
			executor.execute(task);
		}
		try {
			Thread.sleep(10000);
			Long  seckillCount = seckillService.getSeckillCount(seckillId);
			LOGGER.info("一共秒杀出{}件商品",seckillCount);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return Result.ok();
	}
	@ApiOperation(value="秒杀四(Kafka分布式队列)",nickname="科帮网")
	@PostMapping("/startKafkaQueue")
	public Result startKafkaQueue(long seckillId){
		seckillService.deleteSeckill(seckillId);
		final long killId =  seckillId;
		LOGGER.info("开始秒杀四");
		for(int i=0;i<1000;i++){
			final long userId = i;
			Runnable task = new Runnable() {
				@Override
				public void run() {
					//思考如何返回给用户信息ws
					kafkaSender.sendChannelMess("seckill",killId+";"+userId);
				}
			};
			executor.execute(task);
		}
		try {
			Thread.sleep(10000);
			Long  seckillCount = seckillService.getSeckillCount(seckillId);
			LOGGER.info("一共秒杀出{}件商品",seckillCount);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return Result.ok();
	}

	@ApiOperation(value = "秒杀5，利用guava本地缓存，等秒杀结束后，再一起写入数据库")
	@PostMapping("/startRedisLock2")
	@Transactional
	public Result startRedisLock2(long itemId){
		long start = System.currentTimeMillis();
		seckillService.deleteSeckill(itemId);
		final long killItemId = itemId;
		for(int i = 0; i < 1000; i++) {
			final long userId = i;
			Runnable task = () -> {
				Result result = seckillDistributedService.startSeckilRedisLock2(killItemId, userId);
				//LOGGER.info("用户:{}{}",userId,result.get("msg"));
			};
			executor.execute(task);
		}
		try {
			Thread.sleep(10000);
			List<Object> entitys = guavaCache.getList(killItemId);
			Long itemCount = guavaCache.getCount(killItemId);
			LOGGER.info("一共秒杀出{}件商品,还剩{}件商品",entitys.size(), itemCount);
			//将缓存写入数据库
			String nativeSql = "UPDATE seckill  SET number=? WHERE seckill_id=?";
			dynamicQuery.nativeExecuteUpdate(nativeSql, new Object[]{itemCount, killItemId});
			// 将秒杀结果写入数据库  通过查询数据库发现，user_id是没有递增的，因为有个user没有抢到商品
			dynamicQuery.batchInsert(entitys);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		long end = System.currentTimeMillis();
		LOGGER.info("秒杀结束，一共花了{}ms", end-start);
		return  Result.ok();
	}
}
