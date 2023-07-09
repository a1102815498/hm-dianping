package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdWoker;
import com.hmdp.utils.SimpleRedisLock;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import javax.print.DocFlavor;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
@Slf4j
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {
    @Resource
    private ISeckillVoucherService iSeckillVoucherService;

    @Autowired
    StringRedisTemplate stringRedisTemplate;

    @Resource
    private RedisIdWoker redisIdWoker;


    @Resource
    private RedissonClient redissonClient;

    private static final DefaultRedisScript<Long> SECKILL_SCIRPT;


    private   IVoucherOrderService iVoucherOrderService;


    static {
        SECKILL_SCIRPT = new DefaultRedisScript<>();
        SECKILL_SCIRPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCIRPT.setResultType(Long.class);
    }

    private BlockingQueue<VoucherOrder> orderBlockingQueue = new ArrayBlockingQueue<>(1024*1024);

    private static final ExecutorService seckill_order_excutot = Executors.newSingleThreadExecutor();
//    @PostConstruct
//    public void init(){
//        seckill_order_excutot.submit(new VoucherOrderHandler());
//
//    }
//
//    private class VoucherOrderHandler implements Runnable{
//
//        @Override
//        public void run() {
//
//            while (true){
//                try {
//                    VoucherOrder take = orderBlockingQueue.take();
//                    handleVoucherOrder(take);
//
//                } catch (Exception e) {
//                    log.error("处理订单异常",e);
//                }
//            }
//
//
//        }
//    }


        @PostConstruct
    public void init(){
        seckill_order_excutot.submit(new VoucherOrderHandler());

    }

    String queueName = "stream.orders";

    private class VoucherOrderHandler implements Runnable{

        @Override
        public void run() {


            while (true){
                try {

//                    VoucherOrder take = orderBlockingQueue.take();
                    List<MapRecord<String, Object, Object>> read = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create(queueName, ReadOffset.lastConsumed())
                    );

                    if (read == null || read.isEmpty()){
                        continue;
                    }

                    MapRecord<String, Object, Object> entries = read.get(0);
                    Map<Object, Object> value = entries.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);

                    handleVoucherOrder(voucherOrder);
                    stringRedisTemplate.opsForStream().acknowledge(queueName,"g1",entries.getId());

                } catch (Exception e) {

                    handlePendingList();



                    log.error("处理订单异常",e);
                }
            }


        }
    }

    private void handlePendingList() {

            while (true){

                List<MapRecord<String, Object, Object>> read = stringRedisTemplate.opsForStream().read(
                        Consumer.from("g1", "c1"),
                        StreamReadOptions.empty().count(1),
                        StreamOffset.create(queueName, ReadOffset.from("0"))
                );

                if (read == null || read.isEmpty()){
                    break;
                }

                MapRecord<String, Object, Object> entries = read.get(0);
                Map<Object, Object> value = entries.getValue();
                VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);

                handleVoucherOrder(voucherOrder);
                stringRedisTemplate.opsForStream().acknowledge(queueName,"g1",entries.getId());

            }
    }

    private void handleVoucherOrder(VoucherOrder voucherOrder) {


        Long userId = voucherOrder.getUserId();

        RLock lock = redissonClient.getLock("lock:order:" + userId);
        boolean b = lock.tryLock();
        if (!b){
            log.error("用户不能重复下单");
            return;
        }

        try {
           iVoucherOrderService = (IVoucherOrderService) AopContext.currentProxy();
           iVoucherOrderService.createVoucherOrder(voucherOrder);
        }finally {
            lock.unlock();
        }




    }


    @Override
    public Result seckillVoucher(Long voucherId) {

        Long id = UserHolder.getUser().getId();

        long orderId = redisIdWoker.nextId("order");

        //执行lua脚本
        Long result = stringRedisTemplate.execute(
                SECKILL_SCIRPT,
                Collections.emptyList(),
                voucherId.toString(),
                id.toString(),id.toString(),String.valueOf(orderId)
        );

        int r = result.intValue();


        //判断结果
        if (r!=0){
            return  Result.fail(r==1?"库存不足":"不能重复下单");
        }

//        long orderId = redisIdWoker.nextId("order");
//
//
//        VoucherOrder voucherOrder = new VoucherOrder();
//
//        voucherOrder.setVoucherId(voucherId);
//        voucherOrder.setId(orderId);
//        voucherOrder.setUserId(id);
//
//        orderBlockingQueue.add(voucherOrder);

        return Result.ok(orderId );


    }


//    @Override
//    public Result seckillVoucher(Long voucherId) throws InterruptedException {
//        //查询
//        SeckillVoucher voucher = iSeckillVoucherService.getById(voucherId);
//
//        //判断是否开始
//        if (voucher.getBeginTime().isAfter(LocalDateTime.now())) {
//            return Result.fail("秒杀尚未开始");
//        }
//
//        //判断是否结束
//        if (voucher.getEndTime().isBefore(LocalDateTime.now())) {
//            return Result.fail("秒杀已经结束");
//        }
//
//
//        //判断库存
//        if (voucher.getStock() < 1) {
//            return Result.fail("库存不足");
//        }
//
//        Long id = UserHolder.getUser().getId();
//        SimpleRedisLock simpleRedisLock = new SimpleRedisLock(stringRedisTemplate, "order:" + id);
//
//        RLock lock = redissonClient.getLock("lock:order:" + id);
//
//        //获取锁
//        boolean b = lock.tryLock(1L, TimeUnit.SECONDS);
//        //判断是否获锁成功
//        if (!b){
//            return Result.fail("不允许重复下单");
//        }
//
//        try {
//            IVoucherOrderService o = (IVoucherOrderService) AopContext.currentProxy();
//            return o.createVoucherOrder(voucherId);
//        }finally {
//            lock.unlock();
//        }
//
//
//    }

    @Transactional
    public void createVoucherOrder(VoucherOrder voucherOrder) {
        Long id = voucherOrder.getUserId();
        Integer count = query().eq("user_id", id).eq("voucher_id", voucherOrder.getVoucherId()).count();
        if (count>0){
           log.error("用户已经购买过一次");
           return;
        }


        //减库存
        boolean success = iSeckillVoucherService.update()
                .setSql("stock = stock - 1")
                .eq("voucher_id", voucherOrder.getVoucherId())
                .gt("stock",0)
                .update();

        if (!success){
                log.error("库存不足");
                return;
        }

        //创建订单

        save(voucherOrder);


    }
}
