package com.hmdp.service.impl;

import com.hmdp.dto.Result;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdWoker;
import com.hmdp.utils.UserHolder;
import org.springframework.aop.framework.AopContext;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.time.LocalDateTime;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {
    @Resource
    private ISeckillVoucherService iSeckillVoucherService;

    @Resource
    private RedisIdWoker redisIdWoker;
    @Override
    public Result seckillVoucher(Long voucherId) {
        //查询
        SeckillVoucher voucher = iSeckillVoucherService.getById(voucherId);

        //判断是否开始
        if (voucher.getBeginTime().isAfter(LocalDateTime.now())) {
            return Result.fail("秒杀尚未开始");
        }

        //判断是否结束
        if (voucher.getEndTime().isBefore(LocalDateTime.now())) {
            return Result.fail("秒杀已经结束");
        }


        //判断库存
        if (voucher.getStock() < 1) {
            return Result.fail("库存不足");
        }

        Long id = UserHolder.getUser().getId();
        synchronized (id.toString().intern()){
            IVoucherOrderService o = (IVoucherOrderService) AopContext.currentProxy();
            return o.createVoucherOrder(voucherId);
        }
    }

    @Transactional
    public Result createVoucherOrder(Long voucherId) {
        Long id = UserHolder.getUser().getId();
        Integer count = query().eq("user_id", id).eq("voucher_id", voucherId).count();
        if (count>0){
            return Result.fail("用户已经购买过一次！！");
        }


        //减库存
        boolean success = iSeckillVoucherService.update()
                .setSql("stock = stock - 1")
                .eq("voucher_id", voucherId)
                .gt("stock",0)
                .update();

        if (!success){
            return Result.fail("库存不足");
        }

        //创建订单

        VoucherOrder voucherOrder = new VoucherOrder();

        long orderId = redisIdWoker.nextId("order");

        voucherOrder.setVoucherId(voucherId);

        voucherOrder.setId(orderId);

        Long userId = UserHolder.getUser().getId();

        voucherOrder.setUserId(userId);

        save(voucherOrder);

        //返回订单id

        return Result.ok(orderId);
    }
}
