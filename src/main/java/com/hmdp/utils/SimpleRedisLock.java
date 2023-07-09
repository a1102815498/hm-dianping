package com.hmdp.utils;

import cn.hutool.core.lang.UUID;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;

import javax.annotation.Resource;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

public class SimpleRedisLock implements ILock{


    private StringRedisTemplate stringRedisTemplate;

    private String  name;

    public SimpleRedisLock(StringRedisTemplate stringRedisTemplate, String name) {
        this.stringRedisTemplate = stringRedisTemplate;
        this.name = name;
    }

    private static final String KEY_PREFIX = "lock:";

    private static final DefaultRedisScript<Long> unLockScirpt;


    static {
        unLockScirpt = new DefaultRedisScript<>();
        unLockScirpt.setLocation(new ClassPathResource("unlock.lua"));
        unLockScirpt.setResultType(Long.class);
    }


    private static String ID_PREFIX = UUID.randomUUID().toString(true);
    @Override
    public boolean tryLock(long timeoutSec) {
        //获取线程标识
        String id = ID_PREFIX + Thread.currentThread().getId();
        //获取锁
        Boolean success = stringRedisTemplate.opsForValue().setIfAbsent(KEY_PREFIX + name, id , timeoutSec, TimeUnit.SECONDS);
        //避免空指针
        return Boolean.TRUE.equals(success);

    }

    @Override
    public void unlock() {
        stringRedisTemplate
                 .execute(unLockScirpt, Collections.singletonList(KEY_PREFIX+name),ID_PREFIX + Thread.currentThread().getId());
    }

//    @Override
//    public void unlock() {
//        String id = ID_PREFIX + Thread.currentThread().getId();
//        //获取锁的标识
//        String s = stringRedisTemplate.opsForValue().get(KEY_PREFIX + name);
//        if (id.equals(s)){
//
//            stringRedisTemplate.delete(KEY_PREFIX+name);
//        }
//
//    }
}
