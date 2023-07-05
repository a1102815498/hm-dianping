package com.hmdp.utils;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

@Component
public class RedisIdWoker {
    /*
    开始时间戳
     */
    private static final long BEGIN_TIMESTAMP = 1640995200l;

    private static final int COUNT_BITS = 32;

    private StringRedisTemplate stringRedisTemplate;

    public RedisIdWoker(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    public long nextId(String keyPrefix){
        //生成时间戳
        LocalDateTime now = LocalDateTime.now();
        long nowSecond = now.toEpochSecond(ZoneOffset.UTC);
        long timestamp =  nowSecond - BEGIN_TIMESTAMP;
        //生成序列号
        //获取当天生日
        String yyyyMMdd = now.format(DateTimeFormatter.ofPattern("yyyyMMdd"));

        Long count = stringRedisTemplate.opsForValue().increment("icr:" + keyPrefix + ":" + yyyyMMdd);

        return timestamp << COUNT_BITS | count;
    }
}
