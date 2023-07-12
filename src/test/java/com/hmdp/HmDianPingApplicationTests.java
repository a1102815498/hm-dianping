package com.hmdp;

import com.hmdp.entity.Shop;
import com.hmdp.service.IShopService;
import com.hmdp.utils.RedisIdWoker;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.util.StopWatch;

import javax.annotation.Resource;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

@SpringBootTest
class HmDianPingApplicationTests {

    @Autowired
    private RedisIdWoker redisIdWoker;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private  IShopService iShopService;


    @Resource
    private ExecutorService executorService = Executors.newFixedThreadPool(500);

    @Test
    public void testIdWorker(){
        Runnable task = ()->{
            for (int i = 0; i < 100; i++) {
                long order = redisIdWoker.nextId("order");
                System.out.println("order = " + order);

            }
        };

        for (int i = 0; i < 300; i++) {
            executorService.submit(task);
        }
    }


    @Test
    void loadShopData(){
        List<Shop> list = iShopService.list();
        Map<Long, List<Shop>> map = list.stream().collect(Collectors.groupingBy(Shop::getTypeId));
        Set<Map.Entry<Long, List<Shop>>> entries = map.entrySet();
        for (Map.Entry<Long, List<Shop>> entry : entries) {

            List<Shop> value = entry.getValue();
            Long typeId = entry.getKey();
            String key = "shop:geo:"+typeId;

//            for (Shop shop : value) {
//                stringRedisTemplate.opsForGeo().add(key,new Point(shop.getX(),shop.getY()),shop.getId().toString());
//
//            }

            List<RedisGeoCommands.GeoLocation<String>> collect = value.stream().map(shop -> {
                return new RedisGeoCommands.GeoLocation<String>(shop.getId().toString(), new Point(shop.getX(), shop.getY()));
            }).collect(Collectors.toList());

            stringRedisTemplate.opsForGeo().add(key,collect);
        }


    }

}
