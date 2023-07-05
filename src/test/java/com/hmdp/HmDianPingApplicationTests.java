package com.hmdp;

import com.hmdp.utils.RedisIdWoker;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.util.StopWatch;

import javax.annotation.Resource;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@SpringBootTest
class HmDianPingApplicationTests {

    @Autowired
    private RedisIdWoker redisIdWoker;


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

}
