package org.apache.dubbo.demo;

import java.util.concurrent.CompletableFuture;

/**
 * @创建人:Raiden
 * @Descriotion:
 * @Date:Created in 14:04 2020/10/22
 * @Modified By:
 */
public class DemoServiceMock implements DemoService{
    @Override
    public String sayHello(String name) {
        return "这里是服务降级";
    }

    @Override
    public CompletableFuture<String> sayHelloAsync(String name) {
        CompletableFuture<String> cf = CompletableFuture.supplyAsync(() -> {
//            try {
//                Thread.sleep(1000);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
            return "这里是服务降级";
        });
        return cf;
    }
}
