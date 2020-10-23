/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.dubbo.rpc.cluster.loadbalance;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Round robin load balance.
 */
public class RoundRobinLoadBalance extends AbstractLoadBalance {
    public static final String NAME = "roundrobin";

    //回收时间是60秒
    private static final int RECYCLE_PERIOD = 60000;

    protected static class WeightedRoundRobin {
        //主机权重
        private int weight;
        //方法权重 动态值 默认是0 这个是轮询的重点属性
        private AtomicLong current = new AtomicLong(0);
        private long lastUpdate;

        //获取主机权重
        public int getWeight() {
            return weight;
        }

        /**
         * 初始化主机权重 和 方法权重
         * @param weight
         */
        public void setWeight(int weight) {
            this.weight = weight;
            current.set(0);
        }

        //增加方法权重,每一次增加一个主机权重
        public long increaseCurrent() {
            return current.addAndGet(weight);
        }

        //减小方法权重，减去所有的 invoker 的权重之和
        //目的是让当前的方法权重变为最小权重
        public void sel(int total) {
            current.addAndGet(-1 * total);
        }

        public long getLastUpdate() {
            return lastUpdate;
        }

        public void setLastUpdate(long lastUpdate) {
            this.lastUpdate = lastUpdate;
        }
    }

    /**
     * 方法权重Map
     * Key：全限定方法名
     * Value:Map
     * 内层map：Key： 主机信息 ip:prot/接口名称
     * 内层map：Value： 轮询权重
     */
    private ConcurrentMap<String, ConcurrentMap<String, WeightedRoundRobin>> methodWeightMap = new ConcurrentHashMap<String, ConcurrentMap<String, WeightedRoundRobin>>();

    /**
     * get invoker addr list cached for specified invocation
     * <p>
     * <b>for unit test only</b>
     *
     * @param invokers
     * @param invocation
     * @return
     */
    protected <T> Collection<String> getInvokerAddrList(List<Invoker<T>> invokers, Invocation invocation) {
        String key = invokers.get(0).getUrl().getServiceKey() + "." + invocation.getMethodName();
        Map<String, WeightedRoundRobin> map = methodWeightMap.get(key);
        if (map != null) {
            return map.keySet();
        }
        return null;
    }

    @Override
    protected <T> Invoker<T> doSelect(List<Invoker<T>> invokers, URL url, Invocation invocation) {
        String key = invokers.get(0).getUrl().getServiceKey() + "." + invocation.getMethodName();
        //获取 OR 创建 方法权重缓存
        ConcurrentMap<String, WeightedRoundRobin> map = methodWeightMap.computeIfAbsent(key, k -> new ConcurrentHashMap<>());
        int totalWeight = 0;
        //最大方法权重
        long maxCurrent = Long.MIN_VALUE;
        long now = System.currentTimeMillis();
        Invoker<T> selectedInvoker = null;
        WeightedRoundRobin selectedWRR = null;
        //遍历所有服务提供者代理类
        for (Invoker<T> invoker : invokers) {
            //通过 URL 创建唯一的标识
            String identifyString = invoker.getUrl().toIdentityString();
            //获取最新的权重值
            int weight = getWeight(invoker, invocation);
            //获取 OR 创建 权重包装类
            WeightedRoundRobin weightedRoundRobin = map.computeIfAbsent(identifyString, k -> {
                WeightedRoundRobin wrr = new WeightedRoundRobin();
                wrr.setWeight(weight);
                return wrr;
            });
            //如果包装类的权重和最新生成的不一致
            if (weight != weightedRoundRobin.getWeight()) {
                //weight changed
                //将其设置为最新的权重
                weightedRoundRobin.setWeight(weight);
            }
            //增加方法权重 方法权重增加的值是 weight
            //这个是权重轮询的关键点，weight 越大轮询越快，选中的次数就多
            long cur = weightedRoundRobin.increaseCurrent();
            //设置最后更新时间
            weightedRoundRobin.setLastUpdate(now);
            //比较获取最大方法权重的 invoker
            if (cur > maxCurrent) {
                maxCurrent = cur;
                selectedInvoker = invoker;
                selectedWRR = weightedRoundRobin;
            }
            //所有 invoker 权重的和
            totalWeight += weight;
        }
        if (invokers.size() != map.size()) {
            //删除已经过期的权重
            map.entrySet().removeIf(item -> now - item.getValue().getLastUpdate() > RECYCLE_PERIOD);
        }
        if (selectedInvoker != null) {
            //减少方法权重 如果被选中一次，就要降低其方法权重让其他 invoker 有机会被选中
            selectedWRR.sel(totalWeight);
            return selectedInvoker;
        }
        // should not happen here
        return invokers.get(0);
    }

}
