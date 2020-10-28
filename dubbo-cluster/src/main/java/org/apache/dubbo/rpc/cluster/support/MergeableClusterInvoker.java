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
package org.apache.dubbo.rpc.cluster.support;

import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.ConfigUtils;
import org.apache.dubbo.rpc.AsyncRpcResult;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.RpcInvocation;
import org.apache.dubbo.rpc.cluster.Directory;
import org.apache.dubbo.rpc.cluster.LoadBalance;
import org.apache.dubbo.rpc.cluster.Merger;
import org.apache.dubbo.rpc.cluster.merger.MergerFactory;

import java.lang.reflect.Array;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.dubbo.common.constants.CommonConstants.GROUP_KEY;
import static org.apache.dubbo.rpc.Constants.ASYNC_KEY;
import static org.apache.dubbo.rpc.Constants.MERGER_KEY;

/**
 * @param <T>
 */
@SuppressWarnings("unchecked")
public class MergeableClusterInvoker<T> extends AbstractClusterInvoker<T> {

    private static final Logger log = LoggerFactory.getLogger(MergeableClusterInvoker.class);

    public MergeableClusterInvoker(Directory<T> directory) {
        super(directory);
    }

    @Override
    protected Result doInvoke(Invocation invocation, List<Invoker<T>> invokers, LoadBalance loadbalance) throws RpcException {
        checkInvokers(invokers, invocation);
        String merger = getUrl().getMethodParameter(invocation.getMethodName(), MERGER_KEY);
        //如果方法没有进行合并就单独调用并返回
        if (ConfigUtils.isEmpty(merger)) { // If a method doesn't have a merger, only invoke one Group
            for (final Invoker<T> invoker : invokers) {
                if (invoker.isAvailable()) {
                    try {
                        return invoker.invoke(invocation);
                    } catch (RpcException e) {
                        if (e.isNoInvokerAvailableAfterFilter()) {
                            log.debug("No available provider for service" + getUrl().getServiceKey() + " on group " + invoker.getUrl().getParameter(GROUP_KEY) + ", will continue to try another group.");
                        } else {
                            throw e;
                        }
                    }
                }
            }
            return invokers.iterator().next().invoke(invocation);
        }
        //获取返回类型
        Class<?> returnType;
        try {
            returnType = getInterface().getMethod(
                    invocation.getMethodName(), invocation.getParameterTypes()).getReturnType();
        } catch (NoSuchMethodException e) {
            returnType = null;
        }

        //用来装结果集的
        Map<String, Result> results = new HashMap<>();
        //循环调用所有的提供者
        for (final Invoker<T> invoker : invokers) {
            //请求信息
            RpcInvocation subInvocation = new RpcInvocation(invocation, invoker);
            subInvocation.setAttachment(ASYNC_KEY, "true");
            results.put(invoker.getUrl().getServiceKey(), invoker.invoke(subInvocation));
        }

        Object result = null;
        //最终返回的结果集
        List<Result> resultList = new ArrayList<Result>(results.size());
        //遍历用来装结果集的集合获取返回值 装入上面的结果中
        for (Map.Entry<String, Result> entry : results.entrySet()) {
            Result asyncResult = entry.getValue();
            try {
                Result r = asyncResult.get();
                if (r.hasException()) {
                    log.error("Invoke " + getGroupDescFromServiceKey(entry.getKey()) +
                                    " failed: " + r.getException().getMessage(),
                            r.getException());
                } else {
                    resultList.add(r);
                }
            } catch (Exception e) {
                throw new RpcException("Failed to invoke service " + entry.getKey() + ": " + e.getMessage(), e);
            }
        }
        //如果为空就返回一个空的异步默认结果集
        if (resultList.isEmpty()) {
            return AsyncRpcResult.newDefaultAsyncResult(invocation);
        } else if (resultList.size() == 1) {
            //如果只有一个返回就返回这个结果集
            return resultList.iterator().next();
        }

        //处理没有返回类的情况
        if (returnType == void.class) {
            return AsyncRpcResult.newDefaultAsyncResult(invocation);
        }

        //处理合并规则
        //如果是以 . 开头的 就是说明是用指定方法合并 去掉点获取方法名称
        if (merger.startsWith(".")) {
            merger = merger.substring(1);
            Method method;
            try {
                method = returnType.getMethod(merger, returnType);
            } catch (NoSuchMethodException e) {
                throw new RpcException("Can not merge result because missing method [ " + merger + " ] in class [ " +
                        returnType.getName() + " ]");
            }
            //如果方法 是私有或者保护的 开启权限
            if (!Modifier.isPublic(method.getModifiers())) {
                method.setAccessible(true);
            }
            //获取第一个请求的结果集
            result = resultList.remove(0).getValue();
            try {
                if (method.getReturnType() != void.class
                        && method.getReturnType().isAssignableFrom(result.getClass())) {
                    //用指定方法合并结果集 最终返回该结果集
                    for (Result r : resultList) {
                        result = method.invoke(result, r.getValue());
                    }
                } else {
                    //如果不返回 或者返回的不是 Object 类型的处理方式
                    for (Result r : resultList) {
                        method.invoke(result, r.getValue());
                    }
                }
            } catch (Exception e) {
                throw new RpcException("Can not merge result: " + e.getMessage(), e);
            }
        } else {
            //如果没有指定方法 就只指定的合并器
            Merger resultMerger;
            //获取合并器
            if (ConfigUtils.isDefault(merger)) {
                //获取默认合并器
                resultMerger = MergerFactory.getMerger(returnType);
            } else {
                //获取自定义合并器
                resultMerger = ExtensionLoader.getExtensionLoader(Merger.class).getExtension(merger);
            }
            //合并器不为空
            if (resultMerger != null) {
                List<Object> rets = new ArrayList<Object>(resultList.size());
                for (Result r : resultList) {
                    rets.add(r.getValue());
                }
                //调用合并方法
                result = resultMerger.merge(
                        rets.toArray((Object[]) Array.newInstance(returnType, 0)));
            } else {
                throw new RpcException("There is no merger to merge result.");
            }
        }
        //返回类型
        return AsyncRpcResult.newDefaultAsyncResult(result, invocation);
    }


    @Override
    public Class<T> getInterface() {
        return directory.getInterface();
    }

    @Override
    public boolean isAvailable() {
        return directory.isAvailable();
    }

    @Override
    public void destroy() {
        directory.destroy();
    }

    private String getGroupDescFromServiceKey(String key) {
        int index = key.indexOf("/");
        if (index > 0) {
            return "group [ " + key.substring(0, index) + " ]";
        }
        return key;
    }
}
