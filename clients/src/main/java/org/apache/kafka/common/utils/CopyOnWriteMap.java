/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.utils;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

/**
 * A simple read-optimized map implementation that synchronizes only writes and does a full copy on each modification
 */
public class CopyOnWriteMap<K, V> implements ConcurrentMap<K, V> {

    // todo:封装Map， 关键字：volatile 在高并发的情况保证数据可见性
    private volatile Map<K, V> map;

    public CopyOnWriteMap() {
        this.map = Collections.emptyMap();
    }

    public CopyOnWriteMap(Map<K, V> map) {
        this.map = Collections.unmodifiableMap(map);
    }

    @Override
    public boolean containsKey(Object k) {
        return map.containsKey(k);
    }

    @Override
    public boolean containsValue(Object v) {
        return map.containsValue(v);
    }

    @Override
    public Set<java.util.Map.Entry<K, V>> entrySet() {
        return map.entrySet();
    }

    @Override
    public V get(Object k) {
        return map.get(k);
    }

    @Override
    public boolean isEmpty() {
        return map.isEmpty();
    }

    @Override
    public Set<K> keySet() {
        return map.keySet();
    }

    @Override
    public int size() {
        return map.size();
    }

    @Override
    public Collection<V> values() {
        return map.values();
    }

    @Override
    public synchronized void clear() {
        this.map = Collections.emptyMap();
    }

    // todo: 设计线程安全，对方法添加 synchronized 关键字，保证线程的安全性

    /**
     * todo:
     * 1。 整个方法使用synchronized 关键字修饰，他是线程安全的；即使加了锁，但是性能依然很好，因为里面是 "纯内存操作";
     * 2. 读数据流程和写数据流程进行分离，这里采用读写分离的思想；读操作和写操作是互不影响的，所以读写操作是线程安全的
     * 3. 最后把copy数据赋值给map, map使用了volatile关键字修饰，说明map是具有可见性，如果map发生了改变，进行get操作时是可以立刻感知到的
     * @param k
     * @param v
     * @return
     */
    @Override
    public synchronized V put(K k, V v) {
        // todo: 重新开辟一块新内存空间
        Map<K, V> copy = new HashMap<K, V>(this.map);
        // 插入数据
        V prev = copy.put(k, v);
        // 将copy 数据重新赋值给 map 对象
        this.map = Collections.unmodifiableMap(copy);
        return prev;
    }

    @Override
    public synchronized void putAll(Map<? extends K, ? extends V> entries) {
        Map<K, V> copy = new HashMap<K, V>(this.map);
        copy.putAll(entries);
        this.map = Collections.unmodifiableMap(copy);
    }

    @Override
    public synchronized V remove(Object key) {
        Map<K, V> copy = new HashMap<K, V>(this.map);
        V prev = copy.remove(key);
        this.map = Collections.unmodifiableMap(copy);
        return prev;
    }

    @Override
    public synchronized V putIfAbsent(K k, V v) {
        // todo: 判断该 键值对是否存在，不存在则直接新增
        if (!containsKey(k))
            return put(k, v);
        else
            // todo: 如果该key存在，则获取到原始键值对数据
            return get(k);
    }

    @Override
    public synchronized boolean remove(Object k, Object v) {
        if (containsKey(k) && get(k).equals(v)) {
            remove(k);
            return true;
        } else {
            return false;
        }
    }

    @Override
    public synchronized boolean replace(K k, V original, V replacement) {
        if (containsKey(k) && get(k).equals(original)) {
            put(k, replacement);
            return true;
        } else {
            return false;
        }
    }

    @Override
    public synchronized V replace(K k, V v) {
        if (containsKey(k)) {
            return put(k, v);
        } else {
            return null;
        }
    }

}
