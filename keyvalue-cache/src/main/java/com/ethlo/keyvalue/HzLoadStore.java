package com.ethlo.keyvalue;

import com.hazelcast.core.MapLoader;
import com.hazelcast.core.MapStore;

/**
 * 
 * @author Morten Haraldsen
 *
 * @param <K>
 * @param <V>
 */
public interface HzLoadStore<K, V> extends MapLoader<K, V>, MapStore<K, V>
{

}