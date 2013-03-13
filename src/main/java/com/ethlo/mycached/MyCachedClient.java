package com.ethlo.mycached;

import com.ethlo.keyvalue.BatchUpdatableKeyValueDb;
import com.ethlo.keyvalue.CasKeyValueDb;

/**
 * 
 * @author Morten Haraldsen
 */
public interface MyCachedClient extends CasKeyValueDb<byte[], byte[], Long>, BatchUpdatableKeyValueDb<byte[], byte[]>
{
}
