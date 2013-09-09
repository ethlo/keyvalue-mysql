package com.ethlo.mycached;

import java.nio.ByteBuffer;

import com.ethlo.keyvalue.BatchUpdatableKeyValueDb;
import com.ethlo.keyvalue.CasKeyValueDb;

/**
 * 
 * @author Morten Haraldsen
 */
public interface MyCachedClient extends CasKeyValueDb<ByteBuffer, byte[], Long>, BatchUpdatableKeyValueDb<ByteBuffer, byte[]>
{
}
