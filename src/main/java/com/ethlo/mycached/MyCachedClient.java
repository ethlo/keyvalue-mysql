package com.ethlo.mycached;

/**
 * 
 * @author Morten Haraldsen
 */
public interface MyCachedClient
{
	byte[] get(byte[] key);

	boolean set(byte[] key, byte[] value);

	boolean delete(byte[] key);

	void clear();
}
