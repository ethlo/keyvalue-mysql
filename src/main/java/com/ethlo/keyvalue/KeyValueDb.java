package com.ethlo.keyvalue;

/**
 * 
 * @author mha
 */
public interface KeyValueDb extends AutoCloseable
{
	byte[] get(byte[] key);

	void put(byte[] key, byte[] value);
	
	void delete(byte[] key);

	void clear();
	
	void close();
}