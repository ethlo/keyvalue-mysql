package com.ethlo.keyvalue;

import java.io.Closeable;
import java.util.Map.Entry;

/**
 * 
 * @author mha
 */
public interface SeekableIterator<K,V> extends Closeable
{
	void seekToFirst();

	boolean hasNext();
	
	Entry<K,V> next();
	
	boolean hasPrevious();
	
	Entry<K,V> previous();
	
	void seekTo(K key);

	void close();
	
	void seekToLast();
}