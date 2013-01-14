package com.ethlo.mycached;

import java.util.List;

/**
 * 
 * @author mha
 */
public interface MyCachedClientManager
{
	MyCachedClient open(String dbName, boolean allowCreate);

	List<String> list();
}
