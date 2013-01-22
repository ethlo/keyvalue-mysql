package com.ethlo.keyvalue;

/**
 * 
 * @author mha
 */
public interface CasKeyValueDb extends KeyValueDb
{
	CasHolder getCas(byte[] key);
	
	void putCas(CasHolder cas);
}
