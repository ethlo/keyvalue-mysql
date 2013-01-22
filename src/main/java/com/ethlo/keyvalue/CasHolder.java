package com.ethlo.keyvalue;

public class CasHolder
{
	private long casValue;
	private byte[] key;
	private byte[] value;
	
	public CasHolder(long casValue, byte[] key, byte[] value)
	{
		this.casValue = casValue;
		this.key = key;
		this.value = value;
	}

	public long getCasValue()
	{
		return casValue;
	}

	public byte[] getKey()
	{
		return key;
	}

	public byte[] getValue()
	{
		return value;
	}
}