package com.ethlo.keyvalue.sync;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import com.google.common.collect.Range;

/**
 * 
 * @author mha
 *
 * @param <K>
 */
public class RangeCacheInvalidationMessage<K extends Comparable<K>> implements Serializable
{
	private static final long serialVersionUID = -2136816796409575541L;
	private List<Range<K>> keys;
	private boolean all = false;
    private String memberUuid;
	
	/**
	 * Remove specified keys
	 * @param keys The keys to remove from cache
	 */
	@SafeVarargs
	public RangeCacheInvalidationMessage(String memberUuid, Range<K>... keys)
	{
	    this.memberUuid = memberUuid;
		this.keys = Arrays.asList(keys);
	}
	
	/**
	 * Remove all keys from cache
	 */
	public RangeCacheInvalidationMessage()
	{
		this.all = true;
	}

	public List<Range<K>> getKeys()
	{
		return keys;
	}

	public boolean isAll()
	{
		return all;
	}

    public String getMemberUuid()
    {
        return memberUuid;
    }
}