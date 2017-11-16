package com.ethlo.keyvalue.keys;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;

import javax.xml.bind.annotation.adapters.HexBinaryAdapter;

public class ByteArrayKey extends Key implements Comparable<ByteArrayKey>
{
	private static final long serialVersionUID = -6752644647644955221L;
	
	private byte[] keyData;
	
	public ByteArrayKey()
	{
		
	}
	
	public ByteArrayKey(byte[] keyData)
	{
		if (keyData == null)
		{
			throw new IllegalArgumentException("keyData cannot be null");
		}
		this.keyData = keyData;
	}
	
	@Override
	public boolean equals(Object b)
	{
		if (b instanceof ByteArrayKey)
		{
			return Arrays.equals(keyData, ((ByteArrayKey) b).keyData);
		}
		return false;
	}

	@Override
	public int hashCode()
	{
		return Arrays.hashCode(keyData);
	}

	public byte[] getByteArray()
	{
		return keyData;
	}

	@Override
	public String toString()
	{
		return getClass().getSimpleName() + " [" + new HexBinaryAdapter().marshal(keyData) + "]";
	}
	
	@Override
	public void writeExternal(ObjectOutput out) throws IOException
	{
		out.writeInt(keyData.length);
		out.write(keyData);
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
	{
		keyData = new byte[in.readInt()];
		in.readFully(keyData);
	}

	@Override
	public int compareTo(ByteArrayKey b)
	{
		if (this == b)
		{
	        return 0;
		}
	    else if (b == null)
	    {
	        return 1; // "a > b"
	    }

	    // now the item-by-item comparison - the loop runs as long as items in both arrays are equal
	    for (int i = 0;; i++)
	    {
	        // shorter array whose items are all equal to the first items of a longer array is considered 'less than'
	        boolean pastA = (i == this.keyData.length);
	        boolean pastB = (i == b.keyData.length);
	        if (pastA && !pastB)
	        {
	            return -1; // "a < b"
	        }
	        else if (!pastA && pastB)
	        {
	            return 1; // "a > b"
	        }
	        else if (pastA && pastB)
	        {
	            return 0; // "a = b", same length, all items equal
	        }

	        final int ai = (int) this.keyData[i];
	        final int bi = (int) b.keyData[i];
	        final int compare = Integer.compare(ai, bi);
	        if (compare != 0)
	        {
	        	return compare;
	        }
	    }
	}
}
