package com.ethlo.mycached;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.iq80.snappy.SnappyInputStream;
import org.iq80.snappy.SnappyOutputStream;
import org.springframework.dao.DataAccessResourceFailureException;

/**
 * 
 * @author Morten Haraldsen
 */
public class CompressionUtil
{
	public static byte[] uncompress(byte[] compressed)
	{
		if (compressed == null)
		{
			return null;
		}
		
		final ByteArrayInputStream bin = new ByteArrayInputStream(compressed);
		try
		{
			final SnappyInputStream in = new SnappyInputStream(bin);
			return IOUtils.toByteArray(in);
		}
		catch (IOException exc)
		{
			throw new DataAccessResourceFailureException(exc.getMessage(), exc);
		}
	}

	public static byte[] compress(byte[] data)
	{
		try
		{
			final ByteArrayOutputStream bout = new ByteArrayOutputStream();
			final SnappyOutputStream out = new SnappyOutputStream(bout);
			out.write(data);
			out.close();
			return bout.toByteArray();
		}
		catch (IOException exc)
		{
			throw new DataAccessResourceFailureException(exc.getMessage(), exc);
		}
	}
}