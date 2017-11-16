package com.ethlo.mycached;

/**
 * 
 * @author Morten Haraldsen
 */
public class MyCachedIoException extends RuntimeException
{
	private static final long serialVersionUID = -3137134728819435345L;

	public MyCachedIoException(String message, Exception exc)
	{
		super(message, exc);
	}
}
