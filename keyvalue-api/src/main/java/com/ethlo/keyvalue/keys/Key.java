package com.ethlo.keyvalue.keys;

import java.io.Externalizable;
import java.io.Serializable;

public abstract class Key implements Serializable, Externalizable
{
	private static final long serialVersionUID = 361003700255254340L;

	public abstract boolean equals(Object b);
	
	public abstract int hashCode();
	
	public abstract String toString();	
}
