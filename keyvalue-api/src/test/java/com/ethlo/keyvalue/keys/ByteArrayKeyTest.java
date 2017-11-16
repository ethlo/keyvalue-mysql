package com.ethlo.keyvalue.keys;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.charset.StandardCharsets;

import org.junit.Assert;
import org.junit.Test;

public class ByteArrayKeyTest
{
	@Test
	public void testHashCode()
	{
		final byte[] keyData = "eatme".getBytes(StandardCharsets.UTF_8); 
		final ByteArrayKey b1 = new ByteArrayKey(keyData);
		final ByteArrayKey b2 = new ByteArrayKey(keyData);
		Assert.assertEquals(b1.hashCode(), b2.hashCode());
	}
	
	@Test
	public void testEquals()
	{
		final ByteArrayKey b1 = new ByteArrayKey("eatme".getBytes(StandardCharsets.UTF_8));
		final ByteArrayKey b2 = new ByteArrayKey("eatme".getBytes(StandardCharsets.UTF_8));
		final ByteArrayKey c1 = new ByteArrayKey("drinkme".getBytes(StandardCharsets.UTF_8));
		
		Assert.assertTrue(b1.equals(b2));
		Assert.assertTrue(b2.equals(b1));
		Assert.assertTrue(!c1.equals(b1));
		Assert.assertTrue(!c1.equals(b2));
	}
	
	@Test
	public void testSerializeAndDeserialize() throws IOException, ClassNotFoundException
	{
		// Serialize
		final ByteArrayKey b1 = new ByteArrayKey("eatme".getBytes(StandardCharsets.UTF_8));
		final ByteArrayOutputStream bout = new ByteArrayOutputStream();
		final ObjectOutputStream oout = new ObjectOutputStream(bout);
		oout.writeObject(b1);
		oout.flush();

		// Deserialize
		final ByteArrayKey b2 = (ByteArrayKey) new ObjectInputStream(new ByteArrayInputStream(bout.toByteArray())).readObject();
		Assert.assertArrayEquals(b1.getByteArray(), b1.getByteArray());
		Assert.assertEquals(b1,  b2);
	}
}