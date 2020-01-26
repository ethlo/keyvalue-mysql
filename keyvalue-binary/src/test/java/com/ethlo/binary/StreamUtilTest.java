package com.ethlo.binary;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;

import org.assertj.core.api.Assertions;
import org.junit.Test;

public class StreamUtilTest
{
    @Test
    public void copy()
    {
        final byte[] sourceData = "1234567890qwertyuiopåasdfghjklzxcvbnm".getBytes(StandardCharsets.UTF_8);
        final ByteArrayInputStream source = new ByteArrayInputStream(sourceData);
        final ByteArrayOutputStream target = new ByteArrayOutputStream();
        StreamUtil.copy(source, target);
        Assertions.assertThat(target.toByteArray()).isEqualTo(sourceData);
    }
}