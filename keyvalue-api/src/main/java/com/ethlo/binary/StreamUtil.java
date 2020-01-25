package com.ethlo.binary;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Objects;

public class StreamUtil
{
    private StreamUtil(){}

    public static long copy(InputStream from, OutputStream to) throws IOException
    {
        Objects.requireNonNull(from);
        Objects.requireNonNull(to);
        final byte[] buf = new byte[8192];
        long total = 0L;

        while (true)
        {
            int r = from.read(buf);
            if (r == -1)
            {
                return total;
            }

            to.write(buf, 0, r);
            total += r;
        }
    }
}
