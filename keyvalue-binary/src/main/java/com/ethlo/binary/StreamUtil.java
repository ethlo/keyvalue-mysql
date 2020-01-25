package com.ethlo.binary;

/*-
 * #%L
 * keyvalue-binary
 * %%
 * Copyright (C) 2013 - 2020 Morten Haraldsen (ethlo)
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

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
