package com.ethlo.keyvalue.sync;

/*-
 * #%L
 * Key/Value sync
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

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import com.google.common.collect.Range;

/**
 * @param <K> Key type
 * @author mha
 */
public class RangeCacheInvalidationMessage<K extends Comparable<K>> implements Serializable
{
    private static final long serialVersionUID = -2136816796409575541L;
    private List<Range<K>> keys;
    private boolean all = false;
    private String memberUuid;

    /**
     * Remove specified keys
     *
     * @param memberUuid The id of the cluster member that the invalidation message originated
     * @param keys       The keys to remove from cache
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
