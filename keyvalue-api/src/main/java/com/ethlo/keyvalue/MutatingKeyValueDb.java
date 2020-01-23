package com.ethlo.keyvalue;

/*-
 * #%L
 * Key/Value API
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

import java.util.function.Function;

import com.ethlo.keyvalue.keys.Key;

/**
 * Extension of {@link KeyValueDb} that allows atomic mutation of a single value.
 *
 * @param <K> Key type
 * @param <V> Value type
 * @author Morten Haraldsen
 */
public interface MutatingKeyValueDb<K extends Key, V> extends KeyValueDb<K, V>
{
    /**
     * Value mutator method that should handle atomic updates using optimistic/pessimistic locking
     *
     * @param key
     * @param mutator
     */
    void mutate(K key, Function<V, V> mutator);
}
