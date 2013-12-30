/*
 * Copyright 2003-2014 MarkLogic Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.marklogic.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.AbstractMapWritable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.io.Writable;

/**
 * Writable backed by LinkedHashMap.
 * 
 * @author jchen
 */
public class LinkedMapWritable extends AbstractMapWritable 
implements Map<Writable, Writable> {
    private Map<Writable, Writable> instance;
    
    public LinkedMapWritable() {
        super();
        this.instance = new LinkedHashMap<Writable, Writable>();
    }

    /** {@inheritDoc} */
    @Override
    public void clear() {
        instance.clear();
    }

    /** {@inheritDoc} */
    @Override
    public boolean containsKey(Object key) {
        return instance.containsKey(key);
    }

    /** {@inheritDoc} */
    @Override
    public boolean containsValue(Object value) {
        return instance.containsValue(value);
    }

    /** {@inheritDoc} */
    @Override
    public Set<java.util.Map.Entry<Writable, Writable>> entrySet() {
        return instance.entrySet();
    }

    /** {@inheritDoc} */
    @Override
    public Writable get(Object key) {
        return instance.get(key);
    }

    /** {@inheritDoc} */
    @Override
    public boolean isEmpty() {
        return instance.isEmpty();
    }

    /** {@inheritDoc} */
    @Override
    public Set<Writable> keySet() {
        return instance.keySet();
    }

    /** {@inheritDoc} */
    @Override
    public Writable put(Writable key, Writable value) {
        addToMap(key.getClass());
        addToMap(value.getClass());
        return instance.put(key, value);
    }

    /** {@inheritDoc} */
    @Override
    public void putAll(Map<? extends Writable, ? extends Writable> t) {
        for (Map.Entry<? extends Writable, ? extends Writable> e: 
            t.entrySet()) {
            put(e.getKey(), e.getValue());
        }  
    }

    /** {@inheritDoc} */
    @Override
    public Writable remove(Object key) {
        return instance.remove(key);
    }

    /** {@inheritDoc} */
    @Override
    public int size() {
        return instance.size();
    }

    /** {@inheritDoc} */
    @Override
    public Collection<Writable> values() {
        return instance.values();
    }
    
    // Writable

    /** {@inheritDoc} */
    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);

        // Write out the number of entries in the map
        out.writeInt(instance.size());

        // Then write out each key/value pair
        for (Map.Entry<Writable, Writable> e: instance.entrySet()) {
            out.writeByte(getId(e.getKey().getClass()));
            e.getKey().write(out);
            out.writeByte(getId(e.getValue().getClass()));
            e.getValue().write(out);
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);

        // First clear the map.  Otherwise we will just accumulate
        // entries every time this method is called.
        this.instance.clear();

        // Read the number of entries in the map
        int entries = in.readInt();

        // Then read each key/value pair
        for (int i = 0; i < entries; i++) {
            Writable key = (Writable) ReflectionUtils.newInstance(getClass(
                    in.readByte()), getConf());

            key.readFields(in);

            Writable value = (Writable) ReflectionUtils.newInstance(getClass(
                    in.readByte()), getConf());

            value.readFields(in);
            instance.put(key, value);
        }
    }
}
