/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package metadata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.trevni.TrevniRuntimeException;

import columnar.BatchColumnFileReader;
import columnar.InsertColumnFileReader;
import io.InputBuffer;
import io.OutputBuffer;
import misc.ValueType;

/**
 * Metadata for a column.
 */
public class FileColumnMetaData extends MetaData<FileColumnMetaData> {
    private static final long serialVersionUID = 1L;
    static final String NAME_KEY = RESERVED_KEY_PREFIX + "name";
    static final String TYPE_KEY = RESERVED_KEY_PREFIX + "type";
    static final String VALUES_KEY = RESERVED_KEY_PREFIX + "values";
    static final String PARENT_KEY = RESERVED_KEY_PREFIX + "parent";
    static final String ARRAY_KEY = RESERVED_KEY_PREFIX + "array";
    static final String LAYER_KEY = RESERVED_KEY_PREFIX + "layer";
    static final String UNION_KEY = RESERVED_KEY_PREFIX + "union";
    static final String UNION_ARRAY = RESERVED_KEY_PREFIX + "unionArray";
    static final String GROUP_S = RESERVED_KEY_PREFIX + "schema";

    // cache these values for better performance
    private String name;
    private ValueType type;
    private boolean values;
    private FileColumnMetaData parent;
    private boolean isArray;
    private int layer;

    private int union; //the schemas' number of the union
    private int unionBits; //the index bits of a unit
    private ValueType[] unionArray; //the valuetype array of the union
    public Map<ValueType, Integer> unionToInteger; //the index of the valuetype
    private String schema; //the schema string of the group

    private transient List<FileColumnMetaData> children = new ArrayList<FileColumnMetaData>();
    private transient int number = -1;

    public FileColumnMetaData() {

    }

    /**
     * Construct given a name and type.
     */
    public FileColumnMetaData(String name, ValueType type) {
        this.name = name;
        setReserved(NAME_KEY, name);
        this.type = type;
        setReserved(TYPE_KEY, type.getName());
        this.layer = 0;
        setReserved(LAYER_KEY, new String() + layer);
    }

    public FileColumnMetaData(String name, ValueType type, int union, ValueType[] unionArray) {
        this.name = name;
        setReserved(NAME_KEY, name);
        this.type = type;
        setReserved(TYPE_KEY, type.getName());
        this.layer = 0;
        setReserved(LAYER_KEY, new String() + layer);
        this.union = union;
        setReserved(UNION_KEY, new String() + union);
        this.unionArray = unionArray;

        if (union <= 2)
            unionBits = 1;
        else if (union <= 4)
            unionBits = 2;
        else if (union <= 16)
            unionBits = 4;
        else if (union <= 32)
            unionBits = 8;
        else
            throw new TrevniRuntimeException("this union schema has too many children!");

        StringBuilder str = new StringBuilder();
        str.append(unionArray[0].toString());
        for (int i = 1; i < unionArray.length; i++)
            str.append("|" + unionArray[i].toString());
        setReserved(UNION_ARRAY, str.toString());
        createUnionToInteger();
    }

    public void createUnionToInteger() {
        if (type != ValueType.UNION)
            throw new TrevniRuntimeException("This is not a union column!");
        unionToInteger = new HashMap<ValueType, Integer>();
        for (int i = 0; i < union; i++) {
            unionToInteger.put(unionArray[i], i);
        }
    }

    public Integer getUnionIndex(ValueType type) {
        return unionToInteger.get(type);
    }

    public String getGroup_S() {
        if (type != ValueType.GROUP)
            throw new TrevniRuntimeException("This is not a group column!");
        return schema;
    }

    public void setGroup_S(String str) {
        if (type != ValueType.GROUP)
            throw new TrevniRuntimeException("This is not a group column!");
        this.schema = str;
        setReserved(GROUP_S, str);
    }

    /**
     * Return this column's name.
     */
    public String getName() {
        return name;
    }

    /**
     * Return this column's type.
     */
    public ValueType getType() {
        return type;
    }

    /**
     * Return this column's parent or null.
     */
    public FileColumnMetaData getParent() {
        return parent;
    }

    /**
     * Return this column's children or null.
     */
    public List<FileColumnMetaData> getChildren() {
        return children;
    }

    public int getUnion() {
        return union;
    }

    public int getUnionBits() {
        return unionBits;
    }

    public ValueType[] getUnionArray() {
        return unionArray;
    }

    /**
     * Return true if this column is an array.
     */
    public boolean isArray() {
        return isArray;
    }

    /**
     * Return this column's number in a file.
     */
    public int getNumber() {
        return number;
    }

    public void setNumber(int number) {
        this.number = number;
    }

    public int getLayer() {
        return layer;
    }

    public FileColumnMetaData setLayer(int layer) {
        this.layer = layer;
        return setReserved(LAYER_KEY, new String() + layer);
    }

    /**
     * Set whether this column has an index of blocks by value. This only makes
     * sense for sorted columns and permits one to seek into a column by value.
     */
    public FileColumnMetaData hasIndexValues(boolean values) {
        if (isArray)
            throw new TrevniRuntimeException("Array column cannot have index: " + this);
        this.values = values;
        return setReservedBoolean(VALUES_KEY, values);
    }

    /**
     * Set this column's parent. A parent must be a preceding array column.
     */
    public FileColumnMetaData setParent(FileColumnMetaData parent) {
        if (!parent.isArray())
            throw new TrevniRuntimeException("Parent is not an array: " + parent);
        if (values)
            throw new TrevniRuntimeException("Array column cannot have index: " + this);
        this.parent = parent;
        parent.children.add(this);
        return setReserved(PARENT_KEY, parent.getName());
    }

    /**
     * Set whether this column is an array.
     */
    public FileColumnMetaData isArray(boolean isArray) {
        if (values)
            throw new TrevniRuntimeException("Array column cannot have index: " + this);
        this.isArray = isArray;
        return setReservedBoolean(ARRAY_KEY, isArray);
    }

    /**
     * Get whether this column has an index of blocks by value.
     */
    public boolean hasIndexValues() {
        return getBoolean(VALUES_KEY);
    }

    public void read(InputBuffer in, InsertColumnFileReader file) throws IOException {
        read(in, this);
        this.name = this.getString(NAME_KEY);
        this.type = ValueType.forName(this.getString(TYPE_KEY));
        this.values = this.getBoolean(VALUES_KEY);
        this.isArray = this.getBoolean(ARRAY_KEY);
        this.layer = this.getInteger(LAYER_KEY);
        if (this.type.equals(ValueType.UNION)) {
            this.union = this.getInteger(UNION_KEY);
            if (this.union <= 2)
                this.unionBits = 1;
            else if (this.union <= 4)
                this.unionBits = 2;
            else if (this.union <= 16)
                this.unionBits = 4;
            else if (this.union <= 32)
                this.unionBits = 8;
            else
                throw new TrevniRuntimeException("this union schema has too many children!");

            String[] tmp = this.getString(UNION_ARRAY).split("\\|");
            this.unionArray = new ValueType[tmp.length];
            for (int i = 0; i < tmp.length; i++)
                this.unionArray[i] = ValueType.valueOf(tmp[i]);
        }
        String parentName = this.getString(PARENT_KEY);
        if (parentName != null)
            this.setParent(file.getFileColumnMetaData(parentName));
    }

    public void read(InputBuffer in, BatchColumnFileReader file) throws IOException {
        this.read(in, this);
        this.name = this.getString(NAME_KEY);
        this.type = ValueType.forName(this.getString(TYPE_KEY));
        this.values = this.getBoolean(VALUES_KEY);
        this.isArray = this.getBoolean(ARRAY_KEY);
        this.layer = this.getInteger(LAYER_KEY);
        if (this.type.equals(ValueType.UNION)) {
            this.union = this.getInteger(UNION_KEY);
            if (this.union <= 2)
                this.unionBits = 1;
            else if (this.union <= 4)
                this.unionBits = 2;
            else if (this.union <= 16)
                this.unionBits = 4;
            else if (this.union <= 32)
                this.unionBits = 8;
            else
                throw new TrevniRuntimeException("this union schema has too many children!");

            String[] tmp = this.getString(UNION_ARRAY).split("\\|");
            this.unionArray = new ValueType[tmp.length];
            for (int i = 0; i < tmp.length; i++)
                this.unionArray[i] = ValueType.valueOf(tmp[i]);
        }
        String parentName = this.getString(PARENT_KEY);
        if (parentName != null)
            this.setParent(file.getFileColumnMetaData(parentName));
    }

    public void write(OutputBuffer out) throws IOException {
        super.write(out);
    }

}
