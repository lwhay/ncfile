package org.apache.trevni.avro.update;

import btree.Serializable;

public class StringSeri implements Serializable {
    String value;

    public StringSeri() {

    }

    public StringSeri(String value) {
        this.value = value;
    }

    @Override
    public byte[] serialize() {
        return value.getBytes();
    }

    @Override
    public void deseriablize(byte[] data) {
        value = new String(data);
    }

    public String toString() {
        return value;
    }
}
