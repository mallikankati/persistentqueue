package com.persistentqueue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;

public interface PersistentQueueSerializer<E> {

    /**
     * Serialization implementation
     *
     * @param e
     * @return
     */
    default byte[] serialize(E e){
        byte[] buff = null;
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             ObjectOutput out = new ObjectOutputStream(bos)){
            out.writeObject(e);
            buff = bos.toByteArray();
        } catch(Exception ex){
            throw new RuntimeException(ex);
        }
        return buff;
    }

    /**
     * convert bytes to object
     *
     * @param bytes
     * @return
     */
    default E deserialize(byte[] bytes){
        E e = null;
        if (bytes != null && bytes.length > 0) {
            try (ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
                 ObjectInput in = new ObjectInputStream(bis)) {
                Object temp = in.readObject();
                if (temp != null) {
                    e = (E) temp;
                }
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }
        return e;
    }
}
