package com.persistentqueue.storage.utils;

public final class PersistentUtil {

    /**
     * Write the {@code int} to {@code byte[]}
     *
     * @param buff
     * @param offset
     * @param value
     */
    public static void writeInt(byte[] buff, int offset, int value) {
        buff[offset] = (byte) (value >> 24);
        buff[offset + 1] = (byte) (value >> 16);
        buff[offset + 2] = (byte) (value >> 8);
        buff[offset + 3] = (byte) value;
    }

    /**
     * Reads an {@code int} from {@code byte[]}
     *
     * @param buff
     * @param offset
     * @return
     */
    public static int readInt(byte[] buff, int offset) {
        int val = 0;
        val += (buff[offset] & 0xFF) << 24;
        val += (buff[offset + 1] & 0xFF) << 16;
        val += (buff[offset + 2] & 0xFF) << 8;
        val += (buff[offset + 3] & 0xFF);
        return val;
    }

    /**
     * Write the {@code long} to {@code byte[]}
     *
     * @param buff
     * @param offset
     * @param value
     */
    public static void writeLong(byte[] buff, int offset, long value) {
        buff[offset] = (byte) (value >> 56);
        buff[offset + 1] = (byte) (value >> 48);
        buff[offset + 2] = (byte) (value >> 40);
        buff[offset + 3] = (byte) (value >> 32);
        buff[offset + 4] = (byte) (value >> 24);
        buff[offset + 5] = (byte) (value >> 16);
        buff[offset + 6] = (byte) (value >> 8);
        buff[offset + 7] = (byte) value;
    }

    /**
     * Reads {@code long} from {@code byte[]}
     *
     * @param buff
     * @param offset
     * @return
     */
    public static long readLong(byte[] buff, int offset) {
        long val = 0;
        val += (buff[offset] & 0xFF) << 56;
        val += (buff[offset + 1] & 0xFF) << 48;
        val += (buff[offset + 2] & 0xFF) << 40;
        val += (buff[offset + 3] & 0xFF) << 32;
        val += (buff[offset + 4] & 0xFF) << 24;
        val += (buff[offset + 5] & 0xFF) << 16;
        val += (buff[offset + 6] & 0xFF) << 8;
        val += (buff[offset + 7] & 0xFF);
        return val;
    }
}
