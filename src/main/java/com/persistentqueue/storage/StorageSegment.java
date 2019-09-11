package com.persistentqueue.storage;

/**
 * Each segment of storage is backed by {@link java.nio.MappedByteBuffer} or {@link java.io.RandomAccessFile}.
 * <p>
 * It creates a file if doesn't exist and open for read/write. When it closes it will cleanup MappedByteBuffer underneath.
 */
public interface StorageSegment {

    enum SegmentType {
        FILE {
            @Override
            public String toString() {
                return "FILE";
            }
        }, MEMORYMAPPED {
            public String toString() {
                return "MEMORYMAPPED";
            }
        }
    };

    /**
     * Is this segment open for read/write
     *
     * @return
     */
    boolean isOpen();

    /**
     * Is this segment closed for read/write
     *
     * @return
     */
    boolean isClosed();

    /**
     * To initialize a segment with specified name and length
     *
     * @param path          initial file path
     * @param name          name of the file
     * @param ext           extension of the file
     * @param segmentId     unique id for this segment
     * @param initialLength length of the file
     */
    void init(String path, String name, String ext, int segmentId, int initialLength);

    /**
     * Reading bytes from file
     *
     * @param position starting position
     * @param buff     buffer to read into
     * @param offset   offset to start
     * @param count    total bytes to read
     */
    void read(long position, byte[] buff, int offset, int count);

    /**
     * Write the data to storage
     *
     * @param position position to start writing
     * @param buff     byte array
     * @param offset   offset to start
     * @param count    total bytes
     */
    void write(long position, byte[] buff, int offset, int count);

    /**
     * It will calculate given data length fits in the storage
     *
     * @param dataLength length of the data
     * @return if it fits, it will return true otherwise false
     */
    boolean isSpaceAvailable(int dataLength);

    /**
     * Move to the specified position in a storage segment
     *
     * @param position
     */
    void seekToPosition(int position);

    /**
     * Current position in the storage
     *
     * @return
     */
    int getCurrentPosition();

    /**
     * Remaining space on this segment
     *
     * @param position from the position remaining bytes
     * @return remaining space
     */
    int remaining(int position);

    /**
     * Closes open files or buffers
     *
     * @param delete delete underneath the storage when it closed
     */
    void close(boolean delete);

    /**
     * Path where it stores the data. In this case it's a directory
     *
     * @return
     */
    String getPath();

    /**
     * Name of the file backed by this storage
     *
     * @return
     */
    String getName();

    /**
     * Extension of the file, by default either .dat or .meta
     *
     * @return
     */
    String getExtension();

    /**
     * Length of this file in bytes
     *
     * @return
     */
    int getLength();

    /**
     * Id of this segment
     *
     * @return
     */
    int getSegmentId();
}
