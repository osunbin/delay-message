package com.bin.delay.file.common.record;


import java.nio.ByteBuffer;

public interface Record {


    /**
     * The offset of this record in the log
     * @return the offset
     */
    long offset();



    /**
     * Get the size in bytes of this record.
     * @return the size of the record in bytes
     */
    int sizeInBytes();



    /**
     * Get the size in bytes of the value.
     * @return the size of the value, or -1 if the value is null
     */
    int valueSize();



    /**
     * Get the record's value
     * @return the (nullable) value
     */
    ByteBuffer value();




}
