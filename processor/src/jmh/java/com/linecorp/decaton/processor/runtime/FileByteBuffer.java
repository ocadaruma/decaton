package com.linecorp.decaton.processor.runtime;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

/**
 * File backed non-mmap byte buffer
 */
public class FileByteBuffer implements AutoCloseable {
    private final RandomAccessFile randomAccessFile;
    private int limit;
    private int position = 0;

    public FileByteBuffer(RandomAccessFile randomAccessFile) throws IOException {
        this.randomAccessFile = randomAccessFile;
        limit = (int) randomAccessFile.length();
    }

    public RandomAccessFile getRandomAccessFile() {
        return randomAccessFile;
    }

    public int limit() {
        return limit;
    }

    public void updateLimit() throws IOException {
        limit = (int) randomAccessFile.length();
    }

    public int position() {
        return position;
    }

    public FileByteBuffer position(int position) {
        this.position = position;
        return this;
    }

    public FileByteBuffer force() throws IOException {
        randomAccessFile.getChannel().force(true);
        return this;
    }

    public FileByteBuffer duplicate() throws IOException {
        return new FileByteBuffer(randomAccessFile);
    }

    public FileByteBuffer putInt(int value) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(4);
        buffer.putInt(value).flip();
        randomAccessFile.getChannel().write(buffer, position);
        position += 4;
        return this;
    }

    public int getInt(int index) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(4);
        randomAccessFile.getChannel().read(buffer, index);
        buffer.flip();
        return buffer.getInt();
    }

    public FileByteBuffer putLong(long value) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.putLong(value).flip();
        randomAccessFile.getChannel().write(buffer, position);
        position += 8;
        return this;
    }

    public long getLong(int index) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(8);
        randomAccessFile.getChannel().read(buffer, index);
        buffer.flip();
        return buffer.getLong();
    }

    @Override
    public void close() throws IOException {
        randomAccessFile.close();
    }
}
