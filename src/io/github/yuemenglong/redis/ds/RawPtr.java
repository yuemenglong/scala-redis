package io.github.yuemenglong.redis.ds;

import java.util.Arrays;

public class RawPtr implements Ptr {
    final MemoryPool pool;
    final long idx;
    byte[] data;

    RawPtr(MemoryPool pool, long idx, byte[] data) {
        this.pool = pool;
        this.idx = idx; // 56
        this.data = data;
    }

    RawPtr(MemoryPool pool, long addr) {
        this.pool = pool;
        this.idx = addr & 0x00FFFFFFFFFFFFFFL;
        this.data = pool.raw(idx);
    }

    @Override
    public long addr() {
        return ((long) -1 << 56) |
                (idx);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < data.length; i++) {
            if (i % 8 == 0) {
                sb.append(" ");
            }
            sb.append(String.format("%02X", readByte(i)));
        }
        return String.format("[%016X] : %s",
                addr(),
                sb.toString());
    }

    @Override
    public MemoryPool pool() {
        return pool;
    }

    @Override
    public byte[] toByteArray() {
        return data;
    }

    @Override
    public void writeBytes(int p0, byte[] data) {
        writeBytes(p0, data, 0, data.length);
    }

    @Override
    public void readBytes(int p0, byte[] data) {
        readBytes(p0, data, 0, data.length);
    }

    @Override
    public void writeBytes(int p0, byte[] data, int pos, int length) {
        System.arraycopy(data, pos, this.data, p0, length);
    }

    @Override
    public void readBytes(int p0, byte[] data, int pos, int length) {
        System.arraycopy(this.data, p0, data, pos, length);
    }

    @Override
    public void writeByte(int p0, byte value) {
        data[p0] = value;
    }

    @Override
    public byte readByte(int p0) {
        return data[p0];
    }

    @Override
    public void writeShort(int p0, short value) {
        ByteUtil.writeShort(data, p0, value);
    }

    @Override
    public short readShort(int p0) {
        return ByteUtil.readShort(data, p0);
    }

    @Override
    public void writeInt(int p0, int value) {
        ByteUtil.writeInt(data, p0, value);
    }

    @Override
    public int readInt(int p0) {
        return ByteUtil.readInt(data, p0);
    }

    @Override
    public void writeLong(int p0, long value) {
        ByteUtil.writeLong(data, p0, value);
    }

    @Override
    public long readLong(int p0) {
        return ByteUtil.readLong(data, p0);
    }

    @Override
    public void release() {
        pool.release(this);
    }

    @Override
    public void reset() {
        Arrays.fill(data, 0, data.length, (byte) 0);
    }

    @Override
    public int length() {
        return data.length;
    }
}
