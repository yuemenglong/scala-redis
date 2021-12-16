package io.github.yuemenglong.redis.ds;

import static io.github.yuemenglong.redis.ds.ByteUtil.BYTE_SIZE;
import static io.github.yuemenglong.redis.ds.ByteUtil.INT_SIZE;
import static io.github.yuemenglong.redis.ds.Types.TYPE_BYTES;

class BytesPtr {
    private final Ptr ptr;

    public BytesPtr(Ptr ptr) {
        this.ptr = ptr;
    }

    public BytesPtr(MemoryPool pool, byte[] data) {
        ptr = pool.alloc(BYTE_SIZE + INT_SIZE + data.length);
        ptr.writeByte(0, (byte) Types.TYPE_BYTES);
        ptr.writeInt(BYTE_SIZE, data.length);
        ptr.writeBytes(BYTE_SIZE + INT_SIZE, data);
    }

    public Ptr ptr() {
        return ptr;
    }

    public byte[] bytes() {
        int len = ptr.readInt(BYTE_SIZE);
        byte[] ret = new byte[len];
        ptr.readBytes(BYTE_SIZE + INT_SIZE, ret);
        return ret;
    }
}

interface Ptr {
    MemoryPool pool();

    byte[] toByteArray();

    void writeBytes(int p0, byte[] data);

    void readBytes(int p0, byte[] data);

    void writeBytes(int p0, byte[] data, int pos, int length);

    void readBytes(int p0, byte[] data, int pos, int length);

    void writeByte(int p0, byte data);

    byte readByte(int p0);

    void writeShort(int p0, short data);

    short readShort(int p0);

    void writeInt(int p0, int data);

    int readInt(int p0);

    void writeLong(int p0, long data);

    long readLong(int p0);

    void release();

    void reset();

    int length();

    long addr();
}
