package io.github.yuemenglong.redis.ds;

public class PoolPtr implements Ptr {
    final MemoryPool pool;
    int slot; // 8
    int blockIdx; // 24
    int offset; // 32

    PoolPtr(MemoryPool pool, int slot, int blockIdx, int offset) {
        this.pool = pool;
        this.slot = slot;
        this.blockIdx = blockIdx;
        this.offset = offset;
    }

    PoolPtr(MemoryPool pool, long addr) {
        this.pool = pool;
        this.slot = (int) (addr >> 56);
        this.blockIdx = (int) (addr >> 32) & 0x00FFFFFF;
        this.offset = (int) (addr);
    }

    public int pageSize() {
        return MemoryPool.PAGE_SIZE[slot];
    }

    @Override
    public long addr() {
        return ((long) slot << 56) |
                ((long) blockIdx << 32) |
                (offset);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < pageSize(); i++) {
            if (i % 8 == 0) {
                sb.append(" ");
            }
            sb.append(String.format("%02X", readByte(i)));
        }
        return String.format("[%02X][%06X][%08X] : %s",
                slot, blockIdx, offset,
                sb.toString());
    }

    MemoryPool.MemoryBlock getBlock() {
        return pool.getBlock(slot, blockIdx);
    }

    @Override
    public MemoryPool pool() {
        return pool;
    }

    @Override
    public byte[] toByteArray() {
        byte[] ret = new byte[pageSize()];
        readBytes(0, ret);
        return ret;
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
        getBlock().writeBytes(offset, p0, data, pos, length);
    }

    @Override
    public void readBytes(int p0, byte[] data, int pos, int length) {
        getBlock().readBytes(offset, p0, data, pos, length);
    }

    @Override
    public void writeByte(int p0, byte value) {
        getBlock().writeByte(offset, p0, value);
    }

    @Override
    public byte readByte(int p0) {
        return getBlock().readByte(offset, p0);
    }

    @Override
    public void writeShort(int p0, short value) {
        getBlock().writeShort(offset, p0, value);
    }

    @Override
    public short readShort(int p0) {
        return getBlock().readShort(offset, p0);
    }

    @Override
    public void writeInt(int p0, int value) {
        getBlock().writeInt(offset, p0, value);
    }

    @Override
    public int readInt(int p0) {
        return getBlock().readInt(offset, p0);
    }

    @Override
    public void writeLong(int p0, long value) {
        getBlock().writeLong(offset, p0, value);
    }

    @Override
    public long readLong(int p0) {
        return getBlock().readLong(offset, p0);
    }

    @Override
    public void release() {
        pool.release(this);
    }

    @Override
    public void reset() {
        getBlock().resetPage(offset);
    }

    @Override
    public int length() {
        return MemoryPool.PAGE_SIZE[slot];
    }
}
