package io.github.yuemenglong.redis.unsafe;

import io.github.yuemenglong.redis.ds.MurmurHash;
import io.github.yuemenglong.redis.unsafe.base.UObj;
import io.github.yuemenglong.redis.unsafe.base.Unsafe;

public class UBytes extends UObj {

    private static int OFFSET_LENGTH = OFFSET_BASE; // int
    private static int OFFSET_DATA = OFFSET_LENGTH + INT_SIZE; // byte[]

    public UBytes(Unsafe U, long addr) {
        super(U, addr);
    }

    public UBytes(Unsafe U, byte[] data) {
        super(U, OFFSET_DATA + data.length);
        U.putInt(addr + OFFSET_LENGTH, data.length);
        for (int i = 0; i < data.length; i++) {
            U.putByte(addr + OFFSET_DATA + i, data[i]);
        }
//        U.copyMemory(data, 0, null, addr + OFFSET_DATA, data.length);
    }

    @Override
    public int allocSize() {
        return OFFSET_DATA + length();
    }

    @Override
    protected void free0() {
        U.freeMemory(addr, allocSize());
    }

    @Override
    protected void freeDeep0() {
        free0();
    }

    @Override
    public int hashCode() {
        return (int) MurmurHash.hash(getBytes());
    }

    @Override
    public Type getType() {
        return Type.BYTES;
    }

    public byte[] getBytes() {
        if (addr == 0) {
            return null;
        }
        int len = U.getInt(addr + OFFSET_LENGTH);
        byte[] ret = new byte[len];
        for (int i = 0; i < len; i++) {
            ret[i] = U.getByte(addr + OFFSET_DATA + i);
        }
        return ret;
    }

    public int length() {
        return U.getInt(addr + OFFSET_LENGTH);
    }

    public int get(int i) {
        return U.getByte(addr + OFFSET_DATA + i);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof UBytes)) {
            return false;
        }
        UBytes that = (UBytes) obj;
        if (this.addr == that.addr()) {
            return true;
        }
        int thisLen = this.length();
        int thatLen = that.length();
        if (thisLen != thatLen) {
            return false;
        }
        for (int i = 0; i < thisLen; i++) {
            if (this.get(i) != that.get(i)) {
                return false;
            }
        }
        return true;
    }

    public static void main(String[] args) {
        Unsafe U = new Unsafe();
        assertTrue(U.count() == 0);
        assertTrue(U.bytes() == 0);

        {
            UBytes ub = new UBytes(U, "a".getBytes());
            assertTrue(ub.allocSize() == 9);
            assertTrue(U.bytes() == 9);
            assertTrue(U.count() == 1);
            ub.free();
            assertTrue(U.bytes() == 0);
            assertTrue(U.count() == 0);
        }
    }
}
