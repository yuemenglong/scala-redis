package io.github.yuemenglong.redis.unsafe.base;

import java.util.concurrent.atomic.AtomicLong;

public class Unsafe {

    private sun.misc.Unsafe U = UnsafeAccess.U;
//    public static Unsafe I = new Unsafe();

    private AtomicLong count = new AtomicLong();
    private AtomicLong bytes = new AtomicLong();

    public long count() {
        return count.get();
    }

    public long bytes() {
        return bytes.get();
    }

    public byte getByte(long var1) {
        return U.getByte(var1);
    }

    public void putByte(long var1, byte var3) {
        U.putByte(var1, var3);
    }

    public short getShort(long var1) {
        return U.getShort(var1);
    }

    public void putShort(long var1, short var3) {
        U.putShort(var1, var3);
    }

    public char getChar(long var1) {
        return U.getChar(var1);
    }

    public void putChar(long var1, char var3) {
        U.putChar(var1, var3);
    }

    public int getInt(long var1) {
        return U.getInt(var1);
    }

    public void putInt(long var1, int var3) {
        U.putInt(var1, var3);
    }

    public long getLong(long var1) {
        return U.getLong(var1);
    }

    public void putLong(long var1, long var3) {
        U.putLong(var1, var3);
    }

    public float getFloat(long var1) {
        return U.getFloat(var1);
    }

    public void putFloat(long var1, float var3) {
        U.putFloat(var1, var3);
    }

    public double getDouble(long var1) {
        return U.getDouble(var1);
    }

    public void putDouble(long var1, double var3) {
        U.putDouble(var1, var3);
    }

    public long getAddress(long var1) {
        return U.getAddress(var1);
    }

    public void putAddress(long var1, long var3) {
        U.putAddress(var1, var3);
    }

    public long allocateMemory(long var1) {
        return allocateMemory(var1, true);
    }

    public long allocateMemory(long var1, boolean zero) {
        try {
            long addr = U.allocateMemory(var1);
            count.incrementAndGet();
            bytes.addAndGet(var1);
            if (zero) {
                U.setMemory(addr, var1, (byte) 0);
            }
            return addr;
        } catch (Throwable e) {
            String info = String.format("Can't Alloc Memory %d, Total %d, Count %d",
                    var1,
                    bytes(),
                    count()
            );
            throw new RuntimeException(info, e);
        }
    }

    public long reallocateMemory(long var1, long var3) {
        bytes.addAndGet(var3 - var1);
        return U.reallocateMemory(var1, var3);
    }

    public void copyMemory(Object var1, long var2, Object var4, long var5, long var7) {
        U.copyMemory(var1, var2, var4, var5, var7);
    }

    public void freeMemory(long var1, int len) {
        if (var1 == 0) {
            return;
        }
        count.decrementAndGet();
        bytes.addAndGet(-len);
        U.freeMemory(var1);
    }
}
