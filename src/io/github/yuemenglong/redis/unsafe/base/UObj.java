package io.github.yuemenglong.redis.unsafe.base;

import io.github.yuemenglong.redis.unsafe.*;

public abstract class UObj implements IUObj {
    public static final int BYTE_SIZE = 1;
    public static final int SHORT_SIZE = 2;
    public static final int INT_SIZE = 4;
    public static final int LONG_SIZE = 8;

    private static final int OFFSET_TYPE = 0; // int
    public static final int OFFSET_BASE = INT_SIZE;

    public final Unsafe U;
    protected long addr = 0;

    public UObj(Unsafe U, int allocSize) {
        this.U = U;
        if (allocSize != 0) {
            initMemory(allocSize);
//            System.out.println("Alloc " + getType());
        }
    }

    public UObj(Unsafe U, long addr) {
        this.U = U;
        this.addr = addr;
//        System.out.println("Load " + getType());
    }

    protected void initMemory(int allocSize) {
        addr = U.allocateMemory(allocSize, getType() != Type.BYTES);
        U.putInt(addr + OFFSET_TYPE, getType().ordinal());
    }

    @Override
    public Unsafe U() {
        return this.U;
    }

    @Override
    public long addr() {
        return addr;
    }

    @Override
    public boolean isNull() {
        return addr == 0;
    }

    public abstract int allocSize();

    protected abstract void free0();

    protected abstract void freeDeep0();

    @Override
    public void free() {
        if (addr == 0) {
            return;
        }
        free0();
        addr = 0;
//        System.out.println("Free " + getType());
    }

    @Override
    public void freeDeep() {
        if (addr == 0) {
            return;
        }
        freeDeep0();
        addr = 0;
    }

    public static Type getType(Unsafe U, long addr) {
        return Type.values()[U.getInt(addr + OFFSET_TYPE)];
    }

    public static UObj load(Unsafe U, long addr) {
        UObj ret;
        switch (getType(U, addr)) {
            case BYTES:
                ret = new UBytes(U, addr);
                break;
            case LIST:
                ret = new UList(U, addr);
                break;
            case QUEUE:
                ret = new UQueue(U, addr);
                break;
            case SET:
                ret = new USet(U, addr);
                break;
            case MAP:
                ret = new UMap(U, addr);
                break;
            case ENTRY:
                ret = new UEntry(U, addr);
                break;
            default:
                throw new RuntimeException("Unknown Type: " + getType(U, addr));
        }
        return ret;
    }

    @SuppressWarnings("unchecked")
    public static <T extends UObj> T loadT(Unsafe U, long addr) {
        return (T) load(U, addr);
    }

    protected static int fixLongOffset(int offset) {
        return (offset + 7) / 8 * 8;
    }

    protected static void assertTrue(boolean b) {
        if (!b) {
            throw new RuntimeException("Check Fail");
        }
    }

    protected static void assertNull(Object b) {
        if (b != null) {
            throw new RuntimeException("Check Fail");
        }
    }

    private void checkAddr() {
        if (addr == 0) {
            throw new RuntimeException("Addr Is 0");
        }
    }

    protected void putByte(int offset, byte value) {
        checkAddr();
        U.putByte(addr + offset, value);
    }

    protected void putShort(int offset, short value) {
        checkAddr();
        U.putShort(addr + offset, value);
    }

    protected void putInt(int offset, int value) {
        checkAddr();
        U.putInt(addr + offset, value);
    }

    protected void putLong(int offset, long value) {
        checkAddr();
        U.putLong(addr + offset, value);
    }

    protected byte getByte(int offset) {
        checkAddr();
        return U.getByte(addr + offset);
    }

    protected short getShort(int offset) {
        checkAddr();
        return U.getShort(addr + offset);
    }

    protected int getInt(int offset) {
        checkAddr();
        return U.getInt(addr + offset);
    }

    protected long getLong(int offset) {
        checkAddr();
        return U.getLong(addr + offset);
    }
}
