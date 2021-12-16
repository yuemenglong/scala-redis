package io.github.yuemenglong.redis.unsafe.base;

public interface IUObj {

    long addr();

    boolean isNull();

    void free();

    void freeDeep();

    Type getType();

    Unsafe U();

    enum Type {
        BYTES, LIST, MAP, ENTRY, SET, QUEUE,
    }
}
