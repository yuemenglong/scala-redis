# scala-redis

基于scala编写的redis，兼容redis通信协议，可以使用各种语言的redis连接库，官方客户端和telnet方式连接，性能达到原生redis的85%。支持大部分redis常用的数据结构

网络层最开始采用netty，后来发现效果不理想，毕竟相对来说netty还是太重了，因此基于javaNIO自己实现了一个NIO框架，作为网络层使用，经测试单纯网络部分速度是netty的3倍。这部分的代码在nio包中（值得一提的是官方redis也没有使用现有的例如libevent/libev这样的网络库，原因与此类似，都是认为与redis这种精简的服务相比，这些库太重了）

数据结构部分由于java的gc机制导致大内存情况下的卡顿，因此必须使用堆外内存来处理，手动对内存进行管理。开始时使用的是[OpenHFT](https://github.com/OpenHFT)中的SmoothieMap。对比堆上内存性能显著提升，但感觉还是有优化的空间，因此自己又基于堆外内存机制重写了一套支持堆外内存的数据结构，包括map/set/list/queue等，这部分代码在unsafe包下

期间还尝试通过java堆上内存new一块大内存的方式模拟内存池机制，效果并不理想，遂放弃

proto包内是对redis协议的解析，可以通过扩展这个包支持cluster协议

经测试，网络层与堆外数据结构这两部分对比原生redis几乎没有性能差距，唯一产生性能差距的是内存的分配与释放部分，原生redis采用tcmalloc这个库进行内存管理，而java/scala中只能使用jdk的unsafe接口，没有什么优化空间，最终大约效率为原生的85%

