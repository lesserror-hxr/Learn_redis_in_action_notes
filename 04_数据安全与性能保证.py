# coding: utf-8

import os
import time
import unittest
import uuid

import redis

# 维护数据安全以及应对系统故障的方法，保证数据完整性的前提下提升Redis性能的方法。
# Redis提供了两种不同的持久化方法来将数据存储到硬盘里面。一种方法叫快照（snapshotting）,
# 它可以将存在于某一时刻的所有数据都写入硬盘里面。另一种方法叫只追加文件（append-only file，AOF），
# 它会在执行写命令时，将被执行的写命令复制到硬盘里面。
# Redis可以通过创建快照来获得存储在内存里面的数据在某个时间点上的副本。在创建快照之后，用户可以对快照
# 进行备份，可以将快照复制到其他服务器从而创建具有相同数据的服务器副本，还可以将快照留在原地以便重启服务器时使用。
# 代码清单 4-1
'''
# <start id="persistence-options"/>
# 多久执行一次自动快照操作
save 60 1000                        # 快照持久化选项。
stop-writes-on-bgsave-error no      # 在创建快照失败后是否仍然继续执行写命令
rdbcompression yes                  # 是否对快照文件进行压缩
dbfilename dump.rdb                 # 如何命名硬盘上的快照文件

appendonly no                       # 只追加文件持久化选项。
appendfsync everysec                # 多久才将写入的内容同步到硬盘
no-appendfsync-on-rewrite no        # 在对AOF进行压缩（compaction）的时候能否执行同步操作
auto-aof-rewrite-percentage 100     # 多久执行一次AOF压缩
auto-aof-rewrite-min-size 64mb      #
dir ./                              # 共享选项，这个选项决定了快照文件和只追加文件的保存位置。
'''

# 验证快照文件和AOF文件
# 无论是快照持久化还是AOF持久化，都提供了在遇到系统故障时进行数据恢复的工具。
# Redis提供了两个命令行程序redis-check-aof和redis-check-dump，它们可以在系统故障发生后，
# 检查AOF文件和快照文件的状态，并在有需要的情况下对文件进行修复。
# 验证快照文件的完整性，可以通过计算快照文件的SHA1散列值和SHA256散列值来对内容进行验证。
# 当今的Linux平台和Unix平台都包含类似sha1sum和sha256sum这样的用于生成和验证散列值的命令行程序。

# 校验和（checksum）与散列值（hash）从2.6版本开始，Redis会在快照文件中包含快照文件自身的CRC64校验和。
# CRC检验对于发现典型的网络传输错误和硬盘损坏非常有帮助，
# 而SHA加密散列值则更擅长于发现文件中的任意错误（arbitrary error）。
# 简单来说，用户可以翻转文件中任意数量的二进制位，
# 然后通过翻转文件最后64个二进制位的一个子集（subset）来产生与原文件相同的CRC64校验和。
# 而对于SHA1和SHA256，目前还没有任何已知的方法可以做到这一点。



# 如果在新的快照文件创建完毕之前，Redis、系统或者硬件这三者之中的任意一个崩溃了，那么Redis将丢失最近一次创建快照之后写入的所有数据。
# 创建快照的办法有以下几种。
# 客户端可以通过向Redis发送BGSAVE命令来创建一个快照。对于支持BGSAVE命令的平台来说（基本上所有平台都支持，除了Windows平台），Redis会调用
# fork来创建一个子进程，然后子进程负责将快照写入硬盘，而父进程则继续处理命令请求。
# 当一个进程创建子进程的时候，底层的操作系统会创建该进程的一个副本。在Unix和类Unix系统上面，
# 创建子进程的操作会进行如下的优化：在刚开始的时候，父子进程共享相同的内存，直到父进程或者子进程对内存进行了写入之后，
# 对被写入内存的共享才会结束。
# 客户端还可以通过向Redis发送SAVE命令来创建一个快照，接到SAVE命令的Redis服务器在快照创建完毕之前将不再响应任何其他命令。SAVE命令并不常用
# 我们通常只会在没有足够内存去执行BGSAVE命令的情况下，又或者即使等待持久化操作执行完毕也无所谓的情况下，才会使用这个命令。
# 如果用户设置了save配置选项，比如save 60 10000，那么从Redis最近一次创建快照之后开始算起，当"60秒之内有10000次写入"这个条件被满足时，Redis
# 就会自动触发BGSAVE命令。如果用户设置了多个save配置选项，那么当任意一个save配置选项所设置的条件被满足时，Redis就会触发一次BGSAVE命令。
# 当Redis通过SHUTDOWN命令接收到关闭服务器的请求时，或者接收到标准TERM信号时，会执行一个SAVE命令，阻塞所有客户端，不再执行客户端发送的任何
# 命令，并在SAVE命令执行完毕之后关闭服务器
# 当一个Redis服务器连接另一个Redis服务器，并向对方发送SYNC命令来开始一次复制操作的时候，如果主服务器目前没有在执行BGSAVE操作，或者主服务器并非刚刚执行完BGSAVE操作，那么主服务器就会执行BGSAVE命令。
# 代码清单 4-2
# <start id="process-logs-progress"/>
# 日志处理函数接受的其中一个参数为回调函数，
# 这个回调函数接受一个Redis连接和一个日志行作为参数，
# 并通过调用流水线对象的方法来执行Redis命令。
# 在进行数据恢复时，首先要做的就是弄清楚我们丢失了哪些数据
# 对日志进行聚合计算 需要在处理日志的同时记录被处理日志的相关信息
def process_logs(conn, path, callback):
    # callback 待处理日志文件中各个行line的回调函数，可以在处理日志文件的同时，记录被处理日志文件的名字以及偏移量
    # 回调函数接受一个Redis连接和一个日志行作为参数，并通过调用流水线对象的方法来执行Redis命令
    # 获取文件当前的处理进度。
    current_file, offset = conn.mget(
        'progress:file', 'progress:position')

    pipe = conn.pipeline()

    # 通过使用闭包（closure）来减少重复代码
    def update_progress():
        # 更新正在处理的日志文件的名字和偏移量。
        pipe.mset({
            'progress:file': fname,
            'progress:position': offset
        })
        # 这个语句负责执行实际的日志更新操作，
        # 并将日志文件的名字和目前的处理进度记录到Redis里面。
        pipe.execute()

    # 有序地遍历各个日志文件。
    for fname in sorted(os.listdir(path)):
        # 略过所有已处理的日志文件。
        if fname < current_file:
            continue

        inp = open(os.path.join(path, fname), 'rb')
        # 在接着处理一个因为系统崩溃而未能完成处理的日志文件时，略过已处理的内容。
        if fname == current_file:
            inp.seek(int(offset, 10))
        else:
            offset = 0

        current_file = None

        # 枚举函数遍历一个由文件行组成的序列，
        # 并返回任意多个二元组，
        # 每个二元组包含了行号lno和行数据line，
        # 其中行号从0开始。
        for lno, line in enumerate(inp):
            # 处理日志行。
            callback(pipe, line)
            # 更新已处理内容的偏移量。
            offset += int(offset) + len(line)

            # 每当处理完1000个日志行或者处理完整个日志文件的时候，
            # 都更新一次文件的处理进度。
            if not (lno+1) % 1000:
                update_progress()

        update_progress()

        inp.close()
# <end id="process-logs-progress"/>


# 对于真实的硬件，Redis进程每占用一个GB的内存，创建该进程的子进程所需的时间就要增加10 ~ 20毫秒。
# 为了防止Redis因为创建子进程而出现停顿，我们可以考虑关闭自动保存，转而通过手动发送BGSAVE或者SAVE来进行持久化。手动发送BGSAVE一样
# 会引起停顿，唯一不同的是用户可以通过手动发送BGSAVE命令来控制停顿出现的时间。另一方面，虽然SAVE会一直阻塞Redis直到快照生成完毕，但是
# 因为它不需要创建子进程，所以就不会像BGSAVE一样因为创建子进程而导致Redis停顿；
# 并且因为没有子进程在争抢资源，所以SAVE创建快照的速度会比BGSAVE创建快照的速度要来得更快一些。
# 根据我的个人经验，在一台拥有68GB内存的Xen虚拟机上面，对一个占用50GB的内存的Redis服务器执行BGSAVE命令的话，光是创建子进程就需要花费15秒
# 以上，而生成快照则需要花费15~20分钟；但使用SAVE只需要3~5分钟就可以完成快照的生成工作。
# 检验Redis的从服务器是否将主服务器的AOF文件每秒一次的频率同步到了硬盘里面
# INFO命令提供了大量的与Redis服务器当前状态有关的信息，比如内存占用量、客户端连接数、每个数据库包含的键的数量、
# 上一次创建快照文件之后执行的命令数量，等等。总的来说，INFO命令对于了解Redis服务器的综合状态非常有帮助。
# 代码清单 4-3
# <start id="wait-for-sync"/>
def wait_for_sync(mconn, sconn):
    identifier = str(uuid.uuid4())
    # 将令牌添加至主服务器。
    mconn.zadd('sync:wait', identifier, time.time())

    # 如果有必要的话，等待从服务器完成同步。
    while sconn.info()['master_link_status'] != 'up':
        time.sleep(.001)

    # 等待从服务器接收数据更新。
    while not sconn.zscore('sync:wait', identifier):
        time.sleep(.001)

    # 最多只等待一秒钟。
    deadline = time.time() + 1.01
    while time.time() < deadline:
        # 检查数据更新是否已经被同步到了磁盘。
        if sconn.info()['aof_pending_bio_fsync'] == 0:
            break
        time.sleep(.001)

    # 清理刚刚创建的新令牌以及之前可能留下的旧令牌。
    mconn.zrem('sync:wait', identifier)
    mconn.zremrangebyscore('sync:wait', 0, time.time()-900)
# <end id="wait-for-sync"/>

# AOF持久化
# 简单来说，AOF持久化会将被执行的写命令写到AOF文件的末尾，以此来记录数据发生的变化。因此，Redis只要从头到尾重新执行一次AOF文件包含的所有
# 写命令，就可以恢复AOF文件所记录的数据集。
# 用户可以命令操作系统将文件同步（sync）到硬盘，同步操作会一直阻塞到指定的文件被写入硬盘为止。
# 当同步操作执行完毕之后，即使系统出现故障也不会对被同步的文件造成任何影响。
# appendfsync 选项及同步频率
# always  每个Redis写命令都要同步写入硬盘。这样做会严重降低Redis的速度
# everysec 每秒执行一次同步，显示地将多个写命令同步到硬盘
# no       让操作系统来决定应该何时进行同步
# solid-state drive（固态硬盘）在always的情况下，因为这个选项让Redis每次只写入一个命令，而不是像其他appendfsync选项那样一次写入多个命令
# 这种不断写入少量数据的做法有可能会引发严重的写入放大（write amplification）问题，在某些情况下甚至会将固态硬盘的寿命从原来的几年降低为
# 几个月
# 为了兼顾数据安全和写入性能，用户可以考虑使用appendfsync everysec选项，让Redis以每秒一次的频率对AOF文件进行同步。Redis每秒同步一次AOF文
# 件时的性能和不使用任何持久化特性时的性能相差无几，而通过每秒同步一次AOF文件，Redis可以保证，即使出现系统奔溃，用户也最多只会丢失一秒之内
# 产生的数据。当硬盘忙于执行写入操作的时候，Redis还会优雅地放慢自己的速度以便适应硬盘的最大写入速度。
# 如果用户使用appendfsync no选项，那么Redis将不对AOF文件执行任何显示的同步操作，而是由操作系统来决定应该在何时对AOF文件进行同步。这个选项
# 在一般情况下不会对Redis的性能带来影响，但系统崩溃将导致使用这种选项的的Redis服务器丢失不定数量的数据。另外，如果用户的硬盘处理写入操作的
# 速度不够快的话，那么当缓冲区被等待写入硬盘的数据填满时，Redis的写入操作将被阻塞，并导致Redis处理命令请求的速度变慢。因为这个原因，一般
# 来说并不推荐使用appendfsync no 选项，在这里介绍它只是为了完整列举appendfsync选项可用的3个值。
# 代码清单 4-4
# 以下代码更换故障主服务器
# 另一种创建新的主服务器的方法，就是将从服务器升级（turn）为主服务器，
# 并为升级后的主服务器创建从服务器。
# 一次性发送多个命令，然后等待所有回复出现的做法通常被称为流水线（pipelining）,
# 通过减少客户端与Redis服务器之间的网络通信次数来提升Redis执行多个命令时的性能。
#
'''
# <start id="master-failover"/>
user@vpn-master ~:$ ssh root@machine-b.vpn                          # 通过VPN网络连接机器B。
Last login: Wed Mar 28 15:21:06 2012 from ...                       #
root@machine-b ~:$ redis-cli                                        # 启动命令行Redis客户端来执行几个简单的操作。
redis 127.0.0.1:6379> SAVE                                          # 执行SAVE命令，
OK                                                                  # 并在命令完成之后，
redis 127.0.0.1:6379> QUIT                                          # 使用QUIT命令退出客户端。
root@machine-b ~:$ scp \\                                           # 将快照文件发送至新的主服务器——机器C。
> /var/local/redis/dump.rdb machine-c.vpn:/var/local/redis/         #
dump.rdb                      100%   525MB  8.1MB/s   01:05         #
root@machine-b ~:$ ssh machine-c.vpn                                # 连接新的主服务器并启动Redis。
Last login: Tue Mar 27 12:42:31 2012 from ...                       #
root@machine-c ~:$ sudo /etc/init.d/redis-server start              #
Starting Redis server...                                            #
root@machine-c ~:$ exit
root@machine-b ~:$ redis-cli                                        # 告知机器B的Redis，让它将机器C用作新的主服务器。
redis 127.0.0.1:6379> SLAVEOF machine-c.vpn 6379                    #
OK                                                                  #
redis 127.0.0.1:6379> QUIT
root@machine-b ~:$ exit
user@vpn-master ~:$
# <end id="master-failover"/>
#A Connect to machine B on our vpn network
#B Start up the command line redis client to do a few simple operations
#C Start a SAVE, and when it is done, QUIT so that we can continue
#D Copy the snapshot over to the new master, machine C
#E Connect to the new master and start Redis
#F Tell machine B's Redis that it should use C as the new master
#END
'''

# 代码清单 4-5
# 在将一件商品放到市场上进行销售的时候，程序需要将被销售的商品添加到记录市场正在销售商品的有序集合里面，
# 并且在添加操作执行的过程中，监视卖家的包裹以确保被销售的商品的确存在于卖家的包裹当中。
# 什么是DISCARD？UNWATCH命令可以在WATCH命令执行之后、MULTI命令执行之前对连接进行重置（reset）；
# 同样地，DISCARD命令也可以在MULTI命令执行之后、EXEC命令执行之前对连接进行重置。这也就是说，用户在使用WATCH监视一个或多个键，
# 接着使用MULTI开始一个新的事务，并将多个命令入队到事务队列之后，仍然可以通过发送DISCARD命令来取消WATCH命令并清空所有已入队命令。
# <start id="_1313_14472_8342"/>
def list_item(conn, itemid, sellerid, price):
    # 用户的包裹
    inventory = "inventory:%s"%sellerid
    # 需要添加到商品市场的物品ID和卖家ID的组合
    item = "%s.%s"%(itemid, sellerid)
    end = time.time() + 5
    pipe = conn.pipeline()

    while time.time() < end:
        try:
            # 监视用户包裹发生的变化。
            pipe.watch(inventory)
            # 验证用户是否仍然持有指定的物品。
            if not pipe.sismember(inventory, itemid):
                # 如果指定的物品不在用户的包裹里面，
                # 那么停止对包裹键的监视并返回一个空值。
                pipe.unwatch()
                return None

            # 将指定的物品添加到物品买卖市场里面。
            pipe.multi()
            pipe.zadd("market:", item, price)
            pipe.srem(inventory, itemid)
            # 如果执行execute方法没有引发WatchError异常，
            # 那么说明事务执行成功，
            # 并且对包裹键的监视也已经结束。
            pipe.execute()
            return True
        # 用户的包裹已经发生了变化；重试。
        except redis.exceptions.WatchError:
            pass
    return False
# <end id="_1313_14472_8342"/>


# 代码清单 4-6
# 从商品买卖市场购买商品
# <start id="_1313_14472_8353"/>
def purchase_item(conn, buyerid, itemid, sellerid, lprice):
    # 商品买家
    buyer = "users:%s"%buyerid
    # 商品卖家
    seller = "users:%s"%sellerid
    # 商品市场中的一个元素
    item = "%s.%s"%(itemid, sellerid)
    # 买家的包裹
    inventory = "inventory:%s"%buyerid
    end = time.time() + 10
    pipe = conn.pipeline()

    while time.time() < end:
        try:
            # 对物品买卖市场以及买家账号信息的变化进行监视。
            pipe.watch("market:", buyer)

            # 检查指定物品的价格是否出现了变化，
            # 以及买家是否有足够的钱来购买指定的物品。
            price = pipe.zscore("market:", item)
            funds = int(pipe.hget(buyer, "funds"))
            if price != lprice or price > funds:
                pipe.unwatch()
                return None

            # 将买家支付的货款转移给卖家，并将卖家出售的物品移交给买家。
            pipe.multi()
            pipe.hincrby(seller, "funds", int(price))
            pipe.hincrby(buyer, "funds", int(-price))
            pipe.sadd(inventory, itemid)
            pipe.zrem("market:", item)
            pipe.execute()
            return True
        # 如果买家的账号或者物品买卖市场出现了变化，那么进行重试。
        except redis.exceptions.WatchError:
            pass

    return False
# <end id="_1313_14472_8353"/>

# 在访问以写入为目的的数据的时候（SQL中的SELECT FOR UPDATE），关系数据库会对被访问的数据行进行加锁，直到事务被提交（COMMIT）
# 或者被回滚（ROLLBACK）为止。
# Redis只会在数据已经被其他客户端抢先修改了的情况下，通知执行了WATCH命令的客户端，这种做法被称为乐观锁（optimistic locking），
# 而关系数据库实际执行的加锁操作则被称为悲观锁（pessimistic locking）。乐观锁在实际使用中同样非常有效，因为客户端永远不必花时间去
# 等待第一个取得锁的客户端——它们只需要在自己的事务执行失败时进行重试就可以了。

# 非事务型流水线 non-transactional pipeline
# 在不使用事务的情况下，通过使用流水线来进一步提升命令的执行性能
# 代码清单 4-7
# <start id="update-token"/>
def update_token(conn, token, user, item=None):
    # 获取时间戳。
    timestamp = time.time()
    # 创建令牌与已登录用户之间的映射。
    conn.hset('login:', token, user)
    # 记录令牌最后一次出现的时间。
    conn.zadd('recent:', token, timestamp)
    if item:
        # 把用户浏览过的商品记录起来。
        conn.zadd('viewed:' + token, item, timestamp)
        # 移除旧商品，只记录最新浏览的25件商品。
        conn.zremrangebyrank('viewed:' + token, 0, -26)
        # 更新给定商品的被浏览次数。
        conn.zincrby('viewed:', item, -1)
# <end id="update-token"/>


# MULTI和EXEC并不是免费的——它们也会消耗资源，并且可能会导致其他重要的命令被延迟执行。
# 通过将标准的Redis连接替换成流水线连接，程序可以将通信往返的次数减少至原来的二分之一到五分之一，
# 并将update_token_pipeline()函数的预期执行时间降低至1—2毫秒。
# 代码清单 4-8
# <start id="update-token-pipeline"/>
def update_token_pipeline(conn, token, user, item=None):
    timestamp = time.time()
    # 如果用户在执行pipeline()时传入True作为参数，或者不传入任何参数，那么客户端将使用MULTI和EXEC包裹起用户要执行的所有命令。
    # 如果用户在执行pipeline()时传入False为参数，那么客户端同样会像执行事务那样收集起用户要执行的所有命令，只是不再使用MULTI和EXEC包裹这些
    # 命令。
    # 如果用户需要向Redis发送多个命令，并且对于这些命令来说，一个命令的执行结果并不会影响另一个命令的输入，而且这些命令也不需要以事务的方式
    # 来执行的话，那么我们可以通过向pipeline()方法传入False来进一步提升Redis的整体性能。
    # 设置流水线。
    pipe = conn.pipeline(False)                         #A
    pipe.hset('login:', token, user)
    pipe.zadd('recent:', token, timestamp)
    if item:
        pipe.zadd('viewed:' + token, item, timestamp)
        pipe.zremrangebyrank('viewed:' + token, 0, -26)
        pipe.zincrby('viewed:', item, -1)
    # 执行那些被流水线包裹的命令。
    pipe.execute()                                      #B
# <end id="update-token-pipeline"/>


# 性能测试的函数
# 分别通过快速低延迟网络和慢速高延迟网络来访问同一台机器。
# 分别测试以上两个函数每秒执行了多少次
# 高延迟网络使用流水线时的速度要比不使用流水线时的速度快5倍，
# 低延迟网络使用流水线也可以带来接近4倍的速度提升，
# 而本地网络的测试结果实际上已经达到了单核环境下使用Redis协议发送和接收短命令序列的性能极限
# 代码清单 4-9
# 具体的非事务型流水线
# <start id="simple-pipeline-benchmark-code"/>
def benchmark_update_token(conn, duration):
    # 测试会分别执行update_token()函数和update_token_pipeline()函数。
    for function in (update_token, update_token_pipeline):
        # 设置计数器以及测试结束的条件。
        count = 0                                               #B
        start = time.time()                                     #B
        end = start + duration                                  #B
        while time.time() < end:
            count += 1
            # 调用两个函数的其中一个。
            function(conn, 'token', 'user', 'item')             #C
        # 计算函数的执行时长。
        delta = time.time() - start                             #D
        # 打印测试结果。
        print function.__name__, count, delta, count / delta    #E
# <end id="simple-pipeline-benchmark-code"/>

# 要对Redis的性能进行优化，用户首先需要弄清楚各种类型的Redis命令到底能跑多快，而这一点可以通过调用
# Redis附带的性能测试程序redis-benchmark来得知。
# 运行结果展示了一些常用Redis命令在1秒内可以执行的次数。如果用户在不给定任何参数的情况下运行redis-benchmark，
# 那么redis-benchmark将使用50个客户端来进行性能测试
# 如果你发现自己客户端的性能只有redis-benchmark所示性能的25%至30%，或者客户端向你返回了"Cannot assign requested address"错误，
# 那么你可能是不小心在每次发送命令时都创建了新的连接。
#
# 比较Redis在通常情况下的性能表现以及redis-benchmark使用单客户端进行测试时的结果，并说明了一些可能引起性能问题的原因
#   性能或者错误                                    可能的原因                                   解决方法
# 单个客户端的性能达到redis-benchmark的        这是不使用流水线时的预期性能                        无
# 50%~60%
# 单个客户端的性能达到redis-benchmark的        对于每个命令或者每组命令都创建了新的连接             重用已有的Redis连接
# 25%~30%
# 客户端返回错误："Cannot assign requested address"  对于每个命令或者每组命令都创建了新的连接       重用已有的Redis连接
#
# 大部分Redis客户端库都提供了某种级别的内置连接池（connection pool）。
# 对于每个Redis服务器，用户只需要创建一个redis.Redis()对象，该对象就会按需创建连接、
# 重用已有的连接并关闭超时的连接（在使用多个数据库的情况下），
# 即使客户端只连接了一个Redis服务器，它也需要为每一个被使用的数据库创建一个连接
# 客户端的连接池还可以安全地应用于多线程环境和多进程环境。
#
# 关于性能方面的注意事项
# 代码清单 4-10
'''
# <start id="redis-benchmark"/>
$ redis-benchmark  -c 1 -q                               # 给定“-q”选项可以让程序简化输出结果，
PING (inline): 34246.57 requests per second              # 给定“-c 1”选项让程序只使用一个客户端来进行测试。
PING: 34843.21 requests per second
MSET (10 keys): 24213.08 requests per second
SET: 32467.53 requests per second
GET: 33112.59 requests per second
INCR: 32679.74 requests per second
LPUSH: 33333.33 requests per second
LPOP: 33670.04 requests per second
SADD: 33222.59 requests per second
SPOP: 34482.76 requests per second
LPUSH (again, in order to bench LRANGE): 33222.59 requests per second
LRANGE (first 100 elements): 22988.51 requests per second
LRANGE (first 300 elements): 13888.89 requests per second
LRANGE (first 450 elements): 11061.95 requests per second
LRANGE (first 600 elements): 9041.59 requests per second
# <end id="redis-benchmark"/>
#A We run with the '-q' option to get simple output, and '-c 1' to use a single client
#END
'''

#--------------- 以下是用于测试代码的辅助函数 --------------------------------

class TestCh04(unittest.TestCase):
    def setUp(self):
        import redis
        self.conn = redis.Redis(db=15)
        self.conn.flushdb()

    def tearDown(self):
        self.conn.flushdb()
        del self.conn
        print
        print

    # We can't test process_logs, as that would require writing to disk, which
    # we don't want to do.

    # We also can't test wait_for_sync, as we can't guarantee that there are
    # multiple Redis servers running with the proper configuration

    def test_list_item(self):
        import pprint
        conn = self.conn

        print "We need to set up just enough state so that a user can list an item"
        seller = 'userX'
        item = 'itemX'
        conn.sadd('inventory:' + seller, item)
        i = conn.smembers('inventory:' + seller)
        print "The user's inventory has:", i
        self.assertTrue(i)
        print

        print "Listing the item..."
        l = list_item(conn, item, seller, 10)
        print "Listing the item succeeded?", l
        self.assertTrue(l)
        r = conn.zrange('market:', 0, -1, withscores=True)
        print "The market contains:"
        pprint.pprint(r)
        self.assertTrue(r)
        self.assertTrue(any(x[0] == 'itemX.userX' for x in r))

    def test_purchase_item(self):
        self.test_list_item()
        conn = self.conn

        print "We need to set up just enough state so a user can buy an item"
        buyer = 'userY'
        conn.hset('users:userY', 'funds', 125)
        r = conn.hgetall('users:userY')
        print "The user has some money:", r
        self.assertTrue(r)
        self.assertTrue(r.get('funds'))
        print

        print "Let's purchase an item"
        p = purchase_item(conn, 'userY', 'itemX', 'userX', 10)
        print "Purchasing an item succeeded?", p
        self.assertTrue(p)
        r = conn.hgetall('users:userY')
        print "Their money is now:", r
        self.assertTrue(r)
        i = conn.smembers('inventory:' + buyer)
        print "Their inventory is now:", i
        self.assertTrue(i)
        self.assertTrue('itemX' in i)
        self.assertEquals(conn.zscore('market:', 'itemX.userX'), None)

    def test_benchmark_update_token(self):
        benchmark_update_token(self.conn, 5)

if __name__ == '__main__':
    unittest.main()
