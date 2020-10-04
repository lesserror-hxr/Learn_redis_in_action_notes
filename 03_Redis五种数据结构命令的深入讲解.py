# coding: utf-8

import threading
import time
import unittest

import redis

ONE_WEEK_IN_SECONDS = 7 * 86400
VOTE_SCORE = 432
ARTICLES_PER_PAGE = 25

# 如需要完整的命令文档作为参考，那么可以访问https://redis.io/commands

# Redis 2.6 版本以上支持lua脚本

# 以下包括数据操作命令和配置命令

# 字符串
# 代码清单 3-1
# Redis里面，字符串可以存储以下3种类型的值
# 字节串（byte string）
# 整数
# 浮点数
# INCRBYFLOAT INCRBYFLOAT key-name amount --将键存储的值加上浮点数amount，这个命令在Redis2.6或者以上的版本可用。
# 如果用户对一个不存在的键或者一个保存了空串的键执行自增或者自减操作，那么Redis在执行操作时会将这个键的值当作是0来处理。

'''
# <start id="string-calls-1"/>
>>> conn = redis.Redis()
>>> conn.get('key')             # 尝试获取一个不存在的键将得到一个None值，终端不会显示这个值。
>>> conn.incr('key')            # 我们既可以对不存在的键执行自增操作，
1                               # 也可以通过可选的参数来指定自增操作的增量。
>>> conn.incr('key', 15)        # Python的Redis库在内部使用INCRBY命令来实现incr()方法
16                              #
>>> conn.decr('key', 5)         # 和自增操作一样，
11                              # 执行自减操作的函数也可以通过可选的参数来指定减量。
>>> conn.get('key')             # 在尝试获取一个键的时候，命令以字符串格式返回被存储的整数。
'11'                            #
>>> conn.set('key', '13')       # 即使在设置键时输入的值为字符串，
True                            # 但只要这个值可以被解释为整数，
>>> conn.incr('key')            # 我们就可以把它当作整数来处理。
14                              # 在尝试获取一个键的时候，命令将以字符串格式返回被存储的整数。
# <end id="string-calls-1"/>
'''


# 代码清单 3-2 子串操作和二进制位操作
# GETBIT key-name offset -- 将字节串看作是二进制位串（bit string）,并返回位串中偏移量为offset的二进制位的值。
# BITCOUNT key-name [start end] -- 统计二进制位串里面值为1的二进制位的数量，如果给定了可选的start偏移量和
# end偏移量，那么只对偏移量指定范围内的二进制位进行统计
# BITOP operation dest-key key-name [key-name...] -- 对一个或多个二进制位串执行包括并(AND)、或(OR)、异或(XOR)
# 非(NOT)在内的任意一种按位运算操作(bitwise operation)，并将计算得出的结果保存在dest-key键里面
# 在使用SETRANGE或者SETBIT命令对字符串进行写入的时候，如果字符串当前的长度不能满足写入的要求，
# 那么Redis会自动地使用空字节（null）来将字符串扩展至所需的长度，然后才执行写入或者更新操作。
# 在使用GETRANGE读取字符串的时候，超出字符串末尾的数据会被视为是空串，而在使用GETBIT读取二进制位串的时候，
# 超出字符串末尾的二进制位会被视为是0
# 关于二进制位的文章：https://blog.csdn.net/HouXinLin_CSDN/article/details/108506560

'''
# <start id="string-calls-2"/>
>>> conn.append('new-string-key', 'hello ')     # 将字符串'hello'追加到目前并不存在的'new-string-key'键里。
6L                                              # APPEND命令在执行之后会返回字符串当前的长度。
>>> conn.append('new-string-key', 'world!')
12L                                             #
>>> conn.substr('new-string-key', 3, 7)         # Redis的索引以0为开始，在进行范围访问时，范围的终点（endpoint）默认也包含在这个范围之内。
'lo wo'                                         # 字符串'lo wo'位于字符串'hello world!'的中间。
>>> conn.setrange('new-string-key', 0, 'H')     # 对字符串执行范围设置操作。
12                                              # SETRANGE命令在执行之后同样会返回字符串的当前总长度。
>>> conn.setrange('new-string-key', 6, 'W')
12
>>> conn.get('new-string-key')                  # 查看字符串的当前值。
'Hello World!'                                  # 前面执行的两个SETRANGE命令成功地将字母h和w从原来的小写改成了大写。
>>> conn.setrange('new-string-key', 11, ', how are you?')   # SETRANGE命令既可以用于替换字符串里已有的内容，又可以用于增长字符串。
25
>>> conn.get('new-string-key')
'Hello World, how are you?'                     # 前面执行的SETRANGE命令移除了字符串末尾的感叹号，并将更多字符追加到了字符串末尾。
>>> conn.setbit('another-key', 2, 1)            # 对超出字符串长度的二进制位进行设置时，超出的部分会被填充为空字节。
0                                               # SETBIT命令会返回二进制位被设置之前的值。
>>> conn.setbit('another-key', 7, 1)            # 在对Redis存储的二进制位进行解释（interpret）时，
0                                               # 请记住Redis存储的二进制位是按照偏移量从高到低排列的。
>>> conn.get('another-key')                     #
'!'                                             # 通过将第2个二进制位以及第7个二进制位的值设置为1，键的值将变为‘!’，即字符33 。
# <end id="string-calls-2"/>
'''

# 列表操作
# 代码清单 3-3
'''
# <start id="list-calls-1"/>
>>> conn.rpush('list-key', 'last')          # 在向列表推入元素时，
1L                                          # 推入操作执行完毕之后会返回列表当前的长度。
>>> conn.lpush('list-key', 'first')         # 可以很容易地对列表的两端执行推入操作。
2L
>>> conn.rpush('list-key', 'new last')
3L
>>> conn.lrange('list-key', 0, -1)          # 从语义上来说，列表的左端为开头，右端为结尾。
['first', 'last', 'new last']               #
>>> conn.lpop('list-key')                   # 通过重复地弹出列表左端的元素，
'first'                                     # 可以按照从左到右的顺序来获取列表中的元素。
>>> conn.lpop('list-key')                   #
'last'                                      #
>>> conn.lrange('list-key', 0, -1)
['new last']
>>> conn.rpush('list-key', 'a', 'b', 'c')   # 可以同时推入多个元素。
4L
>>> conn.lrange('list-key', 0, -1)
['new last', 'a', 'b', 'c']
>>> conn.ltrim('list-key', 2, -1)           # 可以从列表的左端、右端或者左右两端删减任意数量的元素。
True                                        #
>>> conn.lrange('list-key', 0, -1)          #
['b', 'c']                                  #
# <end id="list-calls-1"/>
'''


# 代码清单 3-4
# 在Redis里面，多个命令原子地执行指的是，在这些命令正在读取或者修改数据的时候，
# 其他客户端不能读取或者修改相同的数据
# 阻塞式的列表弹出命令以及在列表之间移动元素的命令
# BLPOP key-name [key-name ...] timeout 从第一个非空列表中弹出位于最左端
# 的元素，或者在timeout秒之内阻塞并等待可弹出的元素出现
# BRPOP key-name [key-name ...] timeout 从第一个非空列表中弹出位于最右端
# 的元素，或者在timeout秒之内阻塞并等待可弹出的元素出现
# RPOPLPUSH source-key dest-key -- 从source-key 列表中弹出位于最右端的元素，然后
# 将这个元素推入dest-key列表的最左端，并向用户返回这个元素。
# BRPOPLPUSH source-key dest-key timeout -- 从source-key列表中弹出位于最右端的元素，
# 然后将这个元素推入dest-key列表的最左端，并向用户返回这个元素；如果source-key为空，
# 那么在timeout秒之内阻塞并等待可弹出的元素出现
# 对于阻塞弹出命令和弹出并并推入命令，最常见的用例就是消息传递（messaging）和任务队列（task queue）

'''
# <start id="list-calls-2"/>
>>> conn.rpush('list', 'item1')             # 将一些元素添加到两个列表里面。
1                                           #
>>> conn.rpush('list', 'item2')             #
2                                           #
>>> conn.rpush('list2', 'item3')            #
1                                           #
>>> conn.brpoplpush('list2', 'list', 1)     # 将一个元素从一个列表移动到另一个列表，
'item3'                                     # 并返回被移动的元素。
>>> conn.brpoplpush('list2', 'list', 1)     # 当列表不包含任何元素时，阻塞弹出操作会在给定的时限内等待可弹出的元素出现，并在时限到达后返回None（交互终端不会打印这个值）。
>>> conn.lrange('list', 0, -1)              # 弹出“list2”最右端的元素，
['item3', 'item1', 'item2']                 # 并将弹出的元素推入到“list”的左端。
>>> conn.brpoplpush('list', 'list2', 1)
'item2'
>>> conn.blpop(['list', 'list2'], 1)        # BLPOP命令会从左到右地检查传入的列表，
('list', 'item3')                           # 并对最先遇到的非空列表执行弹出操作。
>>> conn.blpop(['list', 'list2'], 1)        #
('list', 'item1')                           #
>>> conn.blpop(['list', 'list2'], 1)        #
('list2', 'item2')                          #
>>> conn.blpop(['list', 'list2'], 1)        #
>>>
# <end id="list-calls-2"/>
'''

# <start id="exercise-update-token"/>
def update_token(conn, token, user, item=None):
    timestamp = time.time()
    conn.hset('login:', token, user)
    conn.zadd('recent:', token, timestamp)
    if item:
        key = 'viewed:' + token
        # 如果指定的元素存在于列表当中，那么移除它
        conn.lrem(key, item)
        # 将元素推入到列表的右端，使得 ZRANGE 和 LRANGE 可以取得相同的结果
        conn.rpush(key, item)
        # 对列表进行修剪，让它最多只能保存 25 个元素
        conn.ltrim(key, -25, -1)
    conn.zincrby('viewed:', item, -1)
# <end id="exercise-update-token"/>


# 集合
# scard key-name -- 返回集合包含的元素的数量
# srandmember key-name [count] -- 从集合里面随机地返回一个或多个元素。当count为正数时，命令返回的随机元素不会重复；
# 当count为负数时，命令返回的随机元素可能会出现重复
# SPOP key-name [count] --随机地移除集合中的一个或多个元素，并返回被移除的元素
# SMOVE source-key dest-key item -- 如果集合source-key包含元素item；那么从集合source-key里面移除元素item，并将
# 元素item添加到集合dest-key中；如果item被成功移除，那么命令返回1，否则返回0
# 代码清单 3-5
'''
# <start id="set-calls-1"/>
>>> conn.sadd('set-key', 'a', 'b', 'c')         # SADD命令会将那些目前并不存在于集合里面的元素添加到集合里面，
3                                               # 并返回被添加元素的数量。
>>> conn.srem('set-key', 'c', 'd')              # srem函数在元素被成功移除时返回True，
True                                            # 移除失败时返回False；
>>> conn.srem('set-key', 'c', 'd')              # 注意这是Python客户端的一个bug，
False                                           # 实际上Redis的SREM命令返回的是被移除元素的数量，而不是布尔值。
>>> conn.scard('set-key')                       # 查看集合包含的元素数量。
2                                               #
>>> conn.smembers('set-key')                    # 获取集合包含的所有元素。
set(['a', 'b'])                                 #
>>> conn.smove('set-key', 'set-key2', 'a')      # 可以很容易地将元素从一个集合移动到另一个集合。
True                                            #
>>> conn.smove('set-key', 'set-key2', 'c')      # 在执行SMOVE命令时，
False                                           # 如果用户想要移动的元素不存在于第一个集合里，
>>> conn.smembers('set-key2')                   # 那么移动操作就不会执行。
set(['a'])                                      #
# <end id="set-calls-1"/>
'''


# 代码清单 3-6
# SDIFFSTORE dest-key key-name [key-name ...] -- 将那些存在于第一个集合但并不存在于其他集合中的
# 元素（数学上的差集运算）存储到dest-key键里面
# SINTER key-name [key-name ...] -- 返回那些同时存在于所有集合中的元素（数学上的交集运算）
# SINTERSTORE dest-key key-name [key-name ...] -- 将那些同时存在于所有集合的元素（数学上的交集运算）存储到dest-key键里面
# SUNION key-name [key-name ...] -- 返回那些至少存在于一个集合中的元素（数学上的并集计算）
# SUNIONSTORE dest-key key-name [key-name ...] -- 将那些同时存在于一个集合中的元素（数学上的并集运算）存储到dest-key键里面
'''
# <start id="set-calls-2"/>
>>> conn.sadd('skey1', 'a', 'b', 'c', 'd')  # 首先将一些元素添加到两个集合里面。
4                                           #
>>> conn.sadd('skey2', 'c', 'd', 'e', 'f')  #
4                                           #
>>> conn.sdiff('skey1', 'skey2')            # 计算从第一个集合中移除第二个集合所有元素之后的结果。
set(['a', 'b'])                             #
>>> conn.sinter('skey1', 'skey2')           # 还可以找出同时存在于两个集合中的元素。
set(['c', 'd'])                             #
>>> conn.sunion('skey1', 'skey2')           # 可以找出两个结合中的所有元素。
set(['a', 'c', 'b', 'e', 'd', 'f'])         #
# <end id="set-calls-2"/>
'''

# 散列
# HMGET key-name key [key ...] -- 从散列里面获取一个或多个键的值
# HMSET key-name key value [key value ...] -- 为散列里面的一个或多个键设置值
# HDEL  key-name key [key ...] -- 删除散列里面的一个或多个键值对，返回成功找到并删除的键值对数量
# HLEN  key-name -- 返回散列包含的键值对数量  通常用于调试一个包含非常多键值对的散列
# 代码清单 3-7
'''
# <start id="hash-calls-1"/>
>>> conn.hmset('hash-key', {'k1':'v1', 'k2':'v2', 'k3':'v3'})   # 使用HMSET命令可以一次将多个键值对添加到散列里面。
True                                                            #
>>> conn.hmget('hash-key', ['k2', 'k3'])                        #  使用HMGET命令可以一次获取多个键的值。
['v2', 'v3']                                                    #
>>> conn.hlen('hash-key')                                       # HLEN命令通常用于调试一个包含非常多键值对的散列。
3                                                               #
>>> conn.hdel('hash-key', 'k1', 'k3')                           # HDEL命令在成功地移除了至少一个键值对时返回True，
True                                                            # 因为HDEL命令已经可以同时删除多个键值对了，所以Redis没有实现HMDEL命令。
# <end id="hash-calls-1"/>
'''

# HEXISTS key-name key -- 检查给定键是否存在于散列中
# HKEYS   key-name -- 获取散列包含的所有键
# HVALS   key-name -- 获取散列包含的所有值
# HGETALL key-name --获取散列包含的所有键值对
# HINCRBY key-name key increment -- 将键key存储的值加上整数increment
# HINCRBYFLOAT key-name key increment -- 将键key存储的值加上浮点数increment
# 尽管有HGETALL存在，但HKEYS和HVALS也是非常有用的：如果散列包含的值非常大，那么
# 用户可以先使用HKEYS取出散列包含的所有键，然后再使用HGET一个接一个地取出键的值，
# 从而避免因为一次获取多个大体积的值而导致服务器阻塞
# 代码清单 3-8
'''
# <start id="hash-calls-2"/>
>>> conn.hmset('hash-key2', {'short':'hello', 'long':1000*'1'}) # 在考察散列的时候，我们可以只取出散列包含的键，而不必传输大的键值。
True                                                            #
>>> conn.hkeys('hash-key2')                                     #
['long', 'short']                                               #
>>> conn.hexists('hash-key2', 'num')                            # 检查给定的键是否存在于散列中。
False                                                           #
>>> conn.hincrby('hash-key2', 'num')                            # 和字符串一样，
1L                                                              # 对散列中一个尚未存在的键执行自增操作时，
>>> conn.hexists('hash-key2', 'num')                            # Redis会将键的值当作0来处理。
True                                                            #
# <end id="hash-calls-2"/>
'''

# 有序集合
# 和散列存储着键与值之间的映射类似，有序集合也存储着成员与分值之间的映射，并且提供了
# 分值处理命令，这些分值在Redis中以IEEE 754双精度浮点数的格式存储，以及根据分值大小有序地获取
# （fetch） 或扫描（scan）成员和分值的命令。
# ZADD key-name score member [score member ...] -- 将带有给定分值的成员添加到有序集合里面
# ZREM key-name member [member ...] -- 从有序集合里面移除给定的成员，并返回被移除成员的数量
# ZCARD key-name -- 返回有序集合包含的成员数量
# ZINCRBY key-name increment member -- 将member成员的分值加上increment
# ZCOUNT  key-name min max -- 返回分值介于min和max之间的成员数量
# ZRANK   key-name member -- 返回成员member在有序集合中的排名
# ZSCORE  key-name member -- 返回成员member的分值
# ZRANGE  key-name start stop [WITHSCORES] -- 返回有序集合中排名介于start和stop
# 之间的成员，如果给定了可选的WITHSCORES选项，那么命令会将成员的分值也一并返回
# 代码清单 3-9
'''
# <start id="zset-calls-1"/>
>>> conn.zadd('zset-key', 'a', 3, 'b', 2, 'c', 1)   # 在Python客户端执行ZADD命令需要先输入成员、后输入分值，
3                                                   # 这跟Redis标准的先输入分值、后输入成员的做法正好相反。
>>> conn.zcard('zset-key')                          # 取得有序集合的大小可以让我们在某些情况下知道是否需要对有序集合进行修剪。
3                                                   #
>>> conn.zincrby('zset-key', 'c', 3)                # 跟字符串和散列一样，
4.0                                                 # 有序集合的成员也可以执行自增操作。
>>> conn.zscore('zset-key', 'b')                    # 获取单个成员的分值对于实现计数器或者排行榜之类的功能非常有用。
2.0                                                 #
>>> conn.zrank('zset-key', 'c')                     # 获取指定成员的排名（排名以0为开始），
2                                                   # 之后可以根据这个排名来决定ZRANGE的访问范围。
>>> conn.zcount('zset-key', 0, 3)                   # 对于某些任务来说，
2L                                                  # 统计给定分值范围内的元素数量非常有用。
>>> conn.zrem('zset-key', 'b')                      # 从有序集合里面移除成员和添加成员一样容易。
True                                                #
>>> conn.zrange('zset-key', 0, -1, withscores=True) # 在进行调试时，我们通常会使用ZRANGE取出有序集合里包含的所有元素，
[('a', 3.0), ('c', 4.0)]                            # 但是在实际用例中，通常一次只会取出一小部分元素。
# <end id="zset-calls-1"/>
'''


# 代码清单 3-10
# ZREVRANK key-name member -- 返回有序集合里成员member的排名，成员按照分值从大到小排序
# ZREVRANGE key-name start stop [WITHSCORES] -- 返回有序集合给定排名范围内的成员，成员按照分值从大到小排列
# ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count] -- 返回有序集合中，分值介于min和max之间的所有成员
# ZREVRANGEBYSCORE key max min [WITHSCORES] [LIMIT offset count] -- 返回有序集合中，分值介于min和max之间的所有成员，并按照
# 分值从大到小的顺序来返回它们
# ZREMRANGEBYRANK key-name start stop -- 移除有序集合中排名介于start和stop之间的所有成员
# ZREMRANGEBYSCORE key-name min max -- 移除有序集合中分值介于min和max之间的所有成员
# ZINTERSTORE dest-key key-count key [key ...] [WEIGHTS weight [weight ...]] https://www.runoob.com/redis/sorted-sets-zinterstore.html
# [AGGREGATE SUM|MIN|MAX] -- 对给定的有序集合执行类似于集合的交集运算
# ZUNIONSTORE dest-key key-count key [key ...] [WEIGHTS weight [weight ...]]
# [AGGREGATE SUM|MIN|MAX] -- 对给定的有序集合执行类似于集合的并集运算

'''
# <start id="zset-calls-2"/>
>>> conn.zadd('zset-1', 'a', 1, 'b', 2, 'c', 3)                         # 首先创建两个有序集合。
3                                                                       #
>>> conn.zadd('zset-2', 'b', 4, 'c', 1, 'd', 0)                         #
3                                                                       #
>>> conn.zinterstore('zset-i', ['zset-1', 'zset-2'])                    # 因为ZINTERSTORE和ZUNIONSTORE默认使用的聚合函数为sum，
2L                                                                      # 所以多个有序集合里成员的分值将被加起来。
>>> conn.zrange('zset-i', 0, -1, withscores=True)                       #
[('c', 4.0), ('b', 6.0)]                                                #
>>> conn.zunionstore('zset-u', ['zset-1', 'zset-2'], aggregate='min')   # 用户可以在执行并集运算和交集运算的时候传入不同的聚合函数，
4L                                                                      # 共有 sum、min、max 三个聚合函数可选。
>>> conn.zrange('zset-u', 0, -1, withscores=True)                       #
[('d', 0.0), ('a', 1.0), ('c', 1.0), ('b', 2.0)]                        #
>>> conn.sadd('set-1', 'a', 'd')                                        # 用户还可以把集合作为输入传给ZINTERSTORE和ZUNIONSTORE，
2                                                                       # 命令会将集合看作是成员分值全为1的有序集合来处理。
>>> conn.zunionstore('zset-u2', ['zset-1', 'zset-2', 'set-1'])          #
4L                                                                      #
>>> conn.zrange('zset-u2', 0, -1, withscores=True)                      #
[('d', 1.0), ('a', 2.0), ('c', 4.0), ('b', 6.0)]                        #
# <end id="zset-calls-2"/>
'''

# 发布与订阅（publish/subscribe）
# SUBSCRIBE channel [channel ...] -- 订阅给定的一个或多个频道
# UNSUBSCRIBE channel [channel ...] -- 退订给定的一个或多个频道，如果执行时
# 没有给定任何频道，那么退订所有频道
# PUBLISH channel message -- 向给定频道发送消息
# PSUBSCRIBE pattern [pattern ...] -- 订阅与给定模式相匹配的所有频道
# PUNSUBSCRIBE [pattern [pattern ...]] -- 退订给定的模式，如果执行时没有给定任何模式，
# 那么退订所有模式

def publisher(n):
    time.sleep(1)
    for i in xrange(n):
        conn.publish('channel', i)
        time.sleep(1)

def run_pubsub():
    threading.Thread(target=publisher, args=(3,)).start()
    pubsub = conn.pubsub()
    pubsub.subscribe(['channel'])
    count = 0
    for item in pubsub.listen():
        print item
        count += 1
        if count == 4:
            pubsub.unsubscribe()
        if count == 5:
            break


# 代码清单 3-11
# Redis的发布与订阅模式有以下两个缺点
# 第一个原因和Redis系统的稳定性有关。对于旧版的Redis来说，如果一个客户端订阅了某个或某些频道，但它读取消息的速度却不够快的话，
# 那么不断积压的消息就会使得Redis输出缓冲区的体积变得越来越大，这可能会导致Redis的速度变慢，甚至直接崩溃。也可能会导致Redis被操作
# 系统强制杀死，甚至操作系统本身不可用。新版的Redis不会出现这种问题，因为它会自动断开不符合client-output-buffer-limit pubsub配置
# 选项要求的订阅客户端。
# 第二个原因和数据传输的可靠性有关。任何网络系统在执行操作时都可能会遇上断线情况，而断线产生的连接错误通常会使得网络连接两端中的其中的一端
# 进行重新连接。但是，如果客户端在执行订阅操作的过程中断线，那么客户端将丢失在断线期间发送的所有消息，因此依靠频道来接受消息的用户可能会对Redis
# 提供的PUBLISH命令和SUBSCRIBE命令的语义感到失望。
'''
# <start id="pubsub-calls-1"/>
>>> def publisher(n):
...     time.sleep(1)                                                   # 函数在刚开始执行时会先休眠，让订阅者有足够的时间来连接服务器并监听消息。
...     for i in xrange(n):
...         conn.publish('channel', i)                                  # 在发布消息之后进行短暂的休眠，
...         time.sleep(1)                                               # 让消息可以一条接一条地出现。
...
>>> def run_pubsub():
...     threading.Thread(target=publisher, args=(3,)).start()
...     pubsub = conn.pubsub()
...     pubsub.subscribe(['channel'])
...     count = 0
...     for item in pubsub.listen():
...         print item
...         count += 1
...         if count == 4:
...             pubsub.unsubscribe()
...         if count == 5:
...             break
...
>>> def run_pubsub():
...     threading.Thread(target=publisher, args=(3,)).start()           # 启动发送者线程发送三条消息。
...     pubsub = conn.pubsub()                                          # 创建发布与订阅对象，并让它订阅给定的频道。
...     pubsub.subscribe(['channel'])                                   #
...     count = 0
...     for item in pubsub.listen():                                    # 通过遍历pubsub.listen()函数的执行结果来监听订阅消息。
...         print item                                                  # 打印接收到的每条消息。
...         count += 1                                                  # 在接收到一条订阅反馈消息和三条发布者发送的消息之后，
...         if count == 4:                                              # 执行退订操作，停止监听新消息。
...             pubsub.unsubscribe()                                    #
...         if count == 5:                                              # 当客户端接收到退订反馈消息时，
...             break                                                   # 需要停止接收消息。
...
>>> run_pubsub()                                                        # 实际运行函数并观察它们的行为。
{'pattern': None, 'type': 'subscribe', 'channel': 'channel', 'data': 1L}# 在刚开始订阅一个频道的时候，客户端会接收到一条关于被订阅频道的反馈消息。
{'pattern': None, 'type': 'message', 'channel': 'channel', 'data': '0'} # 这些结构就是我们在遍历pubsub.listen()函数时得到的元素。
{'pattern': None, 'type': 'message', 'channel': 'channel', 'data': '1'} #
{'pattern': None, 'type': 'message', 'channel': 'channel', 'data': '2'} #
{'pattern': None, 'type': 'unsubscribe', 'channel': 'channel', 'data':  # 在退订频道时，客户端会接收到一条反馈消息，
0L}                                                                     # 告知被退订的是哪个频道，以及客户端目前仍在订阅的频道数量。
# <end id="pubsub-calls-1"/>
'''


# 代码清单 3-12
# SORT source-key [BY pattern] [LIMIT offset count] [GET pattern [GET pattern ...]] [ASC|DESC] [ALPHA] [STORE dest-key] --
# 根据给定的选项，对输入列表、集合或者有序集合进行排序，然后返回或者存储排序的结果
# 负责执行排序操作的SORT命令可以根据字符串、列表、集合、有序集合、散列这5种键里面存储着的数据，对列表、集合以及有序集合进行排序。
# 使用SORT命令提供的选项可以实现以下功能：根据降序而不是默认的升序来排序元素；将元素看作是数字来进行排序，或者将元素看作是二进制字符串
# 来进行排序（比如排序字符串'110'和'12'的结果就跟排序数字110和12的结果不一样）；使用被排序元素之外的其他值作为权重来进行排序，甚至还可以
# 从输入的列表、集合、有序集合以外的其他地方进行取值。
# SORT命令不仅可以对列表进行排序，还可以对集合进行排序，然后返回一个列表形式的排序结果。
# 组合使用集合操作和SORT命令：当集合结构计算交集、并集和差集的能力，与SORT命令获取散列存储的外部数据的能力相结合时，SORT命令将变得非常强大
# 尽管SORT是Redis中唯一一个可以同时处理3种不同类型的数据的命令，但基本的Redis事务同样可以让我们在一连串不间断执行的命令里面操作多种不同类
# 型的数据
'''
# <start id="sort-calls"/>
>>> conn.rpush('sort-input', 23, 15, 110, 7)                    # 首先将一些元素添加到列表里面。
4                                                               #
>>> conn.sort('sort-input')                                     # 根据数字大小对元素进行排序。
['7', '15', '23', '110']                                        #
>>> conn.sort('sort-input', alpha=True)                         # 根据字母表顺序对元素进行排序。
['110', '15', '23', '7']                                        #
>>> conn.hset('d-7', 'field', 5)                                # 添加一些用于执行排序操作和获取操作的附加数据。
1L                                                              #
>>> conn.hset('d-15', 'field', 1)                               #
1L                                                              #
>>> conn.hset('d-23', 'field', 9)                               #
1L                                                              #
>>> conn.hset('d-110', 'field', 3)                              #
1L                                                              #
>>> conn.sort('sort-input', by='d-*->field')                    # 将散列的域（field）用作权重，对sort-input列表进行排序。
['15', '110', '7', '23']                                        #
>>> conn.sort('sort-input', by='d-*->field', get='d-*->field')  # 获取外部数据作为返回值，而不返回被排序的元素。
['1', '3', '5', '9']                                            #
# <end id="sort-calls"/>
'''


# 基本的Redis事务
# 尽管Redis有几个可以在两个键之间复制或者移动元素的命令，但却没有那种可以在两个不同类型之间移动元素的命令
# 虽然可以使用ZUNIONSROTE命令将元素从一个集合复制到一个有序集合。为了对相同或者不同类型的多个键执行操作，Redis
# 有5个命令可以让用户在不断打断（interruption）的情况下对多个键执行操作，它们分别是watch、multi、exec、unwatch、discard。
# Redis的基本事务（basic transaction）需要用到MULTI命令和EXEC命令，这种事务可以让一个客户端在不被其他客户端打断的情况下执行多个命令。
# 和关系数据库那种可以在执行的过程中进行回滚（rollback）的事务不同，在Redis里面，被MULTI命令和EXEC命令包围的所有命令会一个接一个地执行，
# 直到所有命令都执行完毕为止。当一个事务执行完毕之后，Redis才会处理其他客户端的命令。
# 当Redis从一个客户端那里接收到MULTI命令时，Redis会将这个客户端之后发送的所有命令都放入到一个队列里面，直到这个客户端发送EXEC
# 命令为止，然后Redis就会在不被打断的情况下，一个接一个地执行存储在队列里面的命令。
# 代码清单 3-13
# 在并行执行命令时，缺少事务可能会引发的问题
'''
# <start id="simple-pipeline-notrans"/>
>>> def notrans():
...     print conn.incr('notrans:')                     # 对‘notrans:’计数器执行自增操作并打印操作的执行结果。
...     time.sleep(.1)                                  # 等待100毫秒。
...     conn.incr('notrans:', -1)                       # 对‘notrans:’计数器执行自减操作。
...
>>> if 1:
...     for i in xrange(3):                             # 启动三个线程来执行没有被事务包裹的自增、休眠和自减操作。
...         threading.Thread(target=notrans).start()    #
...     time.sleep(.5)                                  # 等待500毫秒，让操作有足够的时间完成。
...
1                                                       # 因为没有使用事务，
2                                                       # 所以三个线程执行的各个命令会互相交错，
3                                                       # 使得计数器的值持续地增大。
# <end id="simple-pipeline-notrans"/>
'''


# 代码清单 3-14
# 使用事务来处理命令的并行执行问题
'''
# <start id="simple-pipeline-trans"/>
>>> def trans():
...     pipeline = conn.pipeline()                      # 创建一个事务型（transactional）流水线对象。
...     pipeline.incr('trans:')                         # 把针对‘trans:’计数器的自增操作放入队列。
...     time.sleep(.1)                                  # 等待100毫秒。
...     pipeline.incr('trans:', -1)                     # 把针对‘trans:’计数器的自减操作放入队列。
...     print pipeline.execute()[0]                     # 执行事务包含的命令并打印自增操作的执行结果。
...
>>> if 1:
...     for i in xrange(3):                             # 启动三个线程来执行被事务包裹的自增、休眠和自减三个操作。
...         threading.Thread(target=trans).start()      #
...     time.sleep(.5)                                  # 等待500毫秒，让操作有足够的时间完成。
...
1                                                       # 因为每组自增、休眠和自减操作都在事务里面执行，
1                                                       # 所以命令之间不会互相交错，
1                                                       # 因此所有事务的执行结果都是1。
# <end id="simple-pipeline-trans"/>
'''

# 第1章展示的article_vote()函数包含一个竞争条件以及一个因为竞争条件而出现的bug。
# MULTI和EXEC事务的一个主要作用是移除竞争条件
# <start id="exercise-fix-article-vote"/>
def article_vote(conn, user, article):
    # 在进行投票之前，先检查这篇文章是否仍然处于可投票的时间之内
    cutoff = time.time() - ONE_WEEK_IN_SECONDS
    posted = conn.zscore('time:', article)
    if posted < cutoff:
        return

    article_id = article.partition(':')[-1]
    pipeline = conn.pipeline()
    pipeline.sadd('voted:' + article_id, user)
    # 为文章的投票设置过期时间
    pipeline.expire('voted:' + article_id, int(posted-cutoff))
    if pipeline.execute()[0]:
        # 因为客户端可能会在执行 SADD/EXPIRE 之间或者执行 ZINCRBY/HINCRBY 之间掉线
        # 所以投票可能会不被计数，但这总比在执行 ZINCRBY/HINCRBY 之间失败并导致不完整的计数要好
        pipeline.zincrby('score:', article, VOTE_SCORE)
        pipeline.hincrby(article, 'votes', 1)
        pipeline.execute()
# <end id="exercise-fix-article-vote"/>

# 从技术上来将，上面的 article_vote() 函数仍然有一些问题，
# 这些问题可以通过下面展示的这段代码来解决，
# 这段代码里面用到了本书第 4 章才会介绍的技术

def article_vote(conn, user, article):
    cutoff = time.time() - ONE_WEEK_IN_SECONDS
    posted = conn.zscore('time:', article)
    article_id = article.partition(':')[-1]
    voted = 'voted:' + article_id

    pipeline = conn.pipeline()
    while posted > cutoff:
        try:
            pipeline.watch(voted)
            if not pipeline.sismember(voted, user):
                pipeline.multi()
                pipeline.sadd(voted, user)
                pipeline.expire(voted, int(posted-cutoff))
                pipeline.zincrby('score:', article, VOTE_SCORE)
                pipeline.hincrby(article, 'votes', 1)
                pipeline.execute()
            else:
                pipeline.unwatch()
            return
        except redis.exceptions.WatchError:
            cutoff = time.time() - ONE_WEEK_IN_SECONDS

# 在使用Redis的事务执行一连串命令时，事务可以减少Redis与客户端之间的通信往返次数可以大幅降低客户端等待回复所需的时间。
# 优化第一章的get_articles()函数在获取整个页面的文章时，需要在Redis与客户端之间进行26次通信返回之后变为只需要2次。
# <start id="exercise-fix-get_articles"/>
def get_articles(conn, page, order='score:'):
    start = max(page-1, 0) * ARTICLES_PER_PAGE
    end = start + ARTICLES_PER_PAGE - 1

    ids = conn.zrevrangebyscore(order, start, end)

    pipeline = conn.pipeline()
    # 将等待执行的多个 HGETALL 调用放入流水线
    map(pipeline.hgetall, ids)                              #A

    articles = []
    # 执行被流水线包含的多个 HGETALL 命令，
    # 并将执行所得的多个 id 添加到 articles 变量里面
    for id, article_data in zip(ids, pipeline.execute()):   #B
        article_data['id'] = id
        articles.append(article_data)

    return articles
# <end id="exercise-fix-get_articles"/>


# 键的过期时间
# Redis会在这个键的过期时间到达时自动删除该键
# Redis只有少数几个命令可以原子地为键设置过期时间，并且对于列表、集合、散列和有序集合这样的容器（container）
# 来说，键过期命令只能为整个键设置过期时间，而没办法为键里面的单个元素设置过期时间（为了解决这个问题，可以采用存储时间戳的有序集合来
# 实现针对单个元素的过期操作）
# PERSIST key-name -- 移除键的过期时间
# TTL key-name -- 查看给定键距离过期还有多少秒
# EXPIRE key-name seconds -- 让给定键在指定的秒数之后过期
# EXPIREAT key-name timestamp -- 将给定键的过期时间设置为给定的UNIX时间戳
# PTTL key-name -- 查看给定键距离过期时间还有多少毫秒，这个命令在Redis2.6或以上版本可用
# PEXPIRE key-name milliseconds -- 让给定键在指定的毫秒数之后过期，这个命令在Reids 2.6 或以上版本可用
# PEXPIREAT key-name timestamp-milliseconds -- 将一个毫秒级精度的UNIX时间戳设置为给定键的过期时间，这个命令在Redis2.6或以上版本可用。
# 代码清单 3-15
'''
# <start id="other-calls-1"/>
>>> conn.set('key', 'value')                    # 设置一个简单的字符串值，作为过期时间的设置对象。
True                                            #
>>> conn.get('key')                             #
'value'                                         #
>>> conn.expire('key', 2)                       # 如果我们为键设置了过期时间，那么当键过期后，
True                                            # 我们再尝试去获取键时，会发现键已经被删除了。
>>> time.sleep(2)                               #
>>> conn.get('key')                             #
>>> conn.set('key', 'value2')
True
>>> conn.expire('key', 100); conn.ttl('key')    # 还可以很容易地查到键距离过期时间还有多久。
True                                            #
100                                             #
# <end id="other-calls-1"/>
'''

# 使用EXXPIRE命令代替时间戳有序集合
# 使用过期时间操作来删除会话ID，从而代替之前使用有序集合来记录并清除会话ID的做法
# <start id="exercise-no-recent-zset"/>
THIRTY_DAYS = 30*86400
def check_token(conn, token):
    # 为了能够对登录令牌进行过期，我们将把它存储为字符串值
    return conn.get('login:' + token)

def update_token(conn, token, user, item=None):
    # 在一次命令调用里面，同时为字符串键设置值和过期时间
    conn.setex('login:' + token, user, THIRTY_DAYS)
    key = 'viewed:' + token
    if item:
        conn.lrem(key, item)
        conn.rpush(key, item)
        conn.ltrim(key, -25, -1)
    # 跟字符串不一样，Redis 并没有提供能够在操作列表的同时，
    # 为列表设置过期时间的命令，
    # 所以我们需要在这里调用 EXPIRE 命令来为列表设置过期时间
    conn.expire(key, THIRTY_DAYS)
    conn.zincrby('viewed:', item, -1)

def add_to_cart(conn, session, item, count):
    key = 'cart:' + session
    if count <= 0:
        conn.hrem(key, item)
    else:
        conn.hset(key, item, count)
    # 散列也和列表一样，需要通过调用 EXPIRE 命令来设置过期时间
    conn.expire(key, THIRTY_DAYS)
# <end id="exercise-no-recent-zset"/>
