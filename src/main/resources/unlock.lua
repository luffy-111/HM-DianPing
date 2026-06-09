-- 获取锁的线程标识
local id = redis.call('get', KEYS[1])
-- 判断锁的线程标识是否和当前线程标识相同
if (id == ARGV[1]) then
    -- 删除锁
    redis.call('del', KEYS[1])
    return 1
else
    return 0
end
