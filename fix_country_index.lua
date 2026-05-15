-- =============================================================================
-- 国家索引修复脚本
-- 用法: redis-cli --eval fix_country_index.lua
-- 或者: 直接粘贴到 redis-cli 中运行 (把整个文件内容复制过去)
-- =============================================================================

-- 1. 删除重建标记，下次启动会重新执行 RebuildCountryIndexes
redis.call("DEL", "proxy:index:rebuilt")

-- 2. 扫描并删除所有旧的国家索引键
local cursor = "0"
local delCount = 0
repeat
    local result = redis.call("SCAN", cursor, "MATCH", "proxy:country:*", "COUNT", "1000")
    cursor = result[1]
    local keys = result[2]
    if #keys > 0 then
        redis.call("DEL", unpack(keys))
        delCount = delCount + #keys
    end
until cursor == "0"

print("已删除 " .. delCount .. " 个旧国家索引键")

-- 3. 从 proxy:stats:country 哈希重建干净的国家索引
local cursor = "0"
local rebuilt = 0
local skipped = 0
repeat
    local result = redis.call("HSCAN", "proxy:stats:country", cursor, "COUNT", "500")
    cursor = result[1]
    local pairs = result[2]
    for i = 1, #pairs, 2 do
        local proxy = pairs[i]
        local country = pairs[i + 1]
        if country ~= nil and #country > 0 and #country <= 10 then
            country = string.upper(country)
            -- 只接受纯字母的国家代码
            local isAlpha = true
            for j = 1, #country do
                local ch = string.byte(country, j)
                if ch < 65 or ch > 90 then  -- 'A'=65, 'Z'=90
                    isAlpha = false
                    break
                end
            end
            if isAlpha then
                redis.call("SADD", "proxy:country:" .. country, proxy)
                rebuilt = rebuilt + 1
            else
                skipped = skipped + 1
            end
        end
    end
until cursor == "0"

print("重建完成: " .. rebuilt .. " 条已索引, " .. skipped .. " 条跳过")

-- 4. 统计 Top 5 国家
print("")
print("===== Top 5 国家 (基于索引) =====")
local stats = {}
cursor = "0"
repeat
    local result = redis.call("SCAN", cursor, "MATCH", "proxy:country:??", "COUNT", "200")
    cursor = result[1]
    local keys = result[2]
    for _, key in ipairs(keys) do
        local count = redis.call("SCARD", key)
        local code = string.sub(key, 15)  -- 去掉 "proxy:country:" 前缀
        table.insert(stats, {code = code, count = count})
    end
until cursor == "0"

-- 排序
table.sort(stats, function(a, b) return a.count > b.count end)
for i = 1, math.min(5, #stats) do
    print("  #" .. i .. " " .. stats[i].code .. ": " .. stats[i].count)
end

print("")
print("===== 完成! 重启服务后生效 =====")
