-- KEYS[1]: Stream名称（如 "mystream"）
-- ARGV[1]: 新消息的 key（格式 "timestamp-period"，直接作为Stream ID）
-- ARGV[2]: 新消息的 info_count
-- ARGV[3]: 新消息的 msg_content
local stream = KEYS[1]
local new_id = ARGV[1]           -- 格式: "tp-period" (如 "bbb-100")
local new_info_count = tonumber(ARGV[2])
local new_msg_content = ARGV[3]

-- 1. 提取 period (如 "100")
local dash_pos = string.find(new_id, "-")
if not dash_pos then
    return {err = "INVALID_KEY_FORMAT"}
end
local new_period = string.sub(new_id, dash_pos + 1)

-- 2. 查找相同 period 的消息
local msgs = redis.call("XREVRANGE", stream, "+", "-", "COUNT", 10)  -- 查最新 10 条
local max_info_count = 0
local msg_to_delete = nil

for _, msg in ipairs(msgs) do
    local fields = msg[2]
    local msg_key = fields["key"]  -- 使用字段名作为键
    if msg_key then
        local msg_dash_pos = string.find(msg_key, "-")
        if msg_dash_pos then
            local msg_period = string.sub(msg_key, msg_dash_pos + 1)
            if msg_period == new_period then
                local current_count = tonumber(fields["info_count"]) or 0
                if current_count > max_info_count then
                    max_info_count = current_count
                    msg_to_delete = msg[1]  -- 记录要删除的消息 ID
                end
            end
        end
    end
end

-- 3. 判断是否要更新
if new_info_count > max_info_count then
    -- 删除旧消息（如果存在）
    if msg_to_delete then
        redis.call("XDEL", stream, msg_to_delete)
    end
    -- 写入新消息
    redis.call("XADD", stream, "*",
        "key", new_id,
        "info_count", new_info_count,
        "msg_content", new_msg_content
    )
    return {ok = "UPDATED"}
elseif new_info_count == max_info_count then
    return {ok = "EQUAL"}
else
    return {ok = "SKIPPED"}
end