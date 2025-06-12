local stream = KEYS[1]
local new_id = ARGV[1]
local new_info_count = tonumber(ARGV[2])
local new_msg_content = ARGV[3]
local max_stream_size = tonumber(ARGV[4]) or 1000 

local logs = {}
local function log(msg)
    table.insert(logs, msg)
end

log("stream: " .. stream)
log("new_id: " .. new_id)
log("new_info_count: " .. new_info_count)
log("msg_content_length: " .. string.len(new_msg_content))
log("max_stream_size: " .. max_stream_size)

local current_size = redis.call("XLEN", stream)
log("当前Stream大小: " .. current_size)
if current_size >= max_stream_size then
    -- 删除最旧的一条
    local oldest_msg = redis.call("XRANGE", stream, "-", "+", "COUNT", 1)
    if #oldest_msg > 0 then
        log("触发FIFO清理, 当前大小=" .. current_size .. "，限制=" .. max_stream_size)
        redis.call("XDEL", stream, oldest_msg[1][1])
    end
end
log("Stream 数量小于限制，不进行清理")

-- 1. 提取 period (如 "100")
local dash_pos = string.find(new_id, "-")
if not dash_pos then
    log("INVALID_KEY_FORMAT")
    return {err = "INVALID_KEY_FORMAT"}
end
local new_period = string.sub(new_id, dash_pos + 1)

-- 2. 查找相同 period 的消息
local msgs = redis.call("XREVRANGE", stream, "+", "-", "COUNT", 100)  -- 查最新 100 条，确保不遗漏
log("查询到的消息数量: " .. #msgs)
local max_info_count = 0
local same_period_msgs = {}  -- 存储相同period的所有消息

for i, msg in ipairs(msgs) do
    log("处理消息 " .. i .. ", 消息ID: " .. msg[1])
    local fields = msg[2]
    log("字段数组长度: " .. #fields)
    
    -- Redis Stream字段是以数组形式存储的: [field1, value1, field2, value2, ...]
    local msg_key = nil
    for j = 1, #fields, 2 do
        local field_name = fields[j]
        local field_value = fields[j + 1]
        if field_name == "msg_content" then
            log("字段: " .. field_name .. " = [binary data, length: " .. string.len(field_value) .. "]")
        else
            log("字段: " .. field_name .. " = " .. field_value)
        end
        if field_name == "key" then
            msg_key = field_value
        end
    end
    
    if msg_key then
        local msg_dash_pos = string.find(msg_key, "-")
        log("msg_dash_pos: " .. msg_dash_pos)
        if msg_dash_pos then
            local msg_period = string.sub(msg_key, msg_dash_pos + 1)
            log("msg_period: " .. msg_period)
            if msg_period == new_period then
                log("找到相同 period 的消息: " .. msg_key)
                -- 从字段数组中查找info_count
                local current_count = 0
                for j = 1, #fields, 2 do
                    if fields[j] == "info_count" then
                        current_count = tonumber(fields[j + 1]) or 0
                        break
                    end
                end
                log("旧消息的 info_count: " .. current_count)
                
                -- 记录这个相同period的消息
                table.insert(same_period_msgs, {
                    id = msg[1],
                    info_count = current_count,
                    key = msg_key
                })
                
                if current_count > max_info_count then
                    max_info_count = current_count
                end
            end
        end
    end
end

-- 清理相同period的重复消息 只保留info_count最大的那一条
if #same_period_msgs > 1 then
    log("发现" .. #same_period_msgs .. "条相同period的消息 需要清理")
    local keep_msg = nil
    for _, msg_info in ipairs(same_period_msgs) do
        if msg_info.info_count == max_info_count then
            if keep_msg == nil then
                keep_msg = msg_info  -- 保留第一个达到最大info_count的消息
                log("保留消息: " .. msg_info.id .. " (info_count=" .. msg_info.info_count .. ")")
            else
                -- 删除重复的消息
                log("删除重复消息: " .. msg_info.id .. " (info_count=" .. msg_info.info_count .. ")")
                redis.call("XDEL", stream, msg_info.id)
            end
        else
            -- 删除info_count较小的消息
            log("删除旧消息: " .. msg_info.id .. " (info_count=" .. msg_info.info_count .. ")")
            redis.call("XDEL", stream, msg_info.id)
        end
    end
elseif #same_period_msgs == 1 then
    log("找到1条相同period的消息 无需清理")
else
    log("没有找到相同period的消息")
end

-- 3. 判断是否要更新
if new_info_count > max_info_count then
    -- 删除所有相同period的旧消息
    for _, msg_info in ipairs(same_period_msgs) do
        log("删除旧消息以便更新: " .. msg_info.id)
        redis.call("XDEL", stream, msg_info.id)
    end
    -- 写入新消息
    log("写入新消息: " .. new_id)
    local operation_type = (#same_period_msgs > 0) and "UPDATE" or "INSERT"
    redis.call("XADD", stream, "*",
        "operation", operation_type,
        "key", new_id,
        "info_count", new_info_count,
        "msg_content", new_msg_content,
        "replaced_count", #same_period_msgs
    )
    local log_string = table.concat(logs, "\n")
    return "UPDATED\n" .. log_string
elseif new_info_count == max_info_count then
    log("新消息与最大信息数相同，跳过")
    local log_string = table.concat(logs, "\n")
    return "EQUAL\n" .. log_string
else
    log("新消息小于最大信息数，跳过")
    local log_string = table.concat(logs, "\n")
    return "SKIPPED\n" .. log_string
end