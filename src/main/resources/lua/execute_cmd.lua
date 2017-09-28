local result = {};
for _, cmd in ipairs(ARGV) do
    local cmdTab = cjson.decode(cmd);
    local op = cmdTab["op"];
    if op == "DEL" then
        redis.call(op, cmdTab["key"])
    elseif op == "HDEL" then
        redis.call(op, cmdTab["key"], cmdTab["field"])
    elseif op == "LPUSH" or op == "RPUSH" then
        redis.call(op, cmdTab["key"], cmdTab["val"])
    elseif op == "HSET" then
        redis.call(op, cmdTab["key"], cmdTab["field"], cmdTab["val"])
    end
end
return result;
