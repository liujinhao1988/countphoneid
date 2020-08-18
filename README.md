利用spark统计固定时间段内的TopN热点商品列表


1.创建一个kafka消息队列access-log
2.在openresty中创建一个kafka—producer用于发送商品访问消息
配置lua


local producer = require("resty.kafka.producer") 
local broker_list = {  
    { host = "192.168.0.102", port = 9092 }
}

local async_producer = producer:new(broker_list, { producer_type = "async" })   
local ok, err = async_producer:send("access-log",nil ,phoneId)  

if not ok then  
    ngx.log(ngx.ERR, "kafka send err:", err)  
    return  
end

3.spark消费kafka数据并统计固定时间段内的TopN热点商品