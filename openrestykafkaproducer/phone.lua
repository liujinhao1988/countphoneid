local uri_args=ngx.var.uri
local phoneId=string.sub(uri_args,7)

local host ={"192.168.0.102:8091","192.168.0.102:8092"}
local hash=ngx.crc32_long(phoneId)

hash=(hash%2) +1
url="http://"..host[hash]

path1="/find/"..phoneId
--ngx.say(backend..path1)
local http =require("resty.http")
local httpc = http.new()

local resp,err=httpc:request_uri(url,{
		method="GET",
		path=path1
})
if not resp then
	ngx.say("error",err)
	return
end

--to kafka

local producer = require("resty.kafka.producer")
local broker_list ={
	{host="192.168.0.102",port=9092}

}

local async_producer= producer:new(broker_list,{producer_type="async"})
local ok,err=async_producer:send("access-log","aaa",phoneId)
if not ok then
	ngx.say(ngx.ERR,"err",err)
	return
end

--to kafka





ngx.say(resp.body)
httpc:close()