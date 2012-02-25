module('resty.mongo',package.seeall)

require("resty.mongo.support")

local Connection = require('resty.mongo.connection')

local function new_connection( ... ) return Connection.new(...) end

return {
    new_connection = new_connection,
}

