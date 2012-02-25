module(...,package.seeall)

require("resty.mongo.support")
local util = require("resty.mongo.util")
local Connection = require("resty.mongo.connection")
local Cursor = require('resty.mongo.cursor')
local host,port = "127.0.0.1",27017
local conn = Connection.new({host = host, port = port })

local function new_conn()
    return  conn
end

local function test_db(db)
    db = db or "test"
    return conn:get_database(db)
end
return {
    new_conn = new_conn,
    test_db  = test_db,
}
