#!/usr/bin/env lua

require 'Test.More'
plan 'no_plan'

local util = require('resty.mongo.util')
local protocol = require("resty.mongo.protocol")
local t_print = util.table_print
local Connection = require("resty.mongo.connection")
local Collection = require("resty.mongo.collection")
local Cursor = require('resty.mongo.cursor')

local host,port = "127.0.0.1",27017
local conn = Connection.new({host = host, port = port })
local db = conn:get_database("test")
local col = db:get_collection("foo")
for i=1,100 do
    col:insert({t = i})
end

local query = { t = {} }
query.t["$gte"] = 20
local cursor = Cursor.new(db,"foo",query)
cursor:sort({t = -1})
local _t = false
for i,item in cursor:next() do
    if item.t ~= (100-i+1) then
        _t = false
    else
        _t = true
    end
end
ok(_t, "cursor:next()")

cursor:reset()

local i, item = cursor:next_doc()
ok(i == 1 and item.t == 100, "reset")

col:drop()
done_testing()