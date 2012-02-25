#!/usr/bin/env lua

require "Test.More"

plan 'no_plan'

if not require_ok("resty.mongo.bson") then
    BAIL_OUT "no lib"
end

local util = require "resty.mongo.util"
local bson = require "resty.mongo.bson"
local to_bson = bson.to_bson
local from_bson = bson.from_bson
local from_bson_buf = bson.from_bson_buf

local oid = require "resty.mongo.object_id"
local new_str_buffer = util.new_str_buffer

local o = {
    a = "lol" ,
    b = "foo" ,
    c = 42,
    d = { 5 , 4 , 3 , 2 , 1 },
    e = { { { { } } } },
    f = { [true] = {baz = "mars"} },
    g = oid.new("abcdefghijkl" ),
    --z = { [{}] = {} } ; -- Can't test as tables are unique
}

local b = to_bson( o )

local f = from_bson(b)
local d = from_bson_buf(new_str_buffer(b))

is_deeply(f,o,"from/to bson")
is_deeply(f,d,"from bson buffer")

done_testing()