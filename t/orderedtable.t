#!/usr/bin/env lua
require 'Test.More'
plan 'no_plan'
local newtable = require("resty.mongo.orderedtable")
local keys = { 'a','b','c' }
local t = newtable()
t.a = 1
t.b = 2
t.c = 3
for k,v in pairs(t) do
    is(keys[v],k,"key:"..k .. " index:"..v)
end
local b  = newtable({"a",1,"b",2,"c",3})
for k,v in pairs(b) do
    is(keys[v],k,"init talbe, key:" .. k .. " index:"..v)
end
local c = newtable()
c:merge({t = 1})
is(c.t,1,"merge")

done_testing()