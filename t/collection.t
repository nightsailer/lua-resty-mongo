#!/usr/bin/env lua
require 'Test.More'
plan 'no_plan'

local test = require("t.testutil")
local db = test.test_db()

local col = db:get_collection('foo')

local c = 0

do
    is(col:count(), c ,"count/empty collection")
end

do
    local d = { t = 301 }
    ok(col:insert(d), "insert")
    ok(d._id, "single doc /oid generated")
    c = c + 1

    d = { {t=302 },{ t = 303 }}
    ok(col:insert(d),"batch insert")
    c = c + #d
    is(col:count(),c,"check insert count")
end

do
    col:insert({ _id =2, t = 304 })
    c = c + 1
    local _ok,err = col:insert({ _id = 2 }, { safe = true })
    ok(_ok == nil, "insert/safe")
end

do
    local query = { t = {} }
    query.t["$gt"] = 100
    n = col:count(query)
    is(n,c,"count/query")
end

do
    col:update({_id = 2 }, { t = 305 })
    local _ok,err = col:update({_id = 2 }, { t = 305 },{ safe = true })
    is(_ok.n,1,"update/safe")
end
do
    local d = col:find_one({_id = 2})
    is(d.t,305,"find_one")
end
do
    col:remove({t = 305 })
    c = c-1
    is(col:count(),c, 'remove(selector)')
    col:remove()
    is(col:count(),0,"remove all")
end

col:drop()

done_testing()