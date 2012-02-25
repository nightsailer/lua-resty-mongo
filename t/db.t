#!/usr/bin/env lua
require 'Test.More'
plan 'no_plan'

local test = require("t.testutil")
local t_ordered = require("resty.mongo.orderedtable")
local db = test.test_db()
local list = db:run_command({ buildInfo = true })

for k,v in pairs(list) do
    print("k",k)
end
ok(list.version, "run_command/buildInfo")

local q = { t = {}}
q.t["$gte"] = 100
local r = db:run_command(t_ordered({"count","foo","query", q }))
dump(r)
done_testing()