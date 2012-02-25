#!/usr/bin/env lua
require 'Test.More'
plan 'no_plan'

local mongo_connection  = require('resty.mongo.connection')
local con = mongo_connection.new()

diag(con.host)
ok(con)
done_testing()