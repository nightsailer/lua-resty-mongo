#!/usr/bin/env lua

require 'Test.More'

plan 'no_plan'

local posix = require 'posix'

local util = require 'resty.mongo.util'

local md5 = require('md5')

local util_md5 = util.md5;

is(util.time(),os.time(),'time')
is(util.getpid(),posix.getpid().pid,'getpid')

is(util_md5('123abc'), md5.sum('123abc'),'md5')

local t = util.split("127.0.0.1:22",":")

is(#t,2,"split")

is(t[1],"127.0.0.1","split[1]")
is(t[2],"22","split[2]")

local long = util.num_to_le_uint(66001 % 0xffff,2)
ok(util.le_uint_to_num(long) > 0 , 'Int64')

done_testing()