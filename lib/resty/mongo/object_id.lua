-- object id class

module(...,package.seeall)

local setmetatable = setmetatable
local strbyte = string.byte
local strformat = string.format
local t_insert = table.insert
local t_concat = table.concat

local md5 = require "md5"
local util = require "resty.mongo.util"
local num_to_le_uint = util.num_to_le_uint
local num_to_be_uint = util.num_to_be_uint

local oid_mt = {
    __tostring = function( ob )
        local t = { }
        for i = 1 , 12 do
            t_insert( t , strformat ( "%02x" , strbyte( ob.id , i , i ) ) )
        end
        return "ObjectId(" .. t_concat ( t ) .. ")"
    end,
    __eq = function( a , b ) return a.id == b.id end,
}

local machineid = md5.sum( util.machineid() ):sub(1,3)

local pid = util.getpid() % 0xffff
pid = num_to_le_uint(pid,2)

local inc = 0

local function generate_id()
    inc = inc + 1
    -- "A BSON ObjectID is a 12-byte value consisting of a 4-byte timestamp (seconds since epoch),
    -- a 3-byte machine id, a 2-byte process id, and a 3-byte counter.
    -- Note that the timestamp and counter fields must be stored big endian unlike the rest of BSON"
    return num_to_be_uint( util.time(), 4 ) .. machineid .. pid .. num_to_be_uint( inc , 3 )
end

local function new_object_id( str )
    if str then
        assert( #str == 12 )
    else
        str = generate_id()
    end
    return setmetatable( { id = str } , oid_mt )
end

return {
    new       = new_object_id,
    metatable = oid_mt,
}
