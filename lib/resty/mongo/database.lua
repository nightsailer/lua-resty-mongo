-- database class

module(..., package.seeall)

local Collection = require("resty.mongo.collection")
local Cursor     = require("resty.mongo.cursor")
local protocol   = require("resty.mongo.protocol")
local NS         = protocol.NS
local t_ordered  = require("resty.mongo.orderedtable")

local database = {}
local database_mt = { __index = database }

-- -------------------------
-- attributes
-- -------------------------
database.name = nil
database.conn = nil

-- -------------------------
-- instance methods
-- -------------------------

function database:collection_names() end

function database:get_collection(name)
    return Collection.new(name,self)
end

--[[ todo

function database:get_gridfs(prefix)
end
--]]

function database:drop()
    return self:run_command({ dropDatabase = true })
end

function database:drop_collection( name )
    local ok =  self:run_command({ drop = name })
    return ok.ok == 1 or ok.ok == true
end

function database:get_last_error(options)
    options = options or {}
    local w = options.w or self.conn.w
    local wtimeout = options.wtimeout or self.conn.wtimeout
    local cmd = t_ordered({"getlasterror",true, "w",w,"wtimeout",wtimeout})
    if options.fsync then cmd.fsync = true end
    if options.j then cmd.j = true end
    return self:run_command(cmd)
end

function database:run_command(cmd)
    local cursor = Cursor.new(self, NS.SYSTEM_COMMAND_COLLECTION,cmd)
    local result = cursor:limit(-1):all()
    if not result[1] then
        -- raise error?
        -- return nil,cursor.last_error_msg
        return { ok = 0, errmsg = cursor.last_error_msg }
    end
    return result[1]
end

--[[ todo

function database:eval(code,args)
end
--]]

-----------------------------
-- consturctor
-----------------------------

local function new(name,conn)
    assert(name,"Database name not provide")
    assert(conn,"Connection is nil")
    local obj = { name = name, conn = conn }
    return setmetatable(obj, database_mt)
end

return {
    new = new,
}
