-- cursor class

module(...,package.seeall)

local t_ordered              = require("resty.mongo.orderedtable")
local t_insert               = table.insert
local t_remove               = table.remove
local t_concat               = table.concat

local strbyte                = string.byte
local strformat              = string.format

local protocol                 = require("resty.mongo.protocol")
local ZERO32                   = protocol.ZERO32
local ZEROID                   = protocol.ZEROID
local mongo_get_more_message   = protocol.get_more_message
local mongo_query_message      = protocol.query_message
local mongo_send_message       = protocol.send_message
local mongo_recv_message       = protocol.recv_message
local mongo_killcusors_message = protocol.kill_cursors_message
local db_ns                    = protocol.db_ns

local cursor = { }
local cursor_mt = { __index = cursor }

cursor_mt.__gc = function(self)
    self:kill_cursor()
end

cursor_mt.__tostring = function(ob)
    local t = { }
    for i = 1 , 8 do
        t_insert( t , strformat ( "%02x" , strbyte ( ob.id , i , i ) ) )
    end
    return "CursorId(" .. t_concat ( t ) .. ")"
end

-- -------------------------
-- attributes
-- -------------------------
cursor.slave_ok = false
cursor.tailable = false
cursor.immortal = false
cursor.batch_size = 10

cursor._limit = 0
cursor._skip  = 0
cursor._fields = false

cursor.ns = nil
cursor.query_run = false
cursor.closed = false
cursor.id = false
cursor.at = 0
cursor.req_id = 0
cursor.number_received = 0

-- private

cursor._result_cache = {}
cursor._special = nil

local function assert_state(self)
    assert(not self.query_run and not self.closed, "Cannot modify the query once it has been run or closed.")
end

local function close_cursor_if_query_complete(self)
    local limit = self._limit
    if limit > 0 and self.number_received >= limit then
        self:close()
    end
end

local function send_init_query(self)
    -- print(">> send_init_query")
    if self.query_run then
        -- print("<< send_init_query run again")
        return false
    end
    local opts = {
        tailable = self.taiable,
        slave_ok = self.slave_ok,
        immortal = self.immortal,
        partial =  self.partial,
    }
    local sock = self.sock
    local query = self._special or self.query or {}
    -- special query process
    if self._special then
        query.query =  self.query
    end
    local req_id,message = mongo_query_message(self.ns,query,self._fields,self._limit,self._skip,opts)
    mongo_send_message(sock,message)
    local number_received = 0
    self.id,number_received,err,self._result_cache = mongo_recv_message(sock,req_id)
    self.query_run = true
    if self.id == ZEROID then
        self.id = false
    end
    self.number_received = self.number_received + number_received
    close_cursor_if_query_complete(self)
    -- print("<< send_init_query")
    return true
end

local function refill_via_getmore(self)
    -- print(">> refill_via_getmore")
    if not self.id then
        return
    end
    -- print("id:",self.id,"batch_size:",self.batch_size)
    local req_id,message = mongo_get_more_message(self.ns,self.id,self.batch_size)
    local sock = self.sock
    mongo_send_message(sock,message)
    local id, number_received, err,docs = mongo_recv_message(sock,req_id)
    -- empty result
    if id == ZEROID then
        self.id = false
    end
    if err.CURSOR_NOT_FOUND then
        -- print("<< refill_via_getmore ERR")
        self.id = false
        self.last_error_msg = "cursor not found"
        self:close()
    end
    if err.QUERY_FAILURE then
        self.id = false
        self.last_error_msg = "query failue"
        self:close()
    end
    self.last_error = err
    if number_received == 0 then
        -- print("<< refill_via_getmore number_received == 0")
        self:close()
        return
    end
    local result = self._result_cache
    for i,v in ipairs(docs) do
        t_insert(result,v)
    end
    self.number_received = self.number_received + number_received
    close_cursor_if_query_complete(self)
    -- print("<< refill_via_getmore ***")
end

local function _add_special_query(self,k,v)
    local special = self._special or {}
    special[k] = v
    self._special = special
end
-- -------------------------
-- instance methods
-- -------------------------

function cursor:fields(fields)
    assert_state(self)
    self._fields = fields
    return self
end

function cursor:sort(order)
    assert_state(self)
    _add_special_query(self,'orderby',order)
    return self
end

function cursor:limit(size)
    assert_state(self)
    self._limit = size
    return self
end

function cursor:skip(size)
    assert_state(self)
    self._skip = size >= 0 and size or 0
    return self
end

function cursor:snapshot()
    assert_state(self)
    _add_special_query(self,"$snapshot",true)
    return self
end

function cursor:count(include_all)
    local cmd = t_ordered({ "count", self.name, "query", self.query })
    if include_all then
        if self._limit ~= 0 then
            cmd.limit = self._limit
        end
        if self._skip ~= 0 then
            cmd.skip  = self._skip
        end
    end
    local r = self.db:run_command(cmd)
    if r.ok == 1 then return r.n end
    if r.errmsg ==  "ns missing" then return 0 end
    error("count failed:" .. r.errmsg)
end

function cursor:hint(index)
    assert_state(self)
    _add_special_query(self,"$hint",index)
    return self
end

function cursor:reset()
    self.id = false
    self.closed = false
    self.at = 0
    self.query_run = false
    self._result_cache = {}
end

function cursor:all()
    local r = {}
    for i,v in self:next() do
        r[i] = v
    end
    return r
end

function cursor:next_doc()
    -- first
    if not self.query_run then
        send_init_query(self)
    end
    if self.id and #self._result_cache == 0 and (self._limit <= 0 or self.at < self._limit ) then
        refill_via_getmore(self)
    end
    local v = t_remove(self._result_cache,1)
    if v ~= nil then
        self.at = self.at+1
        return self.at,v
    end
    return nil
end

-- interator

function cursor:next()
    return self.next_doc,self
end

function cursor:close( )
    self:kill_cursor()
    self.closed = true
end

function cursor:kill_cursor()
    local id = self.id
    if id then
        local m = mongo_killcusors_message({ id })
        mongo_send_message(self.conn.sock, m )
        self.id = false
    end
end
-----------------------------
-- consturctor
-----------------------------

local function new(db, name, query , opts)
    local c = {}
    c.db = db
    c.sock = db.conn.sock
    c.name = name
    c.ns = db_ns(db.name,name)
    c.query = query or {}
    local _limit, _skip, _snapshot, _fields,_sort_by = c.limit,c.skip,c.snapshot,c.fields,c.sort_by
    c._limit = _limit or 0
    c._skip  = _skip or 0
    setmetatable(c,cursor_mt)
    if _snapshot then
        c:snapshot()
    end
    if _fields then
        c:fields(_fields)
    end
    if _sort_by then
        c:sort(_sort_by)
    end
    return c
end

return {
    new = new,
}
