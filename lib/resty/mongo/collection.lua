-- collection class

module(... , package.seeall)

local t_ordered                    = require("resty.mongo.orderedtable")
local Cursor                       = require("resty.mongo.cursor")
local protocol                     = require("resty.mongo.protocol")
local ZERO32                       = protocol.ZERO32
local ZEROID                       = protocol.ZEROID
local NS                           = protocol.NS
local mongo_insert_message         = protocol.insert_message
local mongo_delete_message         = protocol.delete_message
local mongo_update_message         = protocol.update_message
local mongo_query_message          = protocol.query_message
local mongo_send_message           = protocol.send_message
local mongo_send_message_with_safe = protocol.send_message_with_safe
local mongo_recv_message           = protocol.recv_message
local db_ns                        = protocol.db_ns
local collection = {}
local collection_mt = { __index = collection }

----------------------------
-- attributes
----------------------------
collection.name = nil
collection.ns = nil

-- -------------------------
-- instance methods
---------------------------

function collection:find(query,opts)
    return Cursor.new(self.db,self.name,query,opts)
end

function collection:find_one(query,opts)
    opts = opts or {}
    opts.limit = -1
    local i,doc =  self:find(query,opts):next_doc()
    return doc
end

function collection:insert(docs,options)
    local t = {}
    local single = #docs < 1
    if single then
        docs = { docs }
    end
    options = options or {}
    local continue_err = options.continue_on_err
    local no_ids = options.no_ids
    local _,m = mongo_insert_message(self.ns, docs, continue_err,no_ids)
    -- todo, check message bson size
    if options.safe then
        return self:with_safe(m,options)
    else
        mongo_send_message(self.conn.sock,m)
        return true
    end
end

function collection:with_safe(m,options)
    local ok,error = mongo_send_message_with_safe(self.conn.sock,m,self.db.name, {
        w = options.w or self.conn.w,
        wtimeout = options.wtimeout or self.conn.wtimeout,
        j = options.j,
        fsync = options.fsync,
    })
    if not ok then
        return nil,error
    end
    return ok
end

function collection:update(selector,obj,options)
    selector = selector or {}
    options = options or {}
    local multiple = options.multiple or false
    local upsert = options.upsert or false
    local _,m = mongo_update_message(self.ns,selector or {}, obj, upsert, multiple)
    if options.safe then
        return self:with_safe(m,options)
    else
        mongo_send_message(self.conn.sock,m)
        return true
    end
end

-- function collection:find_and_modify(options) end

function collection:remove(selector,options)
    selector = selector or {}
    options = options or {}
    local _,m = mongo_delete_message(self.ns,selector,options.single_remove)
    if options.safe then
        return self:with_safe(m,options)
    else
        mongo_send_message(self.conn.sock,m)
        return true
    end
end

function collection:ensure_index(keys,options)
    assert(keys,"ensure_index:keys is nil")
    assert(type(keys) == "table","ensure_index:keys must be table")
    local doc = t_ordered({"ns",self.ns})
    local _keys = t_ordered():merge(keys)
    doc.key = _keys
    if options.name then
        doc.name = options.name
    else
        doc.name = t_concat(_keys,'_')
    end
    local _v = {}
    for i,v in ipairs({"unique", "drop_dups", "background", "sparse"}) do
        if options[v] ~= nil then
            doc[v] = options[v] and true or false
            options[v] = nil
        end
    end
    options.name = nil
    options.no_ids = true
    return self.db:get_collection(NS.SYSTEM_INDEX_COLLECTION):insert(doc,options)
end

function collection:save(doc,options)
    if doc._id ~= nil then
        if options == nil or type(options) ~= "table" then
            options = { upsert = true }
        else
            options.upsert = true
        end
        return self:update({ _id = doc._id },doc,options)
    else
        return self:insert(doc,options)
    end
end

function collection:count(query)
    return Cursor.new(self.db,self.name,query):count()
    -- local r = self.db:run_command(t_ordered({ "count", self.name, "query", query }))
    -- if r.ok == 1 then return r.n end
    -- if r.missing or r.errmsg ==  "ns missing" then return 0 end
    -- error("count failed:" .. r.errmsg)
end

function collection:validate()
    return self.db:run_command({ validate = self.name })
end

function collection:drop_indexes() return self:drop_index("*") end

function collection:drop_index(index_name)
    assert(index_name,"drop_index:index_name is nil")
    return self.db:run_command(t_ordered("deleteIndexes",self.name, "index",index_name))
end

function collection:get_indexes()
    return self.db:get_collection(NS.SYSTEM_INDEX_COLLECTION):find({ns = self.ns}):all()
end

function collection:drop()
    return self.db:drop_collection(self.name)
end

-- -- -------------------------
-- consturctor
-- -- -------------------------

local function new(name,db)
    return setmetatable({
        name = name,
        db   = db,
        conn = db.conn,
        ns = db_ns(db.name,name),
    },collection_mt)
end

return {
    new = new,
}
