--------------------------------------------------------
-- MongoDB Lua driver for OpenResty
--------------------------------------------------------
-- License: MIT
-- Copyright(c) 2012 Pan Fan( Night Sailer)
--------------------------------------------------------
-- Mongo Wire Protocol
--
-- There are two types of messages, client requests and database responses, each having a slightly different structure.
--
-- Client Request Messages
--
-- Standard Message Header
--
-- In general, each message consists of a standard message header followed by request-specific data.
-- The standard message header is structured as follows :
--
--     struct MsgHeader {
--         int32   messageLength; // total message size, including this
--         int32   requestID;     // identifier for this message
--         int32   responseTo;    // requestID from the original request
--                                //   (used in reponses from db)
--         int32   opCode;        // request type - see table below
--     }
--
-- OP_UPDATE
--
-- struct OP_UPDATE {
--     MsgHeader header;             // standard message header
--     int32     ZERO;               // 0 - reserved for future use
--     cstring   fullCollectionName; // "dbname.collectionname"
--     int32     flags;              // bit vector. see below
--     document  selector;           // the query to select the document
--     document  update;             // specification of the update to perform
-- }
--
-- OP_INSERT
--
-- The OP_INSERT message is used to insert one or more documents into a collection.
-- The format of the OP_INSERT message is
--
--     struct {
--         MsgHeader header;             // standard message header
--         int32     ZERO;               // 0 - reserved for future use
--         cstring   fullCollectionName; // "dbname.collectionname"
--         document* documents;          // one or more documents to insert into the collection
--     }
--
--
-- OP_QUERY
--
-- The OP_QUERY message is used to query the database for documents in a collection.
-- The format of the OP_QUERY message is :
--
--     struct OP_QUERY {
--         MsgHeader header;                // standard message header
--         int32     flags;                  // bit vector of query options.  See below for details.
--         cstring   fullCollectionName;    // "dbname.collectionname"
--         int32     numberToSkip;          // number of documents to skip
--         int32     numberToReturn;        // number of documents to return
--                                          //  in the first OP_REPLY batch
--         document  query;                 // query object.  See below for details.
--         [ document  returnFieldSelector; ] // Optional. Selector indicating the fields
--                                          //  to return.  See below for details.
--     }
--
--
-- OP_GETMORE
--
-- The OP_GETMORE message is used to query the database for documents in a collection.
-- The format of the OP_GETMORE message is :
--
--     struct {
--         MsgHeader header;             // standard message header
--         int32     ZERO;               // 0 - reserved for future use
--         cstring   fullCollectionName; // "dbname.collectionname"
--         int32     numberToReturn;     // number of documents to return
--         int64     cursorID;           // cursorID from the OP_REPLY
--     }
--
-- OP_DELETE
--
-- The OP_DELETE message is used to remove one or more messages from a collection.
-- The format of the OP_DELETE message is :
--
--     struct {
--         MsgHeader header;             // standard message header
--         int32     ZERO;               // 0 - reserved for future use
--         cstring   fullCollectionName; // "dbname.collectionname"
--         int32     flags;              // bit vector - see below for details.
--         document  selector;           // query object.  See below for details.
--     }
--
-- OP_KILL_CURSORS
--
-- The OP_KILL_CURSORS message is used to close an active cursor in the database. This is necessary to ensure
-- that database resources are reclaimed at the end of the query. The format of the OP_KILL_CURSORS message is :
--
-- struct {
--     MsgHeader header;            // standard message header
--     int32     ZERO;              // 0 - reserved for future use
--     int32     numberOfCursorIDs; // number of cursorIDs in message
--     int64*    cursorIDs;         // sequence of cursorIDs to close
-- }
--
--
-- Database Response Messages
--
-- OP_REPLY
--
-- The OP_REPLY message is sent by the database in response to an
-- OP_QUERY or OP_GET_MORE
-- message. The format of an OP_REPLY message is:
--
--     struct {
--         MsgHeader header;         // standard message header
--         int32     responseFlags;  // bit vector - see details below
--         int64     cursorID;       // cursor id if client needs to do get more's
--         int32     startingFrom;   // where in the cursor this reply is starting
--         int32     numberReturned; // number of documents in the reply
--         document* documents;      // documents
--     }
---------------------------------------------------------------
-- More detail about Mongo Wire Protocol, please visit:
-- http://www.mongodb.org/display/DOCS/Mongo+Wire+Protocol
---------------------------------------------------------------

-- Mongo Wire Protocol support functions

module(...,package.seeall)

local bson                            = require('resty.mongo.bson')
local to_bson,from_bson,from_bson_buf = bson.to_bson,bson.from_bson,bson.from_bson_buf
local t_concat,t_insert               = table.concat, table.insert

local util                             = require('resty.mongo.util')
local num_to_le_uint,num_to_le_int     = util.num_to_le_uint,util.num_to_le_int
local new_str_buffer                   = util.new_str_buffer
local le_bpeek                         = util.le_bpeek
local slice_le_uint, extract_flag_bits = util.slice_le_uint,util.extract_flag_bits

local oid       = require("resty.mongo.object_id")
local t_ordered = require("resty.mongo.orderedtable")

-- reserved collection namespace

local ns                        = {
    SYSTEM_NAMESPACE_COLLECTION = "system.namespaces",
    SYSTEM_INDEX_COLLECTION     = "system.indexes",
    SYSTEM_PROFILE_COLLECTION   = "system.profile",
    SYSTEM_USER_COLLECTION      = "system.users",
    SYSTEM_JS_COLLECTION        = "system.js",
    SYSTEM_COMMAND_COLLECTION   = "$cmd",
}


-- opcodes

local op_codes       = {
    OP_REPLY        = 1,
    OP_MSG          = 1000,
    OP_UPDATE       = 2001,
    OP_INSERT       = 2002,
    RESERVED        = 2003,
    OP_QUERY        = 2004,
    OP_GETMORE      = 2005,
    OP_DELETE       = 2006,
    OP_KILL_CURSORS = 2007,
}

-- message header size

local STANDARD_HEADER_SIZE = 16
local RESPONSE_HEADER_SIZE = 20

-- place holder

local ZERO32 = "\0\0\0\0"
local ZEROID = "\0\0\0\0\0\0\0\0"

-- flag bit constant

local flags         = {
    -- used in update message
    update          = {
        -- If set, the database will insert the supplied object into the collection if no matching document is found.
        Upsert      = 1,
        -- If set, the database will update all matching objects in the collection.
        -- Otherwise only updates first matching doc.
        MultiUpdate = 2,
        -- 2-31 reserved
    },
    -- used in insert message
    insert = {
        -- If set, the database will not stop processing a bulk insert if one fails (eg due to duplicate IDs).
        -- This makes bulk insert behave similarly to a series of single inserts, except lastError will be set if any insert fails, not just the last one.
        -- If multiple errors occur, only the most recent will be reported by getLastError. (new in 1.9.1)
        ContinueOnError = 1,
    },
    -- used in query message
    query               = {
        -- Tailable means cursor is not closed when the last data is retrieved.
        -- Rather, the cursor marks the final object's position.
        -- You can resume using the cursor later, from where it was located, if more data were received.
        -- Like any "latent cursor", the cursor may become invalid at some point (CursorNotFound)
        -- – for example if the final object it references were deleted.
        TailableCursor  = 2,
        -- Allow query of replica slave. Normally these return an error except for namespace "local".
        SlaveOk         = 4,
        -- Internal replication use only - driver should not set
        OplogReplay     = 8,
        -- The server normally times out idle cursors after an inactivity period (10 minutes) to prevent excess memory use.
        -- Set this option to prevent that.
        NoCursorTimeout = 16,
        -- Use with TailableCursor. If we are at the end of the data, block for a while rather than returning no data.
        -- After a timeout period, we do return as normal.
        AwaitData       = 32,
        -- Stream the data down full blast in multiple "more" packages, on the assumption that the client will fully read all data queried.
        -- Faster when you are pulling a lot of data and know you want to pull it all down.
        -- Note: the client is not allowed to not read all the data unless it closes the connection.
        Exhaust         = 64,
        -- Get partial results from a mongos if some shards are down (instead of throwing an error)
        Partial         = 128,
    },
    -- used in delete message
    delete           = {
        SingleRemove = 1,
    },
    -- used in reponse message
    reply            = {
        -- CursorNotFound: Set when getMore is called but the cursor id is not valid at the server.
        -- Returned with zero results.
        REPLY_CURSOR_NOT_FOUND   = 1,
        -- QueryFailure: Set when query failed. Results consist of one document containing an "$err" field describing the failure.
        REPLY_QUERY_FAILURE      = 2,
        -- ShardConfigStale: Drivers should ignore this. Only mongos will ever see this set, in which case,
        -- it needs to update config from the server.
        REPLY_SHARD_CONFIG_STALE = 4,
        -- AwaitCapable: Set when the server supports the AwaitData Query option.
        -- If it doesn't, a client should sleep a little between getMore's of a Tailable cursor.
        -- Mongod version 1.6 supports AwaitData and thus always sets AwaitCapable.
        REPLY_AWAIT_CAPABLE      = 8,
        -- Reserved 4-31
    },
}

local ERR = {
    CURSOR_NOT_FOUND = 1,
    QUERY_FAILURE = 2,
}

local  current_request_id = 0;

local function with_header(opcode,message,response_to)
    current_request_id = current_request_id+1
    local request_id = num_to_le_uint(current_request_id)
    response_to = response_to or ZERO32
    opcode = num_to_le_uint(assert(op_codes[opcode]))
    -- header(length,request_id,response_to,opcode) + message
    -- print("message size",#message+STANDARD_HEADER_SIZE)
    return current_request_id, num_to_le_uint (#message + STANDARD_HEADER_SIZE)
        .. request_id .. response_to .. opcode .. message
end

local function query_message(full_collection_name,query,fields,limit,skip,options)
    skip = skip or 0
    local flag = 0
    if options then
        flag = (options.tailable and flags.query.TailableCursor or 0)
            + (options.slave_ok and flags.query.SlaveOk or 0 )
            + (options.oplog_replay and flags.query.OplogReplay or 0)
            + (options.immortal and flags.query.NoCursorTimeout or 0)
            + (options.await_data and flags.query.AwaitData or 0)
            + (options.exhaust and flags.query.Exhaust or 0)
            + (options.partial and flags.query.Partial or 0)
    end
    query = to_bson(query)
    if fields then
        fields = to_bson(fields)
    else
        fields = ""
    end
    return with_header("OP_QUERY",
        num_to_le_uint(flag) .. full_collection_name .. num_to_le_uint(skip) .. num_to_le_int(limit)
         .. query .. fields
        )
end

local function get_more_message(full_collection_name, cursor_id, limit)
    return with_header("OP_GETMORE", ZERO32 .. full_collection_name .. num_to_le_int(limit or 0) .. cursor_id )
end

local function delete_message(full_collection_name,selector,singleremove)
    local flags = (singleremove and flags.delete.SingleRemove or 0)
    selector = to_bson(selector)
    return with_header('OP_DELETE', ZERO32 .. full_collection_name .. num_to_le_uint(flags) .. selector)
end

local function update_message(full_collection_name,selector,update,upsert,multiupdate)
    local flags = (upsert and flags.update.Upsert or 0) + ( multiupdate and flags.update.MultiUpdate or 0)
    selector = to_bson(selector)
    update = to_bson(update)
    return with_header('OP_UPDATE',ZERO32 .. full_collection_name .. num_to_le_uint(flags) .. selector .. update)
end

local function insert_message(full_collection_name,docs,continue_on_error,no_ids)
    local flags = ( continue_on_error and flags.insert.ContinueOnError or 0 )
    local r = {}
    -- local oids = {}
    for i,v in ipairs(docs) do
        local _id = v._id
        if not _id and not no_ids then
            _id = oid.new()
            v._id = _id
        end
        r[i] = to_bson(v)
    end
    return with_header("OP_INSERT", num_to_le_uint(flags) .. full_collection_name .. t_concat(r))
end

local function kill_cursors_message(cursor_id)
    local n = #cursor_id
    cursor_id = t_concat(cursor_id)
    return with_header('OP_KILL_CURSORS',ZERO32 .. num_to_le_uint(n) .. cursor_id )
end

local function recv_message(sock, request_id)
    -- print("recv_message,reqid",request_id)
    -- msg header
    local header = assert(sock:receive(STANDARD_HEADER_SIZE))
    local msg_length,req_id,response_to,opcode = slice_le_uint(header,4)
    -- print("msg_length:",msg_length,"req_id",req_id,"response_to",response_to,"opcode",opcode)
    assert(request_id == response_to, "response_to:".. response_to .. " should:" .. request_id)
    assert(opcode == op_codes.OP_REPLY,"invalid response opcode")
    -- read message data
    local msg_data = assert(sock:receive(msg_length-STANDARD_HEADER_SIZE))
    local msg_buf = new_str_buffer(msg_data)
    -- response header,20 bytes
    local response_flags,cursor_id = msg_buf(4), msg_buf(8)
    local starting_from,number_returned = slice_le_uint(msg_buf(8),2)
    local err = {}
    -- parse reponse flags
    local cursor_not_found,query_failure,shard_config_stale,await_capable = extract_flag_bits(response_flags,4)

    -- print('cursor_id:',cursor_id,"starting_from:",starting_from,"number_returned:",number_returned,"cursor_not_found:",
        -- cursor_not_found,"query_failure:",query_failure)

    -- todo: validate flags?
    -- assert(not cursor_not_found,'cursor not found')
    if cursor_not_found then
        -- print("ERR:cursor_not_found")
        err.CURSOR_NOT_FOUND = true
    end
    if query_failure then
        -- print("ERR:query_failure")
        err.QUERY_FAILURE = true
    end
    -- print("number_returned:"..number_returned)
    -- client should ignore this flag
    -- assert(not shard_config_stale,'shard confi is stale')
    local docs = {}
    -- documents
    if not cursor_not_found then
        for i=1,number_returned do
            docs[i] = from_bson_buf(msg_buf)
        end
    end
    return cursor_id,number_returned,err,docs
end

local function db_ns(db,name )
    return db .. "." .. name .."\0"
end

local function send_message( sock, message ) return sock:send(message) end

local function send_message_with_safe(sock,message,dbname,opts)
    local cmd = t_ordered({"getlasterror",true, "w",opts.w,"wtimeout",opts.wtimeout})
    if opts.fsync then cmd.fsync = true end
    if opts.j then cmd.j =  true end
    local req_id,last_error_msg = query_message(db_ns(dbname,ns.SYSTEM_COMMAND_COLLECTION),cmd,nil,-1,0)
    sock:send(message .. last_error_msg)
    local _, number,err, docs = recv_message(sock,req_id)
    if number == 1 and ( docs[1]['err'] or docs[1]['errmsg'] ) then
        return false, docs[1]
    end
    return docs[1]
end


return {

-- exported constants

    OPCODES              = op_codes,
    NS                   = ns,
    FLAGS                = flags,
    ZERO32               = ZERO32,
    ZEROID               = ZEROID,
    ERR                  = ERR,

-- exported functions

    db_ns                  = db_ns,
    update_message         = update_message,
    get_more_message       = get_more_message,
    delete_message         = delete_message,
    query_message          = query_message,
    insert_message         = insert_message,
    kill_cursors_message   = kill_cursors_message,
    recv_message           = recv_message,
    send_message           = send_message,
    send_message_with_safe = send_message_with_safe,
}
