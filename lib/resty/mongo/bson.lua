-- bson encoder/decoder

require("resty.mongo.support")
module(...,package.seeall)
local assert , error = assert , error
local pairs = pairs
local getmetatable = getmetatable
local type = type
local tonumber , tostring = tonumber , tostring
local t_insert = table.insert
local t_concat = table.concat
local strformat = string.format
local strmatch = string.match
local util = require('resty.mongo.util')
local le_uint_to_num = util.le_uint_to_num
local le_int_to_num = util.le_int_to_num
local num_to_le_uint = util.num_to_le_uint
local from_double = util.from_double
local to_double = util.to_double
local read_terminated_string = util.read_terminated_string
local new_str_buffer = util.new_str_buffer

local oid = require('resty.mongo.object_id')
local new_object_id = oid.new
local object_id_mt = oid.metatable

-- read document from a string buffer
local function read_document( strbuf , numerical )
    local bytes = le_uint_to_num( strbuf(4) )

    local ho , hk , hv = false , false , false
    local t = { }
    while true do
        local op = strbuf(1)
        if op == "\0" then break end

        local e_name = read_terminated_string(strbuf)
        local v
        if op == "\1" then -- Double
            v = from_double(strbuf( 8 ))
        elseif op == "\2" then -- String
            local len = le_uint_to_num( strbuf(4) )
            v = strbuf( len - 1 )
            assert ( strbuf( 1 ) == "\0" )
        elseif op == "\3" then -- Embedded document
            v = read_document(strbuf, false)
        elseif op == "\4" then -- Array
            v = read_document(strbuf, true )
        elseif op == "\5" then -- Binary
            local len = le_uint_to_num(strbuf(4))
            local subtype = strbuf( 1 )
            v = strbuf( len )
        elseif op == "\7" then -- ObjectId
            v = new_object_id(strbuf( 12 ) )
        elseif op == "\8" then -- Boolean
            local f = strbuf( 1 )
            if f == "\0" then
                v = false
            elseif f == "\1" then
                v = true
            else
                error( f:byte() )
            end
        elseif op == "\9" then -- unix time
            v = le_uint_to_num( strbuf( 8 ) , 1 , 8 )
        elseif op == "\10" then -- Null
            v = nil
        elseif op == "\11" then -- Regullar expression
            error( "BSON type:'Regullar expression' not support yet")
        elseif op == "\12" then -- DBPointer — Deprecated
            error( "BSON type:'DBPointer' not support yet")
        elseif op == "\13" then -- JavaScript code
            error( "BSON type:'JavaScript code' not support yet")
        elseif op == "\14" then -- Symbol
            error( "BSON type:'Symbol' not support yet")
        elseif op == "\15" then -- JavaScript code w/ scope
            error( "BSON type:'JavaScript code w/ scope' not support yet")
        elseif op == "\16" then -- int32
            v = le_int_to_num( strbuf(4), 1,8)
        elseif op == "\17" then --timestamp
            error( "BSON type:'Timestamp' not support yet")
        elseif op == "\18" then --int64
            error( "BSON type:'Int64' not support yet")
        else
            error ( "Unknown BSON type" .. strbyte ( op ) )
        end

        if numerical then
            t [ tonumber ( e_name ) ] = v
        else
            t [ e_name ] = v
        end

        -- Check for special universal map
        if e_name == "_keys" then
            hk = v
        elseif e_name == "_vals" then
            hv = v
        else
            ho = true
        end
    end

    if not ho and hk and hv then
        t = { }
            for i=1,#hk do
            t [ hk [ i ] ] = hv [ i ]
        end
    end

    return t
end

local function from_bson_buf( strbuf )
    local t = read_document(strbuf , false)
    return t
end


local function from_bson(str)
    local t = read_document(new_str_buffer(str), false)
    return t
end

local to_bson

local function pack( k , v )
    local ot = type ( v )
    local mt = getmetatable ( v )

    if ot == "number" then
        return "\1" .. k .. "\0" .. to_double ( v )
    elseif ot == "nil" then
        return "\10" .. k .. "\0"
    elseif ot == "string" then
        return "\2" .. k .. "\0" .. num_to_le_uint ( #v + 1 ) .. v .. "\0"
    elseif ot == "boolean" then
        if v == false then
            return "\8" .. k .. "\0\0"
        else
            return "\8" .. k .. "\0\1"
        end
    elseif mt == object_id_mt then
        return "\7" .. k .. "\0" .. v.id
    elseif ot == "table" then
        local doc , array = to_bson ( v )
        if array then
            return "\4" .. k .. "\0" .. doc
        else
            return "\3" .. k .. "\0" .. doc
        end
    else
        error ( "Failure converting " .. ot ..": " .. tostring ( v ) )
    end
end

function to_bson( ob )
    -- Find out if ob if an array; string->value map; or general table
    local onlyarray = true
    local seen_n , high_n = { } , 0
    local onlystring = true
    for k , v in pairs ( ob ) do
        local t_k = type ( k )
        onlystring = onlystring and ( t_k == "string" )
        if onlyarray then
            if t_k == "number" and k >= 0 then
                if k > high_n then
                    high_n = k
                    seen_n [ k ] = v
                end
            else
                onlyarray = false
            end
        end
        if not onlyarray and not onlystring then break end
    end

    local retarray , m = false
    if onlystring then -- Do string first so the case of an empty table is done properly
        local r = { }
        for k , v in pairs ( ob ) do
            t_insert ( r , pack ( k , v ) )
        end
        m = t_concat ( r )
    elseif onlyarray then
        local r = { }

        local low = 1
        if seen_n [ 0 ] then low = 0 end

        for i=1 , high_n do
            r [ i ] = pack ( i , seen_n [ i ] )
        end

        m = t_concat ( r , "" , low , high_n )
        retarray = true
    else
        local ni = 1
        local keys , vals = { } , { }
        for k , v in pairs ( ob ) do
            keys [ ni ] = k
            vals [ ni ] = v
            ni = ni + 1
        end
        return to_bson ( { _keys = keys , _vals = vals } )
    end

    return num_to_le_uint ( #m + 4 + 1 ) .. m .. "\0" , retarray
end

return {
    from_bson     = from_bson,
    from_bson_buf = from_bson_buf,
    to_bson       = to_bson,
}
