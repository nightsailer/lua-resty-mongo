-- some magick support

if _G._resty_mongo then return end

local getmetatable , setmetatable = getmetatable , setmetatable
local pairs = pairs
local next  = next
do
    -- check support __pairs natively if lua 5.2
    local run = false
    local _t = setmetatable({} , { __pairs = function() run = true end })
    pairs(_t)
    if not supported then
        _G.pairs = function( t )
            local mt = getmetatable(t)
            if mt then
                local f = mt.__pairs
                if f then
                    return f(t)
                end
            end
            return pairs(t)
        end
        _G.pairs(_t)
        assert(run)
    end
    _G.dump = function(v)
        if type(v) == 'table' then
            print(v)
            for _k,_v in _G.pairs(v) do
                print("",_k,_v)
            end
            return
        end
        print("type:",type(v), "value:",v)
    end
end
_G._resty_mongo = true