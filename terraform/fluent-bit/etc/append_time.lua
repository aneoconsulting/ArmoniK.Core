function add_timestamp(tag, timestamp, record)
    if (record["@t"] == nil) then
        new_record = record
        new_record["@t"] = os.date("%Y-%m-%dT%H:%M:%S.%sZ")
        return 2, timestamp, new_record
    else
        return 0, timestamp, record
    end
end
