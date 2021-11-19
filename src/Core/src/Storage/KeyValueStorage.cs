// This file is part of ArmoniK project.
// 
// Copyright (c) ANEO. All rights reserved.
//   W. Kirschenmann <wkirschenmann@aneo.fr>

using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using System.Web;

using Google.Protobuf;

using JetBrains.Annotations;

namespace ArmoniK.Core.Storage
{
  [PublicAPI]
  public class KeyValueStorage<TKey, TValue> 
    where TValue: IMessage<TValue>, new()
    where TKey:IMessage<TKey>, new()
  {
    private static readonly MessageParser<TKey>   KeyParser   = new(() => new TKey());
    private static readonly MessageParser<TValue> ValueParser = new(() => new TValue());
    private readonly        IObjectStorage        objectStorage_;
    private readonly        string                keyPrefix_;
    
    public KeyValueStorage(IObjectStorage objectStorage, string keyPrefix)
    {
      objectStorage_ = objectStorage;
      keyPrefix_     = keyPrefix;
    }

    public string SerializeKey(TKey key) => $"{keyPrefix_}{HttpUtility.UrlEncode(key.ToByteArray())}";

    public TKey DeserializeKey(string stringKey)
    {
      var cleanedKey = HttpUtility.UrlDecodeToBytes(stringKey[keyPrefix_.Length..]);

      var key = KeyParser.ParseFrom(cleanedKey);
      return key;
    }

    public async Task<TValue> GetOrAddAsync(TKey key, TValue value, CancellationToken cancellationToken = default)
    {
      var serializedKey    = SerializeKey(key);
      var serializedValue  = value.ToByteArray();
      var serializedOutput = await objectStorage_.GetOrAddAsync(serializedKey, serializedValue, cancellationToken);
      return ValueParser.ParseFrom(serializedOutput);
    }


    public Task AddOrUpdateAsync(TKey key, TValue value, CancellationToken cancellationToken = default)
    {
      var serializedKey    = SerializeKey(key);
      var serializedValue  = value.ToByteArray();
      return objectStorage_.AddOrUpdateAsync(serializedKey, serializedValue, cancellationToken);
    }

    public async Task<TValue> TryGetValuesAsync(TKey key, CancellationToken cancellationToken = default)
    {
      var serializedKey    = SerializeKey(key);
      var serializedOutput = await objectStorage_.TryGetValuesAsync(serializedKey, cancellationToken);
      return ValueParser.ParseFrom(serializedOutput);
    }

    public Task<bool> TryDeleteAsync(TKey key, CancellationToken cancellationToken = default)
    {
      var serializedKey    = SerializeKey(key);
      return objectStorage_.TryDeleteAsync(serializedKey, cancellationToken);
    }
    
  }
}
