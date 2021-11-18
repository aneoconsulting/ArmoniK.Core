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

    public string SerializeKey(TKey key) => $"{keyPrefix_}_{HttpUtility.UrlEncode(key.ToByteArray())}";

    public TKey DeserializeKey(string stringKey)
    {
      var cleanedKey = HttpUtility.UrlDecodeToBytes(stringKey[(keyPrefix_.Length + 1)..]);

      var key = KeyParser.ParseFrom(cleanedKey);
      return key;
    }

    private IEnumerable<(string serializedKey, byte[] serializedVal)> SerializeValuesAsync(IEnumerable<(TKey, TValue)> values)
      => values.Select(tuple =>
      {
        var (key, value) = tuple;

        var stringKey = SerializeKey(key);
        var serializedVal = value.ToByteArray();
        return (serializedKey: stringKey, serializedVal);
      });

    private async IAsyncEnumerable<(TKey, TValue)> DeserializeValuesAsync(IAsyncEnumerable<(string, byte[])>         asyncEnum,
                                                                          [EnumeratorCancellation] CancellationToken cancellationToken)
    {
      await foreach (var (stringKey, serializedValue) in asyncEnum.WithCancellation(cancellationToken))
      {
        var key = DeserializeKey(stringKey);

        var value = ValueParser.ParseFrom(serializedValue);

        yield return (key, value);
      }
    }

    public IAsyncEnumerable<(TKey, TValue)> GetOrAddAsync(IEnumerable<(TKey, TValue)> values,
                                                         CancellationToken           cancellationToken = default)
    {
      var serializedValues = SerializeValuesAsync(values);

      var asyncEnum = objectStorage_.GetOrAddAsync(serializedValues, cancellationToken);

      return DeserializeValuesAsync(asyncEnum, cancellationToken);
    }



    public Task AddOrUpdateAsync(IEnumerable<(TKey, TValue)> values, CancellationToken cancellationToken = default)
    {
      var serializedValues = SerializeValuesAsync(values);

      return objectStorage_.AddOrUpdateAsync(serializedValues, cancellationToken);
    }

    public IAsyncEnumerable<(TKey, TValue)> TryGetValuesAsync(IEnumerable<TKey> keys, CancellationToken cancellationToken = default)
    {
      var asyncEnum = objectStorage_.TryGetValuesAsync(keys.Select(SerializeKey), cancellationToken);

      return DeserializeValuesAsync(asyncEnum, cancellationToken);
    }

    public Task TryDeleteAsync(IEnumerable<TKey> keys, CancellationToken cancellationToken = default) 
      => objectStorage_.TryDeleteAsync(keys.Select(SerializeKey), cancellationToken);
  }
}
