// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2021. All rights reserved.
//   W. Kirschenmann   <wkirschenmann@aneo.fr>
//   J. Gurhem         <jgurhem@aneo.fr>
//   D. Dubuc          <ddubuc@aneo.fr>
//   L. Ziane Khodja   <lzianekhodja@aneo.fr>
//   F. Lemaitre       <flemaitre@aneo.fr>
//   S. Djebbar        <sdjebbar@aneo.fr>
// 
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published
// by the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
// 
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
// 
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System.Threading;
using System.Threading.Tasks;
using System.Web;

using Google.Protobuf;

using JetBrains.Annotations;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Common.Storage;

[PublicAPI]
public class KeyValueStorage<TKey, TValue> : IInitializable
  where TValue : IMessage<TValue>, new()
  where TKey : IMessage<TKey>, new()
{
  public static readonly MessageParser<TKey>                    KeyParser   = new(() => new());
  public static readonly MessageParser<TValue>                  ValueParser = new(() => new());
  private readonly       string                                 keyPrefix_;
  private readonly       ILogger<KeyValueStorage<TKey, TValue>> logger_;
  private readonly       IObjectStorage                         objectStorage_;

  public KeyValueStorage(IObjectStorage objectStorage, ILogger<KeyValueStorage<TKey, TValue>> logger)
  {
    keyPrefix_     = $"{typeof(TKey).Name}{typeof(TValue)}";
    objectStorage_ = objectStorage;
    logger_        = logger;
  }

  public string SerializeKey(TKey key) => $"{keyPrefix_}{HttpUtility.UrlEncode(key.ToByteArray())}";

  public TKey DeserializeKey(string stringKey)
  {
    var cleanedKey = HttpUtility.UrlDecodeToBytes(stringKey[keyPrefix_.Length..]);

    var key = KeyParser.ParseFrom(cleanedKey);
    return key;
  }


  public Task AddOrUpdateAsync(TKey key, TValue value, CancellationToken cancellationToken = default)
  {
    var       serializedKey   = SerializeKey(key);
    using var _               = logger_.LogFunction(serializedKey);
    var       serializedValue = value.ToByteArray();
    return objectStorage_.AddOrUpdateAsync(serializedKey,
                                           serializedValue,
                                           cancellationToken);
  }

  public async Task<TValue> GetValuesAsync(TKey key, CancellationToken cancellationToken = default)
  {
    var       serializedKey = SerializeKey(key);
    using var _             = logger_.LogFunction(serializedKey);
    var serializedOutput = await objectStorage_.GetValuesAsync(serializedKey,
                                                                  cancellationToken);
    return ValueParser.ParseFrom(serializedOutput);
  }

  public Task DeleteAsync(TKey key, CancellationToken cancellationToken = default)
  {
    var       serializedKey = SerializeKey(key);
    using var _             = logger_.LogFunction(serializedKey);
    return objectStorage_.DeleteAsync(serializedKey,
                                         cancellationToken);
  }

  /// <inheritdoc />
  public Task Init(CancellationToken cancellationToken)
    => objectStorage_.Init(cancellationToken);

  /// <inheritdoc />
  public ValueTask<bool> Check(HealthCheckTag tag) => objectStorage_.Check(tag);
}