// This file is part of ArmoniK project.
// 
// Copyright (c) ANEO. All rights reserved.
//   W. Kirschenmann <wkirschenmann@aneo.fr>

using Amqp;

using ArmoniK.Adapters.Amqp.Options;
using ArmoniK.Core.Injection;

using Microsoft.Extensions.Options;

namespace ArmoniK.Adapters.Amqp
{
  // ReSharper disable once ClassNeverInstantiated.Global
  public class SessionProvider : ProviderBase<Session>
  {
    /// <inheritdoc />
    public SessionProvider(IOptions<Options.Amqp> options)
      : base(async () =>
      {
        var connection = await global::Amqp.Connection.Factory.CreateAsync(new global::Amqp.Address(options.Value.Address));
        return new global::Amqp.Session(connection);
      })
    {
    }
  }
}