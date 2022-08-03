// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2022. All rights reserved.
//   W. Kirschenmann   <wkirschenmann@aneo.fr>
//   J. Gurhem         <jgurhem@aneo.fr>
//   D. Dubuc          <ddubuc@aneo.fr>
//   L. Ziane Khodja   <lzianekhodja@aneo.fr>
//   F. Lemaitre       <flemaitre@aneo.fr>
//   S. Djebbar        <sdjebbar@aneo.fr>
//   J. Fonseca        <jfonseca@aneo.fr>
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

using System;

using ArmoniK.Core.Common.Injection;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Adapters.Amqp;

// ReSharper disable once ClassNeverInstantiated.Global
public class SessionProvider : ProviderBase<SessionAmqp>
{
  /// <inheritdoc />
  public SessionProvider(Options.Amqp options,
                         ILogger      logger)
    : base(async () =>
           {
             var session = new SessionAmqp(options,
                                           logger);
             await session.OpenConnection()
                          .ConfigureAwait(false);

             return session;
           })
  {
    if (string.IsNullOrEmpty(options.Host))
    {
      throw new ArgumentNullException(nameof(options),
                                      $"Contains a null or empty {nameof(options.Host)} field");
    }

    if (options.MaxRetries == 0)
    {
      throw new ArgumentNullException(nameof(options),
                                      $"Contains a zero {nameof(options.MaxRetries)} field");
    }

    if (options.Port == 0)
    {
      throw new ArgumentNullException(nameof(options),
                                      $"Contains a zero {nameof(options.Port)} field");
    }

    if (string.IsNullOrEmpty(options.User))
    {
      throw new ArgumentNullException(nameof(options),
                                      $"Contains a null or empty {nameof(options.User)} field");
    }

    if (string.IsNullOrEmpty(options.Password))
    {
      throw new ArgumentNullException(nameof(options),
                                      $"Contains a null or empty {nameof(options.Password)} field");
    }

    if (string.IsNullOrEmpty(options.Scheme))
    {
      throw new ArgumentNullException(nameof(options),
                                      $"Contains a null or empty {nameof(options.Scheme)} field");
    }
  }
}
