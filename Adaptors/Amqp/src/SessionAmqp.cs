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
// but WITHOUT ANY WARRANTY, without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;
using System.Net.Http.Headers;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;

using Amqp;
using Amqp.Framing;

using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Adapters.Amqp;

public class SessionAmqp : ISessionAmqp
{
  private int retriesReconnect_;

  public SessionAmqp(Options.Amqp options,
                     ILogger      logger)
  {
    Options           = options;
    Logger            = logger;
    retriesReconnect_ = options.MaxRetries;
  }

  public Session      Session { get; set; }
  public Options.Amqp Options { get; set; }

  public ILogger Logger { get; set; }

  private static void OnCloseConnection(IAmqpObject sender,
                                        Error?       error,
                                        ILogger     logger)
  {
    if (error == null)
    {
      logger.LogInformation("AMQP Connection closed with no error");
    }
    else
    {
      logger.LogWarning("AMQP Connection closed with error: {0}",
                        error.ToString());
    }
  }

  public async Task<ISessionAmqp> OpenConnection()
  {
    Logger.LogInformation("Opening session");
    if (Session is not null && Session.SessionState == SessionState.Opened)
    {
      Logger.LogInformation("A session is open; close it");

      await Session.CloseAsync()
                   .ConfigureAwait(false);
      if (retriesReconnect_ <= 0)
      {
        throw new AmqpException(new Error($"After {Options.MaxRetries} retries. Fail to reconnect to Amqp"));
      }

      --retriesReconnect_;
    }

    Logger.LogInformation("Get address for session");
    var address = new Address(Options.Host,
                              Options.Port,
                              Options.User,
                              Options.Password,
                              scheme: Options.Scheme);

    var connectionFactory = new ConnectionFactory();
    if (Options.Scheme.Equals("AMQPS"))
    {
      connectionFactory.SSL.RemoteCertificateValidationCallback = delegate(object          sender,
                                                                           X509Certificate certificate,
                                                                           X509Chain       chain,
                                                                           SslPolicyErrors errors)
                                                                  {
                                                                    switch (errors)
                                                                    {
                                                                      case SslPolicyErrors.RemoteCertificateNameMismatch when Options.AllowHostMismatch:
                                                                      case SslPolicyErrors.None:
                                                                        return true;
                                                                      default:
                                                                        Logger.LogError("SSL error : {error}",
                                                                                        errors);
                                                                        return false;
                                                                    }
                                                                  };
    }

    var retry = 0;
    for(; retry < Options.MaxRetries; retry++)
    {
      try
      {
        var connection = await connectionFactory.CreateAsync(address)
                                                .ConfigureAwait(false);
        connection.AddClosedCallback((x,
                                      e) => OnCloseConnection(x,
                                                              e,
                                                              Logger));
        Session = new Session(connection);
        break;
      }
      catch
      {
        Logger.LogInformation("Retrying to create connection");
        Thread.Sleep(1000 * retry);
      }
    }

    if (retry == Options.MaxRetries)
    {
      throw new TimeoutException($"{nameof(Options.MaxRetries)} reached");
    }

    return this;
  }

  public HealthCheckResult Check()
  {
    if (Session == null || (Session.SessionState != SessionState.Opened && retriesReconnect_ >= 0))
    {
      OpenConnection()
        .ConfigureAwait(false);

      return HealthCheckResult.Healthy();
    }

    if (Session.SessionState == SessionState.Opened)
    {
      return HealthCheckResult.Healthy();
    }

    return retriesReconnect_ <= 0
             ? HealthCheckResult.Unhealthy()
             : HealthCheckResult.Healthy();
  }
}
