using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using Amqp;
using Amqp.Framing;

using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Adapters.Amqp
{
  public class SessionAmqp
  {
    public Session      Session { get; set; }
    public Options.Amqp Options { get; set; }

    public ILogger Logger { get; set; }

    public SessionAmqp(Options.Amqp options,
                       ILogger      logger)
    {
      Options = options;
      Logger  = logger;

    }

    private int retriesReconnect_ = 5;

    private static void OnCloseConnection(IAmqpObject sender,
                                          Error       error,
                                          ILogger     logger)
    {
      if (error == null)
        logger.LogInformation("AMQP Connection closed with no error");
      else
      {
        logger.LogWarning("AMQP Connection closed with error: {0}",
                        error.ToString());
      }
    }

    public async Task<SessionAmqp> OpenConnection()
    {
      if (Session != null)
      {
        await Session.CloseAsync()
                     .ConfigureAwait(false);
        if (retriesReconnect_ <= 0)
          throw new AmqpException(new Error($"After 5 retries. Fail to reconnect to Amqp"));

        --retriesReconnect_;

      }

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

      var retries = 1;
      while (true)
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
          if (++retries == 6)
          {
            throw;
          }

          Thread.Sleep(1000 * retries);
        }
      }

      return this;
    }

    public HealthCheckResult Check()
    {
      if (Session == null || Session.SessionState != SessionState.Opened && retriesReconnect_ >= 0)
      {
        OpenConnection()
          .ConfigureAwait(false);

        return HealthCheckResult.Healthy();
      }

      if (Session.SessionState == SessionState.Opened) return HealthCheckResult.Healthy();

      return retriesReconnect_ <= 0
               ? HealthCheckResult.Unhealthy()
               : HealthCheckResult.Healthy();
    }
  }
}
