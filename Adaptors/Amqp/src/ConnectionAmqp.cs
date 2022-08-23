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

using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;

using Amqp;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Adapters.Amqp;

public class ConnectionAmqp : IConnectionAmqp
{
  private readonly ILogger logger_;

  private readonly Options.Amqp options_;

  public ConnectionAmqp(Options.Amqp options,
                        ILogger      logger)
  {
    logger_  = logger;
    options_ = options;
  }

  public Connection? Connection { get; set; }

  public async Task<IConnectionAmqp> OpenConnectionAsync()
  {
    logger_.LogInformation("Get address for session");
    var address = new Address(options_.Host,
                              options_.Port,
                              options_.User,
                              options_.Password,
                              scheme: options_.Scheme);

    var connectionFactory = new ConnectionFactory();
    if (options_.Scheme.Equals("AMQPS"))
    {
      connectionFactory.SSL.RemoteCertificateValidationCallback = delegate(object           _,
                                                                           X509Certificate? _,
                                                                           X509Chain?       _,
                                                                           SslPolicyErrors  errors)
                                                                  {
                                                                    switch (errors)
                                                                    {
                                                                      case SslPolicyErrors.RemoteCertificateNameMismatch when options_.AllowHostMismatch:
                                                                      case SslPolicyErrors.None:
                                                                        return true;
                                                                      default:
                                                                        logger_.LogError("SSL error : {error}",
                                                                                         errors);
                                                                        return false;
                                                                    }
                                                                  };
    }

    Connection = await connectionFactory.CreateAsync(address)
                                        .ConfigureAwait(false);

    return this;
  }
}
