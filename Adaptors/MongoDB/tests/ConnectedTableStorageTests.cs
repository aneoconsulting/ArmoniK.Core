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

using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Configuration.Memory;

using NUnit.Framework;

namespace ArmoniK.Core.Adapters.MongoDB.Tests;

[TestFixture]
[Category("Integration")]
[Ignore("Require a deployed database")]
internal class ConnectedTableStorageTests
{
  [SetUp]
  public void SetUp()
  {
    Dictionary<string, string> baseConfig = new()
                                            {
                                              {
                                                "MongoDB:ConnectionString", "mongodb://localhost"
                                              },
                                              {
                                                "MongoDB:DatabaseName", "ConnectedTableStorageTests"
                                              },
                                              {
                                                "MongoDB:DataRetention", "10.00:00:00"
                                              },
                                              {
                                                "MongoDB:TableStorage:PollingDelay", "00:00:10"
                                              },
                                              {
                                                "MongoDB:LeaseProvider:AcquisitionPeriod", "00:20:00"
                                              },
                                              {
                                                "MongoDB:LeaseProvider:AcquisitionDuration", "00:50:00"
                                              },
                                              {
                                                "MongoDB:ObjectStorage:ChunkSize", "100000"
                                              },
                                              {
                                                "MongoDB:LockedQueueStorage:LockRefreshPeriodicity", "00:20:00"
                                              },
                                              {
                                                "MongoDB:LockedQueueStorage:PollPeriodicity", "00:00:50"
                                              },
                                              {
                                                "MongoDB:LockedQueueStorage:LockRefreshExtension", "00:50:00"
                                              },
                                            };

    var configSource = new MemoryConfigurationSource
                       {
                         InitialData = baseConfig,
                       };

    var builder = new ConfigurationBuilder().Add(configSource);

    configuration_ = builder.Build();
  }


  // TODO : remove this comment when some tests using configuration_ are written
  [SuppressMessage("CodeQuality",
                   "IDE0052:Remove unread private members",
                   Justification = "<Pending>")]
  private IConfiguration configuration_;
}
