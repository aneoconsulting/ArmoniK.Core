// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2024. All rights reserved.
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
using System.Net.Http;
using System.Threading.Tasks;

namespace ArmoniK.Core.HealthChecker;

public static class Program
{
  public static async Task<int> Main(string[] args)
  {
    try
    {
      string uri;
      switch (args.Length)
      {
        case 0:
          uri = "http://localhost:1080/liveness";
          break;
        case 1:
          uri = args[0];
          break;
        default:
          Console.WriteLine("Too many arguments");
          return 1;
      }

      var client = new HttpClient();
      var response = await client.GetStringAsync(uri)
                                 .ConfigureAwait(false);

      switch (response)
      {
        case "Healthy":
          return 0;
        default:
          Console.WriteLine("Received:" + response);
          return 1;
      }
    }
    catch (Exception e)
    {
      Console.WriteLine(e);
      return 1;
    }
  }
}
