// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2023. All rights reserved.
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

using System.Text;
using System.Text.Json;

namespace ArmoniK.Extensions.Common.StreamWrapper.Tests.Common;

public class TestPayload
{
  public enum TaskType
  {
    Result,
    Undefined,
    None,
    Compute,
    Error,
    Transfer,
    DatadepTransfer,
    DatadepCompute,
    ReturnFailed,
    PayloadCheckSum,
  }

  public byte[]? DataBytes { get; set; }

  public TaskType Type { get; set; }

  public string? ResultKey { get; set; }

  public byte[] Serialize()
  {
    var jsonString = JsonSerializer.Serialize(this);
    return Encoding.ASCII.GetBytes(StringToBase64(jsonString));
  }

  public static TestPayload? Deserialize(byte[]? payload)
  {
    if (payload == null || payload.Length == 0)
    {
      return new TestPayload
             {
               Type = TaskType.Undefined,
             };
    }

    var str = Encoding.ASCII.GetString(payload);
    return JsonSerializer.Deserialize<TestPayload>(Base64ToString(str));
  }

  private static string StringToBase64(string serializedJson)
  {
    var serializedJsonBytes       = Encoding.UTF8.GetBytes(serializedJson);
    var serializedJsonBytesBase64 = Convert.ToBase64String(serializedJsonBytes);
    return serializedJsonBytesBase64;
  }

  private static string Base64ToString(string base64)
  {
    var c = Convert.FromBase64String(base64);
    return Encoding.ASCII.GetString(c);
  }
}
