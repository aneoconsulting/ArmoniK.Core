// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2025. All rights reserved.
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

using JetBrains.Annotations;

namespace ArmoniK.Core.Adapters.QueueCommon;

[PublicAPI]
public class Amqp
{
  public const string SettingSection = nameof(Amqp);

  public string Host              { get; set; } = "";
  public string CredentialsPath   { get; set; } = "";
  public string User              { get; set; } = "";
  public string Password          { get; set; } = "";
  public string Scheme            { get; set; } = "";
  public string CaPath            { get; set; } = "";
  public string PartitionId       { get; set; } = "";
  public int    Port              { get; set; }
  public int    MaxPriority       { get; set; }
  public bool   AllowHostMismatch { get; set; }
  public int    MaxRetries        { get; set; }
  public int    LinkCredit        { get; set; }
  public int    ParallelismLimit  { get; set; }
  public bool   AllowInsecureTls  { get; set; }
}
