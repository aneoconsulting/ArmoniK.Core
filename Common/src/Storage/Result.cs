// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2022. All rights reserved.
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

using System;

namespace ArmoniK.Core.Common.Storage;

/// <summary>
/// 
/// </summary>
/// <param name="SessionId">Id of the session that produces and consumes this data</param>
/// <param name="Key">Key to reference and access this result</param>
/// <param name="OwnerTaskId">Id of the task that is responsible of generating this result.</param>
/// <param name="OriginDispatchId">Id of the task that is responsible of generating this result.</param>
/// <param name="IsResultAvailable">if <value>true</value>, the result is available</param>
/// <param name="CreationDate">Date of creation of the current object.</param>
/// <param name="Data">Data for the current <paramref name="Key"/></param>
public record Result(string   SessionId,
                     string   Key,
                     string   OwnerTaskId,
                     string   OriginDispatchId,
                     bool     IsResultAvailable,
                     DateTime CreationDate,
                     byte[]   Data);

