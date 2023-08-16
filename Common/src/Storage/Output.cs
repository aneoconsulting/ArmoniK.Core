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

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.gRPC.V1.Tasks;

namespace ArmoniK.Core.Common.Storage;

/// <summary>
///   Record encoding if a task successfully produced an output
/// </summary>
/// <param name="Success">xWhether the task is successful</param>
/// <param name="Error">Error message if task is not successful</param>
public record Output(bool   Success,
                     string Error)
{
  /// <summary>
  ///   Convert the <see cref="Output" /> to <see cref=" Api.gRPC.V1.Output" />
  /// </summary>
  /// <param name="output">The object to convert</param>
  public static implicit operator Api.gRPC.V1.Output(Output output)
  {
    if (output.Success)
    {
      return new Api.gRPC.V1.Output
             {
               Ok = new Empty(),
             };
    }

    return new Api.gRPC.V1.Output
           {
             Error = new Api.gRPC.V1.Output.Types.Error
                     {
                       Details = output.Error,
                     },
           };
  }

  /// <summary>
  ///   Convert the <see cref=" Api.gRPC.V1.Output" /> to <see cref="Output" />
  /// </summary>
  /// <param name="output">The object to convert</param>
  public static implicit operator Output(Api.gRPC.V1.Output output)
    => output.TypeCase switch
       {
         Api.gRPC.V1.Output.TypeOneofCase.Ok => new Output(true,
                                                           ""),
         _ => new Output(false,
                         output.Error.Details),
       };

  /// <summary>
  ///   Convert the <see cref="Output" /> to <see cref="TaskDetailed.Types.Output" />
  /// </summary>
  /// <param name="output">The object to convert</param>
  public static implicit operator TaskDetailed.Types.Output(Output output)
    => new()
       {
         Error   = output.Error,
         Success = output.Success,
       };

  /// <summary>
  ///   Convert the <see cref="TaskDetailed.Types.Output" /> to <see cref="Output" />
  /// </summary>
  /// <param name="output">The object to convert</param>
  public static implicit operator Output(TaskDetailed.Types.Output output)
    => new(output.Success,
           output.Error);
}
