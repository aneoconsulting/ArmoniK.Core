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

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Core.Common.Storage;

using Output = ArmoniK.Api.gRPC.V1.Output;

namespace ArmoniK.Core.Common.gRPC.Convertors;

public static class OutputExt
{
  /// <summary>
  ///   Convert the <see cref="Storage.Output" /> to <see cref=" Api.gRPC.V1.Output" />
  /// </summary>
  /// <param name="output">The object to convert</param>
  public static Output ToGrpcOutput(this Storage.Output output)
  {
    if (output.Status == OutputStatus.Success)
    {
      return new Output
             {
               Ok = new Empty(),
             };
    }

    return new Output
           {
             Error = new Output.Types.Error
                     {
                       Details = output.Error,
                     },
           };
  }

  /// <summary>
  ///   Convert the <see cref=" Api.gRPC.V1.Output" /> to <see cref="Storage.Output" />
  /// </summary>
  /// <param name="output">The object to convert</param>
  public static Storage.Output ToInternalOutput(this Output output)
    => output.TypeCase switch
       {
         Output.TypeOneofCase.Ok => new Storage.Output(OutputStatus.Success,
                                                       ""),
         _ => new Storage.Output(OutputStatus.Error,
                                 output.Error.Details),
       };
}
