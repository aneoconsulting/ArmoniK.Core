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

using System;
using System.Runtime.Serialization;

namespace ArmoniK.Core.Common.Exceptions;

/// <summary>
///   Base exception for exceptions in ArmoniK core
/// </summary>
[Serializable]
public class ArmoniKException : Exception
{
  /// <summary>
  ///   Initializes a new instance of the <see cref="ArmoniKException" />
  /// </summary>
  public ArmoniKException()
  {
  }

  /// <summary>
  ///   Initializes a new instance of the <see cref="ArmoniKException" /> with the specified error message
  /// </summary>
  /// <param name="message">The error message</param>
  public ArmoniKException(string message)
    : base(message)
  {
  }

  /// <summary>
  ///   Initializes a new instance of the <see cref="ArmoniKException" /> with the specified error message and an exception
  /// </summary>
  /// <param name="message">The error message</param>
  /// <param name="innerException">The inner exception that triggered this exception</param>
  public ArmoniKException(string    message,
                          Exception innerException)
    : base(message,
           innerException)
  {
  }

  /// <summary>
  ///   Initializes a new instance of the <see cref="ArmoniKException" /> with serialized data
  /// </summary>
  /// <param name="info">Information for the perform the serialization</param>
  /// <param name="context">Context passed to the Stream</param>
  protected ArmoniKException(SerializationInfo info,
                             StreamingContext  context)
    : base(info,
           context)
  {
  }
}
