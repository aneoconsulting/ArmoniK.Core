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

using System.IO;
using System.Text;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Adapters.Redis;

public class TextWriterLogger : TextWriter
{
  private readonly ILogger       logger_;
  private readonly StringBuilder builder_           = new StringBuilder();
  private          bool          terminatorStarted_ = false;

  public TextWriterLogger(ILoggerFactory log, string loggerName)
  {
    logger_ = log.CreateLogger(loggerName);
  }

  public override void Write(string value)
  {
    logger_.LogDebug(value);
  }

  public override void Write(char value)
  {
    builder_.Append(value);
    if (value == NewLine[0])
      if (NewLine.Length == 1)
        Flush2Log();
      else
        terminatorStarted_ = true;
    else if (terminatorStarted_)
    {
      terminatorStarted_ = NewLine[1] == value;
      if (terminatorStarted_)
      {
        Flush2Log();
      }

    }
  }

  private void Flush2Log()
  {
    if (builder_.Length > NewLine.Length)
      logger_.LogDebug(builder_.ToString());
    builder_.Clear();
    terminatorStarted_ = false;
  }


  public override Encoding Encoding
    => Encoding.Default;
}
