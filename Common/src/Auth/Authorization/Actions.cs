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
//   D. Brasseur       <dbrasseur@aneo.fr>
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

namespace ArmoniK.Core.Common.Auth.Authorization
{
  public static class Actions
  {
    public class Action
    {
      //TODO
    }
    public static string Get(string prefix, string action)
    {
      return prefix + Separator + action;
    }

    public static string Get(string prefix,
                              string action,
                              string suffix)
    {
      return Get(prefix,
                 action) + Separator + suffix;
    }

    public static void Parse(string actionName, out string prefix, out string action, out string? suffix)
    {
      var parts = actionName.Split(Separator);
      if (parts.Length is < 2 or > 3)
      {
        throw new ArgumentOutOfRangeException("Wrong number of parts in action policy string " + actionName);
      }

      prefix = parts[0];
      action = parts[1];
      suffix = parts.Length == 3 ? parts[2] : null;
    }

    public static readonly char   Separator = ':';
    public static readonly string Suffix    = "admin";


    public static class General
    {
      private static string Get(string action)
        => Actions.Get(Prefix, action);
      public static readonly string Prefix                  = "session";
      public static readonly string GetServiceConfiguration = Get("GetServiceConfiguration");
    }
    
    public static class Session
    {
      private static string Get(string action)
        => Actions.Get(Prefix,action);
      private static string GetAdmin(string action)
        => Actions.Get(Prefix, action, Suffix);
      public static readonly string Prefix        = "session";
      public static readonly string CancelSession = Get("CancelSession");
      public static readonly string CreateSession = Get("CreateSession");
      public static readonly string ListSessions  = Get("ListSessions");
    }

    //TODO TO BE COMPLETED
  }
}
