// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2021. All rights reserved.
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

using Google.Protobuf;

using MongoDB.Bson.Serialization;

namespace ArmoniK.Adapters.MongoDB.Table
{
  public class BsonProtoSerializer<T> : IBsonSerializer<T> where T : IMessage<T>, new()
  {
    /// <inheritdoc />
    object IBsonSerializer.Deserialize(BsonDeserializationContext context, BsonDeserializationArgs args)
      => Deserialize(context,
                     args);

    /// <inheritdoc />
    public void Serialize(BsonSerializationContext context, BsonSerializationArgs args, T value)
    {
      context.Writer.WriteString(value.ToString());
    }

    /// <inheritdoc />
    public T Deserialize(BsonDeserializationContext context, BsonDeserializationArgs args)
    {
      var parser = new JsonParser(JsonParser.Settings.Default);
      return parser.Parse<T>(context.Reader.ReadString());
    }

    /// <inheritdoc />
    public void Serialize(BsonSerializationContext context, BsonSerializationArgs args, object value)
    {
      if (value is T t)
        Serialize(context,
                  args,
                  t);
      else
        throw new("Not supported type");
    }

    /// <inheritdoc />
    public Type ValueType => typeof(T);
  }
}
