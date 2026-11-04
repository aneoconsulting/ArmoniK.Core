// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2026. All rights reserved.
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
using System.Collections.Generic;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace ArmoniK.Core.Common.Enforcement;

/// <summary>
///   Reads the client certificate from an <c>X-Forwarded-Client-Cert</c> header written as Envoy does, in its text or
///   JSON format (https://www.envoyproxy.io/docs/envoy/latest/configuration/http/http_conn_man/headers)
/// </summary>
/// <remarks>
///   The header must hold a single element: each proxy appends its own, and the first one of several may have been
///   written by the client itself, so the ingress verifying the certificate must reset the header. The certificate is
///   taken from <c>Cert</c>, never from <c>Chain</c>, and must match <c>Hash</c> when present.
/// </remarks>
public static class ForwardedClientCertificate
{
  private static readonly JsonSerializerOptions JsonOptions = new()
                                                              {
                                                                AllowDuplicateProperties = false,
                                                              };

  /// <summary>
  ///   Read the client certificate from the header
  /// </summary>
  /// <param name="header">Value of the header</param>
  /// <returns>The client certificate</returns>
  /// <exception cref="FormatException">The header is malformed, holds several elements, or no valid certificate</exception>
  public static X509Certificate2 Parse(string header)
  {
    var value = header.Trim();
    var elements = value.StartsWith('[') && value.EndsWith(']')
                     ? ParseJson(value)
                     : ParseText(value);

    if (elements.Count != 1)
    {
      throw new FormatException($"The forwarded client certificate header holds {elements.Count} elements, exactly one is required");
    }

    return Certificate(elements[0]);
  }

  private static List<Element> ParseJson(string value)
  {
    try
    {
      var elements = JsonSerializer.Deserialize<List<Element?>>(value,
                                                                JsonOptions) ?? [];
      return elements.ConvertAll(element => element ?? throw new FormatException("The forwarded client certificate header holds a null element"));
    }
    catch (JsonException e)
    {
      throw new FormatException("The forwarded client certificate header is not valid JSON",
                                e);
    }
  }

  private static List<Element> ParseText(string value)
  {
    var elements = new List<Element>();
    var current  = new Element();
    var position = 0;

    while (true)
    {
      var key = ReadKey(value,
                        ref position);
      var pair = ReadValue(value,
                           ref position);

      if (key.Equals("Hash",
                     StringComparison.OrdinalIgnoreCase))
      {
        current.Hash = current.Hash is null
                         ? pair
                         : throw new FormatException("The forwarded client certificate header repeats Hash in an element");
      }
      else if (key.Equals("Cert",
                          StringComparison.OrdinalIgnoreCase))
      {
        // Only the text format URL-encodes the certificate
        current.Cert = current.Cert is null
                         ? Uri.UnescapeDataString(pair)
                         : throw new FormatException("The forwarded client certificate header repeats Cert in an element");
      }

      if (position == value.Length)
      {
        elements.Add(current);
        return elements;
      }

      if (value[position] == ',')
      {
        elements.Add(current);
        current = new Element();
      }

      // Skip the delimiter, checked by ReadValue to be ',' or ';'
      position++;
    }
  }

  private static string ReadKey(string  value,
                                ref int position)
  {
    var start = position;
    while (position < value.Length && value[position] is not ('=' or ';' or ',' or '"'))
    {
      position++;
    }

    if (position == value.Length || value[position] != '=' || position == start)
    {
      throw new FormatException($"The forwarded client certificate header has a malformed pair at position {start}");
    }

    var key = value[start..position];
    position++;
    return key;
  }

  private static string ReadValue(string  value,
                                  ref int position)
  {
    var start = position;

    if (position < value.Length && value[position] == '"')
    {
      var builder = new StringBuilder();
      position++;
      while (true)
      {
        if (position == value.Length)
        {
          throw new FormatException($"The forwarded client certificate header has an unterminated quoted value at position {start}");
        }

        var c = value[position];
        if (c == '"')
        {
          position++;
          break;
        }

        // Envoy writes Subject and Issuer as RFC 2253 names, in which a backslash and a quote are escaped by a backslash
        if (c == '\\' && position + 1 < value.Length && value[position + 1] is '"' or '\\')
        {
          position++;
          c = value[position];
        }

        builder.Append(c);
        position++;
      }

      if (position < value.Length && value[position] is not (';' or ','))
      {
        throw new FormatException($"The forwarded client certificate header has a character after a quoted value at position {position}");
      }

      return builder.ToString();
    }

    while (position < value.Length && value[position] is not (';' or ','))
    {
      // The key ends at the first '=', so one in the value is unambiguous: Envoy writes By and URI unquoted
      if (value[position] == '"')
      {
        throw new FormatException($"The forwarded client certificate header has an unquoted value with '{value[position]}' at position {position}");
      }

      position++;
    }

    return value[start..position];
  }

  private static X509Certificate2 Certificate(Element element)
  {
    if (string.IsNullOrEmpty(element.Cert))
    {
      throw new FormatException("The forwarded client certificate header carries no Cert");
    }

    var pem = element.Cert;
    // Exactly one PEM block, labelled CERTIFICATE, with nothing else around it
    if (!PemEncoding.TryFind(pem,
                             out var fields) || pem[fields.Label] != "CERTIFICATE" || !string.IsNullOrWhiteSpace(pem[..fields.Location.Start]) ||
        !string.IsNullOrWhiteSpace(pem[fields.Location.End..]))
    {
      throw new FormatException("The forwarded client certificate is not a single PEM certificate");
    }

    var der = Convert.FromBase64String(pem[fields.Base64Data]);

    if (element.Hash is not null && !element.Hash.Equals(Convert.ToHexString(SHA256.HashData(der)),
                                                         StringComparison.OrdinalIgnoreCase))
    {
      throw new FormatException("The forwarded client certificate does not match its Hash");
    }

    try
    {
      return X509CertificateLoader.LoadCertificate(der);
    }
    catch (CryptographicException e)
    {
      throw new FormatException("The forwarded client certificate cannot be read",
                                e);
    }
  }

  /// <summary>
  ///   The fields of an element read by the control plane, the others are ignored
  /// </summary>
  private sealed class Element
  {
    [JsonPropertyName("hash")]
    public string? Hash { get; set; }

    [JsonPropertyName("cert")]
    public string? Cert { get; set; }
  }
}
