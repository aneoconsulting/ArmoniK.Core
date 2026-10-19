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
using System.Buffers;
using System.Globalization;
using System.Text;

namespace ArmoniK.Core.Authorization;

/// <summary>
///   Text that reads as it compares: no invisible, unassigned or private use character, nor surrounding space
/// </summary>
/// <remarks>
///   Checked by code point rather than by UTF-16 unit, so that characters outside the BMP are seen. Normalization is
///   not required: identity providers and certificate authorities may issue decomposed names.
/// </remarks>
internal static class VisibleText
{
  // Default_Ignorable_Code_Point of https://www.unicode.org/Public/UCD/latest/ucd/DerivedCoreProperties.txt, Unicode
  // 18.0.0, adjacent lines merged. It adds invisible characters that are not Cf, such as variation selectors or fillers
  private static readonly (int First, int Last)[] DefaultIgnorable =
  [
    (0x00AD, 0x00AD),
    (0x034F, 0x034F),
    (0x061C, 0x061C),
    (0x115F, 0x1160),
    (0x17B4, 0x17B5),
    (0x180B, 0x180F),
    (0x200B, 0x200F),
    (0x202A, 0x202E),
    (0x2060, 0x206F),
    (0x3164, 0x3164),
    (0xFE00, 0xFE0F),
    (0xFEFF, 0xFEFF),
    (0xFFA0, 0xFFA0),
    (0xFFF0, 0xFFF8),
    (0x1BCA0, 0x1BCA3),
    (0x1D173, 0x1D17A),
    (0xE0000, 0xE0FFF),
  ];

  /// <summary>
  ///   Whether a text is not empty, does not start nor end with a space, and holds only visible characters
  /// </summary>
  /// <param name="text">The text</param>
  /// <returns>Whether the text reads as it compares</returns>
  public static bool IsValid(ReadOnlySpan<char> text)
    => FindProblem(text) is null;

  /// <summary>
  ///   Why a text does not read as it compares
  /// </summary>
  /// <param name="text">The text</param>
  /// <returns>
  ///   What is wrong, worded to follow the text in a sentence (such as "contains U+200B, an invisible character"), or
  ///   <c>null</c> when the text is valid
  /// </returns>
  public static string? FindProblem(ReadOnlySpan<char> text)
  {
    if (text.IsEmpty)
    {
      return "is empty";
    }

    if (char.IsWhiteSpace(text[0]) || char.IsWhiteSpace(text[^1]))
    {
      return "starts or ends with a space";
    }

    while (!text.IsEmpty)
    {
      // Unlike EnumerateRunes, which replaces a lone surrogate by U+FFFD, decoding reports it
      if (Rune.DecodeFromUtf16(text,
                               out var rune,
                               out var consumed) != OperationStatus.Done)
      {
        return $"contains U+{(int)text[0]:X4}, a lone surrogate";
      }

      if (Describe(rune) is { } kind)
      {
        return $"contains U+{rune.Value:X4}, {kind}";
      }

      text = text[consumed..];
    }

    return null;
  }

  private static string? Describe(Rune rune)
    => Rune.GetUnicodeCategory(rune) switch
       {
         UnicodeCategory.Control                                          => "a control character",
         UnicodeCategory.Format                                           => "an invisible character",
         UnicodeCategory.PrivateUse                                       => "a private use character",
         // Follows the Unicode tables of the .NET runtime: a newer character is refused until the runtime knows it
         UnicodeCategory.OtherNotAssigned                                 => "an unassigned character",
         UnicodeCategory.LineSeparator or UnicodeCategory.ParagraphSeparator => "a line or paragraph separator",
         _ when Array.Exists(DefaultIgnorable,
                             range => rune.Value >= range.First && rune.Value <= range.Last) => "an invisible character",
         _ => null,
       };
}
