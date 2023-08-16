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

namespace ArmoniK.Core.Common.Auth.Authentication;

/// <summary>
///   Certificate(s) object in database
/// </summary>
/// <param name="AuthId">Unique Id of the entry</param>
/// <param name="UserId">Id of the user this entry refers to</param>
/// <param name="Cn">Common Name of the certificate(s)</param>
/// <param name="Fingerprint">
///   fingerprint of the certificate. If null, this entry matches with every certificates
///   matching the Common Name
/// </param>
public record AuthData(string  AuthId,
                       string  UserId,
                       string  Cn,
                       string? Fingerprint);
