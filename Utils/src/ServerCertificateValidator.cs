// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2024. All rights reserved.
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
using System.Linq;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;

using Microsoft.Extensions.Logging;



namespace ArmoniK.Core.Utils;


public static class CertificateValidator
{
    public static bool ValidateServerCertificate(object sender, X509Certificate certificate, X509Chain certChain, SslPolicyErrors sslPolicyErrors, X509Certificate2 authority, ILogger logger)
    {
        // If there is any error other than untrusted root or partial chain, fail the validation
        if ((sslPolicyErrors & ~SslPolicyErrors.RemoteCertificateChainErrors) != 0)
        {
            logger.LogDebug("SSL validation failed with errors: {sslPolicyErrors}", sslPolicyErrors);
            return false;
        }

        if (certificate == null)
        {
            logger.LogDebug("Certificate is null!");
            return false;
        }

        if (certChain == null)
        {
            logger.LogDebug("Certificate chain is null!");
            return false;
        }

        // If there is any error other than untrusted root or partial chain, fail the validation
        if (certChain.ChainStatus.Any(status => status.Status is not X509ChainStatusFlags.UntrustedRoot and not X509ChainStatusFlags.PartialChain))
        {
            logger.LogDebug("SSL validation failed with chain status: {chainStatus}", certChain.ChainStatus);
            return false;
        }

        var cert = new X509Certificate2(certificate);
        certChain.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck;
        certChain.ChainPolicy.VerificationFlags = X509VerificationFlags.AllowUnknownCertificateAuthority;

        certChain.ChainPolicy.ExtraStore.Add(authority);
        if (!certChain.Build(cert))
        {
            return false;
        }

        var isTrusted = certChain.ChainElements.Any(x => x.Certificate.Thumbprint == authority.Thumbprint);
        if (isTrusted)
        {
            logger.LogInformation("SSL validation succeeded");
        }
        else
        {
            logger.LogInformation("SSL validation failed with errors: {sslPolicyErrors}", sslPolicyErrors);
        }
        return isTrusted;
    }
}
