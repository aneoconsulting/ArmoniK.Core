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

using System.IO;
using System.Linq;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Utils;

/// <summary>
///   Provides utilities for validating SSL/TLS certificates.
/// </summary>
public static class CertificateValidator
{
  /// <summary>
  ///   Creates a callback function to validate SSL/TLS certificates during a secure connection.
  /// </summary>
  /// <param name="logger">The logger to use for logging validation details.</param>
  /// <param name="authority">The root certificate authority to trust during validation.</param>
  /// <param name="allowHostMismatch">Ignore HostMismatch error</param>
  /// <returns>
  ///   A <see cref="RemoteCertificateValidationCallback" /> delegate that performs SSL/TLS certificate validation.
  /// </returns>
  public static RemoteCertificateValidationCallback ValidationCallback(ILogger                    logger,
                                                                       X509Certificate2Collection authority,
                                                                       bool                       allowHostMismatch)
    => (sender,
        certificate,
        chain,
        sslPolicyErrors) =>
       {
         if (certificate == null || chain == null)
         {
           logger.LogWarning("Certificate or certificate chain is null");
           return false;
         }

         // If there is a mismatch in the certificate name, we check if we allow it
         if ((sslPolicyErrors & SslPolicyErrors.RemoteCertificateNameMismatch) != 0)
         {
           if (!allowHostMismatch)
           {
             // We don't allow host mismatch, so we fail the validation
             logger.LogWarning("SSL validation failed: certificate name mismatch (errors: {sslPolicyErrors})",
                               sslPolicyErrors);
             return false;
           }

           // We allow host mismatch, so we remove the error
           sslPolicyErrors &= ~SslPolicyErrors.RemoteCertificateNameMismatch;
           logger.LogDebug("Allowing RemoteCertificateNameMismatch since allowHostMismatch=true");
         }

         // If there is any errors other than the chain errors and the mismatch, fail the validation
         if ((sslPolicyErrors & ~SslPolicyErrors.RemoteCertificateChainErrors) != 0)
         {
           logger.LogWarning("SSL validation failed with errors: {sslPolicyErrors}",
                             sslPolicyErrors);
           return false;
         }

         // If there is any error other than untrusted root or partial chain, fail the validation
         if (chain.ChainStatus.Any(status => status.Status is not X509ChainStatusFlags.UntrustedRoot and not X509ChainStatusFlags.PartialChain))
         {
           logger.LogWarning("SSL validation failed with chain status: {ChainStatus}",
                             chain.ChainStatus);
           return false;
         }

         var cert = new X509Certificate2(certificate);
         chain.ChainPolicy.RevocationMode    = X509RevocationMode.NoCheck;
         chain.ChainPolicy.VerificationFlags = X509VerificationFlags.AllowUnknownCertificateAuthority;

         chain.ChainPolicy.ExtraStore.AddRange(authority);
         if (!chain.Build(cert))
         {
           logger.LogWarning("SSL validation failed: unable to build the certificate chain");
           return false;
         }

         var isTrusted = chain.ChainElements.Any(x => authority.Any(ca => x.Certificate.Thumbprint == ca.Thumbprint));
         if (isTrusted)
         {
           logger.LogDebug("SSL validation succeeded: certificate is trusted");
         }
         else
         {
           logger.LogWarning("SSL validation failed: certificate is not trusted");
         }

         return isTrusted;
       };

  /// <summary>
  ///   Creates a certificate validation callback from a Certificate Authority (CA) file.
  /// </summary>
  /// <param name="caFilePath">The file path to the CA certificate.</param>
  /// <param name="allowHostMismatch">Do not consider host mismatch between request and certificate as an error</param>
  /// <param name="logger">The logger to use for logging validation details.</param>
  /// <returns>
  ///   A <see cref="RemoteCertificateValidationCallback" /> delegate that performs SSL/TLS certificate validation.
  /// </returns>
  /// <exception cref="FileNotFoundException">
  ///   Thrown if the specified CA certificate file is not found.
  /// </exception>
  public static RemoteCertificateValidationCallback CreateCallback(string  caFilePath,
                                                                   bool    allowHostMismatch,
                                                                   ILogger logger)
  {
    if (!File.Exists(caFilePath))
    {
      logger.LogError("CA certificate file not found at {path}",
                      caFilePath);
      throw new FileNotFoundException("CA certificate file not found",
                                      caFilePath);
    }

    var content   = File.ReadAllText(caFilePath);
    var authority = new X509Certificate2Collection();
    authority.ImportFromPem(content);
    logger.LogInformation("Loaded CA certificates from file {path}",
                          caFilePath);
    var callback = ValidationCallback(logger,
                                      authority,
                                      allowHostMismatch);
    return callback;
  }
}
