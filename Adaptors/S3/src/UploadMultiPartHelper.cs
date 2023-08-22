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

using Amazon.S3.Model;

namespace ArmoniK.Core.Adapters.S3;

internal class UploadMultiPartHelper
{
  private const long MinPartSize  = 5 * 1024 * 1024; // 5 MiB
  private const int  MaxPartCount = 10000;
  private const long MaxPartSize  = 5L * 1024 * 1024 * 1024; // 5 GiB

  public static async IAsyncEnumerable<UploadPartRequest> PreparePartRequestsAsync(string                                 bucketName,
                                                                                   string                                 objectKey,
                                                                                   string                                 uploadId,
                                                                                   IAsyncEnumerable<ReadOnlyMemory<byte>> valueChunks,
                                                                                   CancellationToken                      cancellationToken)
  {
    var  partNumber        = 1;
    long bytesRead         = 0;
    var  currentPartSize   = MinPartSize;
    var  currentPartStream = new MemoryStream();

    await foreach (var chunk in valueChunks.WithCancellation(cancellationToken)
                                           .ConfigureAwait(false))
    {
      long chunkSize = chunk.Length;
      bytesRead += chunkSize;
      var remainingStream = new MemoryStream();

      if (bytesRead <= MaxPartSize)
      {
        await currentPartStream.WriteAsync(chunk.ToArray(),
                                           cancellationToken)
                               .ConfigureAwait(false);
      }
      else
      {
        var plusSize = bytesRead - MaxPartSize;
        var chunkSlice = chunk.Slice(0,
                                     (int)(chunkSize - plusSize));
        var remainingSlice = chunk.Slice((int)(chunkSize - plusSize),
                                         (int)plusSize);
        await currentPartStream.WriteAsync(chunkSlice.ToArray(),
                                           cancellationToken)
                               .ConfigureAwait(false);
        await remainingStream.WriteAsync(remainingSlice.ToArray(),
                                         cancellationToken)
                             .ConfigureAwait(false);
      }

      if (bytesRead >= currentPartSize && partNumber < MaxPartCount)
      {
        var partRequest = new UploadPartRequest
                          {
                            BucketName  = bucketName,
                            Key         = objectKey,
                            PartNumber  = partNumber,
                            InputStream = new MemoryStream(currentPartStream.ToArray()),
                            UploadId    = uploadId,
                          };
        yield return partRequest;
        currentPartStream = new MemoryStream();
        await remainingStream.CopyToAsync(currentPartStream,
                                          cancellationToken)
                             .ConfigureAwait(false);
        bytesRead = remainingStream.Length;
        partNumber++;
        currentPartSize += 1024 * 1024; // 1MiB
        if (currentPartSize > MaxPartSize)
        {
          currentPartSize = MaxPartSize;
        }
      }
    }

    /* This "may be" the last part, in one of these cases:
     Either we have a remainder part of size < MinPartSize
     Or we have reached the limit of the maximum number of parts and we still have data. We have already prepared MaxPartCount - 1 = 9999, the remaining data is accumulated and sent in the last(the 10000th) part, there is no guarantee here that it is < 5GiB
    */
    if (currentPartStream.Length >= 0)
    {
      var partRequest = new UploadPartRequest
                        {
                          BucketName  = bucketName,
                          Key         = objectKey,
                          PartNumber  = partNumber,
                          InputStream = new MemoryStream(currentPartStream.ToArray()),
                          UploadId    = uploadId,
                        };
      yield return partRequest;
    }
  }
}
