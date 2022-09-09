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
// but WITHOUT ANY WARRANTY, without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Common.Utils;

using NUnit.Framework;

namespace ArmoniK.Core.Common.Tests.Utils;

[TestFixture(TestOf = typeof(ExecutionSingleizer<int>))]
public class GraceDelayCancellationTokenTests
{
  [SetUp]
  public void SetUp()
  {
    source_ = new CancellationTokenSource();
    gdcts_ = new GraceDelayCancellationTokenSource(source_,
                                                   TimeSpan.FromMilliseconds(100),
                                                   TimeSpan.FromSeconds(1));
  }

  [TearDown]
  public void TearDown()
  {
    gdcts_?.Dispose();
    gdcts_ = null;
    source_?.Dispose();
    source_ = null;
  }

  private GraceDelayCancellationTokenSource? gdcts_;
  private CancellationTokenSource?           source_;

  [Test]
  public void CancellationIsNotRequested()
  {
    Assert.IsFalse(gdcts_!.Token0.IsCancellationRequested);
    Assert.IsFalse(gdcts_.Token1.IsCancellationRequested);
    Assert.IsFalse(gdcts_.Token2.IsCancellationRequested);
    Assert.IsFalse(gdcts_.Token3.IsCancellationRequested);
    Assert.IsFalse(gdcts_.Token4.IsCancellationRequested);
    Assert.IsFalse(gdcts_.Token5.IsCancellationRequested);
  }

  [Test]
  public void CancellationIsRequested()
  {
    source_!.Cancel();
    Assert.IsTrue(gdcts_!.Token0.IsCancellationRequested);
    Assert.IsFalse(gdcts_.Token1.IsCancellationRequested);
    Assert.IsFalse(gdcts_.Token2.IsCancellationRequested);
    Assert.IsFalse(gdcts_.Token3.IsCancellationRequested);
    Assert.IsFalse(gdcts_.Token4.IsCancellationRequested);
    Assert.IsFalse(gdcts_.Token5.IsCancellationRequested);
  }

  [Test]
  public async Task CancellationWaitIsRequested()
  {
    source_!.Cancel();
    await Task.Delay(TimeSpan.FromMilliseconds(500))
              .ConfigureAwait(false);
    Assert.IsTrue(gdcts_!.Token0.IsCancellationRequested);
    Assert.IsTrue(gdcts_.Token1.IsCancellationRequested);
    Assert.IsFalse(gdcts_.Token2.IsCancellationRequested);
    Assert.IsTrue(gdcts_.Token3.IsCancellationRequested);
    Assert.IsTrue(gdcts_.Token4.IsCancellationRequested);
    Assert.IsTrue(gdcts_.Token5.IsCancellationRequested);
  }

  [Test]
  public async Task CancellationWait1500IsRequested()
  {
    source_!.Cancel();
    await Task.Delay(TimeSpan.FromMilliseconds(1500))
              .ConfigureAwait(false);
    Assert.IsTrue(gdcts_!.Token0.IsCancellationRequested);
    Assert.IsTrue(gdcts_.Token1.IsCancellationRequested);
    Assert.IsTrue(gdcts_.Token2.IsCancellationRequested);
    Assert.IsTrue(gdcts_.Token3.IsCancellationRequested);
    Assert.IsTrue(gdcts_.Token4.IsCancellationRequested);
    Assert.IsTrue(gdcts_.Token5.IsCancellationRequested);
  }

  [Test]
  public void DisposeShouldNotHaveIssue()
  {
    gdcts_!.Dispose();
    gdcts_ = null;
    source_!.Cancel();
    Assert.IsTrue(source_.IsCancellationRequested);
  }
}
