using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Microsoft.AspNetCore.Authorization;

namespace ArmoniK.Core.Common.Auth.Authorization
{
  public class RequiresActionAttribute : AuthorizeAttribute
  {
    public const string PolicyPrefix = "RequiresAction";

    public RequiresActionAttribute(string action) => Action = action;

    public string Action
    {
      get => Policy?[PolicyPrefix.Length..] ?? "";
      set => Policy = $"{PolicyPrefix}{value}";
    }
  }
}
