using System;
using System.Threading.Tasks;

namespace ArmoniK.Core.Injection
{
  public abstract class ProviderBase<T>
  {
    private T object_;
    private readonly Func<Task<T>> builder_;

    protected ProviderBase(Func<Task<T>> builder) => builder_ = builder;

    public async ValueTask<T> GetAsync()
    {
      if(object_ is null)
      {
        Task<T> task;
        lock(this)
        {
          task = object_ is null ? builder_() : Task.FromResult(object_);
        }
        object_ = await task;
      }
      return object_;
    }
  }
}
