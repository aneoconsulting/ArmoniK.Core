using System.Threading.Tasks;

namespace htck8s.Compute.PollingAgent
{
  class Program
  {
#pragma warning disable CS1998 // Async method lacks 'await' operators and will run synchronously
    static async Task Main(string[] args)
#pragma warning restore CS1998 // Async method lacks 'await' operators and will run synchronously
    {
      // The port number(5001) must match the port of the gRPC server.
      //using var channel = GrpcChannel.ForAddress("https://localhost:49155");
      //var client = Service.
      //var client =  new Greeter.GreeterClient(channel);
      //var reply = await client.SayHelloAsync(
      //  new HelloRequest { Name = "GreeterClient" });
      //Console.WriteLine("Greeting: " + reply.Message);
      //Console.WriteLine("Press any key to exit...");
      //Console.ReadKey();
    }
  }
}
