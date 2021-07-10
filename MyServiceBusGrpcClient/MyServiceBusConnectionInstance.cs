using System;
using System.Threading.Tasks;
using MyServiceBus.GrpcContracts;

namespace MyServiceBusGrpcClient
{
    internal class MyServiceBusConnectionInstance
    {
        public IMyServiceBusGrpcPublisher MyServiceBusGrpcPublisher { get;  }

        private IMyServiceBusGrpcSessions MyServiceBusGrpcSessions { get; }
        
        public long SessionId { get; }
        
        public string GrpcUrl { get; }

        public MyServiceBusConnectionInstance(IMyServiceBusGrpcPublisher myServiceBusGrpcPublisher, 
            IMyServiceBusGrpcSessions myServiceBusGrpcSessions, long sessionId, string grpcUrl)
        {
            MyServiceBusGrpcPublisher = myServiceBusGrpcPublisher;
            MyServiceBusGrpcSessions = myServiceBusGrpcSessions;
            SessionId = sessionId;
            GrpcUrl = grpcUrl;
        }


        public async ValueTask<bool> PingAsync()
        {
            var pingResult = await MyServiceBusGrpcSessions.PingAsync(new PingGrpcContract
            {
                SessionId = SessionId
            });


            return pingResult.Status == MyServiceBusResponseStatus.Ok;
        }
        
    }
}