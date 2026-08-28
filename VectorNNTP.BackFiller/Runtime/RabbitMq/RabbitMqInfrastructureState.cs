namespace VectorNNTP.Backfiller.Runtime.RabbitMq
{
    /// <summary>
    /// RabbitMQ infrastructure readiness states used by startup and runtime lifecycle management.
    /// </summary>
    internal enum RabbitMqInfrastructureState
    {
        NotInitialized,
        Connecting,
        Connected,
        TopologyReady,
        Reconnecting,
        Failed,
        Stopping,
        Stopped,
    }
}
