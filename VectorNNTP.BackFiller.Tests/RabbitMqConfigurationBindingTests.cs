// <copyright file="RabbitMqConfigurationBindingTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Runtime and startup
// Focused tests for rabbit mq configuration binding, covering configuration and validation contracts; dependency integration and failure handling.
// Primary responsibility: documents the executable contracts covered by the rabbit mq configuration binding test suite.

using Microsoft.Extensions.Configuration;
using VectorNNTP.Backfiller.Configuration;
using Xunit;

namespace VectorNNTP.Backfiller.Tests
{
    /// <summary>
    /// Tests RabbitMQ configuration binding from IConfiguration into BackFiller options.
    /// </summary>
    public sealed class RabbitMqConfigurationBindingTests
    {
        /// <summary>
        /// Confirms the back filler rabbit mq section binds all current settings behavior.
        /// </summary>
        [Fact]
        public void BackFillerRabbitMqSection_BindsAllCurrentSettings()
        {
            Dictionary<string, string?> values = new(StringComparer.OrdinalIgnoreCase)
            {
                ["BackFiller:RabbitMQ:ChannelLeaseTimeoutSeconds"] = "60",
                ["BackFiller:RabbitMQ:ChannelPoolSize"] = "512",
                ["BackFiller:RabbitMQ:ConnectionBlockedTimeoutSeconds"] = "30",
                ["BackFiller:RabbitMQ:ConnectionScaleDownIdleSeconds"] = "300",
                ["BackFiller:RabbitMQ:DegradedThreshold"] = "0.75",
                ["BackFiller:RabbitMQ:EnableSsl"] = "true",
                ["BackFiller:RabbitMQ:Hosts:0"] = "rabbit1",
                ["BackFiller:RabbitMQ:Hosts:1"] = "rabbit2",
                ["BackFiller:RabbitMQ:MaxConnections"] = "16",
                ["BackFiller:RabbitMQ:MaxConsecutiveRecoveryFailures"] = "5",
                ["BackFiller:RabbitMQ:MaximumShutdownDrainTimeoutSeconds"] = "30",
                ["BackFiller:RabbitMQ:MaxPendingLeaseWaiters"] = "1024",
                ["BackFiller:RabbitMQ:MinConnections"] = "4",
                ["BackFiller:RabbitMQ:MinimumConnectionLifetimeSeconds"] = "300",
                ["BackFiller:RabbitMQ:NetworkRecoveryIntervalSeconds"] = "5",
                ["BackFiller:RabbitMQ:Password"] = "secret",
                ["BackFiller:RabbitMQ:PoolReconnectBaseDelayMs"] = "250",
                ["BackFiller:RabbitMQ:PoolReconnectMaxDelayMs"] = "30000",
                ["BackFiller:RabbitMQ:Port"] = "5672",
                ["BackFiller:RabbitMQ:PublishConfirmTimeoutSeconds"] = "10",
                ["BackFiller:RabbitMQ:RequestedChannelMax"] = "2047",
                ["BackFiller:RabbitMQ:RequestedHeartbeatSeconds"] = "60",
                ["BackFiller:RabbitMQ:RpcTimeoutSeconds"] = "30",
                ["BackFiller:RabbitMQ:ScaleDownCooldownSeconds"] = "30",
                ["BackFiller:RabbitMQ:SocketTimeoutSeconds"] = "30",
                ["BackFiller:RabbitMQ:UnhealthyThreshold"] = "5",
                ["BackFiller:RabbitMQ:Username"] = "nntparticles",
                ["BackFiller:RabbitMQ:VirtualHost"] = "/",
            };

            IConfiguration configuration = new ConfigurationBuilder()
                .AddInMemoryCollection(values)
                .Build();

            BackFillerOptions? options = configuration.GetSection("BackFiller").Get<BackFillerOptions>();

            Assert.NotNull(options);
            Assert.NotNull(options.RabbitMQ);

            RabbitMqOptions rabbitMq = options.RabbitMQ;
            Assert.Equal(60, rabbitMq.ChannelLeaseTimeoutSeconds);
            Assert.Equal(512, rabbitMq.ChannelPoolSize);
            Assert.Equal(30, rabbitMq.ConnectionBlockedTimeoutSeconds);
            Assert.Equal(300, rabbitMq.ConnectionScaleDownIdleSeconds);
            Assert.Equal(0.75d, rabbitMq.DegradedThreshold);
            Assert.True(rabbitMq.EnableSsl);
            Assert.Equal(new[] { "rabbit1", "rabbit2" }, rabbitMq.Hosts);
            Assert.Equal(16, rabbitMq.MaxConnections);
            Assert.Equal(5, rabbitMq.MaxConsecutiveRecoveryFailures);
            Assert.Equal(30, rabbitMq.MaximumShutdownDrainTimeoutSeconds);
            Assert.Equal(1024, rabbitMq.MaxPendingLeaseWaiters);
            Assert.Equal(4, rabbitMq.MinConnections);
            Assert.Equal(300, rabbitMq.MinimumConnectionLifetimeSeconds);
            Assert.Equal(5, rabbitMq.NetworkRecoveryIntervalSeconds);
            Assert.Equal("secret", rabbitMq.Password);
            Assert.Equal(250, rabbitMq.PoolReconnectBaseDelayMs);
            Assert.Equal(30000, rabbitMq.PoolReconnectMaxDelayMs);
            Assert.Equal(5672, rabbitMq.Port);
            Assert.Equal(10, rabbitMq.PublishConfirmTimeoutSeconds);
            Assert.Equal(2047, rabbitMq.RequestedChannelMax);
            Assert.Equal(60, rabbitMq.RequestedHeartbeatSeconds);
            Assert.Equal(30, rabbitMq.RpcTimeoutSeconds);
            Assert.Equal(30, rabbitMq.ScaleDownCooldownSeconds);
            Assert.Equal(30, rabbitMq.SocketTimeoutSeconds);
            Assert.Equal(5, rabbitMq.UnhealthyThreshold);
            Assert.Equal("nntparticles", rabbitMq.Username);
            Assert.Equal("/", rabbitMq.VirtualHost);
        }
    }
}
