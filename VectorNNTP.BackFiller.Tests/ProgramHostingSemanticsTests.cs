// <copyright file="ProgramHostingSemanticsTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Runtime and startup
// Focused tests for program hosting semantics, covering service lifecycle and shutdown contracts.

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using VectorNNTP.Backfiller.Configuration;
using VectorNNTP.Backfiller.ControlPlane;
using VectorNNTP.Backfiller.Runtime.Accounts;
using VectorNNTP.Backfiller.Runtime.Articles.Processing;
using VectorNNTP.Backfiller.Runtime.Lifecycle;
using VectorNNTP.Backfiller.Runtime.RabbitMq;
using VectorNNTP.Backfiller.Runtime.Shutdown;
using VectorNNTP.Backfiller.Runtime.Transit;
using VectorNNTP.Backfiller.Startup.Hosting;
using Xunit;

namespace VectorNNTP.Backfiller.Tests
{
    /// <summary>
    /// Regression tests for host lifetime and readiness/shutdown hook semantics.
    /// </summary>
    public sealed class ProgramHostingSemanticsTests
    {
        /// <summary>
        /// Exercises register readiness hook  when application stopping  transitions to draining  and signals graceful shutdown behavior, including the expected result and failure semantics.
        /// </summary>
        [Fact]
        public void RegisterReadinessHook_WhenApplicationStopping_TransitionsToDraining_AndSignalsGracefulShutdown()
        {
            FakeHostApplicationLifetime lifetime = new();
            ServiceLifecycle lifecycle = new(TimeProvider.System);
            ShutdownCoordinator shutdownCoordinator = new();

            lifecycle.TransitionTo(ServiceLifecycle.LifecycleState.Validating, "test: begin validation");
            lifecycle.TransitionTo(ServiceLifecycle.LifecycleState.Initializing, "test: begin initialization");
            lifecycle.TransitionTo(ServiceLifecycle.LifecycleState.Ready, "test: service ready");

            HostLifetimeCoordinator.RegisterReadinessHook(
                lifetime,
                TimeSpan.FromSeconds(30),
                shutdownCoordinator,
                lifecycle);

            lifetime.TriggerApplicationStopping();

            Assert.Equal(ServiceLifecycle.LifecycleState.Draining, lifecycle.CurrentState);
            Assert.Equal(ShutdownCoordinator.ShutdownState.GracefulShutdown, shutdownCoordinator.State);
            Assert.Equal(ShutdownCoordinator.ShutdownReason.Unknown, shutdownCoordinator.GracefulShutdownReason);

            shutdownCoordinator.Dispose();
        }
        /// <summary>
        /// Exercises concurrent reentrant shutdown race  does not throw and lifecycle is draining behavior, including the expected result and failure semantics.
        /// </summary>
        [Fact]
        public async Task ConcurrentReentrantShutdownRace_DoesNotThrowAndLifecycleIsDraining()
        {
            FakeHostApplicationLifetime lifetime = new();
            ServiceLifecycle lifecycle = new(TimeProvider.System);
            ShutdownCoordinator shutdownCoordinator = new();

            // Prepare lifecycle to Ready
            lifecycle.TransitionTo(ServiceLifecycle.LifecycleState.Validating, "test: begin validation");
            lifecycle.TransitionTo(ServiceLifecycle.LifecycleState.Initializing, "test: begin initialization");
            lifecycle.TransitionTo(ServiceLifecycle.LifecycleState.Ready, "test: service ready");

            // Synchronization primitives to deterministically block observer notification
            ManualResetEventSlim enteredNotification = new(false);
            ManualResetEventSlim continueNotification = new(false);

            lifecycle.StateTransitioned += transition =>
            {
                if (transition.ToState == ServiceLifecycle.LifecycleState.Draining)
                {
                    enteredNotification.Set();
                    // Block here until the test signals to continue
                    continueNotification.Wait();
                }
            };

            HostLifetimeCoordinator.RegisterReadinessHook(lifetime, TimeSpan.FromSeconds(30), shutdownCoordinator, lifecycle);

            Exception? captured = null;

            Task transitionTask = Task.Run(() =>
            {
                try
                {
                    lifecycle.TransitionTo(ServiceLifecycle.LifecycleState.Draining, "test: concurrent transition");
                }
                catch (Exception ex)
                {
                    captured = ex;
                }
            });

            // Wait until the transition has begun notifying observers and is blocked
            Assert.True(enteredNotification.Wait(5000), "Timeout waiting for observer notification to start");

            try
            {
                // Trigger application stopping while the first transition is in notification
                // This would normally attempt a second TransitionTo and may throw without the fix.
                Exception ex = Record.Exception(lifetime.TriggerApplicationStopping);
                Assert.Null(ex);

                // The lifecycle should now be Draining
                Assert.Equal(ServiceLifecycle.LifecycleState.Draining, lifecycle.CurrentState);

                // Coordinator should have been signaled to GracefulShutdown
                Assert.Equal(ShutdownCoordinator.ShutdownState.GracefulShutdown, shutdownCoordinator.State);
            }
            finally
            {
                // Ensure we always release the observer to avoid hanging the transitionTask
                continueNotification.Set();
                Task completedTask = await Task.WhenAny(transitionTask, Task.Delay(TimeSpan.FromSeconds(5)));
                Assert.Same(
                    transitionTask,
                    completedTask);
            }

            // Ensure the transition task did not capture an unexpected exception
            Assert.Null(captured);

            shutdownCoordinator.Dispose();
        }

        // Note: repeated CancellationToken.Cancel() calls are one-shot and do not re-invoke
        // cancellation callbacks. The intentional idempotency of the shutdown hook is
        // exercised elsewhere; therefore we avoid a duplicate test that relies on
        // multiple Cancel() invocations.
        /// <summary>
        /// Exercises configure host services  when lifecycle provided  registers same instance  and required hosted services behavior, including the expected result and failure semantics.
        /// </summary>
        [Fact]
        public void ConfigureHostServices_WhenLifecycleProvided_RegistersSameInstance_AndRequiredHostedServices()
        {
            HostApplicationBuilder builder = Host.CreateApplicationBuilder();
            _ = builder.Configuration.AddInMemoryCollection(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
            {
                ["BackFiller:Shutdown:GracePeriodSeconds"] = "120",
                ["BackFiller:Shutdown:StopNewWorkAdmission"] = "true",
                ["BackFiller:Shutdown:FinishActiveArticles"] = "true",
                ["BackFiller:Shutdown:DrainQueuedWork"] = "true",
                ["BackFiller:Id"] = "12",
                ["ConnectionStrings:GrabberDB"] = "Server=localhost;Database=GrabberDB;User ID=admin;Password=secret",
            });

            BackFillerRuntimeOptions runtimeOptions = new(
                CanonicalBackFillerFqdn: "bf-12.example.com",
                BackFillerId: 12,
                CanonicalDnsSuffix: "example.com",
                ValidatedLogDirectory: Path.GetTempPath(),
                ValidatedCertificateDirectory: Path.GetTempPath(),
                RabbitMqHosts: ["localhost"],
                RabbitMqPort: 5672,
                RabbitMqEnableSsl: false,
                TransitServerHost: "localhost",
                TransitServerPort: 119,
                TransitServerUseSsl: false,
                BindPort: 119,
                ConfiguredBindAddressTokens: ["127.0.0.1"],
                ShutdownGracePeriodSeconds: 120,
                ShutdownDrainQueuedWork: true,
                ShutdownFinishActiveArticles: true,
                RabbitMqMaximumShutdownDrainTimeoutSeconds: 30,
                WriteBatchCoalesceMicroseconds: 250,
                RabbitMq: CreateRabbitMqRuntimeOptions(enableSsl: false));

            ServiceLifecycle lifecycle = new(TimeProvider.System);

            _ = builder.Services.AddSingleton(runtimeOptions);
            HostComposer.ConfigureHostServices(builder, runtimeOptions, lifecycle);

            ServiceDescriptor[] controlPlaneDescriptors =
                [.. builder.Services.Where(static d =>
                    d.ServiceType == typeof(ControlPlaneService))];

            ServiceDescriptor[] accountInitializerHostedServiceDescriptors =
                [.. builder.Services.Where(static d =>
                    d.ServiceType == typeof(IHostedService)
                    && d.ImplementationType == typeof(NntpAccountSnapshotStartupInitializer))];

            ServiceDescriptor[] transitInitializerHostedServiceDescriptors =
                [.. builder.Services.Where(static d =>
                    d.ServiceType == typeof(IHostedService)
                    && d.ImplementationType == typeof(TransitPublisherStartupInitializer))];

            ServiceDescriptor[] providerDescriptors =
                [.. builder.Services.Where(static d =>
                    d.ServiceType == typeof(MySqlNntpAccountSnapshotProvider))];

            ServiceDescriptor[] transitPublisherDescriptors =
                [.. builder.Services.Where(static d =>
                    d.ServiceType == typeof(TransitPublisher))];

            _ = Assert.Single(controlPlaneDescriptors);
            _ = Assert.Single(accountInitializerHostedServiceDescriptors);
            _ = Assert.Single(transitInitializerHostedServiceDescriptors);
            _ = Assert.Single(providerDescriptors);
            _ = Assert.Single(transitPublisherDescriptors);

            using IHost host = builder.Build();
            ServiceLifecycle resolvedLifecycle = host.Services.GetRequiredService<ServiceLifecycle>();
            ControlPlaneService controlPlane = host.Services.GetRequiredService<ControlPlaneService>();
            IBackboneSessionLeaseProvider leaseProvider = host.Services.GetRequiredService<IBackboneSessionLeaseProvider>();

            Assert.Same(lifecycle, resolvedLifecycle);
            Assert.Same(controlPlane, leaseProvider);
        }
        /// <summary>
        /// Exercises configure host services  does not register shutdown options for runtime resolution behavior, including the expected result and failure semantics.
        /// </summary>
        [Fact]
        public void ConfigureHostServices_DoesNotRegisterShutdownOptionsForRuntimeResolution()
        {
            HostApplicationBuilder builder = Host.CreateApplicationBuilder();
            _ = builder.Configuration.AddInMemoryCollection(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
            {
                ["BackFiller:Shutdown:GracePeriodSeconds"] = "120",
                ["BackFiller:Shutdown:StopNewWorkAdmission"] = "true",
                ["BackFiller:Shutdown:FinishActiveArticles"] = "true",
                ["BackFiller:Shutdown:DrainQueuedWork"] = "true",
                ["BackFiller:Id"] = "12",
                ["ConnectionStrings:GrabberDB"] = "Server=localhost;Database=GrabberDB;User ID=admin;Password=secret",
            });

            BackFillerRuntimeOptions runtimeOptions = new(
                CanonicalBackFillerFqdn: "bf-12.example.com",
                BackFillerId: 12,
                CanonicalDnsSuffix: "example.com",
                ValidatedLogDirectory: Path.GetTempPath(),
                ValidatedCertificateDirectory: Path.GetTempPath(),
                RabbitMqHosts: ["localhost"],
                RabbitMqPort: 5672,
                RabbitMqEnableSsl: false,
                TransitServerHost: "localhost",
                TransitServerPort: 119,
                TransitServerUseSsl: false,
                BindPort: 119,
                ConfiguredBindAddressTokens: ["127.0.0.1"],
                ShutdownGracePeriodSeconds: 120,
                ShutdownDrainQueuedWork: true,
                ShutdownFinishActiveArticles: true,
                RabbitMqMaximumShutdownDrainTimeoutSeconds: 30,
                WriteBatchCoalesceMicroseconds: 250);

            HostComposer.ConfigureHostServices(builder, runtimeOptions);

            using IHost host = builder.Build();

            Assert.Null(host.Services.GetService<ShutdownOptions>());
        }
        /// <summary>
        /// Exercises configure host services  uses runtime snapshot grace period  for host shutdown timeout behavior, including the expected result and failure semantics.
        /// </summary>
        [Fact]
        public void ConfigureHostServices_UsesRuntimeSnapshotGracePeriod_ForHostShutdownTimeout()
        {
            HostApplicationBuilder builder = Host.CreateApplicationBuilder();
            _ = builder.Configuration.AddInMemoryCollection(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
            {
                ["BackFiller:Shutdown:GracePeriodSeconds"] = "30",
                ["BackFiller:Shutdown:StopNewWorkAdmission"] = "true",
                ["BackFiller:Shutdown:FinishActiveArticles"] = "true",
                ["BackFiller:Shutdown:DrainQueuedWork"] = "true",
                ["BackFiller:Id"] = "12",
                ["ConnectionStrings:GrabberDB"] = "Server=localhost;Database=GrabberDB;User ID=admin;Password=secret",
            });

            BackFillerRuntimeOptions runtimeOptions = new(
                CanonicalBackFillerFqdn: "bf-12.example.com",
                BackFillerId: 12,
                CanonicalDnsSuffix: "example.com",
                ValidatedLogDirectory: Path.GetTempPath(),
                ValidatedCertificateDirectory: Path.GetTempPath(),
                RabbitMqHosts: ["localhost"],
                RabbitMqPort: 5672,
                RabbitMqEnableSsl: false,
                TransitServerHost: "localhost",
                TransitServerPort: 119,
                TransitServerUseSsl: false,
                BindPort: 119,
                ConfiguredBindAddressTokens: ["127.0.0.1"],
                ShutdownGracePeriodSeconds: 120,
                ShutdownDrainQueuedWork: true,
                ShutdownFinishActiveArticles: true,
                RabbitMqMaximumShutdownDrainTimeoutSeconds: 30,
                WriteBatchCoalesceMicroseconds: 250);

            HostComposer.ConfigureHostServices(builder, runtimeOptions);

            using IHost host = builder.Build();
            IOptions<HostOptions> hostOptions = host.Services.GetRequiredService<IOptions<HostOptions>>();

            Assert.Equal(TimeSpan.FromSeconds(runtimeOptions.ShutdownGracePeriodSeconds), hostOptions.Value.ShutdownTimeout);
        }
        /// <summary>
        /// Exercises configure host services  rabbit mq graph resolves core services behavior, including the expected result and failure semantics.
        /// </summary>
        [Fact]
        public void ConfigureHostServices_RabbitMqGraphResolvesCoreServices()
        {
            HostApplicationBuilder builder = Host.CreateApplicationBuilder();
            _ = builder.Configuration.AddInMemoryCollection(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
            {
                ["BackFiller:Shutdown:GracePeriodSeconds"] = "120",
                ["BackFiller:Shutdown:StopNewWorkAdmission"] = "true",
                ["BackFiller:Shutdown:FinishActiveArticles"] = "true",
                ["BackFiller:Shutdown:DrainQueuedWork"] = "true",
                ["BackFiller:Id"] = "12",
                ["ConnectionStrings:GrabberDB"] = "Server=localhost;Database=GrabberDB;User ID=admin;Password=secret",
            });

            BackFillerRuntimeOptions runtimeOptions = new(
                CanonicalBackFillerFqdn: "bf-12.example.com",
                BackFillerId: 12,
                CanonicalDnsSuffix: "example.com",
                ValidatedLogDirectory: Path.GetTempPath(),
                ValidatedCertificateDirectory: Path.GetTempPath(),
                RabbitMqHosts: ["localhost"],
                RabbitMqPort: 5672,
                RabbitMqEnableSsl: false,
                TransitServerHost: "localhost",
                TransitServerPort: 119,
                TransitServerUseSsl: false,
                BindPort: 119,
                ConfiguredBindAddressTokens: ["127.0.0.1"],
                ShutdownGracePeriodSeconds: 120,
                ShutdownDrainQueuedWork: true,
                ShutdownFinishActiveArticles: true,
                RabbitMqMaximumShutdownDrainTimeoutSeconds: 30,
                WriteBatchCoalesceMicroseconds: 250,
                RabbitMq: CreateRabbitMqRuntimeOptions(enableSsl: false));

            HostComposer.ConfigureHostServices(builder, runtimeOptions, new ServiceLifecycle(TimeProvider.System));
            _ = builder.Services.AddSingleton(runtimeOptions);

            using IHost host = builder.Build();

            _ = host.Services.GetRequiredService<RabbitMqConnectionManager>();
            _ = host.Services.GetRequiredService<RabbitMqTopologyInitializer>();
            RabbitMqConsumerService rabbitConsumerService = host.Services.GetRequiredService<RabbitMqConsumerService>();
            ControlPlaneService controlPlaneService = host.Services.GetRequiredService<ControlPlaneService>();
            IBackboneUsableCapacityProvider capacityProvider = host.Services.GetRequiredService<IBackboneUsableCapacityProvider>();
            IBackboneUsableCapacityStateWriter capacityWriter = host.Services.GetRequiredService<IBackboneUsableCapacityStateWriter>();
            _ = host.Services.GetRequiredService<IRabbitMqArticleResponsePublisher>();
            _ = host.Services.GetRequiredService<IArticleWorkResultSink>();

            Assert.NotSame(controlPlaneService, capacityProvider);
            Assert.Same(capacityProvider, capacityWriter);
            Assert.IsType<BackboneUsableCapacityState>(capacityProvider);
            Assert.IsAssignableFrom<IRabbitMqCapacityRetirementCoordinator>(rabbitConsumerService);

            IHostedService processingHostedService = host.Services
                .GetServices<IHostedService>()
                .Single(static service => service is RabbitMqArticleProcessingService);
            Assert.IsType<RabbitMqArticleProcessingService>(processingHostedService);
        }
        /// <summary>
        /// Exercises should publish readiness  when application stopping already signaled  returns false behavior, including the expected result and failure semantics.
        /// </summary>
        [Fact]
        public void ShouldPublishReadiness_WhenApplicationStoppingAlreadySignaled_ReturnsFalse()
        {
            FakeHostApplicationLifetime lifetime = new();
            lifetime.TriggerApplicationStopping();

            bool shouldPublish = HostLifetimeCoordinator.ShouldPublishReadinessForTesting(lifetime);

            Assert.False(shouldPublish);
        }
        /// <summary>
        /// Exercises should publish readiness  when application stopping not signaled  returns true behavior, including the expected result and failure semantics.
        /// </summary>
        [Fact]
        public void ShouldPublishReadiness_WhenApplicationStoppingNotSignaled_ReturnsTrue()
        {
            FakeHostApplicationLifetime lifetime = new();

            bool shouldPublish = HostLifetimeCoordinator.ShouldPublishReadinessForTesting(lifetime);

            Assert.True(shouldPublish);
        }

        /// <summary>
        /// Verifies shutdown during host startup keeps lifecycle on shutdown path without entering Ready.
        /// </summary>
        /// <remarks>
        /// The hosted startup probe triggers <see cref="IHostApplicationLifetime.StopApplication"/> during StartAsync,
        /// simulating Ctrl+C while startup initialization is still running.
        /// </remarks>
        [Fact]
        public async Task RunAsync_WhenShutdownRequestedDuringStartup_DoesNotTransitionToReady_AndStopsViaDrainingAsync()
        {
            HostApplicationBuilder builder = Host.CreateApplicationBuilder();
            _ = builder.Services.AddSingleton(CreateRuntimeOptionsForTesting());
            _ = builder.Services.AddSingleton<ShutdownCoordinator>();
            _ = builder.Services.AddHostedService<ShutdownDuringStartupProbeHostedService>();

            using IHost host = builder.Build();

            ServiceLifecycle lifecycle = new(TimeProvider.System);
            lifecycle.TransitionTo(ServiceLifecycle.LifecycleState.Validating, "test: begin validation");
            lifecycle.TransitionTo(ServiceLifecycle.LifecycleState.Initializing, "test: begin initialization");

            await HostLifetimeCoordinator.RunAsync(host, lifecycle, static () => { }).ConfigureAwait(false);

            Assert.Equal(ServiceLifecycle.LifecycleState.Stopped, lifecycle.CurrentState);
            Assert.DoesNotContain(
                lifecycle.TransitionHistory,
                static transition => transition.ToState == ServiceLifecycle.LifecycleState.Ready);

            Assert.Contains(
                lifecycle.TransitionHistory,
                static transition => transition.FromState == ServiceLifecycle.LifecycleState.Initializing &&
                                     transition.ToState == ServiceLifecycle.LifecycleState.Draining);

            Assert.Contains(
                lifecycle.TransitionHistory,
                static transition => transition.FromState == ServiceLifecycle.LifecycleState.Draining &&
                                     transition.ToState == ServiceLifecycle.LifecycleState.Stopped);
        }

        /// <summary>
        /// Creates a minimal immutable runtime snapshot for host-lifetime coordinator tests.
        /// </summary>
        /// <returns>Runtime options used by <see cref="HostLifetimeCoordinator"/>.</returns>
        private static BackFillerRuntimeOptions CreateRuntimeOptionsForTesting()
        {
            return new BackFillerRuntimeOptions(
                CanonicalBackFillerFqdn: "bf-12.example.com",
                BackFillerId: 12,
                CanonicalDnsSuffix: "example.com",
                ValidatedLogDirectory: Path.GetTempPath(),
                ValidatedCertificateDirectory: Path.GetTempPath(),
                RabbitMqHosts: ["localhost"],
                RabbitMqPort: 5672,
                RabbitMqEnableSsl: false,
                TransitServerHost: "localhost",
                TransitServerPort: 119,
                TransitServerUseSsl: false,
                BindPort: 119,
                ConfiguredBindAddressTokens: ["127.0.0.1"],
                ShutdownGracePeriodSeconds: 30,
                ShutdownDrainQueuedWork: true,
                ShutdownFinishActiveArticles: true,
                RabbitMqMaximumShutdownDrainTimeoutSeconds: 30,
                WriteBatchCoalesceMicroseconds: 250,
                RabbitMq: CreateRabbitMqRuntimeOptions(enableSsl: false));
        }

        /// <summary>
        /// Exercises create rabbit mq runtime options behavior, including the expected result and failure semantics.
        /// </summary>
        private static RabbitMqRuntimeOptions CreateRabbitMqRuntimeOptions(bool enableSsl)
        {
            return new RabbitMqRuntimeOptions(
                Hosts: ["localhost"],
                Port: 5672,
                Username: "nntparticles",
                Password: "super-secret",
                VirtualHost: "/",
                EnableSsl: enableSsl,
                ChannelLeaseTimeoutSeconds: 60,
                RpcTimeoutSeconds: 30,
                ConnectionBlockedTimeoutSeconds: 30,
                ChannelPoolSize: 512,
                MinConnections: 4,
                MaxConnections: 16,
                MaxConsecutiveRecoveryFailures: 5,
                MaxPendingLeaseWaiters: 1024,
                ConnectionScaleDownIdleSeconds: 300,
                ScaleDownCooldownSeconds: 30,
                NetworkRecoveryIntervalSeconds: 5,
                PoolReconnectBaseDelayMs: 50,
                PoolReconnectMaxDelayMs: 250,
                MinimumConnectionLifetimeSeconds: 300,
                PublishConfirmTimeoutSeconds: 10,
                MaximumShutdownDrainTimeoutSeconds: 30,
                DegradedThreshold: 0.75,
                UnhealthyThreshold: 5,
                RequestedHeartbeatSeconds: 60,
                SocketTimeoutSeconds: 30,
                RequestedChannelMax: 2047,
                ConsumerPrefetchCount: null);
        }

        /// <summary>
        /// Hosted startup probe that requests host shutdown during startup initialization.
        /// </summary>
        private sealed class ShutdownDuringStartupProbeHostedService(IHostApplicationLifetime hostApplicationLifetime) : IHostedService
        {
            /// <summary>
            /// Requests shutdown immediately during startup to simulate Ctrl+C before readiness.
            /// </summary>
            /// <param name="cancellationToken">Startup cancellation token.</param>
            /// <returns>A completed task.</returns>
            public Task StartAsync(CancellationToken cancellationToken)
            {
                hostApplicationLifetime.StopApplication();
                return Task.CompletedTask;
            }

            /// <summary>
            /// Completes stop without additional work.
            /// </summary>
            /// <param name="cancellationToken">Stop cancellation token.</param>
            /// <returns>A completed task.</returns>
            public Task StopAsync(CancellationToken cancellationToken)
            {
                return Task.CompletedTask;
            }
        }

        /// <summary>
        /// Covers fake host application lifetime behavior and invariants exercised by this test suite.
        /// </summary>
        private sealed class FakeHostApplicationLifetime : IHostApplicationLifetime
        {
            /// <summary>
            /// Exercises  application started behavior, including the expected result and failure semantics.
            /// </summary>
            private readonly CancellationTokenSource _applicationStarted = new();
            /// <summary>
            /// Exercises  application stopping behavior, including the expected result and failure semantics.
            /// </summary>
            private readonly CancellationTokenSource _applicationStopping = new();
            /// <summary>
            /// Exercises  application stopped behavior, including the expected result and failure semantics.
            /// </summary>
            private readonly CancellationTokenSource _applicationStopped = new();

            /// <summary>
            /// Supplies application started for the fixture or scenario under test.
            /// </summary>
            public CancellationToken ApplicationStarted => _applicationStarted.Token;

            /// <summary>
            /// Supplies application stopping for the fixture or scenario under test.
            /// </summary>
            public CancellationToken ApplicationStopping => _applicationStopping.Token;

            /// <summary>
            /// Supplies application stopped for the fixture or scenario under test.
            /// </summary>
            public CancellationToken ApplicationStopped => _applicationStopped.Token;

            /// <summary>
            /// Exercises stop application behavior, including the expected result and failure semantics.
            /// </summary>
            public void StopApplication()
            {
                _applicationStopping.Cancel();
            }

            /// <summary>
            /// Exercises trigger application stopping behavior, including the expected result and failure semantics.
            /// </summary>
            internal void TriggerApplicationStopping()
            {
                _applicationStopping.Cancel();
            }
        }
    }

}


