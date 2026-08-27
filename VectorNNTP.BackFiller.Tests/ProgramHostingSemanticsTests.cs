using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using VectorNNTP.Backfiller.Configuration;
using VectorNNTP.Backfiller.ControlPlane;
using VectorNNTP.Backfiller.Runtime.Accounts;
using VectorNNTP.Backfiller.Runtime.Lifecycle;
using VectorNNTP.Backfiller.Runtime.Shutdown;
using VectorNNTP.Backfiller.Runtime.Transit;
using VectorNNTP.Backfiller.Startup.Hosting;
using Xunit;

namespace VectorNNTP.Backfiller.Tests;

/// <summary>
/// Regression tests for host lifetime and readiness/shutdown hook semantics.
/// </summary>
public sealed class ProgramHostingSemanticsTests
{
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

    [Fact]
    public void ConcurrentReentrantShutdownRace_DoesNotThrowAndLifecycleIsDraining()
    {
        var lifetime = new FakeHostApplicationLifetime();
        var lifecycle = new ServiceLifecycle(TimeProvider.System);
        var shutdownCoordinator = new ShutdownCoordinator();

        // Prepare lifecycle to Ready
        lifecycle.TransitionTo(ServiceLifecycle.LifecycleState.Validating, "test: begin validation");
        lifecycle.TransitionTo(ServiceLifecycle.LifecycleState.Initializing, "test: begin initialization");
        lifecycle.TransitionTo(ServiceLifecycle.LifecycleState.Ready, "test: service ready");

        // Synchronization primitives to deterministically block observer notification
        var enteredNotification = new ManualResetEventSlim(false);
        var continueNotification = new ManualResetEventSlim(false);

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
            var ex = Record.Exception(() => lifetime.TriggerApplicationStopping());
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
            Assert.True(
                transitionTask.Wait(TimeSpan.FromSeconds(5)),
                "The concurrent lifecycle transition did not complete.");
        }

        // Ensure the transition task did not capture an unexpected exception
        Assert.Null(captured);

        shutdownCoordinator.Dispose();
    }

    // Note: repeated CancellationToken.Cancel() calls are one-shot and do not re-invoke
    // cancellation callbacks. The intentional idempotency of the shutdown hook is
    // exercised elsewhere; therefore we avoid a duplicate test that relies on
    // multiple Cancel() invocations.

    [Fact]
    public void ConfigureHostServices_WhenLifecycleProvided_RegistersSameInstance_AndRequiredHostedServices()
    {
        HostApplicationBuilder builder = Host.CreateApplicationBuilder();
        builder.Configuration.AddInMemoryCollection(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
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

        ServiceLifecycle lifecycle = new(TimeProvider.System);

        global::VectorNNTP.Backfiller.Startup.Hosting.HostComposer.ConfigureHostServices(builder, runtimeOptions, lifecycle);

        ServiceDescriptor[] controlPlaneHostedServiceDescriptors =
            builder.Services.Where(static d =>
                d.ServiceType == typeof(IHostedService)
                && d.ImplementationType == typeof(ControlPlaneService))
            .ToArray();

        ServiceDescriptor[] accountInitializerHostedServiceDescriptors =
            builder.Services.Where(static d =>
                d.ServiceType == typeof(IHostedService)
                && d.ImplementationType == typeof(NntpAccountSnapshotStartupInitializer))
            .ToArray();

        ServiceDescriptor[] transitInitializerHostedServiceDescriptors =
            builder.Services.Where(static d =>
                d.ServiceType == typeof(IHostedService)
                && d.ImplementationType == typeof(TransitPublisherStartupInitializer))
            .ToArray();

        ServiceDescriptor[] providerDescriptors =
            builder.Services.Where(static d =>
                d.ServiceType == typeof(MySqlNntpAccountSnapshotProvider))
            .ToArray();

        ServiceDescriptor[] transitPublisherDescriptors =
            builder.Services.Where(static d =>
                d.ServiceType == typeof(TransitPublisher))
            .ToArray();

        Assert.Single(controlPlaneHostedServiceDescriptors);
        Assert.Single(accountInitializerHostedServiceDescriptors);
        Assert.Single(transitInitializerHostedServiceDescriptors);
        Assert.Single(providerDescriptors);
        Assert.Single(transitPublisherDescriptors);

        using IHost host = builder.Build();
        ServiceLifecycle resolvedLifecycle = host.Services.GetRequiredService<ServiceLifecycle>();

        Assert.Same(lifecycle, resolvedLifecycle);
    }

    [Fact]
    public void ConfigureHostServices_DoesNotRegisterShutdownOptionsForRuntimeResolution()
    {
        HostApplicationBuilder builder = Host.CreateApplicationBuilder();
        builder.Configuration.AddInMemoryCollection(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
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

        global::VectorNNTP.Backfiller.Startup.Hosting.HostComposer.ConfigureHostServices(builder, runtimeOptions);

        using IHost host = builder.Build();

        Assert.Null(host.Services.GetService<ShutdownOptions>());
    }

    [Fact]
    public void ConfigureHostServices_UsesRuntimeSnapshotGracePeriod_ForHostShutdownTimeout()
    {
        HostApplicationBuilder builder = Host.CreateApplicationBuilder();
        builder.Configuration.AddInMemoryCollection(new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
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

        global::VectorNNTP.Backfiller.Startup.Hosting.HostComposer.ConfigureHostServices(builder, runtimeOptions);

        using IHost host = builder.Build();
        IOptions<HostOptions> hostOptions = host.Services.GetRequiredService<IOptions<HostOptions>>();

        Assert.Equal(TimeSpan.FromSeconds(runtimeOptions.ShutdownGracePeriodSeconds), hostOptions.Value.ShutdownTimeout);
    }

    [Fact]
    public void ShouldPublishReadiness_WhenApplicationStoppingAlreadySignaled_ReturnsFalse()
    {
        FakeHostApplicationLifetime lifetime = new();
        lifetime.TriggerApplicationStopping();

        bool shouldPublish = HostLifetimeCoordinator.ShouldPublishReadinessForTesting(lifetime);

        Assert.False(shouldPublish);
    }

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
        builder.Services.AddSingleton(CreateRuntimeOptionsForTesting());
        builder.Services.AddSingleton<ShutdownCoordinator>();
        builder.Services.AddHostedService<ShutdownDuringStartupProbeHostedService>();

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
            WriteBatchCoalesceMicroseconds: 250);
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

    private sealed class FakeHostApplicationLifetime : IHostApplicationLifetime
    {
        private readonly CancellationTokenSource _applicationStarted = new();
        private readonly CancellationTokenSource _applicationStopping = new();
        private readonly CancellationTokenSource _applicationStopped = new();

        public CancellationToken ApplicationStarted => _applicationStarted.Token;

        public CancellationToken ApplicationStopping => _applicationStopping.Token;

        public CancellationToken ApplicationStopped => _applicationStopped.Token;

        public void StopApplication()
        {
            _applicationStopping.Cancel();
        }

        internal void TriggerApplicationStopping()
        {
            _applicationStopping.Cancel();
        }
    }
}

