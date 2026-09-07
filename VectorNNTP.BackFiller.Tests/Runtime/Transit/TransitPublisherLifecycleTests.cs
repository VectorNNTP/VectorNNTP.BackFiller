// <copyright file="TransitPublisherLifecycleTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Runtime and startup
// Focused tests for transit publisher lifecycle, covering NNTP article and transport behavior; service lifecycle and shutdown contracts.
// Primary responsibility: documents the executable contracts covered by the transit publisher lifecycle test suite.

using System.Net;
using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using VectorNNTP.Backfiller.Configuration;
using VectorNNTP.Backfiller.Runtime.Articles.Processing;
using VectorNNTP.Backfiller.Runtime.Articles.Retention;
using VectorNNTP.Backfiller.Runtime.Transit;
using Xunit;

namespace VectorNNTP.BackFiller.Tests.Runtime.Transit
{
    /// <summary>
    /// Confirms the transit publisher lifecycle tests behavior.
    /// </summary>
    public sealed class TransitPublisherLifecycleTests
    {
        /// <summary>
        /// Confirms the dispose async before initialize does not throw behavior.
        /// </summary>
        [Fact]
        public async Task DisposeAsync_BeforeInitialize_DoesNotThrow()
        {
            await using TransitPublisher publisher = CreatePublisher(port: 19000, connectionPoolSize: 1);

            Exception? exception = await Record.ExceptionAsync(() => publisher.DisposeAsync().AsTask());

            Assert.Null(exception);
            Assert.Equal(TransitConnectionState.Disconnected, publisher.CurrentState);
        }
        /// <summary>
        /// Confirms the dispose async when partially initialized worker array contains null entries does not throw behavior.
        /// </summary>
        [Fact]
        public async Task DisposeAsync_WhenPartiallyInitializedWorkerArrayContainsNullEntries_DoesNotThrow()
        {
            await using TransitPublisher publisher = CreatePublisher(port: 19001, connectionPoolSize: 2);

            FieldInfo workersField = typeof(TransitPublisher).GetField("_connectionWorkers", BindingFlags.Instance | BindingFlags.NonPublic)
                ?? throw new InvalidOperationException("_connectionWorkers field was not found.");
            object? workersRaw = workersField.GetValue(publisher);
            Task[] workers = Assert.IsType<Task[]>(workersRaw);

            workers[0] = Task.CompletedTask;

            Exception? exception = await Record.ExceptionAsync(() => publisher.DisposeAsync().AsTask());

            Assert.Null(exception);
            Assert.Equal(TransitConnectionState.Disconnected, publisher.CurrentState);
        }
        /// <summary>
        /// Confirms the dispose async after initialize does not throw behavior.
        /// </summary>
        [Fact]
        public async Task DisposeAsync_AfterInitialize_DoesNotThrow()
        {
            await using TransitPublisher publisher = CreatePublisher(port: 19002, connectionPoolSize: 1);

            await publisher.InitializeAsync(CancellationToken.None);
            Exception? exception = await Record.ExceptionAsync(() => publisher.DisposeAsync().AsTask());

            Assert.Null(exception);
            Assert.Equal(TransitConnectionState.Disconnected, publisher.CurrentState);
        }
        /// <summary>
        /// Confirms the dispose async when called repeatedly does not throw behavior.
        /// </summary>
        [Fact]
        public async Task DisposeAsync_WhenCalledRepeatedly_DoesNotThrow()
        {
            await using TransitPublisher publisher = CreatePublisher(port: 19003, connectionPoolSize: 1);

            await publisher.DisposeAsync();
            Exception? exception = await Record.ExceptionAsync(() => publisher.DisposeAsync().AsTask());

            Assert.Null(exception);
            Assert.Equal(TransitConnectionState.Disconnected, publisher.CurrentState);
        }
        /// <summary>
        /// Confirms the initialize async when canceled before start dispose async does not throw behavior.
        /// </summary>
        [Fact]
        public async Task InitializeAsync_WhenCanceledBeforeStart_DisposeAsync_DoesNotThrow()
        {
            await using TransitPublisher publisher = CreatePublisher(port: 19004, connectionPoolSize: 1);
            using CancellationTokenSource cancellation = new();
            cancellation.Cancel();

            await Assert.ThrowsAsync<OperationCanceledException>(() => publisher.InitializeAsync(cancellation.Token));
            Exception? disposeException = await Record.ExceptionAsync(() => publisher.DisposeAsync().AsTask());

            Assert.Null(disposeException);
            Assert.Equal(TransitConnectionState.Disconnected, publisher.CurrentState);
        }
        /// <summary>
        /// Confirms the host startup failure before transit initialization dispose does not mask original exception behavior.
        /// </summary>
        [Fact]
        public async Task HostStartupFailure_BeforeTransitInitialization_DisposeDoesNotMaskOriginalException()
        {
            HostApplicationBuilder builder = Host.CreateApplicationBuilder();
            _ = builder.Services.AddLogging();
            BackFillerRuntimeOptions options = CreateRuntimeOptions(19005);
            _ = builder.Services.AddSingleton(options);
            _ = builder.Services.AddSingleton(TimeProvider.System);
            _ = builder.Services.AddSingleton<IArticleRetentionAuthority>(new ArticleRetentionAuthority(options));
            _ = builder.Services.AddSingleton<TransitPublisher>();
            _ = builder.Services.AddHostedService<FailingStartupHostedService>();

            IHost host = builder.Build();

            InvalidOperationException startupException = await Assert.ThrowsAsync<InvalidOperationException>(() => host.StartAsync());
            Assert.Equal("Synthetic startup failure", startupException.Message);

            Exception? disposeException = await Record.ExceptionAsync(() => ((IAsyncDisposable)host).DisposeAsync().AsTask());
            Assert.Null(disposeException);
        }

        [Fact]
        public async Task StopAsync_WhenProcessingDrainInFlight_WaitsBeforeDisposingTransitPublisher()
        {
            BackFillerRuntimeOptions options = CreateRuntimeOptions(19006);
            ArticleRetentionAuthority retentionAuthority = new(options);
            await using TransitPublisher publisher = new(
                options,
                TimeProvider.System,
                NullLogger<TransitPublisher>.Instance,
                retentionAuthority,
                connectionPoolSize: 1,
                perConnectionPipelineDepth: 2);

            ArticleProcessingDrainBarrier barrier = new();
            barrier.EnterProcessingScope();
            TransitPublisherStartupInitializer initializer = new(publisher, barrier, NullLogger<TransitPublisherStartupInitializer>.Instance);

            using CancellationTokenSource stopTimeout = new(TimeSpan.FromSeconds(10));
            Task stopTask = initializer.StopAsync(stopTimeout.Token);
            await Task.Yield();

            Assert.False(stopTask.IsCompleted);
            Assert.False(GetDisposeRequestedForTesting(publisher));

            barrier.SignalProcessingLoopCompleted();
            barrier.ExitProcessingScope();

            await stopTask.WaitAsync(stopTimeout.Token).ConfigureAwait(false);
            Assert.True(GetDisposeRequestedForTesting(publisher));
        }

        [Fact]
        public async Task StopAsync_WhenDrainDoesNotCompleteBeforeCancellation_DoesNotDisposeTransitPublisher()
        {
            BackFillerRuntimeOptions options = CreateRuntimeOptions(19007);
            ArticleRetentionAuthority retentionAuthority = new(options);
            await using TransitPublisher publisher = new(
                options,
                TimeProvider.System,
                NullLogger<TransitPublisher>.Instance,
                retentionAuthority,
                connectionPoolSize: 1,
                perConnectionPipelineDepth: 2);

            ArticleProcessingDrainBarrier barrier = new();
            TransitPublisherStartupInitializer initializer = new(publisher, barrier, NullLogger<TransitPublisherStartupInitializer>.Instance);

            using CancellationTokenSource stopTimeout = new();
            stopTimeout.Cancel();

            _ = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => initializer.StopAsync(stopTimeout.Token));
            Assert.False(GetDisposeRequestedForTesting(publisher));
        }

        [Fact]
        public async Task StopAsync_WhenProcessingDrainInFlight_TransitAdmissionRemainsAvailableUntilDrainCompletes()
        {
            BackFillerRuntimeOptions options = CreateRuntimeOptions(19008);
            ArticleRetentionAuthority retentionAuthority = new(options);
            await using TransitPublisher publisher = new(
                options,
                TimeProvider.System,
                NullLogger<TransitPublisher>.Instance,
                retentionAuthority,
                connectionPoolSize: 1,
                perConnectionPipelineDepth: 2);

            await publisher.InitializeAsync(CancellationToken.None).ConfigureAwait(false);

            ArticleProcessingDrainBarrier barrier = new();
            barrier.EnterProcessingScope();
            TransitPublisherStartupInitializer initializer = new(publisher, barrier, NullLogger<TransitPublisherStartupInitializer>.Instance);

            using CancellationTokenSource stopTimeout = new(TimeSpan.FromSeconds(10));
            Task stopTask = initializer.StopAsync(stopTimeout.Token);
            await Task.Yield();

            TransitAdmissionResult duringDrain = await publisher.AdmitAsync("<drain-active@example.com>", CancellationToken.None).ConfigureAwait(false);
            Assert.Equal(TransitAdmissionStatus.Accepted, duringDrain.Status);

            barrier.SignalProcessingLoopCompleted();
            barrier.ExitProcessingScope();
            await stopTask.WaitAsync(stopTimeout.Token).ConfigureAwait(false);

            TransitAdmissionResult afterStop = await publisher.AdmitAsync("<post-stop@example.com>", CancellationToken.None).ConfigureAwait(false);
            Assert.Equal(TransitAdmissionStatus.Unavailable, afterStop.Status);
        }

        /// <summary>
        /// Confirms the create publisher behavior.
        /// </summary>
        /// <returns>The value returned by the create publisher helper.</returns>
        /// <summary>
        /// Confirms the create publisher behavior.
        /// </summary>
        /// <param name="port">The port used by this test scenario.</param>
        /// <param name="connectionPoolSize">The connection pool size used by this test scenario.</param>
        /// <returns>The value returned by the create publisher helper.</returns>
        private static TransitPublisher CreatePublisher(int port, int connectionPoolSize)
        {
            BackFillerRuntimeOptions options = CreateRuntimeOptions(port);
            return new TransitPublisher(
                options,
                TimeProvider.System,
                LoggerFactory.Create(static logging => logging.ClearProviders()).CreateLogger<TransitPublisher>(),
                new ArticleRetentionAuthority(options),
                connectionPoolSize,
                perConnectionPipelineDepth: 2);
        }

        /// <summary>
        /// Confirms the create runtime options behavior.
        /// </summary>
        /// <returns>The value returned by the create runtime options helper.</returns>
        /// <summary>
        /// Confirms the create runtime options behavior.
        /// </summary>
        /// <param name="port">The port used by this test scenario.</param>
        /// <returns>The value returned by the create runtime options helper.</returns>
        private static BackFillerRuntimeOptions CreateRuntimeOptions(int port)
        {
            return new BackFillerRuntimeOptions(
                CanonicalBackFillerFqdn: "bf.example.com",
                BackFillerId: 42,
                CanonicalDnsSuffix: "example.com",
                ValidatedLogDirectory: "C:\\logs",
                ValidatedCertificateDirectory: "C:\\certs",
                RabbitMqHosts: ["localhost"],
                RabbitMqPort: 5672,
                RabbitMqEnableSsl: false,
                TransitServerHost: IPAddress.Loopback.ToString(),
                TransitServerPort: port,
                TransitServerUseSsl: false,
                ShutdownGracePeriodSeconds: 60,
                ShutdownDrainQueuedWork: true,
                ShutdownFinishActiveArticles: true,
                RabbitMqMaximumShutdownDrainTimeoutSeconds: 120,
                WriteBatchCoalesceMicroseconds: 250);
        }

        private static bool GetDisposeRequestedForTesting(TransitPublisher publisher)
        {
            FieldInfo disposeRequestedField = typeof(TransitPublisher).GetField("_disposeRequested", BindingFlags.Instance | BindingFlags.NonPublic)
                ?? throw new InvalidOperationException("_disposeRequested field was not found.");

            object? value = disposeRequestedField.GetValue(publisher);
            return value is true;
        }

        /// <summary>
        /// Confirms the failing startup hosted service behavior.
        /// </summary>
        private sealed class FailingStartupHostedService : IHostedService
        {
            /// <summary>
            /// Confirms the start async behavior.
            /// </summary>
            /// <returns>The value returned by the start async helper.</returns>
            /// <summary>
            /// Confirms the start async behavior.
            /// </summary>
            /// <param name="cancellationToken">The cancellation token used by this test scenario.</param>
            /// <returns>The value returned by the start async helper.</returns>
            public Task StartAsync(CancellationToken cancellationToken)
            {
                throw new InvalidOperationException("Synthetic startup failure");
            }

            /// <summary>
            /// Confirms the stop async behavior.
            /// </summary>
            /// <returns>The value returned by the stop async helper.</returns>
            /// <summary>
            /// Confirms the stop async behavior.
            /// </summary>
            /// <param name="cancellationToken">The cancellation token used by this test scenario.</param>
            /// <returns>The value returned by the stop async helper.</returns>
            public Task StopAsync(CancellationToken cancellationToken)
            {
                return Task.CompletedTask;
            }
        }
    }
}
