// <copyright file="TransitPublisherLifecycleTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Runtime and startup
// Focused tests for transit publisher lifecycle, covering NNTP article and transport behavior; service lifecycle and shutdown contracts.

using System.Net;
using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using VectorNNTP.Backfiller.Configuration;
using VectorNNTP.Backfiller.Runtime.Transit;
using Xunit;

namespace VectorNNTP.Backfiller.Tests
{
    /// <summary>
    /// Covers transit publisher lifecycle behavior and invariants exercised by this test suite.
    /// </summary>
    public sealed class TransitPublisherLifecycleTests
    {
        /// <summary>
        /// Exercises dispose async  before initialize  does not throw behavior, including the expected result and failure semantics.
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
        /// Exercises dispose async  when partially initialized worker array contains null entries  does not throw behavior, including the expected result and failure semantics.
        /// </summary>
        [Fact]
        public async Task DisposeAsync_WhenPartiallyInitializedWorkerArrayContainsNullEntries_DoesNotThrow()
        {
            await using TransitPublisher publisher = CreatePublisher(port: 19001, connectionPoolSize: 2);

            FieldInfo workersField = typeof(TransitPublisher).GetField("_connectionWorkers", BindingFlags.Instance | BindingFlags.NonPublic)
                ?? throw new InvalidOperationException("_connectionWorkers field was not found.");
            Task[] workers = (Task[])workersField.GetValue(publisher)
                ?? throw new InvalidOperationException("_connectionWorkers field value was null.");

            workers[0] = Task.CompletedTask;

            Exception? exception = await Record.ExceptionAsync(() => publisher.DisposeAsync().AsTask());

            Assert.Null(exception);
            Assert.Equal(TransitConnectionState.Disconnected, publisher.CurrentState);
        }
        /// <summary>
        /// Exercises dispose async  after initialize  does not throw behavior, including the expected result and failure semantics.
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
        /// Exercises dispose async  when called repeatedly  does not throw behavior, including the expected result and failure semantics.
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
        /// Exercises initialize async  when canceled before start  dispose async  does not throw behavior, including the expected result and failure semantics.
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
        /// Exercises host startup failure  before transit initialization  dispose does not mask original exception behavior, including the expected result and failure semantics.
        /// </summary>
        [Fact]
        public async Task HostStartupFailure_BeforeTransitInitialization_DisposeDoesNotMaskOriginalException()
        {
            HostApplicationBuilder builder = Host.CreateApplicationBuilder();
            _ = builder.Services.AddLogging();
            _ = builder.Services.AddSingleton(CreateRuntimeOptions(19005));
            _ = builder.Services.AddSingleton(TimeProvider.System);
            _ = builder.Services.AddSingleton<TransitPublisher>();
            _ = builder.Services.AddHostedService<FailingStartupHostedService>();

            IHost host = builder.Build();

            InvalidOperationException startupException = await Assert.ThrowsAsync<InvalidOperationException>(() => host.StartAsync());
            Assert.Equal("Synthetic startup failure", startupException.Message);

            Exception? disposeException = await Record.ExceptionAsync(() => ((IAsyncDisposable)host).DisposeAsync().AsTask());
            Assert.Null(disposeException);
        }

        /// <summary>
        /// Exercises create publisher behavior, including the expected result and failure semantics.
        /// </summary>
        private static TransitPublisher CreatePublisher(int port, int connectionPoolSize)
        {
            return new TransitPublisher(
                CreateRuntimeOptions(port),
                TimeProvider.System,
                LoggerFactory.Create(static logging => logging.ClearProviders()).CreateLogger<TransitPublisher>(),
                connectionPoolSize,
                perConnectionPipelineDepth: 2);
        }

        /// <summary>
        /// Exercises create runtime options behavior, including the expected result and failure semantics.
        /// </summary>
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

        /// <summary>
        /// Covers failing startup hosted service behavior and invariants exercised by this test suite.
        /// </summary>
        private sealed class FailingStartupHostedService : IHostedService
        {
            /// <summary>
            /// Exercises start async behavior, including the expected result and failure semantics.
            /// </summary>
            public Task StartAsync(CancellationToken cancellationToken)
            {
                throw new InvalidOperationException("Synthetic startup failure");
            }

            /// <summary>
            /// Exercises stop async behavior, including the expected result and failure semantics.
            /// </summary>
            public Task StopAsync(CancellationToken cancellationToken)
            {
                return Task.CompletedTask;
            }
        }
    }
}
