// <copyright file="TransitConnectionDisposalDiagnosticsTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Runtime and startup
// Focused tests for transit connection disposal diagnostics, covering NNTP article and transport behavior.
// Primary responsibility: documents the executable contracts covered by the transit connection disposal diagnostics test suite.

using System.Net.Sockets;
using System.Reflection;
using Microsoft.Extensions.Logging;
using VectorNNTP.Backfiller.Runtime.Transit;
using Xunit;

namespace VectorNNTP.Backfiller.Tests
{
    /// <summary>
    /// Tests disposal diagnostics emitted by transit transport artifact teardown.
    /// </summary>
    public sealed class TransitConnectionDisposalDiagnosticsTests
    {
        /// <summary>
        /// Verifies the dispose async when transport artifact dispose throws propagates exception without leaking sensitive host scenario and its documented contract.
        /// </summary>
        [Theory]
        [InlineData("read-stream", "object-disposed")]
        [InlineData("write-stream", "io")]
        [InlineData("transport-stream", "socket")]
        public async Task DisposeAsync_WhenTransportArtifactDisposeThrows_PropagatesExceptionWithoutLeakingSensitiveHost(string artifactName, string exceptionKind)
        {
            CapturingLoggerProvider provider = new();

            TransitConnection connection = new(
                host: "superSecretPassword-host",
                port: 119,
                useSsl: false,
                provider.CreateLogger<TransitPublisher>());

            Exception expected = exceptionKind switch
            {
                "object-disposed" => new ObjectDisposedException("artifact"),
                "io" => new IOException("simulated io failure"),
                "socket" => new SocketException((int)SocketError.ConnectionReset),
                _ => throw new ArgumentOutOfRangeException(nameof(exceptionKind), exceptionKind, "Unknown exception kind."),
            };

            SetTransportArtifact(connection, artifactName, new ThrowingDisposeStream(expected));

            Exception? disposeException = await Record.ExceptionAsync(() => connection.DisposeAsync().AsTask());

            Assert.NotNull(disposeException);
            Assert.IsType(expected.GetType(), disposeException);

            Assert.DoesNotContain(provider.Entries, entry => entry.EventId.Id is 2215 or 2216);

            string rendered = string.Join('|', provider.Entries.Select(static entry =>
                entry.Message + string.Join('|', entry.StateValues.Values.Select(static value => value?.ToString() ?? string.Empty))));
            Assert.DoesNotContain("superSecretPassword", rendered, StringComparison.OrdinalIgnoreCase);
        }
        /// <summary>
        /// Verifies the dispose async when transport artifacts dispose normally clears fields without diagnostic failures scenario and its documented contract.
        /// </summary>
        [Fact]
        public async Task DisposeAsync_WhenTransportArtifactsDisposeNormally_ClearsFieldsWithoutDiagnosticFailures()
        {
            CapturingLoggerProvider provider = new();
            TrackingDisposeStream read = new();
            TrackingDisposeStream write = new();
            TrackingDisposeStream transport = new();

            TransitConnection connection = new(
                host: "localhost",
                port: 119,
                useSsl: false,
                provider.CreateLogger<TransitPublisher>());

            SetTransportArtifact(connection, "read-stream", read);
            SetTransportArtifact(connection, "write-stream", write);
            SetTransportArtifact(connection, "transport-stream", transport);

            Exception? disposeException = await Record.ExceptionAsync(() => connection.DisposeAsync().AsTask());
            Assert.Null(disposeException);

            Assert.Equal(1, read.DisposeCount);
            Assert.Equal(1, write.DisposeCount);
            Assert.Equal(1, transport.DisposeCount);

            Assert.Null(GetFieldValue<Stream>(connection, "_readStream"));
            Assert.Null(GetFieldValue<Stream>(connection, "_writeStream"));
            Assert.Null(GetFieldValue<Stream>(connection, "_transportStream"));

            Assert.DoesNotContain(provider.Entries, entry => entry.EventId.Id is 2215 or 2216);
        }

        /// <summary>
        /// Verifies the set transport artifact scenario and its documented contract.
        /// </summary>
        private static void SetTransportArtifact(TransitConnection connection, string artifactName, Stream stream)
        {
            string fieldName = artifactName switch
            {
                "read-stream" => "_readStream",
                "write-stream" => "_writeStream",
                "transport-stream" => "_transportStream",
                _ => throw new ArgumentOutOfRangeException(nameof(artifactName), artifactName, "Unknown artifact name."),
            };

            FieldInfo? field = typeof(TransitConnection).GetField(fieldName, BindingFlags.Instance | BindingFlags.NonPublic);
            Assert.NotNull(field);
            field.SetValue(connection, stream);
        }

        private static T? GetFieldValue<T>(TransitConnection connection, string fieldName) where T : class
        {
            FieldInfo? field = typeof(TransitConnection).GetField(fieldName, BindingFlags.Instance | BindingFlags.NonPublic);
            Assert.NotNull(field);
            return field.GetValue(connection) as T;
        }

        /// <summary>
        /// Verifies the throwing dispose stream scenario and its documented contract.
        /// </summary>
        /// <returns>The throwing dispose stream value produced for the requested scenario.</returns>
        /// <summary>
        /// Verifies the throwing dispose stream scenario and its documented contract.
        /// </summary>
        /// <param name="disposeException">The dispose exception supplied to the helper.</param>
        /// <returns>The throwing dispose stream value produced for the requested scenario.</returns>
        private sealed class ThrowingDisposeStream(Exception disposeException) : Stream
        {
            /// <summary>
            /// Supplies  dispose exception for the fixture or scenario under test.
            /// </summary>
            private readonly Exception _disposeException = disposeException;

            /// <summary>
            /// Supplies can read for the fixture or scenario under test.
            /// </summary>
            public override bool CanRead => false;
            /// <summary>
            /// Supplies can seek for the fixture or scenario under test.
            /// </summary>
            public override bool CanSeek => false;
            /// <summary>
            /// Supplies can write for the fixture or scenario under test.
            /// </summary>
            public override bool CanWrite => false;
            /// <summary>
            /// Supplies length for the fixture or scenario under test.
            /// </summary>
            public override long Length => 0;
            /// <summary>
            /// Exercises position behavior, including the expected result and failure semantics.
            /// </summary>
            public override long Position { get => 0; set => throw new NotSupportedException(); }

            /// <summary>
        /// Verifies the flush scenario and its documented contract.
            /// </summary>
            public override void Flush()
            {
                throw new NotSupportedException();
            }

            /// <summary>
        /// Verifies the read scenario and its documented contract.
            /// </summary>
        /// <returns>The read value produced for the requested scenario.</returns>
        /// <summary>
        /// Verifies the read scenario and its documented contract.
        /// </summary>
        /// <param name="buffer">The buffer supplied to the helper.</param>
        /// <param name="offset">The offset supplied to the helper.</param>
        /// <param name="count">The count supplied to the helper.</param>
        /// <returns>The read value produced for the requested scenario.</returns>
            public override int Read(byte[] buffer, int offset, int count)
            {
                throw new NotSupportedException();
            }

            /// <summary>
        /// Verifies the seek scenario and its documented contract.
            /// </summary>
        /// <returns>The seek value produced for the requested scenario.</returns>
        /// <summary>
        /// Verifies the seek scenario and its documented contract.
        /// </summary>
        /// <param name="offset">The offset supplied to the helper.</param>
        /// <param name="origin">The origin supplied to the helper.</param>
        /// <returns>The seek value produced for the requested scenario.</returns>
            public override long Seek(long offset, SeekOrigin origin)
            {
                throw new NotSupportedException();
            }

            /// <summary>
        /// Verifies the set length scenario and its documented contract.
            /// </summary>
            public override void SetLength(long value)
            {
                throw new NotSupportedException();
            }

            /// <summary>
        /// Verifies the write scenario and its documented contract.
            /// </summary>
            public override void Write(byte[] buffer, int offset, int count)
            {
                throw new NotSupportedException();
            }

            /// <summary>
        /// Verifies the dispose scenario and its documented contract.
            /// </summary>
            protected override void Dispose(bool disposing)
            {
                throw _disposeException;
            }
        }

        /// <summary>
        /// Verifies the tracking dispose stream scenario and its documented contract.
        /// </summary>
        private sealed class TrackingDisposeStream : Stream
        {
            /// <summary>
            /// Supplies dispose count for the fixture or scenario under test.
            /// </summary>
            internal int DisposeCount { get; private set; }

            /// <summary>
            /// Supplies can read for the fixture or scenario under test.
            /// </summary>
            public override bool CanRead => false;
            /// <summary>
            /// Supplies can seek for the fixture or scenario under test.
            /// </summary>
            public override bool CanSeek => false;
            /// <summary>
            /// Supplies can write for the fixture or scenario under test.
            /// </summary>
            public override bool CanWrite => false;
            /// <summary>
            /// Supplies length for the fixture or scenario under test.
            /// </summary>
            public override long Length => 0;
            /// <summary>
            /// Exercises position behavior, including the expected result and failure semantics.
            /// </summary>
            public override long Position { get => 0; set => throw new NotSupportedException(); }

            /// <summary>
        /// Verifies the flush scenario and its documented contract.
            /// </summary>
            public override void Flush()
            {
                throw new NotSupportedException();
            }

            /// <summary>
        /// Verifies the read scenario and its documented contract.
            /// </summary>
        /// <returns>The read value produced for the requested scenario.</returns>
        /// <summary>
        /// Verifies the read scenario and its documented contract.
        /// </summary>
        /// <param name="buffer">The buffer supplied to the helper.</param>
        /// <param name="offset">The offset supplied to the helper.</param>
        /// <param name="count">The count supplied to the helper.</param>
        /// <returns>The read value produced for the requested scenario.</returns>
            public override int Read(byte[] buffer, int offset, int count)
            {
                throw new NotSupportedException();
            }

            /// <summary>
        /// Verifies the seek scenario and its documented contract.
            /// </summary>
        /// <returns>The seek value produced for the requested scenario.</returns>
        /// <summary>
        /// Verifies the seek scenario and its documented contract.
        /// </summary>
        /// <param name="offset">The offset supplied to the helper.</param>
        /// <param name="origin">The origin supplied to the helper.</param>
        /// <returns>The seek value produced for the requested scenario.</returns>
            public override long Seek(long offset, SeekOrigin origin)
            {
                throw new NotSupportedException();
            }

            /// <summary>
        /// Verifies the set length scenario and its documented contract.
            /// </summary>
            public override void SetLength(long value)
            {
                throw new NotSupportedException();
            }

            /// <summary>
        /// Verifies the write scenario and its documented contract.
            /// </summary>
            public override void Write(byte[] buffer, int offset, int count)
            {
                throw new NotSupportedException();
            }

            /// <summary>
        /// Verifies the dispose scenario and its documented contract.
            /// </summary>
            protected override void Dispose(bool disposing)
            {
                DisposeCount++;
                base.Dispose(disposing);
            }
        }

        /// <summary>
        /// Verifies the capturing logger provider scenario and its documented contract.
        /// </summary>
        private sealed class CapturingLoggerProvider
        {
            /// <summary>
            /// Exercises  gate behavior, including the expected result and failure semantics.
            /// </summary>
            private readonly object _gate = new();

            /// <summary>
            /// Supplies entries for the fixture or scenario under test.
            /// </summary>
            internal List<LogEntry> Entries { get; } = [];

            internal ILogger<T> CreateLogger<T>()
            {
                return new CapturingLogger<T>(Entries, _gate);
            }

            /// <summary>
        /// Verifies the log entry scenario and its documented contract.
            /// </summary>
        /// <returns>The log entry value produced for the requested scenario.</returns>
        /// <summary>
        /// Verifies the log entry scenario and its documented contract.
        /// </summary>
        /// <param name="EventId">The event id supplied to the helper.</param>
        /// <param name="LogLevel">The log level supplied to the helper.</param>
        /// <param name="Message">The message supplied to the helper.</param>
        /// <param name="Exception">The exception supplied to the helper.</param>
        /// <param name="string">The string supplied to the helper.</param>
        /// <param name="StateValues">The state values supplied to the helper.</param>
        /// <returns>The log entry value produced for the requested scenario.</returns>
            internal sealed record LogEntry(EventId EventId, LogLevel LogLevel, string Message, Exception? Exception, IReadOnlyDictionary<string, object?> StateValues);

            /// <summary>
        /// Verifies the capturing logger scenario and its documented contract.
            /// </summary>
            private sealed class CapturingLogger<T>(List<LogEntry> entries, object gate) : ILogger<T>
            {
                /// <summary>
                /// Supplies  entries for the fixture or scenario under test.
                /// </summary>
                private readonly List<LogEntry> _entries = entries;
                /// <summary>
                /// Supplies  gate for the fixture or scenario under test.
                /// </summary>
                private readonly object _gate = gate;

                public IDisposable BeginScope<TState>(TState state) where TState : notnull
                {
                    return NullScope.Instance;
                }

                /// <summary>
        /// Verifies the is enabled scenario and its documented contract.
                /// </summary>
        /// <returns>The is enabled value produced for the requested scenario.</returns>
        /// <summary>
        /// Verifies the is enabled scenario and its documented contract.
        /// </summary>
        /// <param name="logLevel">The log level supplied to the helper.</param>
        /// <returns>The is enabled value produced for the requested scenario.</returns>
                public bool IsEnabled(LogLevel logLevel)
                {
                    return true;
                }

                public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
                {
                    string message = formatter(state, exception);
                    Dictionary<string, object?> stateValues = [];
                    if (state is IEnumerable<KeyValuePair<string, object?>> structuredState)
                    {
                        foreach (KeyValuePair<string, object?> item in structuredState)
                        {
                            stateValues[item.Key] = item.Value;
                        }
                    }

                    lock (_gate)
                    {
                        _entries.Add(new LogEntry(eventId, logLevel, message, exception, stateValues));
                    }
                }

                /// <summary>
        /// Verifies the null scope scenario and its documented contract.
                /// </summary>
                private sealed class NullScope : IDisposable
                {
                    /// <summary>
                    /// Exercises instance behavior, including the expected result and failure semantics.
                    /// </summary>
                    internal static readonly NullScope Instance = new();

                    /// <summary>
        /// Verifies the dispose scenario and its documented contract.
                    /// </summary>
                    public void Dispose()
                    {
                    }
                }
            }
        }
    }

}
