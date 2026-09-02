// <copyright file="TransitConnectionDisposalDiagnosticsTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Runtime and startup
// Behavior and contract tests for transit connection disposal diagnostics.

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
        /// Verifies the DisposeAsync_WhenTransportArtifactDisposeThrows_PropagatesExceptionWithoutLeakingSensitiveHost scenario and expected contract.
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
        /// Verifies the DisposeAsync_WhenTransportArtifactsDisposeNormally_ClearsFieldsWithoutDiagnosticFailures scenario and expected contract.
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
        /// Verifies the SetTransportArtifact scenario and expected contract.
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
        /// Documents the ThrowingDisposeStream test type and its protected contract.
        /// </summary>
        private sealed class ThrowingDisposeStream(Exception disposeException) : Stream
        {
            /// <summary>
            /// Stores the _disposeException fixture value used by these tests.
            /// </summary>
            private readonly Exception _disposeException = disposeException;

            /// <summary>
            /// Stores the CanRead value used by this test fixture.
            /// </summary>
            public override bool CanRead => false;
            /// <summary>
            /// Stores the CanSeek value used by this test fixture.
            /// </summary>
            public override bool CanSeek => false;
            /// <summary>
            /// Stores the CanWrite value used by this test fixture.
            /// </summary>
            public override bool CanWrite => false;
            /// <summary>
            /// Stores the Length value used by this test fixture.
            /// </summary>
            public override long Length => 0;
            /// <summary>
            /// Stores the Position value used by this test fixture.
            /// </summary>
            public override long Position { get => 0; set => throw new NotSupportedException(); }

            /// <summary>
            /// Verifies the Flush scenario and expected contract.
            /// </summary>
            public override void Flush()
            {
                throw new NotSupportedException();
            }

            /// <summary>
            /// Verifies the Read scenario and expected contract.
            /// </summary>
            public override int Read(byte[] buffer, int offset, int count)
            {
                throw new NotSupportedException();
            }

            /// <summary>
            /// Verifies the Seek scenario and expected contract.
            /// </summary>
            public override long Seek(long offset, SeekOrigin origin)
            {
                throw new NotSupportedException();
            }

            /// <summary>
            /// Verifies the SetLength scenario and expected contract.
            /// </summary>
            public override void SetLength(long value)
            {
                throw new NotSupportedException();
            }

            /// <summary>
            /// Verifies the Write scenario and expected contract.
            /// </summary>
            public override void Write(byte[] buffer, int offset, int count)
            {
                throw new NotSupportedException();
            }

            /// <summary>
            /// Verifies the Dispose scenario and expected contract.
            /// </summary>
            protected override void Dispose(bool disposing)
            {
                throw _disposeException;
            }
        }

        /// <summary>
        /// Documents the TrackingDisposeStream test type and its protected contract.
        /// </summary>
        private sealed class TrackingDisposeStream : Stream
        {
            /// <summary>
            /// Stores the DisposeCount value used by this test fixture.
            /// </summary>
            internal int DisposeCount { get; private set; }

            /// <summary>
            /// Stores the CanRead value used by this test fixture.
            /// </summary>
            public override bool CanRead => false;
            /// <summary>
            /// Stores the CanSeek value used by this test fixture.
            /// </summary>
            public override bool CanSeek => false;
            /// <summary>
            /// Stores the CanWrite value used by this test fixture.
            /// </summary>
            public override bool CanWrite => false;
            /// <summary>
            /// Stores the Length value used by this test fixture.
            /// </summary>
            public override long Length => 0;
            /// <summary>
            /// Stores the Position value used by this test fixture.
            /// </summary>
            public override long Position { get => 0; set => throw new NotSupportedException(); }

            /// <summary>
            /// Verifies the Flush scenario and expected contract.
            /// </summary>
            public override void Flush()
            {
                throw new NotSupportedException();
            }

            /// <summary>
            /// Verifies the Read scenario and expected contract.
            /// </summary>
            public override int Read(byte[] buffer, int offset, int count)
            {
                throw new NotSupportedException();
            }

            /// <summary>
            /// Verifies the Seek scenario and expected contract.
            /// </summary>
            public override long Seek(long offset, SeekOrigin origin)
            {
                throw new NotSupportedException();
            }

            /// <summary>
            /// Verifies the SetLength scenario and expected contract.
            /// </summary>
            public override void SetLength(long value)
            {
                throw new NotSupportedException();
            }

            /// <summary>
            /// Verifies the Write scenario and expected contract.
            /// </summary>
            public override void Write(byte[] buffer, int offset, int count)
            {
                throw new NotSupportedException();
            }

            /// <summary>
            /// Verifies the Dispose scenario and expected contract.
            /// </summary>
            protected override void Dispose(bool disposing)
            {
                DisposeCount++;
                base.Dispose(disposing);
            }
        }

        /// <summary>
        /// Documents the CapturingLoggerProvider test type and its protected contract.
        /// </summary>
        private sealed class CapturingLoggerProvider
        {
            /// <summary>
            /// Stores the _gate fixture value used by these tests.
            /// </summary>
            private readonly object _gate = new();

            /// <summary>
            /// Stores the Entries value used by this test fixture.
            /// </summary>
            internal List<LogEntry> Entries { get; } = [];

            internal ILogger<T> CreateLogger<T>()
            {
                return new CapturingLogger<T>(Entries, _gate);
            }

            /// <summary>
            /// Documents the LogEntry test type and its protected contract.
            /// </summary>
            internal sealed record LogEntry(EventId EventId, LogLevel LogLevel, string Message, Exception? Exception, IReadOnlyDictionary<string, object?> StateValues);

            /// <summary>
            /// Documents the CapturingLogger test type and its protected contract.
            /// </summary>
            private sealed class CapturingLogger<T>(List<LogEntry> entries, object gate) : ILogger<T>
            {
                /// <summary>
                /// Stores the _entries fixture value used by these tests.
                /// </summary>
                private readonly List<LogEntry> _entries = entries;
                /// <summary>
                /// Stores the _gate fixture value used by these tests.
                /// </summary>
                private readonly object _gate = gate;

                public IDisposable BeginScope<TState>(TState state) where TState : notnull
                {
                    return NullScope.Instance;
                }

                /// <summary>
                /// Verifies the IsEnabled scenario and expected contract.
                /// </summary>
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
                /// Documents the NullScope test type and its protected contract.
                /// </summary>
                private sealed class NullScope : IDisposable
                {
                    /// <summary>
                    /// Stores the Instance fixture value used by these tests.
                    /// </summary>
                    internal static readonly NullScope Instance = new();

                    /// <summary>
                    /// Verifies the Dispose scenario and expected contract.
                    /// </summary>
                    public void Dispose()
                    {
                    }
                }
            }
        }
    }

}
