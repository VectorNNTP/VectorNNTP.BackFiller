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
        /// Confirms the dispose async when transport artifact dispose throws propagates exception without leaking sensitive host behavior.
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
        /// Confirms the dispose async when transport artifacts dispose normally clears fields without diagnostic failures behavior.
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
        /// Confirms the set transport artifact behavior.
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
        /// Confirms the throwing dispose stream behavior.
        /// </summary>
        /// <returns>The value returned by the throwing dispose stream helper.</returns>
        /// <summary>
        /// Confirms the throwing dispose stream behavior.
        /// </summary>
        /// <param name="disposeException">The dispose exception used by this test scenario.</param>
        /// <returns>The value returned by the throwing dispose stream helper.</returns>
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
            /// Confirms position behavior.
            /// </summary>
            public override long Position { get => 0; set => throw new NotSupportedException(); }

            /// <summary>
            /// Confirms the flush behavior.
            /// </summary>
            public override void Flush()
            {
                throw new NotSupportedException();
            }

            /// <summary>
            /// Confirms the read behavior.
            /// </summary>
            /// <returns>The value returned by the read helper.</returns>
            /// <summary>
            /// Confirms the read behavior.
            /// </summary>
            /// <param name="buffer">The buffer used by this test scenario.</param>
            /// <param name="offset">The offset used by this test scenario.</param>
            /// <param name="count">The count used by this test scenario.</param>
            /// <returns>The value returned by the read helper.</returns>
            public override int Read(byte[] buffer, int offset, int count)
            {
                throw new NotSupportedException();
            }

            /// <summary>
            /// Confirms the seek behavior.
            /// </summary>
            /// <returns>The value returned by the seek helper.</returns>
            /// <summary>
            /// Confirms the seek behavior.
            /// </summary>
            /// <param name="offset">The offset used by this test scenario.</param>
            /// <param name="origin">The origin used by this test scenario.</param>
            /// <returns>The value returned by the seek helper.</returns>
            public override long Seek(long offset, SeekOrigin origin)
            {
                throw new NotSupportedException();
            }

            /// <summary>
            /// Confirms the set length behavior.
            /// </summary>
            public override void SetLength(long value)
            {
                throw new NotSupportedException();
            }

            /// <summary>
            /// Confirms the write behavior.
            /// </summary>
            public override void Write(byte[] buffer, int offset, int count)
            {
                throw new NotSupportedException();
            }

            /// <summary>
            /// Confirms the dispose behavior.
            /// </summary>
            protected override void Dispose(bool disposing)
            {
                throw _disposeException;
            }
        }

        /// <summary>
        /// Confirms the tracking dispose stream behavior.
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
            /// Confirms position behavior.
            /// </summary>
            public override long Position { get => 0; set => throw new NotSupportedException(); }

            /// <summary>
            /// Confirms the flush behavior.
            /// </summary>
            public override void Flush()
            {
                throw new NotSupportedException();
            }

            /// <summary>
            /// Confirms the read behavior.
            /// </summary>
            /// <returns>The value returned by the read helper.</returns>
            /// <summary>
            /// Confirms the read behavior.
            /// </summary>
            /// <param name="buffer">The buffer used by this test scenario.</param>
            /// <param name="offset">The offset used by this test scenario.</param>
            /// <param name="count">The count used by this test scenario.</param>
            /// <returns>The value returned by the read helper.</returns>
            public override int Read(byte[] buffer, int offset, int count)
            {
                throw new NotSupportedException();
            }

            /// <summary>
            /// Confirms the seek behavior.
            /// </summary>
            /// <returns>The value returned by the seek helper.</returns>
            /// <summary>
            /// Confirms the seek behavior.
            /// </summary>
            /// <param name="offset">The offset used by this test scenario.</param>
            /// <param name="origin">The origin used by this test scenario.</param>
            /// <returns>The value returned by the seek helper.</returns>
            public override long Seek(long offset, SeekOrigin origin)
            {
                throw new NotSupportedException();
            }

            /// <summary>
            /// Confirms the set length behavior.
            /// </summary>
            public override void SetLength(long value)
            {
                throw new NotSupportedException();
            }

            /// <summary>
            /// Confirms the write behavior.
            /// </summary>
            public override void Write(byte[] buffer, int offset, int count)
            {
                throw new NotSupportedException();
            }

            /// <summary>
            /// Confirms the dispose behavior.
            /// </summary>
            protected override void Dispose(bool disposing)
            {
                DisposeCount++;
                base.Dispose(disposing);
            }
        }

        /// <summary>
        /// Confirms the capturing logger provider behavior.
        /// </summary>
        private sealed class CapturingLoggerProvider
        {
            /// <summary>
            /// Confirms  gate behavior.
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
            /// Confirms the log entry behavior.
            /// </summary>
            /// <returns>The value returned by the log entry helper.</returns>
            /// <summary>
            /// Confirms the log entry behavior.
            /// </summary>
            /// <param name="EventId">The event id used by this test scenario.</param>
            /// <param name="LogLevel">The log level used by this test scenario.</param>
            /// <param name="Message">The message used by this test scenario.</param>
            /// <param name="Exception">The exception used by this test scenario.</param>
            /// <param name="string">The string used by this test scenario.</param>
            /// <param name="StateValues">The state values used by this test scenario.</param>
            /// <returns>The value returned by the log entry helper.</returns>
            internal sealed record LogEntry(EventId EventId, LogLevel LogLevel, string Message, Exception? Exception, IReadOnlyDictionary<string, object?> StateValues);

            /// <summary>
            /// Confirms the capturing logger behavior.
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
                /// Confirms the is enabled behavior.
                /// </summary>
                /// <returns>The value returned by the is enabled helper.</returns>
                /// <summary>
                /// Confirms the is enabled behavior.
                /// </summary>
                /// <param name="logLevel">The log level used by this test scenario.</param>
                /// <returns>The value returned by the is enabled helper.</returns>
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
                /// Confirms the null scope behavior.
                /// </summary>
                private sealed class NullScope : IDisposable
                {
                    /// <summary>
                    /// Confirms instance behavior.
                    /// </summary>
                    internal static readonly NullScope Instance = new();

                    /// <summary>
                    /// Confirms the dispose behavior.
                    /// </summary>
                    public void Dispose()
                    {
                    }
                }
            }
        }
    }

}
