using System.Net.Sockets;
using System.Reflection;
using Microsoft.Extensions.Logging;
using VectorNNTP.Backfiller.Runtime.Transit;
using Xunit;

namespace VectorNNTP.Backfiller.Tests;

/// <summary>
/// Tests disposal diagnostics emitted by transit transport artifact teardown.
/// </summary>
public sealed class TransitConnectionDisposalDiagnosticsTests
{
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

    private sealed class ThrowingDisposeStream(Exception disposeException) : Stream
    {
        private readonly Exception _disposeException = disposeException;

        public override bool CanRead => false;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => 0;
        public override long Position { get => 0; set => throw new NotSupportedException(); }

        public override void Flush() => throw new NotSupportedException();
        public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

        protected override void Dispose(bool disposing)
        {
            throw _disposeException;
        }
    }

    private sealed class TrackingDisposeStream : Stream
    {
        internal int DisposeCount { get; private set; }

        public override bool CanRead => false;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => 0;
        public override long Position { get => 0; set => throw new NotSupportedException(); }

        public override void Flush() => throw new NotSupportedException();
        public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

        protected override void Dispose(bool disposing)
        {
            DisposeCount++;
            base.Dispose(disposing);
        }
    }

    private sealed class CapturingLoggerProvider
    {
        private readonly object _gate = new();

        internal List<LogEntry> Entries { get; } = [];

        internal ILogger<T> CreateLogger<T>()
        {
            return new CapturingLogger<T>(Entries, _gate);
        }

        internal sealed record LogEntry(EventId EventId, LogLevel LogLevel, string Message, Exception? Exception, IReadOnlyDictionary<string, object?> StateValues);

        private sealed class CapturingLogger<T>(List<LogEntry> entries, object gate) : ILogger<T>
        {
            private readonly List<LogEntry> _entries = entries;
            private readonly object _gate = gate;

            public IDisposable BeginScope<TState>(TState state) where TState : notnull
            {
                return NullScope.Instance;
            }

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

            private sealed class NullScope : IDisposable
            {
                internal static readonly NullScope Instance = new();

                public void Dispose()
                {
                }
            }
        }
    }
}

