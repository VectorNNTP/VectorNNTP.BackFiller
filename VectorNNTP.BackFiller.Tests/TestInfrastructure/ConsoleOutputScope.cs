// <copyright file="ConsoleOutputScope.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / TestInfrastructure
// Serializes test ownership of process-global Console.Out during redirected output capture.
// Primary responsibility: prevent concurrent test interference while a test temporarily replaces Console.Out.

namespace VectorNNTP.BackFiller.Tests.TestInfrastructure
{
    /// <summary>
    /// Serializes temporary redirection of process-global <see cref="Console.Out"/> across the test process.
    /// </summary>
    /// <remarks>
    /// Because <see cref="Console.Out"/> is process-global, independent per-test or per-class locks are insufficient to
    /// prevent concurrent redirection races. This scope owns the complete interval from saving the current writer,
    /// through replacement and test execution, until the captured output has been fully read and the previous writer
    /// has been restored.
    /// </remarks>
    internal sealed class ConsoleOutputScope : IDisposable, IAsyncDisposable
    {
        /// <summary>
        /// Shared async-compatible process-wide gate guarding temporary ownership of <see cref="Console.Out"/>.
        /// </summary>
        private static readonly SemaphoreSlim Gate = new(1, 1);

        /// <summary>
        /// Stores the writer that was active before the scope installed its redirected writer.
        /// </summary>
        private readonly TextWriter _originalWriter;

        /// <summary>
        /// Stores the captured string writer when the scope owns in-memory capture.
        /// </summary>
        private readonly StringWriter? _capturedWriter;

        /// <summary>
        /// Indicates whether this scope successfully acquired the shared console gate.
        /// </summary>
        private readonly bool _ownsGate;

        /// <summary>
        /// Indicates whether the scope has already restored the previous console writer and released its gate ownership.
        /// </summary>
        private bool _disposed;

        /// <summary>
        /// Initializes a new scope that redirects <see cref="Console.Out"/> to the supplied writer under exclusive ownership.
        /// </summary>
        /// <param name="redirectedWriter">Writer that should temporarily receive console output.</param>
        /// <param name="capturedWriter">Backing captured writer when the scope owns in-memory capture; otherwise <see langword="null"/>.</param>
        /// <param name="ownsGate"><see langword="true"/> when the shared console gate has already been acquired for this scope.</param>
        private ConsoleOutputScope(TextWriter redirectedWriter, StringWriter? capturedWriter, bool ownsGate)
        {
            ArgumentNullException.ThrowIfNull(redirectedWriter);
            _capturedWriter = capturedWriter;
            _originalWriter = Console.Out;
            _ownsGate = ownsGate;
            Console.SetOut(redirectedWriter);
        }

        /// <summary>
        /// Creates a new scope that grants exclusive redirected ownership of <see cref="Console.Out"/> using synchronized in-memory capture.
        /// </summary>
        /// <returns>A scope that must be disposed after output has been captured and assertions are complete.</returns>
        internal static ConsoleOutputScope Capture()
        {
            StringWriter capturedWriter = new();
            TextWriter redirectedWriter = TextWriter.Synchronized(capturedWriter);
            Gate.Wait();

            try
            {
                return new ConsoleOutputScope(redirectedWriter, capturedWriter, ownsGate: true);
            }
            catch
            {
                capturedWriter.Dispose();
                Gate.Release();
                throw;
            }
        }

        /// <summary>
        /// Creates a new scope that grants exclusive redirected ownership of <see cref="Console.Out"/> using a caller-provided writer.
        /// </summary>
        /// <param name="redirectedWriter">Writer that should receive console output while the scope is active.</param>
        /// <returns>A scope that restores the previous writer when disposed.</returns>
        internal static ConsoleOutputScope CaptureTo(TextWriter redirectedWriter)
        {
            ArgumentNullException.ThrowIfNull(redirectedWriter);
            Gate.Wait();

            try
            {
                return new ConsoleOutputScope(redirectedWriter, capturedWriter: null, ownsGate: true);
            }
            catch
            {
                Gate.Release();
                throw;
            }
        }

        /// <summary>
        /// Creates a new scope that grants exclusive redirected ownership of <see cref="Console.Out"/> using a caller-provided writer and async gate acquisition.
        /// </summary>
        /// <param name="redirectedWriter">Writer that should receive console output while the scope is active.</param>
        /// <param name="cancellationToken">Cancellation token applied while waiting for the shared console gate.</param>
        /// <returns>A scope that restores the previous writer when disposed.</returns>
        internal static async Task<ConsoleOutputScope> CaptureToAsync(TextWriter redirectedWriter, CancellationToken cancellationToken = default)
        {
            ArgumentNullException.ThrowIfNull(redirectedWriter);
            await Gate.WaitAsync(cancellationToken).ConfigureAwait(false);

            try
            {
                return new ConsoleOutputScope(redirectedWriter, capturedWriter: null, ownsGate: true);
            }
            catch
            {
                Gate.Release();
                throw;
            }
        }

        /// <summary>
        /// Returns the captured console output accumulated so far.
        /// </summary>
        /// <returns>The current buffered output written through the redirected console writer.</returns>
        /// <exception cref="InvalidOperationException">The scope was not created with in-memory capture.</exception>
        internal string GetCapturedOutput()
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            Console.Out.Flush();
            return _capturedWriter?.ToString()
                ?? throw new InvalidOperationException("Captured output is available only for in-memory console capture scopes.");
        }

        /// <summary>
        /// Restores the previously active console writer and releases the shared process-wide console gate.
        /// </summary>
        public void Dispose()
        {
            if (_disposed)
            {
                return;
            }

            try
            {
                Console.Out.Flush();
                Console.SetOut(_originalWriter);
            }
            finally
            {
                _capturedWriter?.Dispose();
                _disposed = true;
                if (_ownsGate)
                {
                    Gate.Release();
                }
            }
        }

        /// <summary>
        /// Asynchronously disposes the scope after restoring the previous console writer and releasing the shared gate.
        /// </summary>
        /// <returns>A completed value task after synchronous restore/release work finishes.</returns>
        public ValueTask DisposeAsync()
        {
            Dispose();
            return ValueTask.CompletedTask;
        }
    }
}
