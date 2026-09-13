// <copyright file="ConsoleCaptureSerializationCollection.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Benchmarks
// Shared xUnit collection definition that serializes tests mutating process-global Console.Out.

using Xunit;

namespace VectorNNTP.BackFiller.Tests.Benchmarks
{
    /// <summary>
    /// Defines the shared non-parallel collection for benchmark tests that capture and restore Console.Out.
    /// </summary>
    [CollectionDefinition(Name, DisableParallelization = true)]
    public sealed class ConsoleCaptureSerializationCollection
    {
        /// <summary>
        /// Shared collection name for console-capturing benchmark contract tests.
        /// </summary>
        public const string Name = "Benchmarks.ConsoleCapture";
    }
}
