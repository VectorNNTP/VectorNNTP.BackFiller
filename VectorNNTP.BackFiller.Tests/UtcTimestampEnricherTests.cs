// <copyright file="UtcTimestampEnricherTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Runtime and startup
// Focused tests for utc timestamp enricher, covering configuration, runtime, and failure-handling contracts exercised by the tests.
// Primary responsibility: documents the executable contracts covered by the utc timestamp enricher test suite.

using Serilog.Core;
using Serilog.Events;
using VectorNNTP.Backfiller.Startup.Logging;
using Xunit;

namespace VectorNNTP.Backfiller.Tests
{
    /// <summary>
    /// Verifies that UTC timestamp enrichment emits deterministic UTC values for sink formatting.
    /// </summary>
    public sealed class UtcTimestampEnricherTests
    {
        /// <summary>
        /// Confirms the enricher publishes a UTC timestamp property from the source event timestamp.
        /// </summary>
        [Fact]
        public void Enrich_WhenInvoked_AddsUtcTimestampPropertyUsingUtcValue()
        {
            DateTimeOffset sourceTimestamp = new(2025, 1, 2, 3, 4, 5, TimeSpan.FromHours(-7));
            LogEvent logEvent = new(
                sourceTimestamp,
                LogEventLevel.Information,
                exception: null,
                new MessageTemplate("test", []),
                []);

            UtcTimestampEnricher enricher = new();
            TestLogEventPropertyFactory propertyFactory = new();

            enricher.Enrich(logEvent, propertyFactory);

            Assert.True(logEvent.Properties.TryGetValue(UtcTimestampEnricher.UtcTimestampPropertyName, out LogEventPropertyValue? property));
            ScalarValue scalar = Assert.IsType<ScalarValue>(property);
            DateTime utcValue = Assert.IsType<DateTime>(scalar.Value);
            Assert.Equal(DateTimeKind.Utc, utcValue.Kind);
            Assert.Equal(sourceTimestamp.UtcDateTime, utcValue);
        }

        /// <summary>
        /// Minimal property factory used to validate enricher behavior without external sink dependencies.
        /// </summary>
        private sealed class TestLogEventPropertyFactory : ILogEventPropertyFactory
        {
            /// <summary>
            /// Creates a Serilog event property from a name/value pair.
            /// </summary>
            /// <param name="name">Property name.</param>
            /// <param name="value">Property value.</param>
            /// <param name="destructureObjects">Destructuring hint.</param>
            /// <returns>Log event property instance.</returns>
            /// <summary>
            /// Confirms the create property behavior.
            /// </summary>
            /// <param name="name">The name used by this test scenario.</param>
            /// <param name="value">The value used by this test scenario.</param>
            /// <param name="destructureObjects">The destructure objects used by this test scenario.</param>
            /// <returns>The value returned by the create property helper.</returns>
            public LogEventProperty CreateProperty(string name, object? value, bool destructureObjects = false)
            {
                return new LogEventProperty(name, new ScalarValue(value));
            }
        }
    }
}
