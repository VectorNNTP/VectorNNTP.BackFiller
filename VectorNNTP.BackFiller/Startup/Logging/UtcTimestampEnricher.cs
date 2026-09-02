// <copyright file="UtcTimestampEnricher.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
// Architectural responsibility: utc timestamp enricher in the startup logging subsystem.
// The file owns this boundary; executable behavior is intentionally unchanged.

// <copyright file="UtcTimestampEnricher.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Startup / Logging
// Serilog enricher that emits an explicit UTC timestamp property for deterministic UTC-only sink formatting.

using Serilog.Core;
using Serilog.Events;

namespace VectorNNTP.Backfiller.Startup.Logging
{
    /// <summary>
    /// Adds a deterministic UTC timestamp property to each Serilog event so sink templates can render UTC consistently.
    /// </summary>
    internal sealed class UtcTimestampEnricher : ILogEventEnricher
    {
        /// <summary>
        /// Name of the UTC timestamp property injected into each log event.
        /// </summary>
        internal const string UtcTimestampPropertyName = "UtcTimestamp";

        /// <summary>
        /// Populates the <c>UtcTimestamp</c> property using the event timestamp converted to UTC.
        /// </summary>
        /// <param name="logEvent">The Serilog event being enriched.</param>
        /// <param name="propertyFactory">Factory used to create structured event properties.</param>
        public void Enrich(LogEvent logEvent, ILogEventPropertyFactory propertyFactory)
        {
            ArgumentNullException.ThrowIfNull(logEvent);
            ArgumentNullException.ThrowIfNull(propertyFactory);

            LogEventProperty utcTimestamp = propertyFactory.CreateProperty(UtcTimestampPropertyName, logEvent.Timestamp.UtcDateTime);
            logEvent.AddPropertyIfAbsent(utcTimestamp);
        }
    }
}
