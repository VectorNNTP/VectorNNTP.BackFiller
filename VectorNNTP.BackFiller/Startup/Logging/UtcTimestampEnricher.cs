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
    /// Serilog enricher that projects each event timestamp into a dedicated UTC property used by startup and production sink templates.
    /// </summary>
    /// <remarks>
    /// This enricher supports deterministic UTC-only formatting across bootstrap and host-configured log pipelines by
    /// providing a stable <c>UtcTimestamp</c> field independent of sink-local timestamp rendering behavior.
    /// </remarks>
    internal sealed class UtcTimestampEnricher : ILogEventEnricher
    {
        /// <summary>
        /// Structured property name emitted on each enriched event for UTC timestamp rendering.
        /// </summary>
        internal const string UtcTimestampPropertyName = "UtcTimestamp";

        /// <summary>
        /// Adds the <c>UtcTimestamp</c> property when absent, using <see cref="LogEvent.Timestamp"/> converted to UTC.
        /// </summary>
        /// <param name="logEvent">Serilog event instance to enrich.</param>
        /// <param name="propertyFactory">Factory that creates the structured UTC timestamp property value.</param>
        /// <exception cref="ArgumentNullException"><paramref name="logEvent"/> or <paramref name="propertyFactory"/> is <see langword="null"/>.</exception>
        /// <remarks>
        /// Uses <see cref="LogEvent.AddPropertyIfAbsent(LogEventProperty)"/> so existing upstream
        /// <c>UtcTimestamp</c> values are preserved rather than overwritten.
        /// </remarks>
        public void Enrich(LogEvent logEvent, ILogEventPropertyFactory propertyFactory)
        {
            ArgumentNullException.ThrowIfNull(logEvent);
            ArgumentNullException.ThrowIfNull(propertyFactory);

            LogEventProperty utcTimestamp = propertyFactory.CreateProperty(UtcTimestampPropertyName, logEvent.Timestamp.UtcDateTime);
            logEvent.AddPropertyIfAbsent(utcTimestamp);
        }
    }
}
