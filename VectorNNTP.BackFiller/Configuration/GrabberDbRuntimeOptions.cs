// <copyright file="GrabberDbRuntimeOptions.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
// Architectural responsibility: immutable grabber database runtime projection in the vector nntp.back filler configuration subsystem.

using MySqlConnector;

namespace VectorNNTP.Backfiller.Configuration
{
    /// <summary>
    /// Immutable validated GrabberDB runtime projection frozen during startup validation.
    /// </summary>
    /// <param name="ConnectionString">Canonical provider connection string used for runtime provisioning and queries.</param>
    /// <param name="Server">Canonical MySQL server/host selected during startup projection.</param>
    /// <param name="Port">Canonical MySQL TCP port selected during startup projection.</param>
    /// <param name="Database">Canonical MySQL database selected during startup projection.</param>
    /// <param name="UserId">Canonical MySQL user identifier selected during startup projection.</param>
    /// <param name="SslMode">Canonical MySQL TLS mode selected during startup projection.</param>
    internal sealed record GrabberDbRuntimeOptions(
        string ConnectionString,
        string Server,
        uint Port,
        string Database,
        string UserId,
        MySqlSslMode SslMode);
}
