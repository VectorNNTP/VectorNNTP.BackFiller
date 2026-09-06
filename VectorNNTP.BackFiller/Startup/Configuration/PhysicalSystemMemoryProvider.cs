// <copyright file="PhysicalSystemMemoryProvider.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Startup / Configuration
// Resolves total physical system memory using platform-specific mechanisms.

using System.Runtime.InteropServices;

namespace VectorNNTP.Backfiller.Startup.Configuration
{
    /// <summary>
    /// Resolves total physical system memory in bytes for startup configuration policy validation.
    /// </summary>
    internal sealed class PhysicalSystemMemoryProvider : IPhysicalSystemMemoryProvider
    {
        private const string ProcMemInfoPath = "/proc/meminfo";

        /// <inheritdoc/>
        public ulong GetTotalPhysicalMemoryBytes()
        {
            return RuntimeInformation.IsOSPlatform(OSPlatform.Linux)
                ? GetLinuxMemTotalBytes()
                : RuntimeInformation.IsOSPlatform(OSPlatform.Windows)
                    ? GetWindowsTotalPhysicalMemoryBytes()
                    : throw new PlatformNotSupportedException("Total physical memory detection is supported only on Linux and Windows.");
        }

        /// <summary>
        /// Gets the total physical memory in bytes on Linux by reading the /proc/meminfo file.
        /// </summary>
        /// <returns>The total physical memory in bytes.</returns>
        /// <exception cref="InvalidOperationException">Thrown if the /proc/meminfo file does not exist or has an unexpected format.</exception>
        private static ulong GetLinuxMemTotalBytes()
        {
            if (!File.Exists(ProcMemInfoPath))
            {
                throw new InvalidOperationException("Unable to determine physical memory: /proc/meminfo does not exist.");
            }

            foreach (string line in File.ReadLines(ProcMemInfoPath))
            {
                if (!line.StartsWith("MemTotal:", StringComparison.Ordinal))
                {
                    continue;
                }

                string[] parts = line.Split(' ', StringSplitOptions.RemoveEmptyEntries);
                return parts.Length < 3 || !parts[2].Equals("kB", StringComparison.Ordinal)
                    ? throw new InvalidOperationException("Unable to determine physical memory: MemTotal entry in /proc/meminfo has unexpected format.")
                    : ulong.TryParse(parts[1], out ulong kibibytes)
                    ? checked(kibibytes * 1024UL)
                    : throw new InvalidOperationException("Unable to determine physical memory: MemTotal value in /proc/meminfo is not numeric.");
            }

            throw new InvalidOperationException("Unable to determine physical memory: MemTotal entry was not found in /proc/meminfo.");
        }

        /// <summary>
        /// Gets the total physical memory in bytes on Windows using the GlobalMemoryStatusEx API.
        /// </summary>
        /// <returns>The total physical memory in bytes.</returns>
        /// <exception cref="InvalidOperationException">Thrown if the GlobalMemoryStatusEx API call fails.</exception>
        private static ulong GetWindowsTotalPhysicalMemoryBytes()
        {
            MEMORYSTATUSEX status = new()
            {
                dwLength = (uint)Marshal.SizeOf<MEMORYSTATUSEX>(),
            };

            return GlobalMemoryStatusEx(ref status)
                ? status.ullTotalPhys
                : throw new InvalidOperationException("Unable to determine physical memory: GlobalMemoryStatusEx failed.");
        }

        /// <summary>
        /// The GlobalMemoryStatusEx function obtains information about the system's current usage of both physical and virtual memory.
        /// </summary>
        /// <param name="lpBuffer">A pointer to a MEMORYSTATUSEX structure that receives information about current memory availability.</param>
        /// <returns><see langword="true"/> if the function succeeds; otherwise, <see langword="false"/>.</returns>
        [DllImport("kernel32.dll", SetLastError = true)]
        [return: MarshalAs(UnmanagedType.Bool)]
        private static extern bool GlobalMemoryStatusEx(ref MEMORYSTATUSEX lpBuffer);

        /// <summary>
        /// The MEMORYSTATUSEX structure contains information about the current state of both physical and virtual memory, including extended memory.
        /// </summary>
        [StructLayout(LayoutKind.Sequential, CharSet = CharSet.Auto)]
        private struct MEMORYSTATUSEX
        {
            /// <summary>
            /// The size of the structure, in bytes. You must set this member before calling GlobalMemoryStatusEx.
            /// </summary>
            public uint dwLength;
            /// <summary>
            /// The number between 0 and 100 that specifies the approximate percentage of physical memory that is in use (0 indicates no memory use and 100 indicates full memory use).
            /// </summary>
            public uint dwMemoryLoad;
            /// <summary>
            /// The amount of actual physical memory, in bytes.
            /// </summary>
            public ulong ullTotalPhys;
            /// <summary>
            /// The amount of physical memory currently available, in bytes.
            /// </summary>
            public ulong ullAvailPhys;
            /// <summary>
            /// The current committed memory limit for the system or the current process, whichever is smaller, in bytes.
            /// </summary>
            public ulong ullTotalPageFile;
            /// <summary>
            /// The amount of physical memory currently available in the page file, in bytes.
            /// </summary>
            public ulong ullAvailPageFile;
            /// <summary>
            /// The total amount of virtual memory, in bytes.
            /// </summary>
            public ulong ullTotalVirtual;
            /// <summary>
            /// The amount of available virtual memory, in bytes.
            /// </summary>
            public ulong ullAvailVirtual;
            /// <summary>
            /// The amount of available extended virtual memory, in bytes.
            /// </summary>
            public ulong ullAvailExtendedVirtual;
        }
    }
}
