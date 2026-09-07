// <copyright file="PhysicalSystemMemoryProvider.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Startup / Configuration
// Resolves total physical system memory using platform-specific mechanisms.

using System.ComponentModel;
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
        /// <exception cref="PhysicalSystemMemoryDiscoveryException">Thrown when /proc/meminfo cannot be read or does not provide a valid MemTotal value.</exception>
        private static ulong GetLinuxMemTotalBytes()
        {
            if (!File.Exists(ProcMemInfoPath))
            {
                throw new PhysicalSystemMemoryDiscoveryException("Unable to determine physical system memory from /proc/meminfo: file does not exist.");
            }

            try
            {
                foreach (string line in File.ReadLines(ProcMemInfoPath))
                {
                    if (!line.StartsWith("MemTotal:", StringComparison.Ordinal))
                    {
                        continue;
                    }

                    string[] parts = line.Split(' ', StringSplitOptions.RemoveEmptyEntries);
                    if (parts.Length < 3 || !parts[2].Equals("kB", StringComparison.Ordinal))
                    {
                        throw new PhysicalSystemMemoryDiscoveryException("Unable to determine physical system memory from /proc/meminfo: MemTotal entry format is invalid.");
                    }

                    if (!ulong.TryParse(parts[1], out ulong kibibytes))
                    {
                        throw new PhysicalSystemMemoryDiscoveryException("Unable to determine physical system memory from /proc/meminfo: MemTotal value is not numeric.");
                    }

                    try
                    {
                        return checked(kibibytes * 1024UL);
                    }
                    catch (OverflowException ex)
                    {
                        throw new PhysicalSystemMemoryDiscoveryException("Unable to determine physical system memory from /proc/meminfo: MemTotal value overflowed byte conversion.", ex);
                    }
                }
            }
            catch (IOException ex)
            {
                throw new PhysicalSystemMemoryDiscoveryException("Unable to determine physical system memory from /proc/meminfo: file could not be read.", ex);
            }
            catch (UnauthorizedAccessException ex)
            {
                throw new PhysicalSystemMemoryDiscoveryException("Unable to determine physical system memory from /proc/meminfo: access was denied.", ex);
            }

            throw new PhysicalSystemMemoryDiscoveryException("Unable to determine physical system memory from /proc/meminfo: MemTotal entry was not found.");
        }

        /// <summary>
        /// Gets the total physical memory in bytes on Windows using the GlobalMemoryStatusEx API.
        /// </summary>
        /// <returns>The total physical memory in bytes.</returns>
        /// <exception cref="PhysicalSystemMemoryDiscoveryException">Thrown if the GlobalMemoryStatusEx API call fails.</exception>
        private static ulong GetWindowsTotalPhysicalMemoryBytes()
        {
            MEMORYSTATUSEX status = new()
            {
                dwLength = (uint)Marshal.SizeOf<MEMORYSTATUSEX>(),
            };

            if (GlobalMemoryStatusEx(ref status))
            {
                return status.ullTotalPhys;
            }

            int lastError = Marshal.GetLastWin32Error();
            throw new PhysicalSystemMemoryDiscoveryException(
                "Unable to determine physical system memory from GlobalMemoryStatusEx.",
                new Win32Exception(lastError));
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
