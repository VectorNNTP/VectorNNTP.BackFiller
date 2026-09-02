// <copyright file="HexUInt32Parser.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Runtime / Articles / YEnc
// Bounded hexadecimal parser for yEnc trailer CRC metadata that reads ASCII bytes
// directly into a uint value without allocations or string conversions.

using System.Runtime.CompilerServices;

namespace VectorNNTP.Backfiller.Runtime.Articles.YEnc
{
    /// <summary>
    /// Parses variable-length ASCII hexadecimal values for yEnc trailer metadata fields.
    /// </summary>
    internal static class HexUInt32Parser
    {
        /// <summary>
        /// Parses hexadecimal ASCII bytes into a <see cref="uint"/> value.
        /// </summary>
        /// <param name="hexBytes">Hexadecimal byte span that must contain only hexadecimal characters.</param>
        /// <param name="value">Parsed value when the method returns <see langword="true"/>.</param>
        /// <returns><see langword="true"/> when one to eight hexadecimal digits were parsed and consumed fully; otherwise <see langword="false"/>.</returns>
        /// <typeparam name="byte">The byte type parameter.</typeparam>
        internal static bool TryParseHexUInt32(ReadOnlySpan<byte> hexBytes, out uint value)
        {
            value = 0;

            if (hexBytes.IsEmpty || hexBytes.Length > 8)
            {
                return false;
            }

            for (int i = 0; i < hexBytes.Length; i++)
            {
                int nibble = HexByteToNibble(hexBytes[i]);
                if (nibble < 0)
                {
                    value = 0;
                    return false;
                }

                value = (value << 4) | (uint)nibble;
            }

            return true;
        }

        /// <summary>
        /// Converts one ASCII hex digit byte to its nibble value.
        /// </summary>
        /// <param name="b">ASCII byte candidate.</param>
        /// <returns>Nibble value in range 0-15, or -1 when not a valid hexadecimal digit.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int HexByteToNibble(byte b)
        {
            return (uint)(b - (byte)'0') <= 9
                ? b - (byte)'0'
                : (uint)(b - (byte)'a') <= 5 ? b - (byte)'a' + 10 : (uint)(b - (byte)'A') <= 5 ? b - (byte)'A' + 10 : -1;
        }
    }
}
