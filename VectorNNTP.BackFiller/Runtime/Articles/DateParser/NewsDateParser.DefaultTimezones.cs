// <copyright file="NewsDateParser.DefaultTimezones.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Runtime / Articles / DateParser
// Default timezone-abbreviation mapping table used by NewsDateParser canonicalization.

using System.Collections.Frozen;

namespace VectorNNTP.Backfiller.Runtime.Articles.DateParser
{
    /// <summary>
    /// Default-timezone mapping partial for <see cref="NewsDateParser"/>.
    /// </summary>
    internal static partial class NewsDateParser
    {
        /// <summary>
        /// Builds the default timezone abbreviation mapping table.
        /// </summary>
        /// <returns>Case-insensitive frozen map from abbreviation to numeric UTC offset.</returns>
        private static FrozenDictionary<string, string> CreateDefaultTimezoneMappings()
        {
            Dictionary<string, string> d = new(StringComparer.OrdinalIgnoreCase)
            {
                ["UT"] = "+00:00",
                ["UTC"] = "+00:00",
                ["GMT"] = "+00:00",
                ["BST"] = "+01:00",
                ["WET"] = "+00:00",
                ["WEST"] = "+01:00",
                ["CET"] = "+01:00",
                ["CEST"] = "+02:00",
                ["EET"] = "+02:00",
                ["EEST"] = "+03:00",
                ["EST"] = "-05:00",
                ["EDT"] = "-04:00",
                ["CST"] = "+08:00",
                ["CDT"] = "-05:00",
                ["MST"] = "-07:00",
                ["MDT"] = "-06:00",
                ["PST"] = "-08:00",
                ["PDT"] = "-07:00",
                ["AKST"] = "-09:00",
                ["AKDT"] = "-08:00",
                ["HST"] = "-10:00",
                ["AEST"] = "+10:00",
                ["AEDT"] = "+11:00",
                ["ACST"] = "+09:30",
                ["ACDT"] = "+10:30",
                ["AWST"] = "+08:00",
                ["JST"] = "+09:00",
                ["KST"] = "+09:00",
                ["HKT"] = "+08:00",
                ["SGT"] = "+08:00",
                ["IST"] = "+05:30",
                ["PKT"] = "+05:00",
                ["MSK"] = "+03:00",
                ["TRT"] = "+03:00",
                ["SAST"] = "+02:00",
                ["WIB"] = "+07:00",
                ["WITA"] = "+08:00",
                ["WIT"] = "+09:00",
                ["NZST"] = "+12:00",
                ["NZDT"] = "+13:00",
                ["NPT"] = "+05:45",
                ["IRST"] = "+03:30",
                ["UTC+0"] = "+00:00",
                ["UTC+1"] = "+01:00",
                ["UTC+2"] = "+02:00",
                ["UTC+3"] = "+03:00",
                ["UTC+4"] = "+04:00",
                ["UTC+5"] = "+05:00",
                ["UTC+6"] = "+06:00",
                ["UTC+7"] = "+07:00",
                ["UTC+8"] = "+08:00",
                ["UTC+9"] = "+09:00",
                ["UTC-1"] = "-01:00",
                ["UTC-2"] = "-02:00",
                ["UTC-3"] = "-03:00",
                ["UTC-4"] = "-04:00",
                ["UTC-5"] = "-05:00",
                ["UTC-6"] = "-06:00",
                ["UTC-7"] = "-07:00",
                ["UTC-8"] = "-08:00",
                ["UTC-9"] = "-09:00",
            };

            return d.ToFrozenDictionary(StringComparer.OrdinalIgnoreCase);
        }
    }
}
