// <copyright file="FixedArticleLimiterTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Tests / yEnc
// Corpus-backed and synthetic contract tests for the yEnc article validator,
// covering protocol parsing, integrity classification, malformed input handling,
// and NNTP dot-stuffing interactions.

using VectorNNTP.BackFiller.Benchmarks;
using Xunit;

namespace VectorNNTP.BackFiller.Tests.Benchmarks
{
    /// <summary>
    /// Validates exact global reservation behavior for fixed-count benchmark production.
    /// </summary>
    public sealed class FixedArticleLimiterTests
    {
        /// <summary>
        /// Verifies one-article fixed-count reservations admit exactly one token.
        /// </summary>
        [Fact]
        public void TryReserveNext_WhenTargetIsOne_AllowsExactlyOneReservation()
        {
            FixedArticleLimiter limiter = new(1);

            bool first = limiter.TryReserveNext();
            bool second = limiter.TryReserveNext();

            Assert.True(first);
            Assert.False(second);
        }

        /// <summary>
        /// Verifies two-hundred-article fixed-count reservations admit exactly two hundred tokens.
        /// </summary>
        [Fact]
        public void TryReserveNext_WhenTargetIsTwoHundred_AllowsExactlyTwoHundredReservations()
        {
            FixedArticleLimiter limiter = new(200);

            int granted = 0;
            for (int i = 0; i < 250; i++)
            {
                if (limiter.TryReserveNext())
                {
                    granted++;
                }
            }

            Assert.Equal(200, granted);
        }

        /// <summary>
        /// Verifies concurrent multi-worker reservations are globally bounded to exactly the configured target.
        /// </summary>
        [Fact]
        public async Task TryReserveNext_WhenInvokedConcurrently_IsGloballyBoundedToTargetAsync()
        {
            const int Target = 200;
            const int Workers = 8;

            FixedArticleLimiter limiter = new(Target);
            int granted = 0;

            Task[] tasks = new Task[Workers];
            for (int i = 0; i < Workers; i++)
            {
                tasks[i] = Task.Run(() =>
                {
                    while (limiter.TryReserveNext())
                    {
                        _ = Interlocked.Increment(ref granted);
                    }
                });
            }

            await Task.WhenAll(tasks).ConfigureAwait(false);

            Assert.Equal(Target, granted);
        }

        /// <summary>
        /// Verifies invalid non-positive targets are rejected.
        /// </summary>
        [Theory]
        [InlineData(0)]
        [InlineData(-1)]
        public void Constructor_WhenTargetIsNotPositive_ThrowsArgumentOutOfRangeException(int target)
        {
            _ = Assert.Throws<ArgumentOutOfRangeException>(() => new FixedArticleLimiter(target));
        }
    }
}
