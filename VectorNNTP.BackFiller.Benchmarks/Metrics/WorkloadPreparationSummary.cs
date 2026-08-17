namespace VectorNNTP.BackFiller.Benchmarks;

internal readonly record struct WorkloadPreparationSummary(
    double PreGenerationDurationMilliseconds,
    double PayloadPreparationDurationMilliseconds,
    int MessageIdPoolSize,
    int UniqueMessageIdCount,
    int DuplicateMessageIdCount,
    int ReusablePayloadBytes);
