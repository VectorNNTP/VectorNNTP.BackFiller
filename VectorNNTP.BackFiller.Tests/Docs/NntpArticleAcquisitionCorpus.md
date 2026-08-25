# NNTP Article Acquisition Local Corpus Workflow

This workflow creates a local development corpus for real-world NNTP compatibility testing without committing Usenet content to source control.

## Storage location

Generated corpus lives under:

- `VectorNNTP.BackFiller.Tests/LocalCorpus/yenc-valid`
- `VectorNNTP.BackFiller.Tests/LocalCorpus/plain-valid`
- `VectorNNTP.BackFiller.Tests/LocalCorpus/yenc-corrupt`

`LocalCorpus` is ignored by git and must remain local/private.

## Generation mechanism

The generation entry point is test method:

- `VectorNNTP.Backfiller.Tests.NntpArticleAcquisitionCorpusGenerationTests.GenerateLocalCorpus_WhenEnabled_DownloadsAndCorruptsConfiguredSamples`

By default this method is inert. It executes only when explicitly enabled.

## Environment variables

Set these before running generation:

- `VNNTP_ENABLE_LOCAL_CORPUS_GENERATION=1`
- `VNNTP_CORPUS_HOST=<your-nntp-host>`
- `VNNTP_CORPUS_PORT=119` (or SSL port)
- `VNNTP_CORPUS_USE_SSL=true|false`
- `VNNTP_CORPUS_USERNAME=<optional-user>`
- `VNNTP_CORPUS_PASSWORD=<optional-pass>`
- `VNNTP_CORPUS_YENC_MESSAGE_IDS=<newline-delimited <message-id> list>`
- `VNNTP_CORPUS_PLAIN_MESSAGE_IDS=<newline-delimited <message-id> list>`

## Run command

```pwsh
cd VectorNNTP.BackFiller.Tests
dotnet test --filter FullyQualifiedName~NntpArticleAcquisitionCorpusGenerationTests.GenerateLocalCorpus_WhenEnabled_DownloadsAndCorruptsConfiguredSamples
```

## Corrupted fixtures

Corrupted yEnc fixtures are generated deterministically from downloaded valid yEnc bytes by controlled byte flips plus an invalid yEnc suffix append.

This preserves real-world structure while forcing parser/validator failure paths.

## CI behavior

CI does not set `VNNTP_ENABLE_LOCAL_CORPUS_GENERATION`, so corpus generation is skipped and CI remains independent of private NNTP infrastructure.
