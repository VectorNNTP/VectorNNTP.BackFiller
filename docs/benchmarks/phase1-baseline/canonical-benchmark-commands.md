# Canonical Benchmark Commands (Phase 1 Baseline)

## Goal
Freeze command lines used for parity checks during zero-behavior-change extraction.

## Notes
- Do not use `--no-build`.
- Run from repository root unless specified.
- Keep expected identity flags aligned with baseline assembly/runtime identity.

## Canonical Commands

### Transit Stress (primary parity command)
```powershell
dotnet .\VectorNNTP.BackFiller.Benchmarks\bin\Debug\net8.0\win-x64\VectorNNTP.BackFiller.Benchmarks.dll transit-stress --duration-seconds 30 --warmup-seconds 10 --connections 64 --pipeline-depth 16 --dispatch-workers 512 --queue-mib 2048 --queue-articles 2048 --article-kib 1024 --generator-workers 32 --write-batch-coalesce-us 250 --expected-assembly-path "C:\Users\chrisk\source\repos\VectorNNTP.BackFiller\VectorNNTP.BackFiller.Benchmarks\bin\Debug\net8.0\win-x64\VectorNNTP.BackFiller.Benchmarks.dll" --expected-assembly-version 1.1.229.8569 --expected-file-version 1.1.229.8569 --expected-target-framework .NETCoreApp,Version=v8.0 --expected-architecture X64
```

### Transit Validate
```powershell
dotnet .\VectorNNTP.BackFiller.Benchmarks\bin\Debug\net8.0\win-x64\VectorNNTP.BackFiller.Benchmarks.dll transit-validate --connections 4 --pipeline-depth 8 --dispatch-workers 32 --queue-mib 512 --queue-articles 512 --article-kib 1024 --generator-workers 1 --write-batch-coalesce-us 250 --expected-assembly-path "C:\Users\chrisk\source\repos\VectorNNTP.BackFiller\VectorNNTP.BackFiller.Benchmarks\bin\Debug\net8.0\win-x64\VectorNNTP.BackFiller.Benchmarks.dll" --expected-assembly-version 1.1.229.8569 --expected-file-version 1.1.229.8569 --expected-target-framework .NETCoreApp,Version=v8.0 --expected-architecture X64
```

### Transit Saturate
```powershell
dotnet .\VectorNNTP.BackFiller.Benchmarks\bin\Debug\net8.0\win-x64\VectorNNTP.BackFiller.Benchmarks.dll transit-saturate --duration-seconds 30 --warmup-seconds 10 --connections 64 --pipeline-depth 16 --dispatch-workers 512 --queue-mib 2048 --queue-articles 2048 --article-kib 1024 --generator-workers 32 --write-batch-coalesce-us 250 --expected-assembly-path "C:\Users\chrisk\source\repos\VectorNNTP.BackFiller\VectorNNTP.BackFiller.Benchmarks\bin\Debug\net8.0\win-x64\VectorNNTP.BackFiller.Benchmarks.dll" --expected-assembly-version 1.1.229.8569 --expected-file-version 1.1.229.8569 --expected-target-framework .NETCoreApp,Version=v8.0 --expected-architecture X64
```

### Generator Baseline
```powershell
dotnet .\VectorNNTP.BackFiller.Benchmarks\bin\Debug\net8.0\win-x64\VectorNNTP.BackFiller.Benchmarks.dll transit-generator-baseline --duration-seconds 30 --warmup-seconds 10 --article-kib 1024
```

## Artifact Capture
After each command run, capture:
- Console output transcript
- Generated JSON/CSV artifact paths and contents
