# MXFJ2KSource

Vapoursynth video source plugin for reading JPEG 2000 picture essence from MXF files (e.g. DCP OP1a) and returning `RGB36` frames.

## Usage

### Source

Loads a MXF file and returns a clip with the JPEG 2000 picture essence:

```
core.MXFJ2KSource.Source(string source[, int track, string cache_path])
```

- `source`: MXF file path.
- `track`: Track number (optional)
- `cache_path`: Index cache file path (optional). Default: `source + ".mxfj2kindex"`
  - If you explicitly set `cache_path` and the cache cannot be written, the filter fails.
  - If you use the default auto cache path and it cannot be written, a warning is logged and indexing continues.

## Building

Requirements:
- VapourSynth headers
- Grok JPEG 2000 library installed

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
```

Output:
- `build/libMXFJ2KSource.[dll|dylib|so]`
