# MXFJ2KSource

Vapoursynth video source plugin for reading JPEG 2000 picture essence from MXF files (e.g. DCP OP1a) and returning `RGB36` frames.

## Usage

### Source

Loads a MXF file and returns a clip with the JPEG 2000 picture essence:

```
core.MXFJ2KSource.Source(clip clip[, int track])
```

- `clip`: MXF file path.
- `track`: Track number (optional)

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
