# MXFJ2KSource (VapourSynth)

VapourSynth video source plugin for reading JPEG 2000 picture essence from MXF files (e.g. DCP OP1a) and returning `RGB36` frames (RGB, integer, 12-bit).

## Build

Requirements:
- VapourSynth headers
- Grok JPEG 2000 library installed

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
```

Output:
- `build/libMXFJ2KSource.[dll|dylib|so]`

## Usage

```python
import vapoursynth as vs
core = vs.core

core.std.LoadPlugin(path="/path/to/libMXFJ2KSource.dylib")
clip = core.MXFJ2KSource.Source("/path/to/file.mxf")
```

Note: This plugin returns the three channels in an RGB container format without colour conversion.
