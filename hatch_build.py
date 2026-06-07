from __future__ import annotations

import os
import platform
import shutil
import subprocess
import sys
import sysconfig
from pathlib import Path

from hatchling.builders.hooks.plugin.interface import BuildHookInterface
from packaging.tags import mac_platforms


PLUGIN_DIR = "mxfj2ksource"
LIB_STEM = "libMXFJ2KSource"
GROK_REF = "v20.3.3"


class CustomBuildHook(BuildHookInterface):
    def initialize(self, version: str, build_data: dict) -> None:
        root = Path(self.root).resolve()
        stage_dir = root / "vapoursynth" / "plugins" / PLUGIN_DIR

        build_data["pure_python"] = False
        build_data["tag"] = f"py3-none-{self._platform_tag()}"
        build_data.setdefault("force_include", {})

        self._clean_stage(stage_dir)
        stage_dir.mkdir(parents=True, exist_ok=True)

        build_dir = root / "build" / "hatch" / self._build_dir_name()
        if build_dir.exists():
            shutil.rmtree(build_dir)
        build_dir.mkdir(parents=True, exist_ok=True)

        cmake_args = self._base_cmake_args(root, build_dir)
        if self._truthy(os.environ.get("MXFJ2K_USE_SYSTEM_GROK")):
            cmake_args.append("-DMXFJ2K_USE_BUNDLED_GROK=OFF")
        else:
            grok_source, grok_build = self._ensure_grok(root)
            cmake_args.extend(
                [
                    "-DMXFJ2K_USE_BUNDLED_GROK=ON",
                    f"-DMXFJ2K_GROK_SOURCE_DIR={grok_source}",
                    f"-DMXFJ2K_GROK_BUILD_DIR={grok_build}",
                ]
            )

        self._run(cmake_args)
        self._run([self._cmake(), "--build", str(build_dir), "--parallel"])

        library = self._find_library(build_dir)
        staged_library = stage_dir / library.name
        shutil.copy2(library, staged_library)
        shutil.copy2(root / "packaging" / "manifest.vs", stage_dir / "manifest.vs")

        build_data["force_include"][str(staged_library)] = (
            f"vapoursynth/plugins/{PLUGIN_DIR}/{staged_library.name}"
        )
        build_data["force_include"][str(stage_dir / "manifest.vs")] = (
            f"vapoursynth/plugins/{PLUGIN_DIR}/manifest.vs"
        )

    def clean(self, versions: list[str]) -> None:
        root = Path(self.root).resolve()
        self._clean_stage(root / "vapoursynth" / "plugins" / PLUGIN_DIR)

    def _base_cmake_args(self, root: Path, build_dir: Path) -> list[str]:
        import vapoursynth

        env = os.environ
        args = [
            self._cmake(),
            "-S",
            str(root),
            "-B",
            str(build_dir),
            "-G",
            env.get("CMAKE_GENERATOR", "Ninja"),
            "-DCMAKE_BUILD_TYPE=Release",
            "-DMXFJ2K_PREFER_STATIC=ON",
            "-DUSE_LTO=ON",
            f"-DPython3_EXECUTABLE={sys.executable}",
            f"-DVAPOURSYNTH_INCLUDE_DIRECTORY={vapoursynth.get_include()}",
        ]

        if self._truthy(env.get("MXFJ2K_GROK_EXTERNAL_FMT")):
            args.append("-DMXFJ2K_GROK_EXTERNAL_FMT=ON")

        if sys.platform == "darwin":
            deployment_target = env.get("MACOSX_DEPLOYMENT_TARGET", "14.0")
            os.environ["MACOSX_DEPLOYMENT_TARGET"] = deployment_target
            args.append(f"-DCMAKE_OSX_DEPLOYMENT_TARGET={deployment_target}")
            arch = env.get("CMAKE_OSX_ARCHITECTURES") or env.get("CIBW_ARCHS_MACOS")
            if arch:
                args.append(f"-DCMAKE_OSX_ARCHITECTURES={arch}")

        extra_args = env.get("MXFJ2K_CMAKE_ARGS")
        if extra_args:
            args.extend(extra_args.split())

        return args

    def _ensure_grok(self, root: Path) -> tuple[Path, Path]:
        env = os.environ
        ref = env.get("GROK_REF", GROK_REF)
        safe_ref = ref.replace("/", "_")
        default_base = root / "build" / "hatch" / "deps"
        source_dir = Path(env.get("MXFJ2K_GROK_SOURCE_DIR", default_base / f"grok-src-{safe_ref}")).resolve()
        build_dir = Path(env.get("MXFJ2K_GROK_BUILD_DIR", default_base / f"grok-build-{safe_ref}-{self._build_dir_name()}-minimal")).resolve()
        external_fmt = self._truthy(env.get("MXFJ2K_GROK_EXTERNAL_FMT"))

        if not source_dir.exists():
            source_dir.parent.mkdir(parents=True, exist_ok=True)
            self._run(
                [
                    "git",
                    "clone",
                    "--depth",
                    "1",
                    "--branch",
                    ref,
                    "--recursive",
                    "https://github.com/GrokImageCompression/grok.git",
                    str(source_dir),
                ]
            )

        self._patch_grok_source(source_dir)

        expected_libs = [
            build_dir / "bin" / "libgrokj2kcodec.a",
            build_dir / "bin" / "libgrokj2k.a",
            build_dir / "bin" / "libspdlog.a",
            build_dir / "bin" / "libhwy.a",
            build_dir / "bin" / "liblcms2.a",
        ]
        if not external_fmt:
            expected_libs.append(build_dir / "bin" / "libfmt.a")

        if all(path.exists() for path in expected_libs):
            return source_dir, build_dir

        build_dir.mkdir(parents=True, exist_ok=True)
        self._run(
            [
                self._cmake(),
                "-S",
                str(source_dir),
                "-B",
                str(build_dir),
                "-G",
                os.environ.get("CMAKE_GENERATOR", "Ninja"),
                "-DCMAKE_BUILD_TYPE=Release",
                "-DBUILD_SHARED_LIBS=OFF",
                "-DBUILD_TESTING=OFF",
                "-DGRK_BUILD_PKGCONFIG_FILES=ON",
                "-DGRK_BUNDLE_STATIC_CORE=ON",
                "-DGRK_BUILD_CORE_EXAMPLES=OFF",
                "-DGRK_BUILD_CODEC_EXAMPLES=OFF",
                "-DGRK_BUILD_CORE_SWIG_BINDINGS=OFF",
                "-DGRK_BUILD_CSHARP_SWIG_BINDINGS=OFF",
                "-DGRK_BUILD_JAVA_SWIG_BINDINGS=OFF",
                "-DGRK_BUILD_LIBPNG=OFF",
                "-DGRK_BUILD_LIBTIFF=OFF",
                "-DGRK_BUILD_JPEG=OFF",
                "-DPKG_CONFIG_EXECUTABLE=PKG_CONFIG_EXECUTABLE-NOTFOUND",
                "-DCMAKE_DISABLE_FIND_PACKAGE_JPEG=ON",
                "-DCMAKE_DISABLE_FIND_PACKAGE_PNG=ON",
                "-DCMAKE_DISABLE_FIND_PACKAGE_TIFF=ON",
                f"-DSPDLOG_FMT_EXTERNAL={'ON' if external_fmt else 'OFF'}",
            ]
        )
        self._build_grok_libraries(build_dir, expected_libs)

        missing = [str(path) for path in expected_libs if not path.exists()]
        if missing:
            raise RuntimeError(f"Bundled Grok build did not produce expected libraries: {', '.join(missing)}")

        return source_dir, build_dir

    def _patch_grok_source(self, source_dir: Path) -> None:
        mem_manager = source_dir / "src" / "lib" / "core" / "util" / "MemManager.h"
        text = mem_manager.read_text()
        patched = text.replace(
            "#elif defined(__linux__)\n#include <malloc.h>",
            "#elif defined(__linux__) && defined(__GLIBC__)\n#include <malloc.h>",
        )
        patched = patched.replace(
            "#ifdef __linux__\n    malloc_trim(0);\n#elif defined(_WIN32)",
            "#if defined(__linux__) && defined(__GLIBC__)\n    malloc_trim(0);\n#elif defined(_WIN32)",
        )
        if patched != text:
            mem_manager.write_text(patched)

        stream_io = source_dir / "src" / "lib" / "core" / "stream" / "StreamIO.h"
        text = stream_io.read_text()
        patched = text.replace(
            """#elif defined(__linux__) || defined(__FreeBSD__) || defined(__NetBSD__) || \\
    defined(__OpenBSD__) // POSIX with byteswap.h
#include <byteswap.h>
  if(numBytes == 8)
  {
    *value = (TYPE)bswap_64((uint64_t)*value);
  }
  else if(numBytes == 4)
  {
    *value = (TYPE)bswap_32((uint32_t)*value);
  }
  else if(numBytes == 2)
  {
    *value = (TYPE)bswap_16((uint16_t)*value);
  }
""",
            """#elif defined(__GNUC__) || defined(__clang__)
  if(numBytes == 8)
  {
    *value = (TYPE)__builtin_bswap64((uint64_t)*value);
  }
  else if(numBytes == 4)
  {
    *value = (TYPE)__builtin_bswap32((uint32_t)*value);
  }
  else if(numBytes == 2)
  {
    *value = (TYPE)__builtin_bswap16((uint16_t)*value);
  }
""",
        )
        if patched != text:
            stream_io.write_text(patched)

    def _build_grok_libraries(self, build_dir: Path, expected_libs: list[Path]) -> None:
        self._run(
            [
                self._cmake(),
                "--build",
                str(build_dir),
                "--parallel",
                "--target",
                "grokj2kcodec",
            ]
        )
        if all(path.exists() for path in expected_libs):
            return

        available_targets = self._available_targets(build_dir)
        for target in ("bundling_target", "libgrokj2kcodec.a", "libgrokj2k.a", "libspdlog.a", "libfmt.a", "libhwy.a", "liblcms2.a"):
            if target in available_targets:
                self._run(
                    [
                        self._cmake(),
                        "--build",
                        str(build_dir),
                        "--parallel",
                        "--target",
                        target,
                    ]
                )
                if all(path.exists() for path in expected_libs):
                    return

    def _available_targets(self, build_dir: Path) -> set[str]:
        proc = subprocess.run(
            [self._cmake(), "--build", str(build_dir), "--target", "help"],
            check=True,
            capture_output=True,
            text=True,
        )
        targets: set[str] = set()
        for line in proc.stdout.splitlines():
            stripped = line.strip()
            if not stripped or stripped.startswith("["):
                continue
            target = stripped.split(":", 1)[0].strip()
            if target:
                targets.add(target)
        return targets

    def _find_library(self, build_dir: Path) -> Path:
        suffixes = (".dll", ".dylib", ".so")
        matches = [
            path
            for path in build_dir.rglob(f"*{LIB_STEM}*")
            if path.is_file() and path.suffix.lower() in suffixes
        ]
        if not matches:
            raise FileNotFoundError(f"Could not find built {LIB_STEM} library under {build_dir}")
        return sorted(matches, key=lambda path: (len(path.parts), str(path)))[0]

    def _platform_tag(self) -> str:
        override = os.environ.get("MXFJ2K_WHEEL_PLATFORM_TAG")
        if override:
            return override

        if sys.platform == "darwin":
            deployment_target = os.environ.get("MACOSX_DEPLOYMENT_TARGET", "14.0")
            major, minor = (int(part) for part in deployment_target.split(".")[:2])
            arch = os.environ.get("CIBW_ARCHS_MACOS") or os.environ.get("CMAKE_OSX_ARCHITECTURES") or platform.machine()
            return next(mac_platforms((major, minor), arch))

        if sys.platform.startswith("linux"):
            auditwheel_plat = os.environ.get("AUDITWHEEL_PLAT")
            if auditwheel_plat:
                return auditwheel_plat

        return sysconfig.get_platform().replace("-", "_").replace(".", "_")

    def _build_dir_name(self) -> str:
        return self._platform_tag().replace("-", "_").replace(".", "_")

    def _cmake(self) -> str:
        return os.environ.get("CMAKE", "cmake")

    def _clean_stage(self, stage_dir: Path) -> None:
        if stage_dir.exists():
            shutil.rmtree(stage_dir)

    def _run(self, command: list[str]) -> None:
        print("+", " ".join(command), flush=True)
        subprocess.run(command, check=True)

    def _truthy(self, value: str | None) -> bool:
        return value is not None and value.lower() in {"1", "true", "yes", "on"}
