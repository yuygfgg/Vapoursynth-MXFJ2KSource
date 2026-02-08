#include <VSHelper4.h>
#include <VapourSynth4.h>

#include <libMXF++/MXF.h>

#include <mxf/mxf.h>
#include <mxf/mxf_essence_container.h>
#include <mxf/mxf_file.h>
#include <mxf/mxf_labels_and_keys.h>

#include <grok.h>

#include <algorithm>
#include <array>
#include <bit>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <expected>
#include <filesystem>
#include <format>
#include <fstream>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

namespace mxfj2ksource {

namespace fs = std::filesystem;

static fs::path path_from_utf8(std::string_view s) {
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
    return {(std::u8string_view(reinterpret_cast<const char8_t*>(s.data()),
                                s.size()))};
}

static constexpr int DEFAULT_FPS_NUM = 24;
static constexpr int DEFAULT_FPS_DEN = 1;

static constexpr uint8_t J2K_SOC_FIRST_BYTE = 0xFF;
static constexpr uint8_t J2K_SOC_SECOND_BYTE = 0x4F;
static constexpr size_t J2K_SOC_MARKER_SIZE = 2;

static constexpr size_t MXF_KEY_SIZE_BYTES = 16;
static constexpr int64_t MXF_MIN_KL_BYTES =
    static_cast<int64_t>(MXF_KEY_SIZE_BYTES) + 1; // key + length byte

static constexpr uint8_t BER_LONG_FORM_FLAG = 0x80;
static constexpr uint8_t BER_LONG_FORM_LEN_MASK = 0x7F;
static constexpr uint8_t BER_MAX_LEN_BYTES = 8;
static constexpr uint8_t BITS_PER_BYTE = 8;

static constexpr uint8_t PARTITION_LABEL_PREFIX_BYTES = 7;
static constexpr uint8_t PARTITION_LABEL_SUFFIX_BYTES = 3;
static constexpr uint8_t PARTITION_LABEL_BYTES =
    PARTITION_LABEL_PREFIX_BYTES + 1 + PARTITION_LABEL_SUFFIX_BYTES;
static constexpr uint8_t PARTITION_LABEL_REGVER_OFFSET =
    PARTITION_LABEL_PREFIX_BYTES;
static constexpr uint8_t PARTITION_LABEL_REGVER_1 = 0x01;
static constexpr uint8_t PARTITION_LABEL_REGVER_2 = 0x02;

static constexpr int VAPOURSYNTH_RGB36_BITS = 12;
static constexpr int VAPOURSYNTH_RGB48_BITS = 16;
static constexpr size_t FRAME_INDEX_RESERVE = 4096;

static void grok_init_once() {
    static std::once_flag once;
    std::call_once(once, []() { grk_initialize(nullptr, 0, nullptr); });
}

struct FrameIndex {
    uint64_t value_offset = 0; // file offset of KLV value (payload start)
    uint32_t value_size = 0;   // KLV value length in bytes
};

struct VideoTrackInfo {
    // Generic Container track number used in essence element keys
    // (mxf_get_track_number)
    std::optional<uint32_t> track_number;
    int fps_num = DEFAULT_FPS_NUM;
    int fps_den = DEFAULT_FPS_DEN;
};

struct J2KHeaderInfo {
    int width = 0;
    int height = 0;
    uint8_t prec = 0;
    uint16_t num_comps = 0;
    bool first_three_full_res = false; // "4:4:4" / full-res for 3-comp images
    bool first_three_same_prec = false;
};

class MxfDiskFile final {
  public:
    explicit MxfDiskFile(const std::string& path) {
        if (mxf_disk_file_open_read(path.c_str(), &file) == 0) {
            throw std::runtime_error("mxf_disk_file_open_read failed");
        }
    }

    ~MxfDiskFile() {
        if (file != nullptr) {
            mxf_file_close(&file);
        }
    }

    MxfDiskFile(const MxfDiskFile&) = delete;
    MxfDiskFile& operator=(const MxfDiskFile&) = delete;

    MxfDiskFile(MxfDiskFile&& other) noexcept
        : file(std::exchange(other.file, nullptr)),
          scratch(std::move(other.scratch)),
          scratch_capacity(std::exchange(other.scratch_capacity, 0)),
          scratch_size(std::exchange(other.scratch_size, 0)) {}
    MxfDiskFile& operator=(MxfDiskFile&& other) noexcept {
        if (this != &other) {
            if (file != nullptr) {
                mxf_file_close(&file);
            }
            file = std::exchange(other.file, nullptr);
            scratch = std::move(other.scratch);
            scratch_capacity = std::exchange(other.scratch_capacity, 0);
            scratch_size = std::exchange(other.scratch_size, 0);
        }
        return *this;
    }

    [[nodiscard]] MXFFile* get() const { return file; }

    [[nodiscard]] std::span<uint8_t> scratchBuf(size_t size) {
        if (size == 0) {
            scratch_size = 0;
            return {};
        }
        if (scratch_capacity < size) {
            scratch = std::make_unique_for_overwrite<uint8_t[]>(size);
            scratch_capacity = size;
        }
        scratch_size = size;
        return {scratch.get(), scratch_size};
    }

  private:
    MXFFile* file = nullptr;

    std::unique_ptr<uint8_t[]> scratch;
    size_t scratch_capacity = 0;
    size_t scratch_size = 0;
};

struct GrokCodecDeleter {
    // NOLINTNEXTLINE(readability-non-const-parameter)
    void operator()(grk_object* obj) const noexcept {
        if (obj != nullptr) {
            grk_object_unref(obj);
        }
    }
};
using GrokCodecPtr = std::unique_ptr<grk_object, GrokCodecDeleter>;

[[nodiscard]] static std::expected<VideoTrackInfo, std::string>
probe_mxf_video_track(const std::string& path) {
    using namespace mxfpp;

    try {
        std::unique_ptr<File> file(File::openRead(path));
        if (!file) {
            return std::unexpected("Failed to open MXF");
        }

        if (!file->readHeaderPartition()) {
            return std::unexpected("No header partition found");
        }

        Partition& header_partition = file->getPartition(0);

        auto data_model = std::make_unique<DataModel>();
        auto header_metadata =
            std::make_unique<HeaderMetadata>(data_model.get());

        mxfKey key{};
        uint8_t llen = 0;
        uint64_t len = 0;
        file->readNextNonFillerKL(&key, &llen, &len);
        if (mxf_is_header_metadata(&key) == 0) {
            return std::unexpected(
                "Header metadata KLV not found after header partition");
        }

        header_metadata->read(file.get(), &header_partition, &key, llen, len);

        Preface* preface = header_metadata->getPreface();
        if (preface == nullptr) {
            return std::unexpected("Missing Preface");
        }

        ContentStorage* content = preface->getContentStorage();
        if (content == nullptr) {
            return std::unexpected("Missing ContentStorage");
        }

        // Find the file SourcePackage (the one that has a descriptor).
        SourcePackage* file_pkg = nullptr;
        for (GenericPackage* pkg : content->getPackages()) {
            auto* sp = dynamic_cast<SourcePackage*>(pkg);
            if ((sp == nullptr) || !sp->haveDescriptor()) {
                continue;
            }
            file_pkg = sp;
            break;
        }
        if (file_pkg == nullptr) {
            return std::unexpected("No SourcePackage with a descriptor found");
        }

        // Find the first picture track in the file package.
        VideoTrackInfo info{};
        for (GenericTrack* gt : file_pkg->getTracks()) {
            auto* track = dynamic_cast<Track*>(gt);
            if (track == nullptr) {
                continue;
            }

            StructuralComponent* seq = track->getSequence();
            if (seq == nullptr) {
                continue;
            }

            const mxfUL data_def = seq->getDataDefinition();
            if (mxf_is_picture(&data_def) == 0) {
                continue;
            }

            const mxfRational er = track->getEditRate();
            if (er.numerator > 0 && er.denominator > 0) {
                info.fps_num = er.numerator;
                info.fps_den = er.denominator;
            }

            info.track_number = track->getTrackNumber();
            break;
        }

        return info;
    } catch (const MXFException& e) {
        return std::unexpected(
            std::format("libMXFpp error: {}", e.getMessage()));
    } catch (const std::exception& e) {
        return std::unexpected(std::format("Error: {}", e.what()));
    }
}

static bool looks_like_j2k_codestream_prefix(std::span<const uint8_t> buf) {
    return buf.size() >= J2K_SOC_MARKER_SIZE && buf[0] == J2K_SOC_FIRST_BYTE &&
           buf[1] == J2K_SOC_SECOND_BYTE;
}

struct KLVHeader {
    mxfKey key{};
    uint8_t llen = 0;
    uint64_t len = 0;
    uint64_t value_start = 0; // file offset of payload start
};

[[nodiscard]] static std::expected<void, std::string>
skip_bytes(MXFFile* file, int64_t file_size, uint64_t len) {
    if (len == 0) {
        return {};
    }

    if (len > static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
        return std::unexpected("KLV length out of range");
    }

    if (file_size >= 0) {
        const int64_t pos = mxf_file_tell(file);
        if (pos < 0) {
            return std::unexpected("mxf_file_tell failed");
        }
        const int64_t remaining = file_size - pos;
        if (remaining < 0) {
            return std::unexpected("File position beyond EOF");
        }
        if (std::cmp_greater(len, remaining)) {
            return std::unexpected("Truncated MXF while skipping KLV payload");
        }
    }

    if (mxf_skip(file, len) == 0) {
        return std::unexpected("mxf_skip failed");
    }
    return {};
}

[[nodiscard]] static std::expected<std::optional<KLVHeader>, std::string>
read_klv_header(MXFFile* file, int64_t file_size) {
    const int64_t pos = mxf_file_tell(file);
    if (pos < 0) {
        return std::unexpected("mxf_file_tell failed");
    }

    if (file_size >= 0) {
        if (pos >= file_size) {
            return std::nullopt;
        }
        if ((file_size - pos) < MXF_MIN_KL_BYTES) {
            return std::nullopt;
        }
    }

    KLVHeader h{};
    std::array<uint8_t, MXF_KEY_SIZE_BYTES> key_bytes{};
    const auto want_key = static_cast<uint32_t>(key_bytes.size());
    const uint32_t got_key = mxf_file_read(file, key_bytes.data(), want_key);
    if (got_key == 0) {
        return std::nullopt;
    }
    if (got_key != want_key) {
        return std::unexpected("Truncated MXF while reading KLV key");
    }
    std::memcpy(&h.key, key_bytes.data(), key_bytes.size());

    int c = mxf_file_getc(file);
    if (c == EOF) {
        return std::unexpected("Truncated MXF while reading KLV length");
    }
    const auto c_u8 = static_cast<uint8_t>(c);

    uint64_t length = 0;
    uint8_t llength = 1;
    if (c_u8 < BER_LONG_FORM_FLAG) {
        length = c_u8;
    } else {
        const uint8_t bytes_to_read = c_u8 & BER_LONG_FORM_LEN_MASK;
        if (bytes_to_read == 0) {
            return std::unexpected(
                "Invalid BER length (indefinite length not supported)");
        }
        if (bytes_to_read > BER_MAX_LEN_BYTES) {
            return std::unexpected("Invalid BER length (too many bytes)");
        }

        if (file_size >= 0) {
            const int64_t pos2 = mxf_file_tell(file);
            if (pos2 < 0) {
                return std::unexpected("mxf_file_tell failed");
            }
            if ((file_size - pos2) < bytes_to_read) {
                return std::unexpected(
                    "Truncated MXF while reading BER length");
            }
        }

        for (uint8_t i = 0; i < bytes_to_read; ++i) {
            c = mxf_file_getc(file);
            if (c == EOF) {
                return std::unexpected(
                    "Truncated MXF while reading BER length");
            }
            length = (length << BITS_PER_BYTE) | static_cast<uint8_t>(c);
        }
        llength += bytes_to_read;
    }

    const int64_t value_start_i = mxf_file_tell(file);
    if (value_start_i < 0) {
        return std::unexpected("mxf_file_tell failed");
    }

    h.llen = llength;
    h.len = length;
    h.value_start = static_cast<uint64_t>(value_start_i);
    return h;
}

[[nodiscard]] static std::expected<std::optional<KLVHeader>, std::string>
read_next_nonfiller_klv_header(MXFFile* file, int64_t file_size) {
    while (true) {
        auto hdr_exp = read_klv_header(file, file_size);
        if (!hdr_exp) {
            return std::unexpected(hdr_exp.error());
        }
        if (!*hdr_exp) {
            return std::nullopt;
        }

        KLVHeader h = **hdr_exp;
        if (mxf_is_filler(&h.key) == 0) {
            return h;
        }

        if (auto skip_exp = skip_bytes(file, file_size, h.len); !skip_exp) {
            return std::unexpected(skip_exp.error());
        }
    }
}

[[nodiscard]] static std::expected<uint16_t, std::string>
detect_and_set_runin_len(MXFFile* file) {
    if (mxf_file_seek(file, 0, SEEK_SET) == 0) {
        return std::unexpected("mxf_file_seek failed");
    }

    static constexpr std::array<uint8_t, PARTITION_LABEL_PREFIX_BYTES> PREFIX{
        0x06, 0x0e, 0x2b, 0x34, 0x02, 0x05, 0x01};
    static constexpr std::array<uint8_t, PARTITION_LABEL_SUFFIX_BYTES> SUFFIX{
        0x0d, 0x01, 0x02};

    uint32_t runin = 0;
    uint8_t matched = 0;
    while (runin <= MAX_RUNIN_LEN && matched < PARTITION_LABEL_BYTES) {
        const int ch = mxf_file_getc(file);
        if (ch == EOF) {
            return std::unexpected("Unexpected EOF while scanning MXF run-in");
        }

        const auto b = static_cast<uint8_t>(ch);
        bool ok = false;
        if (matched < PREFIX.size()) {
            ok = b == PREFIX.at(matched);
        } else if (matched == PARTITION_LABEL_REGVER_OFFSET) {
            ok = (b == PARTITION_LABEL_REGVER_1) ||
                 (b == PARTITION_LABEL_REGVER_2);
        } else {
            const size_t suffix_index =
                static_cast<size_t>(matched) - (PREFIX.size() + 1U);
            ok = b == SUFFIX.at(suffix_index);
        }

        if (ok) {
            matched++;
        } else {
            runin += matched + 1;
            matched = 0;
        }
    }

    if (runin > MAX_RUNIN_LEN) {
        return std::unexpected("MXF run-in exceeds MAX_RUNIN_LEN");
    }

    mxf_set_runin_len(file, static_cast<uint16_t>(runin));
    return static_cast<uint16_t>(runin);
}

struct FileStamp {
    uint64_t size = 0;
    int64_t mtime = 0;
};

static bool operator==(const FileStamp& a, const FileStamp& b) noexcept {
    return a.size == b.size && a.mtime == b.mtime;
}

template <typename T> static void write_le(std::ostream& os, T v) {
    static_assert(std::is_integral_v<T> && !std::is_same_v<T, bool>);
    if constexpr (std::endian::native == std::endian::big && sizeof(T) > 1) {
        v = std::byteswap(v);
    }
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
    os.write(reinterpret_cast<const char*>(&v), sizeof(v));
}

template <typename T> static bool read_le(std::istream& is, T& v) {
    static_assert(std::is_integral_v<T> && !std::is_same_v<T, bool>);
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
    is.read(reinterpret_cast<char*>(&v), sizeof(v));
    if (!is) {
        return false;
    }
    if constexpr (std::endian::native == std::endian::big && sizeof(T) > 1) {
        v = std::byteswap(v);
    }
    return true;
}

static std::optional<FileStamp> try_get_file_stamp(const fs::path& path) {
    std::error_code ec;
    const uintmax_t size_umax = fs::file_size(path, ec);
    if (ec) {
        return std::nullopt;
    }
    if (size_umax > std::numeric_limits<uint64_t>::max()) {
        return std::nullopt;
    }

    const fs::file_time_type ft = fs::last_write_time(path, ec);
    if (ec) {
        return std::nullopt;
    }

    using Rep = decltype(ft.time_since_epoch().count());
    const Rep rep = ft.time_since_epoch().count();
    int64_t mtime = 0;
    if constexpr (std::numeric_limits<Rep>::is_signed) {
        if (rep < std::numeric_limits<int64_t>::min() ||
            rep > std::numeric_limits<int64_t>::max()) {
            return std::nullopt;
        }
        mtime = static_cast<int64_t>(rep);
    } else {
        if (rep > static_cast<std::make_unsigned_t<int64_t>>(
                      std::numeric_limits<int64_t>::max())) {
            return std::nullopt;
        }
        mtime = static_cast<int64_t>(rep);
    }

    return FileStamp{.size = static_cast<uint64_t>(size_umax), .mtime = mtime};
}

struct IndexCacheEntry {
    std::optional<uint32_t> desired_track_number;
    std::vector<FrameIndex> frames;
};

struct IndexCacheFile {
    FileStamp src_stamp{};
    std::vector<IndexCacheEntry> entries;
};

static constexpr std::array<uint8_t, 8> INDEX_CACHE_MAGIC{'M', 'X', 'F', 'J',
                                                          '2', 'K', 'I', 'X'};
static constexpr uint32_t INDEX_CACHE_VERSION = 1;
static constexpr uint32_t INDEX_CACHE_MAX_ENTRIES = 128;
static constexpr uint32_t INDEX_CACHE_MAX_FRAMES = 50'000'000;

static std::optional<IndexCacheFile>
try_read_index_cache_file(const fs::path& cache_path) {
    std::ifstream in(cache_path, std::ios::binary);
    if (!in) {
        return std::nullopt;
    }

    std::array<uint8_t, INDEX_CACHE_MAGIC.size()> magic{};
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
    in.read(reinterpret_cast<char*>(magic.data()),
            static_cast<std::streamsize>(magic.size()));
    if (!in || magic != INDEX_CACHE_MAGIC) {
        return std::nullopt;
    }

    uint32_t version = 0;
    if (!read_le(in, version) || version != INDEX_CACHE_VERSION) {
        return std::nullopt;
    }

    IndexCacheFile cache{};
    if (!read_le(in, cache.src_stamp.size) ||
        !read_le(in, cache.src_stamp.mtime)) {
        return std::nullopt;
    }

    uint32_t entry_count = 0;
    if (!read_le(in, entry_count) || entry_count > INDEX_CACHE_MAX_ENTRIES) {
        return std::nullopt;
    }

    cache.entries.reserve(entry_count);
    for (uint32_t i = 0; i < entry_count; ++i) {
        uint8_t has_track = 0;
        if (!read_le(in, has_track)) {
            return std::nullopt;
        }

        uint32_t track_number = 0;
        if (!read_le(in, track_number)) {
            return std::nullopt;
        }

        uint32_t frame_count = 0;
        if (!read_le(in, frame_count) || frame_count > INDEX_CACHE_MAX_FRAMES) {
            return std::nullopt;
        }

        IndexCacheEntry e{};
        e.desired_track_number = (has_track != 0)
                                     ? std::optional<uint32_t>(track_number)
                                     : std::nullopt;
        e.frames.reserve(frame_count);

        for (uint32_t j = 0; j < frame_count; ++j) {
            FrameIndex fi{};
            if (!read_le(in, fi.value_offset) || !read_le(in, fi.value_size)) {
                return std::nullopt;
            }
            e.frames.push_back(fi);
        }

        cache.entries.push_back(std::move(e));
    }

    return cache;
}

[[nodiscard]] static std::expected<void, std::string>
write_index_cache_file(const fs::path& cache_path,
                       const IndexCacheFile& cache) {
    std::error_code ec;
    const fs::path parent = cache_path.parent_path();
    if (!parent.empty() && !fs::exists(parent, ec)) {
        return std::unexpected("Cache directory does not exist");
    }

    fs::path tmp = cache_path;
    tmp += ".tmp.";
    tmp += std::to_string(
        std::chrono::steady_clock::now().time_since_epoch().count());

    std::ofstream out(tmp, std::ios::binary | std::ios::trunc);
    if (!out) {
        return std::unexpected("Failed to open cache file for writing");
    }

    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
    out.write(reinterpret_cast<const char*>(INDEX_CACHE_MAGIC.data()),
              static_cast<std::streamsize>(INDEX_CACHE_MAGIC.size()));
    write_le(out, INDEX_CACHE_VERSION);

    write_le(out, cache.src_stamp.size);
    write_le(out, cache.src_stamp.mtime);

    const auto entry_count_u32 = static_cast<uint32_t>(
        std::min<size_t>(cache.entries.size(), INDEX_CACHE_MAX_ENTRIES));
    write_le(out, entry_count_u32);

    for (uint32_t i = 0; i < entry_count_u32; ++i) {
        const IndexCacheEntry& e = cache.entries.at(i);
        const uint8_t has_track = e.desired_track_number ? 1 : 0;
        write_le(out, has_track);
        write_le(out, e.desired_track_number.value_or(0U));

        const auto frame_count_u32 = static_cast<uint32_t>(
            std::min<size_t>(e.frames.size(), INDEX_CACHE_MAX_FRAMES));
        write_le(out, frame_count_u32);
        for (uint32_t j = 0; j < frame_count_u32; ++j) {
            const FrameIndex& fi = e.frames.at(j);
            write_le(out, fi.value_offset);
            write_le(out, fi.value_size);
        }
    }

    out.flush();
    if (!out) {
        out.close();
        (void)fs::remove(tmp, ec);
        return std::unexpected("Failed while writing cache file");
    }
    out.close();

    fs::rename(tmp, cache_path, ec);
    if (ec) {
        (void)fs::remove(cache_path, ec);
        ec.clear();
        fs::rename(tmp, cache_path, ec);
        if (ec) {
            (void)fs::remove(tmp, ec);
            return std::unexpected("Failed to replace cache file");
        }
    }

    return {};
}

[[nodiscard]] static std::expected<std::vector<FrameIndex>, std::string>
index_j2k_essence_frames_scan(
    const std::string& path,
    const std::optional<uint32_t> desired_track_number) {
    try {
        MxfDiskFile f(path);

        MXFFile* file = f.get();
        const int64_t file_size = mxf_file_size(file);

        (void)detect_and_set_runin_len(file);
        if (mxf_file_seek(file, mxf_get_runin_len(file), SEEK_SET) == 0) {
            return std::unexpected("mxf_file_seek failed");
        }

        std::vector<FrameIndex> frames;
        frames.reserve(FRAME_INDEX_RESERVE);

        while (true) {
            auto kl_exp = read_next_nonfiller_klv_header(file, file_size);
            if (!kl_exp) {
                return std::unexpected(kl_exp.error());
            }
            if (!*kl_exp) {
                break;
            }

            const KLVHeader kl = **kl_exp;
            const mxfKey& key = kl.key;
            const uint64_t len = kl.len;
            const uint64_t value_start = kl.value_start;

            if (mxf_is_gc_essence_element(&key) != 0) {
                const uint32_t track_number = mxf_get_track_number(&key);
                if (!desired_track_number ||
                    track_number == *desired_track_number) {
                    if (len > std::numeric_limits<uint32_t>::max()) {
                        return std::unexpected(
                            "Essence KLV too large for in-memory decode");
                    }

                    bool accept = true;
                    if (!desired_track_number) {
                        const auto sniff = static_cast<uint32_t>(
                            std::min<uint64_t>(len, J2K_SOC_MARKER_SIZE));
                        std::array<uint8_t, J2K_SOC_MARKER_SIZE> prefix{};
                        uint32_t nread = 0;
                        if (sniff != 0) {
                            nread = mxf_file_read(file, prefix.data(), sniff);
                        }
                        if (nread != sniff) {
                            return std::unexpected(
                                "Truncated MXF while reading essence prefix");
                        }

                        accept = (sniff == J2K_SOC_MARKER_SIZE) &&
                                 looks_like_j2k_codestream_prefix(prefix);
                        if (auto skip_exp =
                                skip_bytes(file, file_size, len - nread);
                            !skip_exp) {
                            return std::unexpected(skip_exp.error());
                        }
                    } else {
                        if (auto skip_exp = skip_bytes(file, file_size, len);
                            !skip_exp) {
                            return std::unexpected(skip_exp.error());
                        }
                    }

                    if (accept) {
                        frames.push_back(FrameIndex{
                            .value_offset = value_start,
                            .value_size = static_cast<uint32_t>(len)});
                    }
                    continue;
                }
            }

            if (auto skip_exp = skip_bytes(file, file_size, len); !skip_exp) {
                return std::unexpected(skip_exp.error());
            }
        }

        return frames;
    } catch (const std::exception& e) {
        return std::unexpected(std::format("Indexing error: {}", e.what()));
    }
}

[[nodiscard]] static std::expected<std::vector<FrameIndex>, std::string>
index_j2k_essence_frames(const std::string& path,
                         const std::optional<uint32_t> desired_track_number,
                         const std::optional<fs::path>& cache_path,
                         bool cache_path_forced, VSCore* core,
                         const VSAPI* vsapi) {
    const fs::path src_fs = path_from_utf8(path);
    const std::optional<FileStamp> stamp = try_get_file_stamp(src_fs);

    std::optional<IndexCacheFile> existing_cache;
    if (cache_path && stamp) {
        try {
            existing_cache = try_read_index_cache_file(*cache_path);
            if (existing_cache && existing_cache->src_stamp == *stamp) {
                for (const IndexCacheEntry& e : existing_cache->entries) {
                    if (e.desired_track_number == desired_track_number) {
                        return e.frames;
                    }
                }
            }
        } catch (...) {
            existing_cache.reset();
        }
    }

    auto frames_exp = index_j2k_essence_frames_scan(path, desired_track_number);
    if (!frames_exp) {
        return frames_exp;
    }

    if (cache_path && stamp) {
        try {
            IndexCacheFile cache{};
            cache.src_stamp = *stamp;
            if (existing_cache && existing_cache->src_stamp == *stamp) {
                cache = std::move(*existing_cache);
            }

            bool updated = false;
            for (IndexCacheEntry& e : cache.entries) {
                if (e.desired_track_number == desired_track_number) {
                    e.frames = *frames_exp;
                    updated = true;
                    break;
                }
            }
            if (!updated) {
                cache.entries.push_back(IndexCacheEntry{
                    .desired_track_number = desired_track_number,
                    .frames = *frames_exp,
                });
            }

            auto write_exp = write_index_cache_file(*cache_path, cache);
            if (!write_exp) {
                const std::string why =
                    std::format("Failed to write index cache '{}': {}",
                                cache_path->string(), write_exp.error());
                if (cache_path_forced) {
                    return std::unexpected(why);
                }
                if (vsapi != nullptr && core != nullptr) {
                    const std::string msg =
                        std::format("MXFJ2KSource: {}", why);
                    vsapi->logMessage(mtWarning, msg.c_str(), core);
                }
            }
        } catch (...) {
            if (cache_path_forced) {
                return std::unexpected(std::format(
                    "Failed to write index cache '{}': unknown error",
                    cache_path->string()));
            }
            if (vsapi != nullptr && core != nullptr) {
                const std::string msg = std::format(
                    "MXFJ2KSource: Failed to write index cache '{}': unknown "
                    "error",
                    cache_path->string());
                vsapi->logMessage(mtWarning, msg.c_str(), core);
            }
        }
    }

    return frames_exp;
}

[[nodiscard]] static std::expected<void, std::string>
read_at(MXFFile* file, uint64_t offset, std::span<uint8_t> buf) {
    if (buf.empty()) {
        return std::unexpected("Zero-sized essence frame");
    }

    if (offset > static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
        return std::unexpected("Essence offset out of range");
    }

    if (buf.size() > std::numeric_limits<uint32_t>::max()) {
        return std::unexpected("Essence frame too large");
    }
    const auto size = static_cast<uint32_t>(buf.size());

    if (mxf_file_seek(file, static_cast<int64_t>(offset), SEEK_SET) == 0) {
        return std::unexpected("mxf_file_seek failed");
    }

    uint32_t got = 0;
    while (got < size) {
        const uint32_t n = mxf_file_read(file, buf.data() + got,
                                         static_cast<uint32_t>(size - got));
        if (n == 0) {
            break;
        }
        got += n;
    }
    if (got != size) {
        return std::unexpected("Short read while reading essence frame");
    }

    return {};
}

[[nodiscard]] static std::expected<J2KHeaderInfo, std::string>
probe_j2k_header(std::span<uint8_t> buf) {
    if (!looks_like_j2k_codestream_prefix(buf)) {
        return std::unexpected(
            "Essence does not look like a JPEG 2000 codestream (missing SOC)");
    }

    grok_init_once();

    grk_stream_params sp{};
    sp.buf = buf.data();
    sp.buf_len = buf.size();

    grk_decompress_parameters params{};
    params.decod_format = GRK_CODEC_J2K;
    params.cod_format = GRK_FMT_J2K;

    GrokCodecPtr codec(grk_decompress_init(&sp, &params));
    if (!codec) {
        return std::unexpected("grok: grk_decompress_init failed");
    }

    grk_header_info header{};
    header.decompress_fmt = GRK_FMT_J2K;
    if (!grk_decompress_read_header(codec.get(), &header)) {
        return std::unexpected("grok: grk_decompress_read_header failed");
    }

    grk_image* img = grk_decompress_get_image(codec.get());
    if (img == nullptr) {
        return std::unexpected("grok: grk_decompress_get_image returned null");
    }

    const int w = static_cast<int>(img->x1 - img->x0);
    const int h = static_cast<int>(img->y1 - img->y0);
    if (w <= 0 || h <= 0) {
        return std::unexpected("grok: invalid image dimensions");
    }

    if (img->numcomps == 0 || (img->comps == nullptr)) {
        return std::unexpected("grok: invalid component data in header image");
    }

    const std::span<const grk_image_comp> comps(img->comps, img->numcomps);

    J2KHeaderInfo info{};
    info.width = w;
    info.height = h;
    info.num_comps = img->numcomps;
    info.prec = comps.front().prec;
    if (info.num_comps >= 3) {
        info.first_three_full_res = true;
        info.first_three_same_prec = true;

        const uint8_t p0 = comps[0].prec;
        for (size_t i = 0; i < 3; ++i) {
            if (comps[i].prec != p0) {
                info.first_three_same_prec = false;
            }
            if (!std::cmp_equal(comps[i].w, w) ||
                !std::cmp_equal(comps[i].h, h)) {
                info.first_three_full_res = false;
            }
        }
    }
    return info;
}

static inline void copy_plane_to_u12(int w, int h, const int32_t* src,
                                     uint32_t src_stride_samples, uint8_t* dstp,
                                     ptrdiff_t dst_stride_bytes,
                                     bool signed_in) {
    static constexpr int64_t OUT_MAX =
        (int64_t{1} << VAPOURSYNTH_RGB36_BITS) - 1;
    const int64_t offset =
        signed_in ? (int64_t{1} << (VAPOURSYNTH_RGB36_BITS - 1)) : 0;

    for (int y = 0; y < h; ++y) {
        const auto* src_row = src + static_cast<size_t>(y) *
                                        static_cast<size_t>(src_stride_samples);
        auto* dst_row =
            // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
            reinterpret_cast<uint16_t*>(
                dstp +
                static_cast<size_t>(y) * static_cast<size_t>(dst_stride_bytes));
        for (int x = 0; x < w; ++x) {
            const int64_t vv =
                static_cast<int64_t>(src_row[static_cast<size_t>(x)]) + offset;
            dst_row[static_cast<size_t>(x)] =
                static_cast<uint16_t>(std::clamp<int64_t>(vv, 0, OUT_MAX));
        }
    }
}

static inline void copy_plane_to_u16(int w, int h, const int32_t* src,
                                     uint32_t src_stride_samples, uint8_t* dstp,
                                     ptrdiff_t dst_stride_bytes,
                                     bool signed_in) {
    static constexpr int64_t OUT_MAX =
        (int64_t{1} << VAPOURSYNTH_RGB48_BITS) - 1;
    const int64_t offset =
        signed_in ? (int64_t{1} << (VAPOURSYNTH_RGB48_BITS - 1)) : 0;

    for (int y = 0; y < h; ++y) {
        const auto* src_row = src + (static_cast<size_t>(y) *
                                     static_cast<size_t>(src_stride_samples));
        auto* dst_row =
            // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
            reinterpret_cast<uint16_t*>(
                dstp + (static_cast<size_t>(y) *
                        static_cast<size_t>(dst_stride_bytes)));
        for (int x = 0; x < w; ++x) {
            const int64_t vv =
                static_cast<int64_t>(src_row[static_cast<size_t>(x)]) + offset;
            dst_row[static_cast<size_t>(x)] =
                static_cast<uint16_t>(std::clamp<int64_t>(vv, 0, OUT_MAX));
        }
    }
}

struct MXFJ2KSourceData {
    VSVideoInfo vi{};
    std::string path;
    std::vector<FrameIndex> frames;

    std::mutex file_pool_mutex;
    std::unordered_map<std::thread::id, std::unique_ptr<MxfDiskFile>> file_pool;
};

static MxfDiskFile* get_thread_file(MXFJ2KSourceData* d) {
    const auto tid = std::this_thread::get_id();
    std::scoped_lock lk(d->file_pool_mutex);
    if (auto it = d->file_pool.find(tid); it != d->file_pool.end()) {
        return it->second.get();
    }

    auto fh = std::make_unique<MxfDiskFile>(d->path);
    MxfDiskFile* raw = fh.get();
    d->file_pool.emplace(tid, std::move(fh));
    return raw;
}

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
static const VSFrame* VS_CC mxfj2k_get_frame(int n, int activation_reason,
                                             void* instance_data,
                                             void** /*unused*/,
                                             VSFrameContext* frame_ctx,
                                             VSCore* core, const VSAPI* vsapi) {
    auto* d = static_cast<MXFJ2KSourceData*>(instance_data);

    if (activation_reason != arInitial) {
        return nullptr;
    }

    struct VSFrameDeleter {
        const VSAPI* vsapi = nullptr;
        void operator()(VSFrame* f) const noexcept {
            if (f != nullptr) {
                vsapi->freeFrame(f);
            }
        }
    };

    try {
        const int last = std::max(0, d->vi.numFrames - 1);
        const int fn = std::clamp(n, 0, last);
        const FrameIndex& idx = d->frames.at(static_cast<size_t>(fn));

        MxfDiskFile* disk = get_thread_file(d);
        std::span<uint8_t> frame_buf =
            disk->scratchBuf(static_cast<size_t>(idx.value_size));
        if (auto read_exp = read_at(disk->get(), idx.value_offset, frame_buf);
            !read_exp) {
            throw std::runtime_error(read_exp.error());
        }

        grok_init_once();

        grk_stream_params sp{};
        sp.buf = frame_buf.data();
        sp.buf_len = frame_buf.size();

        grk_decompress_parameters params{};
        params.decod_format = GRK_CODEC_J2K;
        params.cod_format = GRK_FMT_J2K;

        GrokCodecPtr codec(grk_decompress_init(&sp, &params));
        if (!codec) {
            throw std::runtime_error("grok: grk_decompress_init failed");
        }

        grk_header_info header{};
        header.decompress_fmt = GRK_FMT_J2K;
        if (!grk_decompress_read_header(codec.get(), &header)) {
            throw std::runtime_error("grok: grk_decompress_read_header failed");
        }

        if (!grk_decompress(codec.get(), nullptr)) {
            throw std::runtime_error("grok: grk_decompress failed");
        }

        grk_image* img = grk_decompress_get_image(codec.get());
        if ((img == nullptr) || (img->comps == nullptr)) {
            throw std::runtime_error("grok: null decoded image");
        }

        if (img->numcomps < 1) {
            throw std::runtime_error("grok: decoded image has no components");
        }

        std::unique_ptr<VSFrame, VSFrameDeleter> dst(
            vsapi->newVideoFrame(&d->vi.format, d->vi.width, d->vi.height,
                                 nullptr, core),
            VSFrameDeleter{vsapi});
        if (!dst) {
            throw std::runtime_error("VapourSynth: newVideoFrame failed");
        }

        const int w = d->vi.width;
        const int h = d->vi.height;

        const int img_w = static_cast<int>(img->x1 - img->x0);
        const int img_h = static_cast<int>(img->y1 - img->y0);
        if (img_w != w || img_h != h) {
            throw std::runtime_error("grok: decoded dimensions don't match "
                                     "probed header dimensions");
        }

        const uint16_t ncomps = img->numcomps;
        const std::span<const grk_image_comp> comps(img->comps, ncomps);
        const grk_image_comp* c0 = &comps.front();
        const grk_image_comp* c1 = (ncomps >= 2) ? &comps[1] : c0;
        const grk_image_comp* c2 = (ncomps >= 3) ? &comps[2] : c0;

        auto validate_comp = [&](const grk_image_comp* c) {
            if (c->data_type != GRK_INT_32) {
                throw std::runtime_error(
                    "grok: unsupported component data type (expected int32)");
            }
            if (std::cmp_less(c->w, w) || std::cmp_less(c->h, h)) {
                throw std::runtime_error(
                    "grok: component size smaller than output frame");
            }
            if (std::cmp_less(c->stride, w)) {
                throw std::runtime_error(
                    "grok: component stride smaller than width");
            }
            if (!c->data) {
                throw std::runtime_error("grok: null component buffer");
            }
        };

        validate_comp(c0);
        validate_comp(c1);
        validate_comp(c2);

        auto copy_plane = [&](int plane, const grk_image_comp* comp) {
            const uint8_t prec_in = comp->prec;
            const bool signed_in = comp->sgnd;
            const auto* src = static_cast<const int32_t*>(comp->data);

            const ptrdiff_t dst_stride_bytes =
                vsapi->getStride(dst.get(), plane);
            uint8_t* dstp = vsapi->getWritePtr(dst.get(), plane);

            const uint32_t src_stride = comp->stride; // samples, not bytes
            const int out_bits = d->vi.format.bitsPerSample;
            if (out_bits == VAPOURSYNTH_RGB36_BITS) {
                if (prec_in != VAPOURSYNTH_RGB36_BITS) {
                    throw std::runtime_error(std::format(
                        "grok: unexpected component precision {} (expected {})",
                        prec_in, VAPOURSYNTH_RGB36_BITS));
                }
                copy_plane_to_u12(w, h, src, src_stride, dstp, dst_stride_bytes,
                                  signed_in);
                return;
            }
            if (out_bits == VAPOURSYNTH_RGB48_BITS) {
                if (prec_in != VAPOURSYNTH_RGB48_BITS) {
                    throw std::runtime_error(std::format(
                        "grok: unexpected component precision {} (expected {})",
                        prec_in, VAPOURSYNTH_RGB48_BITS));
                }
                copy_plane_to_u16(w, h, src, src_stride, dstp, dst_stride_bytes,
                                  signed_in);
                return;
            }

            throw std::runtime_error(
                "Internal error: unsupported output bit depth");
        };

        copy_plane(0, c0);
        copy_plane(1, c1);
        copy_plane(2, c2);

        return dst.release();
    } catch (const std::exception& e) {
        const std::string msg = std::format("MXFJ2KSource: {}", e.what());
        vsapi->setFilterError(msg.c_str(), frame_ctx);
        return nullptr;
    }
}

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
static void VS_CC mxfj2k_free(void* instance_data, VSCore* /*unused*/,
                              const VSAPI* /*unused*/) {
    std::unique_ptr<MXFJ2KSourceData> d(
        static_cast<MXFJ2KSourceData*>(instance_data));
}

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
static void VS_CC mxfj2k_create(const VSMap* in, VSMap* out, void* /*unused*/,
                                VSCore* core, const VSAPI* vsapi) {
    try {
        int err = 0;
        const char* src = vsapi->mapGetData(in, "source", 0, &err);
        if ((err != 0) || (src == nullptr) || (*src == '\0')) {
            vsapi->mapSetError(out, "Source: 'source' is required");
            return;
        }

        const int64_t forced_track = vsapi->mapGetInt(in, "track", 0, &err);
        const std::optional<uint32_t> forced_track_number =
            ((err == 0) && forced_track > 0)
                ? std::optional<uint32_t>(static_cast<uint32_t>(forced_track))
                : std::nullopt;
        const bool track_forced = forced_track_number.has_value();

        const char* cache_path_str =
            vsapi->mapGetData(in, "cache_path", 0, &err);
        const bool cache_path_forced = (err == 0) &&
                                       (cache_path_str != nullptr) &&
                                       (*cache_path_str != '\0');
        const std::optional<fs::path> cache_path =
            cache_path_forced
                ? std::optional<fs::path>(path_from_utf8(cache_path_str))
                : std::optional<fs::path>(
                      path_from_utf8(src).concat(".mxfj2kindex"));

        VideoTrackInfo track_info{};
        if (auto track_info_exp = probe_mxf_video_track(src); track_info_exp) {
            track_info = *track_info_exp;
        } else {
            if (!track_info_exp.error().empty()) {
                const std::string msg =
                    std::format("MXFJ2KSource: {}", track_info_exp.error());
                vsapi->logMessage(mtWarning, msg.c_str(), core);
            }
        }

        if (forced_track_number) {
            track_info.track_number = forced_track_number;
        }

        auto frames_exp =
            index_j2k_essence_frames(src, track_info.track_number, cache_path,
                                     cache_path_forced, core, vsapi);
        if (!frames_exp || frames_exp->empty()) {
            if (track_info.track_number) {
                if (!track_forced) {
                    auto retry = index_j2k_essence_frames(
                        src, std::nullopt, cache_path, cache_path_forced, core,
                        vsapi);
                    if (retry && !retry->empty()) {
                        frames_exp = std::move(retry);
                    }
                }
            }
        }

        if (!frames_exp || frames_exp->empty()) {
            std::string why;
            if (!frames_exp) {
                why = frames_exp.error();
            } else if (track_forced) {
                why = std::format("Requested track {} not found",
                                  *forced_track_number);
            } else {
                why = "No JPEG 2000 essence frames found";
            }
            const std::string msg = std::format("Source: {}", why);
            vsapi->mapSetError(out, msg.c_str());
            return;
        }

        MxfDiskFile file(src);
        std::span<uint8_t> first_frame = file.scratchBuf(
            static_cast<size_t>(frames_exp->front().value_size));
        if (auto first_exp = read_at(
                file.get(), frames_exp->front().value_offset, first_frame);
            !first_exp) {
            const std::string msg =
                std::format("Source: {}", first_exp.error());
            vsapi->mapSetError(out, msg.c_str());
            return;
        }

        auto hdr_exp = probe_j2k_header(first_frame);
        if (!hdr_exp && track_info.track_number) {
            if (!track_forced) {
                auto retry_frames =
                    index_j2k_essence_frames(src, std::nullopt, cache_path,
                                             cache_path_forced, core, vsapi);
                if (retry_frames && !retry_frames->empty()) {
                    first_frame = file.scratchBuf(
                        static_cast<size_t>(retry_frames->front().value_size));
                    if (read_at(file.get(), retry_frames->front().value_offset,
                                first_frame)) {
                        auto retry_hdr = probe_j2k_header(first_frame);
                        if (retry_hdr) {
                            vsapi->logMessage(
                                mtWarning,
                                "MXFJ2KSource: MXF track metadata didn't match "
                                "J2K essence; falling back to sniffed J2K KLVs",
                                core);
                            hdr_exp = std::move(retry_hdr);
                            frames_exp = std::move(retry_frames);
                        }
                    }
                }
            }
        }
        if (!hdr_exp) {
            std::string why = hdr_exp.error();
            if (track_forced) {
                why = std::format("Requested track {} is not JPEG 2000: {}",
                                  *forced_track_number, why);
            }
            const std::string msg = std::format("Source: {}", why);
            vsapi->mapSetError(out, msg.c_str());
            return;
        }
        const J2KHeaderInfo hdr = *hdr_exp;

        if (hdr.num_comps < 3) {
            const std::string msg = std::format(
                "Source: JPEG 2000 must have at least 3 components (got {})",
                hdr.num_comps);
            vsapi->mapSetError(out, msg.c_str());
            return;
        }
        if (!hdr.first_three_full_res) {
            vsapi->mapSetError(out,
                               "Source: only 4:4:4 JPEG 2000 is supported");
            return;
        }
        if (!hdr.first_three_same_prec) {
            vsapi->mapSetError(out, "Source: component bit depths must match "
                                    "(12-bit or 16-bit for all components)");
            return;
        }
        if (hdr.prec != VAPOURSYNTH_RGB36_BITS &&
            hdr.prec != VAPOURSYNTH_RGB48_BITS) {
            const std::string msg =
                std::format("Source: unsupported JPEG 2000 bit depth {} "
                            "(expected 12 or 16)",
                            hdr.prec);
            vsapi->mapSetError(out, msg.c_str());
            return;
        }

        const int out_bits = hdr.prec;
        const char* out_name =
            (out_bits == VAPOURSYNTH_RGB48_BITS) ? "RGB48" : "RGB36";

        VSVideoFormat fmt{};
        if (vsapi->queryVideoFormat(&fmt, cfRGB, stInteger, out_bits, 0, 0,
                                    core) == 0) {
            const std::string msg = std::format(
                "Source: VapourSynth core does not support {}", out_name);
            vsapi->mapSetError(out, msg.c_str());
            return;
        }

        auto d = std::make_unique<MXFJ2KSourceData>();
        d->path = src;
        d->frames = std::move(*frames_exp);
        d->vi.format = fmt;
        d->vi.width = hdr.width;
        d->vi.height = hdr.height;
        d->vi.numFrames =
            vsh::int64ToIntS(static_cast<int64_t>(d->frames.size()));
        d->vi.fpsNum = track_info.fps_num;
        d->vi.fpsDen = track_info.fps_den;
        vsh::reduceRational(&d->vi.fpsNum, &d->vi.fpsDen);

        vsapi->createVideoFilter(out, "Source", &d->vi, mxfj2k_get_frame,
                                 mxfj2k_free, fmUnordered, nullptr, 0, d.get(),
                                 core);
        (void)d.release();
    } catch (const std::exception& e) {
        const std::string msg = std::format("Source: {}", e.what());
        vsapi->mapSetError(out, msg.c_str());
    } catch (...) {
        vsapi->mapSetError(out, "Source: unknown error");
    }
}

} // namespace mxfj2ksource

VS_EXTERNAL_API(void)
VapourSynthPluginInit2( // NOLINT(readability-identifier-naming)
    VSPlugin* plugin, const VSPLUGINAPI* vspapi) {
    vspapi->configPlugin("com.yuygfgg.mxfj2ksource", "MXFJ2KSource",
                         "JPEG 2000 MXF source", VS_MAKE_VERSION(1, 1),
                         VAPOURSYNTH_API_VERSION, 0, plugin);
    vspapi->registerFunction(
        "Source", "source:data;track:int:opt;cache_path:data:opt;",
        "clip:vnode;", mxfj2ksource::mxfj2k_create, nullptr, plugin);
}
