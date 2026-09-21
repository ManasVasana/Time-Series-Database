#pragma once
#include <string>
#include <unordered_map>
#include <vector>
#include <filesystem>
#include <optional>
#include <mutex>
#include <cstdint>

namespace kronos {

class SymbolTable {
public:
    explicit SymbolTable(std::filesystem::path path);

    // Get or assign symbol_id for name; creates if missing
    uint32_t intern(const std::string& symbol);

    std::optional<uint32_t>    lookup(const std::string& symbol) const;
    std::optional<std::string> lookup_name(uint32_t id) const;

    // Snapshot of all interned symbol names. Used by /v1/symbols so the
    // workbench can browse what is actually in the database.
    std::vector<std::string> names() const;

    void load();
    void flush();

    size_t size() const;

private:
    std::filesystem::path path_;
    std::unordered_map<std::string, uint32_t> name_to_id_;
    std::unordered_map<uint32_t, std::string> id_to_name_;
    uint32_t next_id_{1};
    mutable std::mutex mu_;

    void append_entry(uint32_t id, const std::string& name);
};

} // namespace kronos
