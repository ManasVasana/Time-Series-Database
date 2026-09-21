#include "kronosdb/util/symbol_table.hpp"
#include <fstream>
#include <stdexcept>
#include <cstring>

namespace kronos {

// File format:
//   For each entry (appended incrementally):
//     [id : 4 LE] [name_len : 2 LE] [name : name_len bytes]

SymbolTable::SymbolTable(std::filesystem::path path)
    : path_(std::move(path)) {
    if (std::filesystem::exists(path_)) {
        load();
    }
}

uint32_t SymbolTable::intern(const std::string& symbol) {
    std::lock_guard lock(mu_);
    auto it = name_to_id_.find(symbol);
    if (it != name_to_id_.end()) return it->second;

    uint32_t id = next_id_++;
    name_to_id_[symbol] = id;
    id_to_name_[id]     = symbol;
    append_entry(id, symbol);
    return id;
}

std::optional<uint32_t> SymbolTable::lookup(const std::string& symbol) const {
    std::lock_guard lock(mu_);
    auto it = name_to_id_.find(symbol);
    if (it == name_to_id_.end()) return std::nullopt;
    return it->second;
}

std::optional<std::string> SymbolTable::lookup_name(uint32_t id) const {
    std::lock_guard lock(mu_);
    auto it = id_to_name_.find(id);
    if (it == id_to_name_.end()) return std::nullopt;
    return it->second;
}

size_t SymbolTable::size() const {
    std::lock_guard lock(mu_);
    return name_to_id_.size();
}

std::vector<std::string> SymbolTable::names() const {
    std::lock_guard lock(mu_);
    std::vector<std::string> out;
    out.reserve(name_to_id_.size());
    for (auto& [name, _id] : name_to_id_) out.push_back(name);
    return out;
}

void SymbolTable::append_entry(uint32_t id, const std::string& name) {
    // Called with mu_ held
    std::ofstream f(path_, std::ios::binary | std::ios::app);
    if (!f) throw std::runtime_error("cannot open symbol table: " + path_.string());

    uint8_t buf[6];
    buf[0] = (id      ) & 0xFF;
    buf[1] = (id >>  8) & 0xFF;
    buf[2] = (id >> 16) & 0xFF;
    buf[3] = (id >> 24) & 0xFF;
    uint16_t len = static_cast<uint16_t>(name.size());
    buf[4] = (len     ) & 0xFF;
    buf[5] = (len >> 8) & 0xFF;
    f.write(reinterpret_cast<const char*>(buf), 6);
    f.write(name.data(), static_cast<std::streamsize>(name.size()));
}

void SymbolTable::load() {
    std::ifstream f(path_, std::ios::binary);
    if (!f) return;

    while (f) {
        uint8_t buf[6];
        f.read(reinterpret_cast<char*>(buf), 6);
        if (f.gcount() < 6) break;

        uint32_t id  = static_cast<uint32_t>(buf[0]) |
                      (static_cast<uint32_t>(buf[1]) << 8)  |
                      (static_cast<uint32_t>(buf[2]) << 16) |
                      (static_cast<uint32_t>(buf[3]) << 24);
        uint16_t len = static_cast<uint16_t>(buf[4]) |
                      (static_cast<uint16_t>(buf[5]) << 8);

        std::string name(len, '\0');
        f.read(name.data(), len);
        if (static_cast<uint16_t>(f.gcount()) < len) break;

        name_to_id_[name] = id;
        id_to_name_[id]   = name;
        if (id >= next_id_) next_id_ = id + 1;
    }
}

void SymbolTable::flush() {
    // Rewrite entire file from in-memory state
    std::lock_guard lock(mu_);
    std::ofstream f(path_, std::ios::binary | std::ios::trunc);
    if (!f) throw std::runtime_error("cannot open symbol table for flush");
    for (auto& [name, id] : name_to_id_) {
        uint8_t buf[6];
        buf[0] = (id      ) & 0xFF;
        buf[1] = (id >>  8) & 0xFF;
        buf[2] = (id >> 16) & 0xFF;
        buf[3] = (id >> 24) & 0xFF;
        uint16_t len = static_cast<uint16_t>(name.size());
        buf[4] = (len     ) & 0xFF;
        buf[5] = (len >> 8) & 0xFF;
        f.write(reinterpret_cast<const char*>(buf), 6);
        f.write(name.data(), static_cast<std::streamsize>(name.size()));
    }
}

} // namespace kronos
