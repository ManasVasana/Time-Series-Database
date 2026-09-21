#include <gtest/gtest.h>
#include "kronosdb/util/symbol_table.hpp"
#include <filesystem>

using namespace kronos;

class SymbolTableTest : public ::testing::Test {
protected:
    std::filesystem::path dir_;
    std::filesystem::path path_;
    void SetUp() override {
        dir_  = std::filesystem::temp_directory_path() / "kronos_symtable_test";
        std::filesystem::remove_all(dir_);
        std::filesystem::create_directories(dir_);
        path_ = dir_ / "symbols.bin";
    }
    void TearDown() override {
        std::filesystem::remove_all(dir_);
    }
};

TEST_F(SymbolTableTest, InternAndLookup) {
    SymbolTable st(path_);
    uint32_t id1 = st.intern("AAPL");
    uint32_t id2 = st.intern("MSFT");
    uint32_t id3 = st.intern("NVDA");

    EXPECT_NE(id1, id2);
    EXPECT_NE(id2, id3);
    EXPECT_EQ(st.lookup("AAPL"), id1);
    EXPECT_EQ(st.lookup("MSFT"), id2);
    EXPECT_EQ(st.lookup("NVDA"), id3);
}

TEST_F(SymbolTableTest, InternIdempotent) {
    SymbolTable st(path_);
    uint32_t id1 = st.intern("TSLA");
    uint32_t id2 = st.intern("TSLA");
    EXPECT_EQ(id1, id2);
    EXPECT_EQ(st.size(), 1u);
}

TEST_F(SymbolTableTest, LookupUnknownReturnsNullopt) {
    SymbolTable st(path_);
    EXPECT_FALSE(st.lookup("UNKNOWN").has_value());
}

TEST_F(SymbolTableTest, PersistenceAcrossRestart) {
    uint32_t id_aapl, id_msft;
    {
        SymbolTable st(path_);
        id_aapl = st.intern("AAPL");
        id_msft = st.intern("MSFT");
    }
    // Reload from disk
    {
        SymbolTable st(path_);
        EXPECT_EQ(st.lookup("AAPL"), id_aapl);
        EXPECT_EQ(st.lookup("MSFT"), id_msft);
        EXPECT_EQ(st.size(), 2u);
    }
}

TEST_F(SymbolTableTest, NewSymbolsAfterReloadGetFreshIds) {
    uint32_t id_aapl;
    {
        SymbolTable st(path_);
        id_aapl = st.intern("AAPL");
    }
    {
        SymbolTable st(path_);
        uint32_t id_nvda = st.intern("NVDA");
        // NVDA should get a new id that doesn't conflict with AAPL
        EXPECT_NE(id_nvda, id_aapl);
        EXPECT_EQ(st.lookup("AAPL"), id_aapl);
        EXPECT_EQ(st.lookup("NVDA"), id_nvda);
    }
}

TEST_F(SymbolTableTest, LookupNameById) {
    SymbolTable st(path_);
    uint32_t id = st.intern("GOOG");
    auto name = st.lookup_name(id);
    ASSERT_TRUE(name.has_value());
    EXPECT_EQ(*name, "GOOG");
}

TEST_F(SymbolTableTest, ManySymbols) {
    SymbolTable st(path_);
    for (int i = 0; i < 1000; ++i) {
        std::string sym = "SYM" + std::to_string(i);
        st.intern(sym);
    }
    EXPECT_EQ(st.size(), 1000u);
    // Reload and verify
    SymbolTable st2(path_);
    EXPECT_EQ(st2.size(), 1000u);
    for (int i = 0; i < 1000; ++i) {
        std::string sym = "SYM" + std::to_string(i);
        EXPECT_TRUE(st2.lookup(sym).has_value());
    }
}
