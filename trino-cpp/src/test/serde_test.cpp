#include <gtest/gtest.h>

#include "test_utils.h"

using namespace facebook::velox;
using namespace io::trino::bridge;
using namespace io::trino::bridge::test;

TEST(serde_test, empty) {
  std::vector<std::string> names{};
  std::vector<TypePtr> types{};
  std::shared_ptr<const RowType> rowType =
      std::make_shared<const RowType>(std::move(names), std::move(types));
  auto page = TestUtils::createEmptyPage(rowType);
  auto trinoBuffer = TestUtils::test_serialize(page, rowType);
}

TEST(serde_test, OneRow) {
  std::vector<std::string> names{"int"};
  std::vector<TypePtr> types{std::make_shared<IntegerType>()};
  std::shared_ptr<const RowType> rowType =
      std::make_shared<const RowType>(std::move(names), std::move(types));
  auto page = TestUtils::createPage1();
  auto trinoBuffer = TestUtils::test_serialize(page, rowType);
}

TEST(serde_test, test2) {
  std::vector<std::string> names{"int", "long"};
  std::vector<TypePtr> types{std::make_shared<IntegerType>(),std::make_shared<BigintType>()};
  std::shared_ptr<const RowType> rowType =
      std::make_shared<const RowType>(std::move(names), std::move(types));
  auto page = TestUtils::createPage2();
  auto trinoBuffer = TestUtils::test_serialize(page, rowType);
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);

  int err{0};
  err = RUN_ALL_TESTS();

  return err;
}
