namespace io::trino::protocol {

struct CatalogHandle {
  std::string catalogName;
};

void to_json(nlohmann::json& j, const CatalogHandle& p);
void from_json(const nlohmann::json& j, CatalogHandle& p);

}  // namespace io::trino::protocol