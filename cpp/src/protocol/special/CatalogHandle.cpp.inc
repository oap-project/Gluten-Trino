namespace io::trino::protocol {

void to_json(nlohmann::json& j, const CatalogHandle& p) { j = p.catalogName; }

void from_json(const nlohmann::json& j, CatalogHandle& p) {
  p.catalogName = std::string(j);
}

}  // namespace io::trino::protocol