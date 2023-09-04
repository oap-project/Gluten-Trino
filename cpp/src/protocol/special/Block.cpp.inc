namespace io::trino::protocol {

void to_json(json& j, const Block& p) { j = p.data; }

void from_json(const json& j, Block& p) { p.data = std::string(j); }
}  // namespace io::trino::protocol
