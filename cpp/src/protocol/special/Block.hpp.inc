namespace io::trino::protocol {

struct Block {
  std::string data;
};

void to_json(json& j, const Block& p);

void from_json(const json& j, Block& p);

} // namespace io::trino::protocol
