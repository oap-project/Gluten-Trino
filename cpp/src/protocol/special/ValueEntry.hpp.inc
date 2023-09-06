namespace io::trino::protocol {

class ValueEntry {
 public:
  Type type;
  std::shared_ptr<Block> block;
};

void to_json(json& j, const ValueEntry& p);
void from_json(const json& j, ValueEntry& p);

}  // namespace io::trino::protocol
