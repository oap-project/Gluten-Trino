namespace io::trino::protocol {

struct MockVariableReferenceExpression : MockRowExpression {
  String name;
  Type type; // dependency

  MockVariableReferenceExpression() noexcept;
  explicit MockVariableReferenceExpression(const String& str) {
    _type = "variable";

    std::vector<std::string, std::allocator<std::string>> parts;

    folly::split("<", str, parts);
    name = parts[0];
    type = parts[1].substr(0, parts[1].length() - 1);
  }

  bool operator<(const MockVariableReferenceExpression& o) const {
    if (name == o.name) {
      return type < o.type;
    }

    return name < o.name;
  }
};

void to_json(json& j, const MockVariableReferenceExpression& p);
void from_json(const json& j, MockVariableReferenceExpression& p);
std::string json_map_key(const MockVariableReferenceExpression& p);
} // namespace io::trino::protocol
