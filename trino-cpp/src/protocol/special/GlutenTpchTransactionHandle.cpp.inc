
// TpchTransactionHandle is special since
// the corresponding class in Java is an enum.

namespace io::trino::protocol {

void to_json(json& j, const GlutenTpchTransactionHandle& p) {
  j = json::array();
  j.push_back(p._type);
  j.push_back(p.instance);
}

void from_json(const json& j, GlutenTpchTransactionHandle& p) {
  j[0].get_to(p._type);
  j[1].get_to(p.instance);
}
} // namespace io::trino::protocol
