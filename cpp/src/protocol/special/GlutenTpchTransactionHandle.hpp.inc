
// TpchTransactionHandle is special since
// the corresponding class in Java is an enum.

namespace io::trino::protocol {

struct GlutenTpchTransactionHandle : public GlutenConnectorTransactionHandle {
  String instance = {};
};

void to_json(json& j, const GlutenTpchTransactionHandle& p);

void from_json(const json& j, GlutenTpchTransactionHandle& p);

}  // namespace io::trino::protocol
