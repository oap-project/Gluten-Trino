
// TpchTransactionHandle is special since
// the corresponding class in Java is an enum.

namespace io::trino::protocol {

struct MockTpchTransactionHandle : public MockConnectorTransactionHandle {
  String instance = {};
};

void to_json(json& j, const MockTpchTransactionHandle& p);

void from_json(const json& j, MockTpchTransactionHandle& p);

} // namespace io::trino::protocol
