// dependency GlutenTpchTransactionHandle
// dependency GlutenTupleDomain

namespace io::trino::protocol {
void to_json(json& j, const std::shared_ptr<ConnectorTransactionHandle>& p) {
  if (p == nullptr) {
    return;
  }
  String type = p->_type;

  /*
  if (type == "$remote") {
    j = *std::static_pointer_cast<RemoteTransactionHandle>(p);
    return;
  }
  */
  if (type == "system:io.trino.plugin.hive.protocol.HiveTransactionHandle") {
    j = *std::static_pointer_cast<HiveTransactionHandle>(p);
    return;
  }
  if (type == "system:io.trino.plugin.tpch.protocol.TpchTransactionHandle") {
    j = *std::static_pointer_cast<TpchTransactionHandle>(p);
    return;
  }

  throw TypeError(type + " no abstract type ConnectorTransactionHandle");
}

void from_json(const json& j, std::shared_ptr<ConnectorTransactionHandle>& p) {
  String type;
  try {
    // TPC-H transactionHandle is an array ["tpch","INSTANCE"].
    if (j.is_array()) {
      type = j[0];
    } else {
      type = p->getSubclassKey(j);
    }
  } catch (json::parse_error& e) {
    throw ParseError(
        std::string(e.what()) +
        " ConnectorTransactionHandle  ConnectorTransactionHandle");
  }

  /*
  if (type == "$remote") {
    auto k = std::make_shared<RemoteTransactionHandle>();
    j.get_to(*k);
    p = k;
    return;
  }
  */
  if (type == "system:io.trino.plugin.hive.protocol.HiveTransactionHandle") {
    auto k = std::make_shared<HiveTransactionHandle>();
    j.get_to(*k);
    p = k;
    return;
  }
  if (type == "system:io.trino.plugin.tpch.protocol.TpchTransactionHandle") {
    auto k = std::make_shared<TpchTransactionHandle>();
    j.get_to(*k);
    p = k;
    return;
  }

  throw TypeError(type + " no abstract type ConnectorTransactionHandle");
}
} // namespace io::trino::protocol
