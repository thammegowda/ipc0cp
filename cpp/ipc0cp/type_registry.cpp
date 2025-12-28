#include "type_registry.hpp"
#include "logger.hpp"
#include <algorithm>
#include <stdexcept>

namespace ipc0cp {

TypeRegistry& TypeRegistry::instance() {
    static TypeRegistry registry;
    return registry;
}

TypeRegistry::TypeRegistry() = default;

void TypeRegistry::register_type(
    const std::string& type_name,
    CustomDeserializer deserializer,
    const std::string& version
) {
    if (type_name.empty() || type_name[0] == '/') {
        throw std::invalid_argument("Invalid type name");
    }
    
    std::string key = make_key(type_name, version);
    if (registry_.count(key) > 0) {
        IPC_LOG_WARNING("Type '" << key << "' already registered - overwriting");
    }
    
    registry_[key] = deserializer;
}

CustomDeserializer TypeRegistry::get_deserializer(
    const std::string& type_name,
    const std::string& version
) {
    auto it = registry_.find(make_key(type_name, version));
    return it != registry_.end() ? it->second : nullptr;
}

std::vector<std::string> TypeRegistry::get_registered_types() const {
    std::vector<std::string> keys;
    keys.reserve(registry_.size());
    for (const auto& [key, _] : registry_) {
        keys.push_back(key);
    }
    return keys;
}

std::string TypeRegistry::make_key(
    const std::string& type_name,
    const std::string& version
) const {
    if (version.empty()) {
        return type_name;
    }
    return type_name + "@" + version;
}

} // namespace ipc0cp
