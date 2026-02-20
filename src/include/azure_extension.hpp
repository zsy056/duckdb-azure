#pragma once

#include "duckdb.hpp"

namespace duckdb {

#define AZURE_EXTENSION_VERSION "1.0.0"

class AzureExtension : public Extension {
public:
	void Load(ExtensionLoader &loader) override;
	std::string Name() override;
};

} // namespace duckdb
