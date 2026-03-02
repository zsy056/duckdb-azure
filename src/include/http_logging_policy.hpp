#pragma once

#include "duckdb/common/shared_ptr.hpp"
#include <azure/core/context.hpp>
#include <azure/core/http/http.hpp>
#include <azure/core/http/policies/policy.hpp>
#include <azure/core/http/raw_response.hpp>
#include <memory>

namespace duckdb {

class Logger;

class HttpLoggingPolicy : public Azure::Core::Http::Policies::HttpPolicy {
public:
	HttpLoggingPolicy(shared_ptr<Logger> logger);

	std::unique_ptr<Azure::Core::Http::RawResponse> Send(Azure::Core::Http::Request &request,
	                                                     Azure::Core::Http::Policies::NextHttpPolicy next_policy,
	                                                     Azure::Core::Context const &context) const override;

	std::unique_ptr<Azure::Core::Http::Policies::HttpPolicy> Clone() const override;

private:
	shared_ptr<Logger> logger;
};

} // namespace duckdb
