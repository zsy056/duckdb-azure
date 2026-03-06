#include "http_logging_policy.hpp"
#include <azure/core/http/http.hpp>
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/logging/log_type.hpp"
#include "duckdb/logging/logger.hpp"
#include <memory>
#include <string>
#include <utility>

namespace duckdb {

HttpLoggingPolicy::HttpLoggingPolicy(shared_ptr<Logger> logger_p) : logger(std::move(logger_p)) {
}

static Value CreateAzureHeadersValue(const Azure::Core::CaseInsensitiveMap &headers) {
	vector<Value> keys;
	vector<Value> values;
	for (const auto &header : headers) {
		keys.emplace_back(StringUtil::Lower(header.first));
		values.emplace_back(header.second);
	}
	return Value::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR, std::move(keys), std::move(values));
}

std::unique_ptr<Azure::Core::Http::RawResponse>
HttpLoggingPolicy::Send(Azure::Core::Http::Request &request,
                        Azure::Core::Http::Policies::NextHttpPolicy next_policy,
                        Azure::Core::Context const &context) const {
	auto result = next_policy.Send(request, context);

	if (logger && logger->ShouldLog(HTTPLogType::NAME, HTTPLogType::LEVEL)) {
		// Build request struct
		child_list_t<Value> request_fields = {
		    {"type", Value(request.GetMethod().ToString())},
		    {"url", Value(request.GetUrl().GetAbsoluteUrl())},
		    {"start_time", Value()},
		    {"duration_ms", Value()},
		    {"headers", CreateAzureHeadersValue(request.GetHeaders())},
		};
		auto request_value = Value::STRUCT(std::move(request_fields));

		// Build response struct (if available)
		Value response_value;
		if (result) {
			child_list_t<Value> response_fields = {
			    {"status", Value(std::to_string(static_cast<int>(result->GetStatusCode())))},
			    {"reason", Value(result->GetReasonPhrase())},
			    {"headers", CreateAzureHeadersValue(result->GetHeaders())},
			};
			response_value = Value::STRUCT(std::move(response_fields));
		}

		child_list_t<Value> top_fields = {
		    {"request", std::move(request_value)},
		    {"response", std::move(response_value)},
		};
		auto log_message = Value::STRUCT(std::move(top_fields)).ToString();

		logger->WriteLog(HTTPLogType::NAME, HTTPLogType::LEVEL, log_message);
	}

	return result;
}

std::unique_ptr<Azure::Core::Http::Policies::HttpPolicy> HttpLoggingPolicy::Clone() const {
	return std::unique_ptr<Azure::Core::Http::Policies::HttpPolicy>(new HttpLoggingPolicy(logger));
}

} // namespace duckdb
