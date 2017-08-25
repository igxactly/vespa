// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
#pragma once

#include "rpcsend.h"

namespace mbus {

class RPCSendV2 : public RPCSend {
private:
    void encodeRequest(FRT_RPCRequest &req, const vespalib::Version &version, const Route & route,
                       const RPCServiceAddress & address, const Message & msg, uint32_t traceLevel,
                       const PayLoadFiller &filler, uint64_t timeRemaining) const override;

    void build(FRT_ReflectionBuilder & builder) override;
    const char * getReturnSpec() const override;
    std::unique_ptr<Reply> createReply(const FRT_Values & response, const string & serviceName,
                                       Error & error, vespalib::TraceNode & rootTrace) const override;
public:
    static bool isCompatible(vespalib::stringref method, vespalib::stringref request, vespalib::stringref respons);
    void handleReply(std::unique_ptr<Reply> reply) override;
    void invoke(FRT_RPCRequest *req);
};

} // namespace mbus
