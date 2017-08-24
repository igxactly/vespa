// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
#pragma once

#include "rpcsend.h"

namespace mbus {

class RPCSendV1 : public RPCSend {
private:
    void send(RoutingNode &recipient, const vespalib::Version &version,
              const PayLoadFiller & filler, uint64_t timeRemaining) override;

    void build(FRT_ReflectionBuilder & builder) override;
public:
    static bool isCompatible(vespalib::stringref method, vespalib::stringref request, vespalib::stringref respons);
    void handleReply(std::unique_ptr<Reply> reply) override;
    void invoke(FRT_RPCRequest *req);
    void RequestDone(FRT_RPCRequest *req) override;
};

} // namespace mbus
