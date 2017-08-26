package com.yahoo.messagebus.network.rpc;

import com.yahoo.component.Version;
import com.yahoo.compress.CompressionType;
import com.yahoo.compress.Compressor;
import com.yahoo.jrt.DataValue;
import com.yahoo.jrt.DoubleValue;
import com.yahoo.jrt.Int32Array;
import com.yahoo.jrt.Int32Value;
import com.yahoo.jrt.Int64Value;
import com.yahoo.jrt.Int8Value;
import com.yahoo.jrt.Method;
import com.yahoo.jrt.Request;
import com.yahoo.jrt.StringArray;
import com.yahoo.jrt.StringValue;
import com.yahoo.jrt.Values;
import com.yahoo.messagebus.EmptyReply;
import com.yahoo.messagebus.Error;
import com.yahoo.messagebus.Message;
import com.yahoo.messagebus.Reply;
import com.yahoo.messagebus.Trace;
import com.yahoo.messagebus.TraceNode;
import com.yahoo.messagebus.routing.Route;
import com.yahoo.slime.BinaryFormat;
import com.yahoo.slime.Cursor;
import com.yahoo.slime.Inspector;
import com.yahoo.slime.Slime;
import com.yahoo.text.Utf8;
import com.yahoo.text.Utf8Array;

/**
 * Implements the request adapter for method "mbus.send1".
 *
 * @author <a href="mailto:simon@yahoo-inc.com">Simon Thoresen</a>
 */
public class RPCSendV2 extends RPCSend {

    private final static String METHOD_NAME = "mbus.slime";
    private final static String METHOD_PARAMS = "bix";
    private final static String METHOD_RETURN = "sdISSsxs";
    private final Compressor compressor = new Compressor(CompressionType.LZ4, 3, 90, 1024);

    @Override
    protected String getReturnSpec() { return METHOD_RETURN; }
    @Override
    protected Method buildMethod() {

        Method method = new Method(METHOD_NAME, METHOD_PARAMS, METHOD_RETURN, this);
        method.methodDesc("Send a message bus request and get a reply back.");
        method.paramDesc(0, "encoding", "Encoding type.")
                .paramDesc(1, "decodedSize", "Number of bytes after decoding.")
                .paramDesc(2, "payload", "Slime encoded payload.");
        method.returnDesc(0, "version", "The lowest version the message was serialized as.")
                .returnDesc(1, "retryDelay", "The retry request of the reply.")
                .returnDesc(2, "errorCodes", "The reply error codes.")
                .returnDesc(3, "errorMessages", "The reply error messages.")
                .returnDesc(4, "errorServices", "The reply error service names.")
                .returnDesc(5, "protocol", "The name of the protocol that knows how to decode this reply.")
                .returnDesc(6, "payload", "The protocol specific reply payload.")
                .returnDesc(7, "trace", "A string representation of the trace.");
        return method;
    }
    private static final String VERSION_F = new String("version");
    private static final String ROUTE_F = new String("route");
    private static final String SESSION_F = new String("session");
    private static final String PROTOCOL_F = new String("prot");
    private static final String TRACELEVEL_F = new String("tracelevel");
    private static final String USERETRY_F = new String("useretry");
    private static final String RETRY_F = new String("retry");
    private static final String TIMEREMAINING_F = new String("timeleft");
    private static final String BLOB_F = new String("msg");



    @Override
    protected Request encodeRequest(Version version, Route route, RPCServiceAddress address, Message msg,
                                    long timeRemaining, byte[] payload, int traceLevel) {
        Slime slime = new Slime();
        Cursor root = slime.setObject();

        root.setString(VERSION_F, version.toString());
        root.setString(ROUTE_F, route.toString());
        root.setString(SESSION_F, address.getSessionName());
        root.setString(PROTOCOL_F, msg.getProtocol().toString());
        root.setBool(USERETRY_F, msg.getRetryEnabled());
        root.setLong(RETRY_F, msg.getRetry());
        root.setLong(TIMEREMAINING_F, msg.getTimeRemaining());
        root.setLong(TRACELEVEL_F, traceLevel);
        root.setData(BLOB_F, payload);

        byte[] serializedSlime = BinaryFormat.encode(slime);
        Compressor.Compression compressionResult = compressor.compress(serializedSlime);
        Request req = new Request(METHOD_NAME);
        Values v = req.parameters();

        v.add(new Int8Value(compressionResult.type().getCode()));
        v.add(new Int32Value(compressionResult.uncompressedSize()));
        v.add(new DataValue(compressionResult.data()));

        return req;
    }

    @Override
    protected Reply createReply(Request req, String serviceName, Trace trace) {
        // Retrieve all reply components from JRT request object.
        Version version = new Version(req.returnValues().get(0).asUtf8Array());
        double retryDelay = req.returnValues().get(1).asDouble();
        int[] errorCodes = req.returnValues().get(2).asInt32Array();
        String[] errorMessages = req.returnValues().get(3).asStringArray();
        String[] errorServices = req.returnValues().get(4).asStringArray();
        Utf8Array protocolName = req.returnValues().get(5).asUtf8Array();
        byte[] payload = req.returnValues().get(6).asData();
        String replyTrace = req.returnValues().get(7).asString();

        // Make sure that the owner understands the protocol.
        Reply reply = null;
        Error error = null;
        if (payload.length > 0) {
            Object ret = decode(protocolName, version, payload);
            if (ret instanceof Reply) {
                reply = (Reply) ret;
            } else {
                error = (Error) ret;
            }
        }
        if (reply == null) {
            reply = new EmptyReply();
        }
        if (error != null) {
            reply.addError(error);
        }
        reply.setRetryDelay(retryDelay);
        for (int i = 0; i < errorCodes.length && i < errorMessages.length; i++) {
            reply.addError(new Error(errorCodes[i], errorMessages[i],
                    errorServices[i].length() > 0 ? errorServices[i] : serviceName));
        }
        if (trace.getLevel() > 0) {
            trace.getRoot().addChild(TraceNode.decode(replyTrace));
        }
        return reply;
    }

    protected Params toParams(Values args) {
        CompressionType compression = CompressionType.valueOf(args.get(0).asInt8());
        byte[] slimeBytes = compressor.decompress(args.get(2).asData(), compression, args.get(1).asInt32());
        Slime slime = BinaryFormat.decode(slimeBytes);
        Inspector root = slime.get();
        Params p = new Params();
        p.version = new Version(root.field(VERSION_F).asString());
        p.route = root.field(ROUTE_F).asString();
        p.session = root.field(SESSION_F).asString();
        p.retryEnabled = root.field(USERETRY_F).asBool();
        p.retry = (int)root.field(RETRY_F).asLong();
        p.timeRemaining = root.field(TIMEREMAINING_F).asLong();
        p.protocolName = new Utf8Array(Utf8.toBytes(root.field(PROTOCOL_F).asString()));
        p.payload = root.field(BLOB_F).asData();
        p.traceLevel = (int)root.field(TRACELEVEL_F).asLong();
        return p;
    }

    @Override
    protected void createReponse(Values ret, Reply reply, Version version, byte [] payload) {
        int[] eCodes = new int[reply.getNumErrors()];
        String[] eMessages = new String[reply.getNumErrors()];
        String[] eServices = new String[reply.getNumErrors()];
        for (int i = 0; i < reply.getNumErrors(); ++i) {
            Error error = reply.getError(i);
            eCodes[i] = error.getCode();
            eMessages[i] = error.getMessage();
            eServices[i] = error.getService() != null ? error.getService() : "";
        }
        ret.add(new StringValue(version.toString()));
        ret.add(new DoubleValue(reply.getRetryDelay()));
        ret.add(new Int32Array(eCodes));
        ret.add(new StringArray(eMessages));
        ret.add(new StringArray(eServices));
        ret.add(new StringValue(reply.getProtocol()));
        ret.add(new DataValue(payload));
        ret.add(new StringValue(reply.getTrace().getRoot() != null ? reply.getTrace().getRoot().encode() : ""));
    }

}
