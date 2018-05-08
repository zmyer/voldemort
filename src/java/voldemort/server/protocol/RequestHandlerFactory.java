package voldemort.server.protocol;

import voldemort.client.protocol.RequestFormatType;

/**
 * A factory that gets the appropriate request handler for a given
 * {@link voldemort.client.protocol.RequestFormatType}.
 *
 */
// TODO: 2018/4/26 by zmyer
public interface RequestHandlerFactory {

    public RequestHandler getRequestHandler(RequestFormatType type);

    public boolean shareReadWriteBuffer();
}
