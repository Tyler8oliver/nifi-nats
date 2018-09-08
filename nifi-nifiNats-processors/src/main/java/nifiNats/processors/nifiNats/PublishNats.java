package nifiNats.processors.nifiNats;

import io.nats.client.Connection;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.stream.io.StreamUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;


public class PublishNats extends NatsProcessor {
    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        final FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final String topic = context.getProperty(TOPIC).evaluateAttributeExpressions(flowFile).getValue();
//        final String queue = context.getProperty(QUEUE).evaluateAttributeExpressions(flowFile).getValue();
//        final String requestId = String.valueOf(flowFile.getId());
        final Charset charset = Charset.forName(context.getProperty(CHARSET).getValue());
        final Connection nc = this.getConnection(context);
//        final long maxBufferSize = context.getProperty(MAX_BUFFER_SIZE).asDataSize(DataUnit.B).longValue();

        final byte[] value = new byte[(int) flowFile.getSize()];

        session.read(flowFile, new InputStreamCallback() {
            @Override
            public void process(InputStream in) throws IOException {
                StreamUtils.fillBuffer(in, value);
            }
        });

        try {
            String message = new String(value, charset);
            nc.publish(topic, message.getBytes(charset));
            session.getProvenanceReporter().send(flowFile, "nats://" + topic);
            session.transfer(flowFile, SUCCESS);
            getLogger().info("Successfully send {} to NATS", new Object[] { flowFile });
        } catch (final Exception ex) {
            getLogger().error("Failed to send {} to NATS due to {}; routing to failure", new Object[] { flowFile, ex });
            session.transfer(flowFile, FAILURE);
            closeConnection(nc);
        }

    }
}
