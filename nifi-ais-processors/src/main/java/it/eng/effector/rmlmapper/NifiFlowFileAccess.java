package it.eng.effector.rmlmapper;

import be.ugent.rml.access.Access;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.stream.io.StreamUtils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import static be.ugent.rml.Utils.getHashOfString;
import static be.ugent.rml.Utils.getInputStreamFromFile;
import static org.apache.commons.io.FileUtils.getFile;

/**
 * This class represents access to a local file.
 */
public class NifiFlowFileAccess implements Access {

    ProcessSession session;
    FlowFile flowFile;
    InputStream data;



    public NifiFlowFileAccess(ProcessSession session, FlowFile flowFile) {
        this.session=session;
        this.flowFile=flowFile;
        this.flowFile.getId();
    }

    /**
     * This method returns the InputStream of the local file.
     * @return an InputStream.
     * @throws IOException
     */
    @Override
    public InputStream getInputStream() throws IOException {
        this.session.read(this.flowFile, new InputStreamCallback() {
            @Override
            public void process(final InputStream inputStream) throws IOException {
                data=inputStream;
            }
        });
        return data;
    }

    /**
     * This methods returns the datatypes of the file.
     * This method always returns null, because the datatypes can't be determined from a local file for the moment.
     * @return the datatypes of the file.
     */
    @Override
    public Map<String, String> getDataTypes() {
        return null;
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof NifiFlowFileAccess) {
            NifiFlowFileAccess access  = (NifiFlowFileAccess) o;
            return flowFile.getId()==access.getFlowFile().getId();
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return getHashOfString(this.toString());
    }


    @Override
    public String toString() {
        return String.valueOf(this.flowFile.getId());
    }

    public ProcessSession getSession() {
        return session;
    }

    public void setSession(ProcessSession session) {
        this.session = session;
    }

    public FlowFile getFlowFile() {
        return flowFile;
    }

    public void setFlowFile(FlowFile flowFile) {
        this.flowFile = flowFile;
    }
}
