package it.eng.effector.rmlmapper;

import be.ugent.rml.access.Access;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import static be.ugent.rml.Utils.getHashOfString;
import static org.apache.commons.io.FileUtils.getFile;

/**
 * This class represents access to a local file.
 */
public class NifiFlowFileAccess implements Access {

    InputStream data;





    public NifiFlowFileAccess(InputStream inputStream) {
        this.data=inputStream;
    }

    /**
     * This method returns the InputStream of the local file.
     * @return an InputStream.
     * @throws IOException
     */
    @Override
    public InputStream getInputStream() throws IOException {
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
            return toString()==access.toString();
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
        return String.valueOf(this.data.toString());
    }

}
