package it.eng.effector.rmlmapper;

import be.ugent.rml.Utils;
import be.ugent.rml.store.QuadStore;
import be.ugent.rml.store.RDF4JStore;
import be.ugent.rml.store.SimpleQuadStore;
import be.ugent.rml.target.Target;
import be.ugent.rml.target.TargetFactory;
import be.ugent.rml.term.Term;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.io.StreamCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class NifiFlowFileWrite {

    private static final Logger logger = LoggerFactory.getLogger(RMLEngine.class);

    OutputStream outputStream;

    public NifiFlowFileWrite( OutputStream outputStream) {
        this.outputStream=outputStream;
    }

    public void writeOutputTargets(HashMap<Term, QuadStore> targets, QuadStore rmlStore, String basePath, String outputFileDefault, String outputFormatDefault) throws Exception {
        boolean hasNoResults = true;

        logger.debug("Writing to Targets: " + targets.keySet());
        TargetFactory targetFactory = new TargetFactory(basePath);

        // Go over each term and export to the Target if needed
        for (Map.Entry<Term, QuadStore> termTargetMapping: targets.entrySet()) {
            Term term = termTargetMapping.getKey();
            QuadStore store = termTargetMapping.getValue();

            if (store.size() > 0) {
                hasNoResults = false;
                logger.info("Target: " + term + " has " + store.size() + " results");
            }

            // Default target is exported separately for backwards compatibility reasons
/*            if (term.getValue().equals("rmlmapper://default.store")) {
                logger.debug("Exporting to default Target");
                writeOutput(store, outputFileDefault, outputFormatDefault);
            }
            else {
                logger.debug("Exporting to Target: " + term);
                if (store.size() > 1) {
                    logger.info(store.size() + " quads were generated for " + term + " Target");
                } else {
                    logger.info(store.size() + " quad was generated " + term + " Target");
                }

                Target target = targetFactory.getTarget(term, rmlStore);
                String serializationFormat = target.getSerializationFormat();
                //this.outputStream = target.getOutputStream();

                // Set character encoding
                Writer out = new BufferedWriter(new OutputStreamWriter(this.outputStream, Charset.defaultCharset()));

                // Write store to target
                store.write(out, serializationFormat);

                // Close OS resources
                out.close();
                target.close();
            }*/
            logger.info("write to flowfile nifi");
            Writer out = new BufferedWriter(new OutputStreamWriter(this.outputStream, Charset.defaultCharset()));

            // Write store to target
            store.write(out, outputFormatDefault);

            // Close OS resources
            out.close();
            //target.close();
        }

        if (hasNoResults) {
            logger.info("No results!");
        }
    }


    public void writeOutput(QuadStore store, String outputFile, String format) {
        boolean hdt = format != null && format.equals("hdt");

        if (hdt) {
            try {
                format = "nquads";
                File tmpFile = File.createTempFile("file", ".nt");
                tmpFile.deleteOnExit();
                String uncompressedOutputFile = tmpFile.getAbsolutePath();

                File nquadsFile = writeOutputUncompressed(store, uncompressedOutputFile, format);
                Utils.ntriples2hdt(uncompressedOutputFile, outputFile);
                nquadsFile.deleteOnExit();
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            if (format != null) {
                format = format.toLowerCase();
            } else {
                format = "nquads";
            }

            writeOutputUncompressed(store, outputFile, format);
        }
    }
    public File writeOutputUncompressed(QuadStore store, String outputFile, String format) {
        File targetFile = null;

        if (store.size() > 1) {
            logger.info(store.size() + " quads were generated for default Target");
        } else {
            logger.info(store.size() + " quad was generated for default Target");
        }

        try {
            Writer out;
            String doneMessage = null;

            //if output file provided, write to triples output file
            if (outputFile != null) {
                targetFile = new File(outputFile);
                logger.info("Writing quads to " + targetFile.getPath() + "...");

                if (!targetFile.isAbsolute()) {
                    targetFile = new File(System.getProperty("user.dir") + "/" + outputFile);
                }

                doneMessage = "Writing to " + targetFile.getPath() + " is done.";

                out = new BufferedWriter(new FileWriter(targetFile));

            } else {
                out = new BufferedWriter(new OutputStreamWriter(System.out));
            }

            store.write(out, format);
            out.close();

            if (doneMessage != null) {
                logger.info(doneMessage);
            }
        } catch (Exception e) {
            System.err.println("Writing output failed. Reason: " + e.getMessage());
        }

        return targetFile;
    }

    private  QuadStore getStoreForFormat(String outputFormat) {
        if (outputFormat == null || outputFormat.equals("nquads") || outputFormat.equals("hdt")) {
            return new SimpleQuadStore();
        } else {
            return new RDF4JStore();
        }
    }


}
