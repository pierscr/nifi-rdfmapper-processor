package it.eng.effector.rmlmapper;

import be.ugent.rml.Executor;
import be.ugent.rml.Utils;
import be.ugent.rml.conformer.MappingConformer;
import be.ugent.rml.functions.FunctionLoader;
import be.ugent.rml.functions.lib.IDLabFunctions;
import be.ugent.rml.metadata.MetadataGenerator;
import be.ugent.rml.records.RecordsFactory;
import be.ugent.rml.store.QuadStore;
import be.ugent.rml.store.RDF4JStore;
import be.ugent.rml.store.SimpleQuadStore;
import be.ugent.rml.target.Target;
import be.ugent.rml.target.TargetFactory;
import be.ugent.rml.term.NamedNode;
import be.ugent.rml.term.Term;
import ch.qos.logback.classic.Level;
import com.jayway.jsonpath.PathNotFoundException;
import org.apache.commons.cli.*;
import org.apache.nifi.flowfile.FlowFile;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

import java.io.*;
import java.nio.charset.Charset;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

public class RMLEngine {

    private static final Logger logger = LoggerFactory.getLogger(RMLEngine.class);
    private static final Marker fatal = MarkerFactory.getMarker("FATAL");
    NifiFlowFileAccess nifiAccess;
    NifiFlowFileWrite nifiFlowFileWrite;

    public RMLEngine(NifiFlowFileAccess nifiAccess, NifiFlowFileWrite nifiFlowFileWrite) {
        this.nifiAccess=nifiAccess;
        this.nifiFlowFileWrite=nifiFlowFileWrite;

    }


    public void run(String basePath,String flowfilename ) throws Exception {



        CommandLineParser parser = new DefaultParser();
        try {
            // parse the command line arguments


                String[] opt=new String[]{basePath+"/mapping.rml.nojoin.ttl"};
                List<InputStream> lis = Arrays.stream(opt)
                        .map(Utils::getInputStreamFromFileOrContentString)
                        .collect(Collectors.toList());
                InputStream is = new SequenceInputStream(Collections.enumeration(lis));


                // Read mapping file.
                RDF4JStore rmlStore = new RDF4JStore();
                try {
                    rmlStore.read(is, null, RDFFormat.TURTLE);
                }
                catch (RDFParseException e) {
                    logger.error(fatal, "Unable to parse mapping rules as Turtle. Does the file exist and is it valid Turtle?", e);
                    throw e;
                }




                NifiRecordsFactory factory = new NifiRecordsFactory(this.nifiAccess);

                String outputFormat = "turtle";
                QuadStore outputStore = getStoreForFormat(outputFormat);

                Executor executor;

                // Extract required information and create the MetadataGenerator
                MetadataGenerator metadataGenerator = null;
                String metadataFile = null;
                String requestedDetailLevel = null;



                FunctionLoader functionLoader = new FunctionLoader();


                executor = new Executor(rmlStore, factory, functionLoader, outputStore, Utils.getBaseDirectiveTurtle(is));

                List<Term> triplesMaps = new ArrayList<>();
                boolean checkOptionPresence=false;


                // Get start timestamp for post mapping metadata
                String startTimestamp = Instant.now().toString();

                HashMap<Term, QuadStore> targets = executor.executeV5(triplesMaps, checkOptionPresence,null);
                QuadStore result = targets.get(new NamedNode("rmlmapper://default.store"));

                // Get stop timestamp for post mapping metadata
                String stopTimestamp = Instant.now().toString();

                String outputFile = basePath+"/outputfileNifi"+flowfilename+".ttl";
                result.copyNameSpaces(rmlStore);

                this.nifiFlowFileWrite.writeOutputTargets(targets, rmlStore, basePath, outputFile, outputFormat);

        } catch (ParseException exp) {
            logger.error("Parsing failed. Reason: " + exp.getMessage(),exp);
            throw exp;
        }catch (PathNotFoundException pathErr){
            logger.debug("PathNotFoundException: " + pathErr.getMessage(),pathErr);
        }
    }




    private static void printHelp(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("java -jar mapper.jar <options>\noptions:", options);
    }

    private static void setLoggerLevel(Level level) {
        Logger root = LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
       /* ((ch.qos.logback.classic.Logger) root).setLevel(level);*/
    }



    private static QuadStore getStoreForFormat(String outputFormat) {
        if (outputFormat == null || outputFormat.equals("nquads") || outputFormat.equals("hdt")) {
            return new SimpleQuadStore();
        } else {
            return new RDF4JStore();
        }
    }
}
