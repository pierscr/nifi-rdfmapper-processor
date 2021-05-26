package it.eng.effector.processors.ais.parser;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import dk.tbsalling.aismessages.ais.messages.AISMessage;
import dk.tbsalling.aismessages.nmea.messages.NMEAMessage;
import it.eng.effector.processors.ais.domain.AisMsg;
import it.eng.effector.processors.ais.domain.numerical.NavigationStatus;
import it.eng.effector.processors.ais.domain.numerical.ShipType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.*;

public class MessageParser {

    private final Logger log = LoggerFactory.getLogger(MessageParser.class);
    private final ObjectMapper objectMapper;
    private SimpleDateFormat dateFormat;

    public MessageParser() {
        this.objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
        objectMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        this.dateFormat = new SimpleDateFormat("yyyyMMddHHmmssSSS");
    }

    public MessageParser(String dataPattern) {
        this();
        if(dataPattern!=null&&!dataPattern.isEmpty()){
            this.dateFormat = new SimpleDateFormat(dataPattern);
        }
    }

    public String convert(String message) throws Exception {
        Optional<AisMsg> aisMsg = parse(message);
        if(!aisMsg.isPresent()){
            throw new Exception("not valid input");
        }
        JsonNode jsonNode = objectMapper.valueToTree(aisMsg.get());
        if(jsonNode.isEmpty()){
            throw new Exception("Unable to convert to JSON");
        }
        Iterator<JsonNode> it = jsonNode.iterator();
        while (it.hasNext()) {
            JsonNode child = it.next();
            if (child.isNull())
                it.remove();
        }
        return jsonNode.toString();
    }

    public Optional<AisMsg> parse(String message) {
        String[] msgs = message.split("\n");
        ArrayList<String> msgsArr = new ArrayList<>();
        for (int i=0; i<msgs.length;i++){
            String currentMsg = msgs[i].trim();
            if(!currentMsg.isEmpty()){
                msgsArr.add(currentMsg);
            }
        }
        return this.parse(Arrays.copyOf(msgsArr.toArray(), msgsArr.size(), String[].class));
    }

    public Optional<AisMsg> parse(String... messages) {
        String dateStr="";
        NMEAMessage[] nmeaMessages = new NMEAMessage[messages.length];
        for(int i = 0; i< messages.length; i++){
            String message = messages[i];
            int lastCommaPos = message.lastIndexOf(',');
            dateStr = message.substring(lastCommaPos + 1).trim();
            try {
                String nmeaRawMessage = message.substring(0, lastCommaPos);
                NMEAMessage nmeaMessage = NMEAMessage.fromString(nmeaRawMessage);
                nmeaMessages[i]= nmeaMessage;
            }catch (Exception e){
                log.error(e.getMessage());
            }
        }
        AISMessage m = AISMessage.create(nmeaMessages);
        return getAisMsgString(m, dateStr);
    }

    private Optional<AisMsg> getAisMsgString(AISMessage m, String dateStr) {
        Instant instant = Instant.now();
        try {
            // We could have two types of date string:
            // 1) 1614615950         => UTC seconds
            // 2) 20200101000000291  => parse using string format
            // Evaluate by string length 1)  <= 10 2) > 10
            if(dateStr.length() <= 10) {
                Long epochSeconds = Long.parseLong(dateStr);
                instant = Instant.ofEpochSecond(epochSeconds);
            } else {
                Date date = this.dateFormat.parse(dateStr);
                instant = date.toInstant();
            }
        } catch (ParseException e) {
            log.error(e.getMessage());
        }
        AisMsg aisMsg = new AisMsg();

        aisMsg.setTimestamp(instant);

        if(m.dataFields().get("imo.IMO")!=null)
            aisMsg.setImo((Integer) m.dataFields().get("imo.IMO"));
        if(m.getMessageType().getCode()!=null)
            aisMsg.setMessageType(m.getMessageType().getCode());
        if(m.dataFields().get("latitude")!=null)
            aisMsg.setLatitude((Float) m.dataFields().get("latitude"));
        if(m.dataFields().get("longitude")!=null)
            aisMsg.setLongitude((Float) m.dataFields().get("longitude"));
        if(m.getSourceMmsi().getMMSI()!=null)
            aisMsg.setMmsi(m.getSourceMmsi().getMMSI());
        if(m.dataFields().get("courseOverGround")!=null)
            aisMsg.setCog((Float) m.dataFields().get("courseOverGround"));
        if(m.dataFields().get("speedOverGround")!=null)
            aisMsg.setSog((Float) m.dataFields().get("speedOverGround"));
        if(m.dataFields().get("trueHeading")!=null)
            aisMsg.setHeading((Integer) m.dataFields().get("trueHeading"));
        if(m.dataFields().get("second")!=null)
            aisMsg.setSeconds((Integer)m.dataFields().get("second"));

        String ns = (String) m.dataFields().get("navigationStatus");
        aisMsg.setNavigationStatus(ns == null ? null : NavigationStatus.valueOf(ns));

        aisMsg.setAccuracy((Boolean) m.dataFields().getOrDefault("positionAccuracy", Boolean.valueOf(false))? "high" : "default");

        if(m.dataFields().get("destination")!=null)
            aisMsg.setDestination((String) m.dataFields().get("destination"));
        if(m.dataFields().get("eta")!=null)
            aisMsg.setEta((String) m.dataFields().get("eta"));
        if(m.dataFields().get("callsign")!=null)
            aisMsg.setCallSign((String) m.dataFields().get("callsign"));
        if(m.dataFields().get("draught")!=null)
            aisMsg.setDraught((Float) m.dataFields().get("draught"));

        //	<Length>30</Length><!-- Dimension/ reference for position : A+B-->
        Integer a = (Integer) m.dataFields().getOrDefault("toBow", 0);
        Integer b = (Integer) m.dataFields().getOrDefault("toStern", 0);
        if( (a+b) != 0 ){
            aisMsg.setLength(a+b);
        }

        // 	<Beam>20</Beam><!-- Dimension/ reference for position : C+D-->
        Integer c = (Integer) m.dataFields().getOrDefault("toPort", 0);
        Integer d = (Integer) m.dataFields().getOrDefault("toStarboard", 0);
        if( (c+d) != 0) {
            aisMsg.setBeam(c+d);
        }

        String st = (String) m.dataFields().get("shipType");
        aisMsg.setShipType(st == null? null : ShipType.valueOf(st));

        if(m.dataFields().get("shipName")!=null)
            aisMsg.setShipName((String) m.dataFields().get("shipName"));

        if(m.dataFields().get("nmeaMessages")!=null){
            NMEAMessage[] nmeaMessages = (NMEAMessage[])m.dataFields().get("nmeaMessages");
            String[] strNmeaMessages = new String[nmeaMessages.length];
            int i = 0;
            for(NMEAMessage nmeaMessage : nmeaMessages){
                strNmeaMessages[i] = nmeaMessage.getRawMessage();
                i++;
            }
            aisMsg.setNmeaMessages(strNmeaMessages);
        }

        return Optional.of(aisMsg);
    }

}
