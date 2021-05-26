package it.eng.effector.processors.ais.domain;

import it.eng.effector.processors.ais.domain.numerical.NavigationStatus;
import it.eng.effector.processors.ais.domain.numerical.ShipType;

import java.time.Instant;

public class AisMsg {
    private Integer imo;
    private Integer messageType;
    private Float latitude;
    private Float longitude;
    private Integer mmsi;
    private Float cog;
    private Integer heading;
    private Instant timestamp; // Remember: The Instant class from Java Date Time API models a single instantaneous point on the timeline in UTC.
    private Integer seconds;
    private Float sog;
    private NavigationStatus navigationStatus;
    private String accuracy;
    private String destination;
    private String eta;
    private String callSign;
    private Float draught;
    private Integer beam; //
    private Integer length;
    private ShipType shipType;
    private String shipName;
    private String[] nmeaMessages;

    public Integer getImo() {
        return imo;
    }

    public void setImo(Integer imo) {
        this.imo = imo;
    }

    public Integer getMessageType() {
        return messageType;
    }

    public void setMessageType(Integer messageType) {
        this.messageType = messageType;
    }

    public Float getLatitude() {
        return latitude;
    }

    public void setLatitude(Float latitude) {
        this.latitude = latitude;
    }

    public Float getLongitude() {
        return longitude;
    }

    public void setLongitude(Float longitude) {
        this.longitude = longitude;
    }

    public Integer getMmsi() {
        return mmsi;
    }

    public void setMmsi(Integer mmsi) {
        this.mmsi = mmsi;
    }

    public Float getCog() {
        return cog;
    }

    public void setCog(Float cog) {
        this.cog = cog;
    }

    public Integer getHeading() {
        return heading;
    }

    public void setHeading(Integer heading) {
        this.heading = heading;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Instant timestamp) {
        this.timestamp = timestamp;
    }

    public Integer getSeconds() {
        return seconds;
    }

    public void setSeconds(Integer seconds) {
        this.seconds = seconds;
    }

    public Float getSog() {
        return sog;
    }

    public void setSog(Float sog) {
        this.sog = sog;
    }

    public NavigationStatus getNavigationStatus() {
        return navigationStatus;
    }

    public void setNavigationStatus(NavigationStatus navigationStatus) {
        this.navigationStatus = navigationStatus;
    }

    public String getAccuracy() {
        return accuracy;
    }

    public void setAccuracy(String accuracy) {
        this.accuracy = accuracy;
    }

    public String getDestination() {
        return destination;
    }

    public void setDestination(String destination) {
        this.destination = destination;
    }

    public String getEta() {
        return eta;
    }

    public void setEta(String eta) {
        this.eta = eta;
    }

    public String getCallSign() {
        return callSign;
    }

    public void setCallSign(String callSign) {
        this.callSign = callSign;
    }

    public Float getDraught() {
        return draught;
    }

    public void setDraught(Float draught) {
        this.draught = draught;
    }

    public Integer getBeam() {
        return beam;
    }

    public void setBeam(Integer beam) {
        this.beam = beam;
    }

    public Integer getLength() {
        return length;
    }

    public void setLength(Integer length) {
        this.length = length;
    }

    public ShipType getShipType() {
        return shipType;
    }

    public void setShipType(ShipType shipType) {
        this.shipType = shipType;
    }

    public String getShipName() {
        return shipName;
    }

    public void setShipName(String shipName) {
        this.shipName = shipName;
    }

    public String[] getNmeaMessages() {
        return nmeaMessages;
    }

    public void setNmeaMessages(String[] nmeaMessages) {
        this.nmeaMessages = nmeaMessages;
    }
}
