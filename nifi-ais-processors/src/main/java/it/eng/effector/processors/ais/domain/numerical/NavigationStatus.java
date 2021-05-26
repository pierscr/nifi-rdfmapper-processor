package it.eng.effector.processors.ais.domain.numerical;

public enum NavigationStatus {

    UnderwayUsingEngine(0),
    AtAnchor(1),
    NotUnderCommand(2),
    RestrictedManoeuverability(3),
    ConstrainedByHerDraught(4),
    Moored(5),
    Aground(6),
    EngagedInFising(7),
    UnderwaySailing(8),
    ReservedForFutureUse9(9),
    ReservedForFutureUse10(10),
    PowerDrivenVesselTowingAstern(11),
    PowerDrivenVesselPushingAheadOrTowingAlongside(12),
    ReservedForFutureUse13(13),
    SartMobOrEpirb(14),
    Undefined(15);

    private final Integer code;

    /**
     * The code is the numeric code of the navigation status as it's encoded in
     * the AIS message encoding.
     *
     * @param code the encoded status number
     */
    NavigationStatus(Integer code) {
        this.code = code;
    }

    /**
     * Parse an integer as a navigation status code and returns the
     * corresponding enum value.
     *
     * @param integer the code to be interpreted.
     * @return the navigation status enum item or null in case the code does
     * not correspond to any item.
     */
    public static NavigationStatus fromInteger(Integer integer) {
        if (integer != null) {
            for (NavigationStatus b : NavigationStatus.values()) {
                if (integer.equals(b.code)) {
                    return b;
                }
            }
        }
        return null;
    }

    /**
     * @return the AIS navigation status code
     */
    public Integer getCode() {
        return code;
    }

    /**
     * @return teh navigation status description
     */
    public String getValue() {
        return toString();
    }
}
