import datetime as dt

import datazimmer as dz


class RealEstate(dz.AbstractEntity):
    property_id = dz.Index & int


class RealEstateRecord(dz.AbstractEntity):
    propety_id = dz.Index & RealEstate
    recorded = dz.Index & dt.datetime
